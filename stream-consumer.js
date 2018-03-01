'use strict';

// AWS core utilities
const streamEvents = require('aws-core-utils/stream-events');

const strings = require('core-functions/strings');
const isBlank = strings.isBlank;
const isNotBlank = strings.isNotBlank;
const stringify = strings.stringify;

const isInstanceOf = require('core-functions/objects').isInstanceOf;
const Arrays = require('core-functions/arrays');
const tries = require('core-functions/tries');
const Try = tries.Try;
const Failure = tries.Failure;
const Success = tries.Success;
const Promises = require('core-functions/promises');
const CancelledError = Promises.CancelledError;
const DelayCancelledError = Promises.DelayCancelledError;

const errors = require('core-functions/errors');
const FatalError = errors.FatalError;
const TimeoutError = errors.TimeoutError;

// Task factory, tasks, task definitions, task states, task errors & task utilities
const TaskDef = require('task-utils/task-defs');
const taskUtils = require('task-utils');
const ReturnMode = taskUtils.ReturnMode;

const Batch = require('./batch');

const initiateTaskOpts = {returnMode: ReturnMode.PROMISE}; // was ReturnMode.NORMAL
const processTaskOpts = {returnMode: ReturnMode.PROMISE}; // was ReturnMode.SUCCESS_OR_FAILURE
const finaliseTaskOpts = {returnMode: ReturnMode.PROMISE}; // was ReturnMode.SUCCESS_OR_FAILURE

const logging = require('logging-utils');
const LogLevel = logging.LogLevel;
const log = logging.log;

const Settings = require('./settings');
const isDynamoDBStreamType = Settings.isDynamoDBStreamType;
const isKinesisStreamType = Settings.isKinesisStreamType;

const taskDefSettings = require('./taskdef-settings');
const batchSettings = taskDefSettings.batchSettings;
const messageSettings = taskDefSettings.messageSettings;
const batchAndIncompleteMessagesSettings = taskDefSettings.batchAndIncompleteMessagesSettings;
const batchAndCancellableSettings = taskDefSettings.batchAndCancellableSettings;
const batchAndUnusableRecordsSettings = taskDefSettings.batchAndUnusableRecordsSettings;
const batchAndProcessOutcomesSettings = taskDefSettings.batchAndProcessOutcomesSettings;

const tracking = require('./tracking');
const toCountString = tracking.toCountString;

const sequencing = require('./sequencing');

const identify = require('./identify');
const getMessageStateKeySeqNoId = identify.getMessageStateKeySeqNoId;

const streamProcessing = require('./stream-processing');

const flattenOpts = {skipSimplifyOutcomes: false};

// Constants
const MIN_FINALISE_TIMEOUT_AT_PERCENTAGE = 0.8;

/**
 * Utilities and functions to be used to robustly consume messages from an AWS Kinesis or DynamoDB event stream.
 * @module aws-stream-consumer-core/stream-consumer
 * @author Byron du Preez
 */
exports._$_ = '_$_'; //IDE workaround

// Processing
exports.processStreamEvent = processStreamEvent;

exports.validateTaskDefinitions = validateTaskDefinitions;

// noinspection JSUnusedGlobalSymbols
exports.FOR_TESTING_ONLY = {
  logStreamEvent: logStreamEvent,

  // Initiating
  initiateBatch: initiateBatch,
  extractAndSequenceMessages: extractAndSequenceMessages,
  extractMessagesFromStreamEventRecord: extractMessagesFromStreamEventRecord,
  loadBatchState: loadBatchState,
  reviveTasks: reviveTasks,
  preProcessBatch: preProcessBatch,

  // Processing
  processBatch: processBatch,
  //processStreamEventRecord: processStreamEventRecord,
  executeAllProcessOneTasks: executeAllProcessOneTasks,
  executeProcessOneTasks: executeProcessOneTasks,
  // executeProcessOneTask: executeProcessOneTask,
  executeAllProcessAllTasks: executeAllProcessAllTasks,
  // executeProcessAllTask: executeProcessAllTask,
  createTimeoutPromise: createTimeoutPromise,
  createCompletedPromise: createCompletedPromise,
  discardAnyUnusableRecords: discardUnusableRecords,

  // Finalising
  preFinaliseBatch: preFinaliseBatch,
  finaliseBatch: finaliseBatch,
  discardAnyRejectedMessages: discardAnyRejectedMessages,
  postFinaliseBatch: postFinaliseBatch
};

// =====================================================================================================================
// Process stream event
// =====================================================================================================================

/**
 * Processes the given Kinesis or DynamoDB stream event using the given AWS context and context by applying each of
 * the tasks defined by the task definitions in the given processOneTaskDefs and processAllTaskDefs to each message
 * extracted from the event.
 * Precondition: isStreamConsumerConfigured(context)
 * @param {AnyStreamEvent|*} event - the AWS stream event (or any other garbage passed as an event)
 * @param {ProcessOneTaskDef[]|undefined} [processOneTaskDefsOrNone] - an "optional" list of "processOne" task definitions that
 * will be used to generate the tasks to be executed on each message independently
 * @param {ProcessAllTaskDef[]|undefined} [processAllTaskDefsOrNone] - an "optional" list of "processAll" task definitions that
 * will be used to generate the tasks to be executed on all of the event's messages collectively
 * @param {StreamConsumerContext} context - the context to use
 * @returns {Promise.<Batch|BatchError>} a promise that will resolve with the batch processed or reject with an error
 */
function processStreamEvent(event, processOneTaskDefsOrNone, processAllTaskDefsOrNone, context) {

  process.on('unhandledRejection', (reason, p) => {
    context.warn('*** UnhandledPromiseRejectionWarning *** Unhandled rejection at: Promise', p, '\n  reason:', reason);
    // application specific logging, throwing an error, or other logic here
  });

  // Precondition - NB: Ensure that your stream consumer is fully configured before calling this function!
  // Check if the Lambda as configured will be unusable or useless, and if so, trigger a replay of all the records until it can be fixed
  try {
    validateTaskDefinitions(processOneTaskDefsOrNone, processAllTaskDefsOrNone, context);
  }
  catch (err) {
    return Promise.reject(err);
  }

  if (context.traceEnabled) logStreamEvent(event, "Processing stream event", LogLevel.TRACE, context);

  // Clean up any undefined or null task definition lists
  const processOneTaskDefs = processOneTaskDefsOrNone ? processOneTaskDefsOrNone : [];
  processOneTaskDefs.forEach(td => td.setDescribeItem(messageSettings.describeItem, false));

  const processAllTaskDefs = processAllTaskDefsOrNone ? processAllTaskDefsOrNone : [];
  processAllTaskDefs.forEach(td => td.setDescribeItem(batchAndIncompleteMessagesSettings.describeItem, false));

  const records = event.Records;

  if (!records || !Array.isArray(records) || records.length <= 0) {
    logStreamEvent(event, "Missing Records on stream event", LogLevel.ERROR, context);
    // Consume this useless event rather than throwing an error that will potentially replay it
    return Promise.resolve(new Batch([], processOneTaskDefs, processAllTaskDefs, context));
  }

  try {
    // Create a batch to track all of the records & messages
    const batch = new Batch(records, processOneTaskDefs, processAllTaskDefs, context);

    // Initiate the batch ... and then process the batch ... and then finalise the batch
    return executeInitiateBatchTask(batch, {}, context)
      .then(() => executeProcessBatchTask(batch, {}, context))
      .then(processOutcomes => executeFinaliseBatchTask(batch, processOutcomes, {}, context))
      .catch(err => {
        context.error(`Stream consumer failed`, err);
        Object.defineProperty(err, 'batch', {value: batch, enumerable: false});// i.e. err.batch = batch;

        if (isInstanceOf(err, FatalError)) {
          return streamProcessing.handleFatalError(err, batch, context);
        }
        throw err;
      });

  } catch (err) {
    context.error(`Stream consumer failed (in try-catch)`, err);
    return Promise.reject(err);
  }
}

/**
 * Creates and executes a new initiate batch task on the given batch.
 * @param {Batch} batch - the batch to be initiated
 * @param {Cancellable|Object|undefined} [cancellable] - a cancellable object onto which to install cancel functionality
 * @param {StreamConsumerContext} context - the context to use
 * @returns {Promise.<Batch>} a promise that will resolve with the updated batch when the initiate batch task's execution completes
 */
function executeInitiateBatchTask(batch, cancellable, context) {
  /**
   * Defines the initiate batch task (and its sub-tasks) to be used to track the state of the initiating phase.
   * @returns {InitiateBatchTaskDef} a new initiate batch task definition (with its sub-task definitions)
   */
  function defineInitiateBatchTask() {
    const taskDef = TaskDef.defineTask(initiateBatch.name, initiateBatch, batchAndCancellableSettings);
    taskDef.defineSubTask(extractAndSequenceMessages.name, extractAndSequenceMessages, batchSettings);
    taskDef.defineSubTask(loadBatchState.name, loadBatchState, batchSettings);
    taskDef.defineSubTask(reviveTasks.name, reviveTasks, batchSettings);
    taskDef.defineSubTask(preProcessBatch.name, preProcessBatch, batchSettings);
    return taskDef;
  }

  /**
   * Creates a new initiate batch task to be used to initiate the batch and to track the state of the initiating
   * (pre-processing) phase.
   * @param {Batch} batch - the batch to be initiated
   * @param {StreamConsumerContext} context - the context to use
   * @returns {InitiateBatchTask} a new initiate batch task (with sub-tasks)
   */
  function createInitiateBatchTask(batch, context) {
    // Define a new initiate batch task definition for the batch & update the batch with it
    batch.taskDefs.initiateTaskDef = defineInitiateBatchTask();

    // Create a new initiate batch task (and all of its sub-tasks) from the task definition
    const task = context.taskFactory.createTask(batch.taskDefs.initiateTaskDef, initiateTaskOpts);

    // Cache it on the batch
    const initiatingTasks = batch.getOrSetInitiatingTasks();
    initiatingTasks[task.name] = task;

    return task;
  }

  // Create a new initiate batch task to handle and track the state of the initiating phase
  const initiateBatchTask = createInitiateBatchTask(batch, context);

  // Initiate the batch
  const p = initiateBatchTask.execute(batch, cancellable, context);

  // Terminate the initiate batch task with its first Failure outcome (if any); otherwise return its successful values
  return whenDone(initiateBatchTask, p, cancellable, context);
  // const donePromise = whenDone(initiateBatchTask, p, cancellable, context);
  // Promises.avoidUnhandledPromiseRejectionWarnings([p, donePromise], context);
  // return donePromise;
}

/**
 * Creates and executes a new process batch task on the given batch.
 * @param {Batch} batch - the batch to be processed
 * @param {Cancellable|Object|undefined} [cancellable] - a cancellable object onto which to install cancel functionality
 * @param {StreamConsumerContext} context - the context to use
 * @returns {Promise.<Batch>} a promise that will resolve with the updated batch when the process batch task's execution completes
 */
function executeProcessBatchTask(batch, cancellable, context) {
  /**
   * Defines the process batch task (and its sub-tasks) to be used to track the state of the processing phase.
   * @returns {ProcessBatchTaskDef} a new process batch task definition (with its sub-task definitions)
   */
  function defineProcessBatchTask() {
    const taskDef = TaskDef.defineTask(processBatch.name, processBatch, batchAndCancellableSettings);
    taskDef.defineSubTask(executeAllProcessOneTasks.name, executeAllProcessOneTasks, batchAndCancellableSettings);
    taskDef.defineSubTask(executeAllProcessAllTasks.name, executeAllProcessAllTasks, batchAndCancellableSettings);
    taskDef.defineSubTask(discardUnusableRecords.name, discardUnusableRecords, batchAndUnusableRecordsSettings);
    taskDef.defineSubTask(preFinaliseBatch.name, preFinaliseBatch, batchSettings);
    return taskDef;
  }

  /**
   * Creates a new process batch task to be used to process the batch and to strack the state of the processing phase.
   * @param {Batch} batch - the batch to be processed
   * @param {StreamConsumerContext} context - the context to use
   * @returns {ProcessBatchTask} a new process batch task (with sub-tasks)
   */
  function createProcessBatchTask(batch, context) {
    // Define a new process task definition for the batch & updates the batch with it
    batch.taskDefs.processTaskDef = defineProcessBatchTask();

    const task = context.taskFactory.createTask(batch.taskDefs.processTaskDef, processTaskOpts);

    // Cache it on the batch
    const processingTasks = batch.getOrSetProcessingTasks();
    processingTasks[task.name] = task;

    return task;
  }

  // Create a new process batch task to handle and track the state of the processing phase
  const processBatchTask = createProcessBatchTask(batch, context);

  // Process the batch
  const p = processBatchTask.execute(batch, cancellable, context);
  return whenDone(processBatchTask, p, cancellable, context);
  // const donePromise = whenDone(processBatchTask, p, cancellable, context);
  // Promises.avoidUnhandledPromiseRejectionWarnings([p, donePromise], context);
  // return donePromise;
}

/**
 * Creates and executes a new finalise batch task on the given batch.
 * @param {Batch} batch - the batch to be finalised
 * @param {Outcomes} processOutcomes - all of the outcomes of the preceding process stage
 * @param {Cancellable|Object|undefined} [cancellable] - a cancellable object onto which to install cancel functionality
 * @param {StreamConsumerContext} context - the context to use
 * @returns {Promise.<Batch>} a promise that will resolve with the batch when the finalise batch task's execution completes
 */
function executeFinaliseBatchTask(batch, processOutcomes, cancellable, context) {
  /**
   * Defines the finalise batch task (and its sub-tasks) to be used to track the state of the finalising phase.
   * @returns {FinaliseBatchTaskDef} a new finalise batch task definition (with its sub-task definitions)
   */
  function defineFinaliseBatchTask() {
    const taskDef = TaskDef.defineTask(finaliseBatch.name, finaliseBatch, batchAndProcessOutcomesSettings);
    taskDef.defineSubTask(discardAnyRejectedMessages.name, discardAnyRejectedMessages, batchAndCancellableSettings);
    taskDef.defineSubTask(saveBatchState.name, saveBatchState, batchSettings);
    taskDef.defineSubTask(postFinaliseBatch.name, postFinaliseBatch, batchSettings);
    return taskDef;
  }

  /**
   * Creates a new finalise batch task to be used to finalise the batch and to track the state of the finalising phase.
   * @param {Batch} batch - the batch to be processed
   * @param {StreamConsumerContext} context - the context to use
   * @returns {FinaliseBatchTask} a new finalise batch task (with sub-tasks)
   */
  function createFinaliseBatchTask(batch, context) {
    // Define a new finalise task definition for the batch & updates the batch with it
    batch.taskDefs.finaliseTaskDef = defineFinaliseBatchTask();

    const task = context.taskFactory.createTask(batch.taskDefs.finaliseTaskDef, finaliseTaskOpts);

    // Cache it on the batch
    const finalisingTasks = batch.getOrSetFinalisingTasks();
    finalisingTasks[task.name] = task;

    return task;
  }

  // Create a new finalise batch task to handle and track the state of the finalising phase
  const finaliseBatchTask = createFinaliseBatchTask(batch, context);

  // Regardless of whether processing completes or times out, finalise the batch as best as possible
  const p = finaliseBatchTask.execute(batch, processOutcomes, cancellable, context);

  return whenDone(finaliseBatchTask, p, cancellable, context).then(() =>  batch);
}

/**
 * Validates the given processOneTaskDefs and processAllTaskDefs and raises an appropriate error if these task
 * definitions are invalid (and effectively make this Lambda unusable or useless). Any error thrown must subsequently
 * trigger a replay of all the records in this batch until the Lambda can be fixed.
 *
 * @param {ProcessOneTaskDef[]|undefined} processOneTaskDefs - an "optional" list of "processOne" task definitions that will be
 * used to generate the tasks to be executed on each message independently
 * @param {ProcessAllTaskDef[]|undefined} processAllTaskDefs - an "optional" list of "processAll" task definitions that will be
 * used to generate the tasks to be executed on all of the event's messages collectively
 * @param {StreamConsumerContext} context - the context to use
 * @throws {Error} a validation failure Error (if this Lambda is unusable or useless)
 */
function validateTaskDefinitions(processOneTaskDefs, processAllTaskDefs, context) {
  function validateTaskDefs(taskDefs, name) {
    if (taskDefs) {
      // Must be an array of executable TaskDef instances with valid execute functions
      if (!Arrays.isArrayOfType(taskDefs, TaskDef) || !taskDefs.every(t => t && t.executable && typeof t.execute === 'function')) {
        // This Lambda is unusable, so trigger an exception to put all records back until it can be fixed!
        const errMsg = `FATAL - ${name} must be an array of executable TaskDef instances with valid execute functions! Fix your Lambda and redeploy ASAP, since this issue is blocking all of your stream's shards!`;
        context.error(errMsg);
        throw new FatalError(errMsg);
      }
      // All of the task definitions must have unique names
      const taskNames = taskDefs.map(d => d.name);
      if (!Arrays.isDistinct(taskNames)) {
        // This Lambda is unusable, so trigger an exception to put all records back until it can be fixed!
        const errMsg = `FATAL - ${name} must have no duplicate task names ${stringify(taskNames)}! Fix your Lambda and redeploy ASAP, since this issue is blocking all of your stream's shards!`;
        context.error(errMsg);
        throw new FatalError(errMsg);
      }
    }
  }

  // Check that processOneTaskDefs (if defined) is an array of executable TaskDef instances with valid execute functions
  validateTaskDefs(processOneTaskDefs, 'processOneTaskDefs');

  // Check that processAllTaskDefs (if defined) is an array of executable TaskDef instances with valid execute functions
  validateTaskDefs(processAllTaskDefs, 'processAllTaskDefs');

  // Check that at least one task is defined across both processOneTaskDefs and processAllTaskDefs - otherwise no progress can be made at all!
  if ((!processOneTaskDefs || processOneTaskDefs.length <= 0) && (!processAllTaskDefs || processAllTaskDefs.length <= 0)) {
    // This Lambda is useless, so trigger an exception to put all records back until it can be fixed!
    const errMsg = `FATAL - There must be at least one task definition in either of processOneTaskDefs or processAllTaskDefs! Fix your Lambda and redeploy ASAP, since this issue is blocking all of your stream's shards!`;
    context.error(errMsg);
    throw new FatalError(errMsg);
  }

  // Check that all task definitions across both processOneTaskDefs and processOneTaskDefs are distinct (i.e. any given task
  // definition must NOT appear in both lists)
  const allTaskDefs = (processOneTaskDefs ? processOneTaskDefs : []).concat(processAllTaskDefs ? processAllTaskDefs : []);
  if (!Arrays.isDistinct(allTaskDefs)) {
    // This Lambda is useless, so trigger an exception to put all records back until it can be fixed!
    const errMsg = `FATAL - Any given task definition must NOT exist in both processOneTaskDefs and processAllTaskDefs! Fix your Lambda and redeploy ASAP, since this issue is blocking all of your stream's shards!`;
    context.error(errMsg);
    throw new FatalError(errMsg);
  }
}

function logStreamEvent(event, prefix, logLevel, context) {
  const eventText = (prefix ? prefix + " - " : "") + JSON.stringify(event); //JSON.stringify(event, null, 2);
  log(context, logLevel, eventText);
}

function whenDone(task, promise, cancellable, context) {
  return task.donePromise || Promises.flatten(promise, cancellable, flattenOpts, context);
}

/**
 * Initiates / pre-processes the current batch of stream event records, which includes executing the following steps:
 * 1. Extracts and sequences all of the messages from all of the batch's records
 * 2. Loads and restores the previous state (if any) of the current batch
 * 3. Revives all of the tasks on all of the messages, all of the unusable records and on the batch itself
 * @this {InitiateBatchTask} this is the main initiating task and this function is the `execute` function of the main initiating task
 * @param {Batch} batch - the batch of AWS Kinesis or DynamoDB stream event records to be initiated / pre-processed
 * @param {Object|Cancellable} cancellable -
 * @param {StreamConsumerContext} context - the context to use
 * @returns {Promise.<Batch>} a promise that will resolve with the updated batch when this initiating task is resolved
 */
function initiateBatch(batch, cancellable, context) {
  const task = this;

  // Extract all of the messages from all of the batch's records and then sequence the messages
  const extractAndSequenceMessagesTask = task.getSubTask(extractAndSequenceMessages.name);
  const p1 = extractAndSequenceMessagesTask.execute(batch, context);
  const messagesExtractedPromise = whenDone(extractAndSequenceMessagesTask, p1, cancellable, context);

  return messagesExtractedPromise.then(
    extractOutcomes => {
      // Load and restore the previous state (if any) of the current batch
      const loadBatchStateTask = task.getSubTask(loadBatchState.name);
      const p2 = loadBatchStateTask.execute(batch, context);
      const batchStateLoadedPromise = whenDone(loadBatchStateTask, p2, cancellable, context);

      return batchStateLoadedPromise.then(
        loadOutcomes => {
          // Revive all of the tasks on all of the messages, on all of the unusable records and on the batch itself
          const reviveTasksTask = task.getSubTask(reviveTasks.name);
          const p3 = reviveTasksTask.execute(batch, context);
          const tasksRevivedPromise = whenDone(reviveTasksTask, p3, cancellable, context);

          return tasksRevivedPromise.then(
            reviveOutcomes => {
              // Pre-process the batch
              const preProcessBatchTask = task.getSubTask(preProcessBatch.name);
              const p4 = preProcessBatchTask.execute(batch, context);
              const preProcessedPromise = whenDone(preProcessBatchTask, p4, cancellable, context);

              return preProcessedPromise.then(
                preProcessOutcomes => {
                  return [extractOutcomes, loadOutcomes, reviveOutcomes, preProcessOutcomes];
                },
                err => {
                  context.error(`Failed to pre-process ${batch.describe(true)}`, err);
                  throw err;
                }
              );
            },
            err => {
              context.error(`Failed to revive tasks for ${batch.describe(true)}`, err);
              throw err;
            }
          );
        },
        err => {
          context.error(`Failed to load state for ${batch.describe(true)}`, err);
          throw err;
        }
      );
    },
    err => {
      context.error(`Unexpected error during extraction of messages for batch (${batch.shardOrEventID})`, err);
      throw err;
    }
  );
}

/**
 * Processes the current batch of stream event records, which includes executing the following steps:
 * 1. Starts executing every one of the defined process one tasks on each and every one of the messages
 * 2. Starts executing every one of the defined process all tasks on the batch
 * 3. Starts discarding any and every one of the unusable records encountered
 * 4. Sets up a race between completion of all of the above and a timeout promise
 * @this {ProcessBatchTask} this is the main processing task and this function is the `execute` function of the main processing task
 * @param {Batch} batch - the batch of AWS Kinesis or DynamoDB stream event records to be processed
 * @param {Cancellable|Object|undefined} [cancellable] - a cancellable object onto which to install cancel functionality
 * @param {StreamConsumerContext} context - the context to use
 * @returns {Promise.<Outcomes>} a promise that will resolve with EITHER the completed outcomes of: the "process one"
 * tasks; the "process all" tasks; and the discard unusable records tasks, OR with a timed out result
 */
function processBatch(batch, cancellable, context) {
  const task = this;

  // Start execution of every one of the process one tasks on each and every one of the batch's messages
  const executeAllProcessOneTasksTask = task.getSubTask(executeAllProcessOneTasks.name);
  const p1 = executeAllProcessOneTasksTask.execute(batch, cancellable, context);
  const processOneTasksPromise = whenDone(executeAllProcessOneTasksTask, p1, cancellable, context);

  // Start execution of every one of the process all tasks on the batch
  const executeAllProcessAllTasksTask = task.getSubTask(executeAllProcessAllTasks.name);
  const p2 = executeAllProcessAllTasksTask.execute(batch, cancellable, context);
  const processAllTasksPromise = whenDone(executeAllProcessAllTasksTask, p2, cancellable, context);

  // Discard any and all unusable records
  const discardAnyUnusableRecordsTask = task.getSubTask(discardUnusableRecords.name);
  const p3 = discardAnyUnusableRecordsTask.execute(batch, cancellable, context);
  const discardUnusableRecordsPromise = whenDone(discardAnyUnusableRecordsTask, p3, cancellable, context);

  const preFinaliseBatchTask = task.getSubTask(preFinaliseBatch.name);

  // Create a processed promise that will only complete when the process one, process all and discardUnusableRecords promises complete
  const processedPromise = Promises.every([processOneTasksPromise, processAllTasksPromise, discardUnusableRecordsPromise],
    cancellable, context);

  // Set a timeout to trigger when a configurable percentage of the remaining time in millis is reached, which will give
  // us a bit of time to finalise at least some of the message processing before we run out of time to complete
  // everything in this invocation
  const timeoutCancellable = {};
  const timeoutMs = calculateTimeoutMs(context.streamProcessing.timeoutAtPercentageOfRemainingTime, context);

  context.debug(`Creating a process batch timeout to trigger after ${timeoutMs} ms (remaining time: ${context.awsContext.getRemainingTimeInMillis()} ms)`);

  // let timedOut = false;

  const timeoutPromise = createTimeoutPromise(task, timeoutMs, timeoutCancellable, cancellable, context)
    .then(timeoutTriggered => {
      if (timeoutTriggered) { // If the timeout triggered then
        // timeout any and all of the process one and all tasks on the messages (reusing the timeout error set on the processing task by createTimeoutPromise if any)
        const timeoutError = isInstanceOf(task.error, TimeoutError) ? task.error :
          new TimeoutError(`Ran out of time to complete ${task.name}`);
        batch.timeoutProcessingTasks(timeoutError);
        context.warn(timeoutError);
        // timedOut = true;
        // Promises.avoidUnhandledPromiseRejectionWarning(processedPromise, context);
        return new Failure(timeoutError);
      }
      return timeoutTriggered;
    });

  // Promises.avoidUnhandledPromiseRejectionWarning(timeoutPromise, context);

  // Build a completed promise that will only complete when the processed promise completes
  const completedPromise = createCompletedPromise(task, processedPromise, batch, timeoutCancellable, context);
  // .then(outcomes => {
  //   if (!timedOut) Promises.avoidUnhandledPromiseRejectionWarning(timeoutPromise, context);
  //   return outcomes;
  // });

  // Promises.avoidUnhandledPromiseRejectionWarning(completedPromise, context);

  // Set up a race between the completed promise and the timeout promise
  return Promise.race([completedPromise, timeoutPromise]).then(
    result => {
      // Pre-finalise the batch after successful processing
      const p = preFinaliseBatchTask.execute(batch, context);
      return whenDone(preFinaliseBatchTask, p, cancellable, context)
        .then(() => result, () => result);
    },
    err => {
      // Pre-finalise the batch after failed processing
      const p = preFinaliseBatchTask.execute(batch, context);
      return whenDone(preFinaliseBatchTask, p, cancellable, context)
        .then(() => Promise.reject(err), () => Promise.reject(err));
    });
}

/**
 * Extracts all of the messages from all of the given batch's stream event records and then, after the entire batch of
 * messages has been extracted, sequences the batch of messages into the correct processing sequence and loads the
 * previous tracked state (if any) for the current batch.
 * Precondition: batch.records.length > 0
 * @param {Batch} batch - the batch of AWS Kinesis or DynamoDB stream event records to be processed
 * @param {StreamConsumerContext} context - the context to use
 * @return {Promise.<Batch>} a promise of the given batch that will be updated with: an array of zero or more
 * successfully extracted message objects; and an array of zero or more unusable, unparseable records.
 */
function extractAndSequenceMessages(batch, context) {
  // const task = this;
  const records = batch.records;

  // Get the configured extractMessagesFromRecord function to be used to do the actual extraction
  const extractMessagesFromRecord = Settings.getExtractMessagesFromRecordFunction(context);
  const extractMessageFromRecord = Settings.getExtractMessageFromRecordFunction(context);

  if (!extractMessagesFromRecord) {
    // No extract function available - should never get here, since validation of configuration should catch this case earlier on
    const errMsg = `FATAL - Cannot extract any messages for batch (${batch.shardOrEventID}) WITHOUT a valid, configured extractMessagesFromRecord function`;
    context.error(errMsg);
    return Promise.reject(new FatalError(errMsg));
  }

  // Extract the message(s) from the records using the configured `extractMessagesFromRecord` & optional `extractMessageFromRecord` functions
  // Convert all of the event's records back into their original message object forms
  const extractPromises = records.map(record =>
    extractMessagesFromStreamEventRecord(record, batch, extractMessagesFromRecord, extractMessageFromRecord, context)
  );

  return Promises.every(extractPromises, undefined, context).then(
    outcomes => {
      if (context.debugEnabled) {
        context.debug(`Finished extracting messages for ${batch.describe(true)} - ${Try.describeSuccessAndFailureCounts(outcomes)}`);
      }

      // Rethrow any FatalError
      const fatal = outcomes.find(o => isInstanceOf(o, Failure) && isInstanceOf(o.error, FatalError));
      if (fatal) {
        throw fatal.error;
      }

      // At this point all messages have been extracted and added to the batch's list of messages, so next ...
      // Resolve the sequence in which the messages must be processed & set batch.firstMessagesToProcess
      batch.firstMessagesToProcess = sequencing.sequenceMessages(batch, context);
      return outcomes;
    },
    err => {
      context.error(`Unexpected error during extraction of messages for ${batch.describe(true)})`, err);
      throw err;
    }
  );
}

/**
 * Attempts to extract one or more messages from the given stream event record using the given `extractMessagesFromRecord`
 * & optional `extractMessageFromRecord` functions and adds the extracted messages, rejected messages and/or unusable
 * records to the given batch.
 *
 * @param {StreamEventRecord|KinesisEventRecord|DynamoDBEventRecord} record - the AWS Kinesis or DynamoDB stream event record from which to extract its message(s)
 * @param {Batch} batch - the batch currently being processed and to be updated
 * @param {ExtractMessagesFromRecord} extractMessagesFromRecord - the actual function to use to extract the messages from the record
 * @param {ExtractMessageFromRecord|undefined} [extractMessageFromRecord] - the actual function to use to extract a message from the record or from each of the record's user records (if any)
 * @param {StreamConsumerContext} context - the context to use
 * @return {Promise.<MsgOrUnusableRec[]>} a promise that will resolve with an array containing one or more successfully
 * extracted messages, rejected messages and/or unusable records
 */
function extractMessagesFromStreamEventRecord(record, batch, extractMessagesFromRecord, extractMessageFromRecord, context) {
  if (!record || typeof record !== 'object') {
    context.warn(`Adding invalid record (${record}) as an unusable record`);
    return Promise.resolve([{unusableRec: batch.addUnusableRecord(record, undefined, `invalid record (${record})`, context)}]);
  }

  // Validate the record
  const kinesisStreamType = isKinesisStreamType(context);
  try {
    if (kinesisStreamType) {
      streamEvents.validateKinesisStreamEventRecord(record);
    } else if (isDynamoDBStreamType(context)) {
      streamEvents.validateDynamoDBStreamEventRecord(record);
    } else {
      // Record is neither a Kinesis nor a DynamoDB stream event record, so return no message, no promises and the unusable record
      const errMsg = `Record is neither a Kinesis nor a DynamoDB stream event record - unexpected stream type (${Settings.getStreamType(context)})`;
      context.error(errMsg);
      return Promise.resolve([{unusableRec: batch.addUnusableRecord(record, undefined, errMsg, context)}]);
    }
  } catch (err) {
    context.error(err);
    return Promise.resolve([{unusableRec: batch.addUnusableRecord(record, undefined, err.message, context)}]);
  }

  // Extract the message(s) from the record using the configured `extractMessagesFromRecord` function
  return Promises.try(() => extractMessagesFromRecord(record, batch, extractMessageFromRecord, context)).then(
    results => {
      if (context.traceEnabled) {
        let m = 0, rm = 0, u = 0;
        results.forEach(r => {
          if (r) {
            if (r.msg) ++m;
            else if (r.rejectedMsg) ++rm;
            else if (r.unusableRec) ++u;
          }
        });
        const mru = `${m} message${m !== 1 ? 's' : ''}, ${rm} rejected message${rm !== 1 ? 's' : ''} & ${u} unusable record${u !== 1 ? 's' : ''}`;
        context.trace(`Extracted ${mru} from record (${record && record.eventID})`);
      }
      return results;
    },
    err => {
      // Promise rejected with an error
      context.error(`Failed to extract messages from record (${record && record.eventID})`, err);
      if (isInstanceOf(err, FatalError)) {
        throw err; // rethrow any FatalError
      }
      // NB: Do NOT throw an error in a non-fatal case, since an unparseable record will most likely remain an
      // unparseable record forever and throwing an error would result in an "infinite" loop back to the stream until
      // the record eventually expires
      return [{unusableRec: batch.addUnusableRecord(record, undefined, err.message, context)}];
    }
  );
}

/**
 * Loads and restores the previous tracked state (if any) for the current batch of records being processed from an
 * external store using the configured loadBatchState function of type {@link LoadBatchState}.
 * @param {Batch} batch - the batch being processed
 * @param {StreamConsumerContext} context - the context to use
 * @returns {Promise.<*>} a promise that will be completed when the previous state of the batch is fully loaded & restored
 */
function loadBatchState(batch, context) {
  const task = this;
  // const initiatingTask = task.parent;

  const contents = batch.describeContents();

  if (batch.records.length <= 0) {
    const reason = `Cannot load state for an empty batch - ${contents}`;
    context.warn(reason);
    if (task) task.reject(reason);
    return Promise.resolve([]);
  }

  const streamConsumerId = batch.streamConsumerId || (batch.key && batch.key.streamConsumerId);
  const shardOrEventID = batch.shardOrEventID || (batch.key && batch.key.shardOrEventID);

  if (batch.messages.length <= 0 && batch.rejectedMessages.length <= 0 && batch.unusableRecords.length <= 0) {
    context.info(`Skipping load state of batch (${shardOrEventID}), since ${contents}`);
    return Promise.resolve([]);
  }

  const batchDesc = `batch (${shardOrEventID}) with ${contents}`;

  if (isBlank(streamConsumerId) || isBlank(shardOrEventID)) {
    const errMsg = `Cannot load state of batch WITHOUT a complete batch key - streamConsumerId (${streamConsumerId}) & shardOrEventID (${shardOrEventID})`;
    context.warn(errMsg, '-', batchDesc);
    return Promise.resolve([]);
  }

  // Look up the actual function to be used to do the load of the previous tracked state (if any) of the current batch
  const loadBatchStateFn = Settings.getLoadBatchStateFunction(context);

  if (!loadBatchStateFn) {
    // Should never get here, since configuration validation should eliminate this case
    return Promise.reject(new FatalError(`FATAL - Failed to load state of ${batchDesc}, since no loadBatchState function configured`));
  }

  // Create a task to be used to execute and track the configured loadBatchState function
  const fnName = isNotBlank(loadBatchStateFn.name) ? loadBatchStateFn.name : 'loadBatchState';
  const loadBatchStateTask = task.createSubTask(fnName, loadBatchStateFn, batchSettings, initiateTaskOpts);

  // Load the previous tracked state of the batch (if any)
  const p = loadBatchStateTask.execute(batch, context);
  return whenDone(loadBatchStateTask, p, undefined, context);
}

/**
 * Revives all of the tasks of the given batch and of all of the given batch's messages and unusable records by re-
 * incarnating any prior task-like versions of these tasks as new task objects according to the configured task
 * definitions.
 * @param {Batch} batch - the batch being processed
 * @param {StreamConsumerContext} context - the context to use
 */
function reviveTasks(batch, context) {
  batch.reviveTasks(context);
}

/**
 * Provides a hook for an optional pre-process batch function to be invoked after the batch has been successfully
 * initiated and before the batch is processed.
 * @param {Batch} batch
 * @param {StreamConsumerContext} context
 * @param {PreFinaliseBatch} [context.preProcessBatch] - an optional pre-process batch function to execute
 * @returns {Promise.<Batch|undefined>}
 */
function preProcessBatch(batch, context) {
  const task = this;
  // const initiatingTask = task.parent;

  // Look up the actual function to be used to do the load of the previous tracked state (if any) of the current batch
  const preProcessBatchFn = Settings.getPreProcessBatchFunction(context);

  if (!preProcessBatchFn) {
    if (context.traceEnabled) context.trace(`Skipping pre-process of ${batch.describe(true)}, since no preProcessBatch function configured`);
    return Promise.resolve([]);
  }

  // Create a task to be used to execute and track the configured preProcessBatch function
  const fnName = isNotBlank(preProcessBatchFn.name) ? preProcessBatchFn.name : 'preProcessBatch';
  const preProcessBatchTask = task.createSubTask(fnName, preProcessBatchFn, batchSettings, initiateTaskOpts);

  // Preprocess the batch
  const p = preProcessBatchTask.execute(batch, context);
  return whenDone(preProcessBatchTask, p, undefined, context).then(
    () => {
      if (context.traceEnabled) context.trace(`Pre-processed ${batch.describe(true)}`);
      return batch;
    },
    err => {
      context.error(`Failed to pre-process ${batch.describe(true)}`, err);
      throw err;
    }
  );
}

/**
 * Executes all of the incomplete "process one" tasks on each of the messages of the given batch, starting with the
 * messages that do not have to wait for any preceding messages to finish first and finally returns a promise of an
 * array of Success and/or Failure execution outcomes (one for each incomplete task executed on each message).
 *
 * When all of the incomplete "process one" tasks of a processed message become fully finalised, then any "next" message
 * that was waiting for it to finish will be triggered.
 *
 * @param {Batch} batch - the batch to be used as a source of messages on which to execute each of the "process one" tasks
 * @param {Cancellable|Object|undefined} [cancellable] - a cancellable object onto which to install cancel functionality
 * @param {StreamConsumerContext} context - the context to use (passed as the 3rd argument)
 * @return {Promise.<Outcomes>} a promise of a list of zero or more execution outcomes for all of the batch's messages'
 * initially incomplete "process one" tasks
 */
function executeAllProcessOneTasks(batch, cancellable, context) {
  const states = batch.states;
  const messages = batch.messages;
  const m = messages.length;
  const ms = toCountString(m, 'message');

  const t = batch.taskDefs.processOneTaskDefs.length;
  const ts = toCountString(t, 'process one task');

  if (m <= 0) {
    if (context.debugEnabled) context.debug(`Skipping execution of ${ts} for ${batch.describe(true)}, since ${ms}`);
    return Promise.resolve([]);
  }
  if (t <= 0) {
    context.warn(`Skipping execution of ${ts} for ${batch.describe(true)}, since ${ts}`);
    return Promise.resolve([]);
  }

  // Find all of the messages that do NOT have any preceding messages, which must be processed before they can be processed
  const independentMessages = batch.firstMessagesToProcess && batch.firstMessagesToProcess.length > 0 ?
    batch.firstMessagesToProcess : messages.filter(message => !states.get(message).prevMessage);

  if (context.debugEnabled) {
    const i = independentMessages.length;
    context.debug(`About to execute ${ts} on ${i} independent of ${ms} for ${batch.describe(true)}`);
  }

  // Execute all of the incomplete processOne tasks on each of the independent messages and collect their promises
  const promises = independentMessages.map(message => executeProcessOneTasks(batch, message, cancellable, context));

  return Promises.flatten(promises, cancellable, flattenOpts, context).then(
    independentOutcomes => {
      const outcomes = Arrays.flatten(independentOutcomes);
      context.trace(`executeAllProcessOneTasks - independent outcomes (${independentOutcomes.length}) vs flattened outcomes (${outcomes.length})`);
      return outcomes;
    }
  );
}

/**
 * Executes all of the incomplete "process one" tasks of the given message and of its chain of "next" messages (if any),
 * which share the same key(s) and were sequenced after the given message, and finally returns a promise of an array of
 * Success and/or Failure execution outcomes (one for each task executed on each message).
 *
 * If all of the incomplete "process one" tasks of the given message become fully finalised, then this function will be
 * invoked "recursively" on the "next" message (if any) that was waiting for the given message to finish.
 *
 * @param {Batch} batch - the current batch being processed
 * @param {Message} message - the message to be processed (passed as the 1st argument)
 * @param {Cancellable|Object} cancellable - a cancellable object onto which to install cancel functionality
 * @param {StreamConsumerContext} context - the context to use (passed as the 2nd argument)
 * @return {Promise.<Outcomes>} a promise of an array of zero or more execution outcomes for the initially incomplete
 * "process one" tasks of the given message and of its chain of "next" messages (if any)
 */
function executeProcessOneTasks(batch, message, cancellable, context) {
  const states = batch.states;
  const messageState = states.get(message);

  let completed = false;
  let cancelled = false;
  if (cancellable) Promises.installCancel(cancellable, () => {
    if (!completed) cancelled = true;
    return completed;
  });

  // First get all of the message's process one tasks
  // const tasksByName = batch.getProcessOneTasks(message);
  const tasks = taskUtils.getTasks(messageState.ones);

  const t = tasks.length;
  const ts = toCountString(t, 'process one task');

  // Collect all of the message's process one tasks that are NOT yet fully finalised
  const incompleteTasks = tasks.filter(task => !task.isFullyFinalised());

  const i = incompleteTasks.length;

  if (i <= 0) {
    const is = toCountString(i, 'incomplete task');
    if (context.debugEnabled) context.debug(`Skipping execution of ${ts} on message (${messageState.id}), since ${is}`);
  } else {
    if (context.traceEnabled) context.trace(`About to execute ${i} incomplete of ${ts} on message (${messageState.id})`);
  }

  // Start executing each of the incomplete tasks on the message and collect their done promises
  const promises = incompleteTasks.map(task => {
    const p = task.execute(message, batch, context);
    return whenDone(task, p, cancellable, context);
  });

  return Promises.every(promises, cancellable, context).then(
    outcomes => {
      // Check if there is a next message that was waiting for the current message to finish processing
      const nextMessage = messageState.nextMessage;

      // Start processing the next message (if any), but ONLY if we have completely finalised the current message's process one tasks
      if (nextMessage) {
        const nextMessageState = states.get(nextMessage);
        if (!cancelled) {
          // Check if we have now completely finalised the processing of all of the current message's incomplete process one tasks
          const allTasksFullyFinalised = incompleteTasks.every(task => task.isFullyFinalised());
          if (allTasksFullyFinalised) {
            // Start executing all of the process one tasks on the next message
            context.debug(`About to process NEXT message (${getMessageStateKeySeqNoId(nextMessageState)}) after (${getMessageStateKeySeqNoId(messageState)}) finalised`);
            return executeProcessOneTasks(batch, nextMessage, cancellable, context)
              .then(nextOutcomes => {
                completed = true;
                return outcomes.concat(nextOutcomes);
              });
          } else {
            completed = true;
            const stillIncompleteTasks = incompleteTasks.filter(task => !task.isFullyFinalised());
            context.debug(`Cannot process next message (${getMessageStateKeySeqNoId(nextMessageState)}) yet, since (${getMessageStateKeySeqNoId(messageState)}) is still not finalised (incomplete tasks: ${stillIncompleteTasks.map(t => `${t.name} - ${t.state.name}`).join('; ')})`);
            return outcomes;
          }
        } else {
          context.debug(`Skipped processing of next message (${getMessageStateKeySeqNoId(nextMessageState)}), since batch processing has been cancelled`);
          return outcomes;
        }
      } else {
        completed = true;
        return outcomes;
      }
    },
    err => {
      context.error(`Failed to process message (${messageState.id}, ${messageState.key})`, err);
      throw err;
    }
  );
}

/**
 * Executes all of the incomplete "process all" tasks on the batch and returns a promise of an array with a Success or
 * Failure execution outcome for each task.
 *
 * For each "process all" task to be executed, also resolves and passes all of its incomplete messages, which are all
 * of the batch's messages that are not yet fully finalised for the particular task. The incomplete messages are passed
 * as the 2nd argument to the task's `execute` function.
 *
 * Any and all errors encountered along the way are logged, but no errors are allowed to escape from this function.
 *
 * @param {Batch} batch - the batch on which to execute each of the "process all" tasks (passed as the 1st argument)
 * @param {Cancellable|Object|undefined} [cancellable] - a cancellable object onto which to install cancel functionality
 * @param {StreamConsumerContext} context - the context to use (passed as the 3rd argument)
 * @return {Promise.<Outcomes>} a promise to return all of the "process all" tasks' outcomes
 */
function executeAllProcessAllTasks(batch, cancellable, context) {
  const messages = batch.messages;
  const m = messages.length;
  const ms = toCountString(m, 'message');

  const t = batch.taskDefs.processAllTaskDefs.length;
  const ts = toCountString(t, 'process all task');

  if (m <= 0) {
    if (context.debugEnabled) context.debug(`Skipping execution of ${ts} for ${batch.describe(true)}, since ${ms}`);
    return Promise.resolve([]);
  }
  if (t <= 0) {
    context.warn(`Skipping execution of ${ts} for ${batch.describe(true)}), since ${ts}`);
    return Promise.resolve([]);
  }

  // First get all of the batch's process all tasks
  const tasksByName = batch.getProcessAllTasks();
  const tasks = taskUtils.getTasks(tasksByName);

  const incompleteTasks = tasks.filter(task => !task.isFullyFinalised());

  const i = incompleteTasks.length;

  if (i <= 0) {
    const is = toCountString(i, 'incomplete task');
    if (context.debugEnabled) context.debug(`Skipping execution of ${ts} on ${batch.describe(true)}, since ${is}`);
    return Promise.resolve([]);
  } else {
    if (context.traceEnabled) context.trace(`About to execute ${i} incomplete of ${ts} on ${batch.describe(true)}`);
  }

  // Execute all of the incomplete processAll tasks on the batch and collect their promises
  const promises = incompleteTasks.map(task => {
    // Collect all of the incomplete messages from the batch that are not fully finalised yet for the given task
    const incompleteMessages = messages.filter(msg => {
      const msgTask = batch.getProcessAllTask(msg, task.name);
      return !msgTask || !msgTask.isFullyFinalised();
    });

    const p = task.execute(batch, incompleteMessages, context);
    return whenDone(task, p, cancellable, context);
  });

  return Promises.every(promises, cancellable, context);
}

/**
 * Calculates the number of milliseconds to wait before timing out using the given timeoutAtPercentageOfRemainingTime
 * and the Lambda's remaining time to execute.
 * @param {number} timeoutAtPercentageOfRemainingTime - the percentage of the remaining time at which to timeout
 * the given task (expressed as a number between 0.0 and 1.0, e.g. 0.9 would mean timeout at 90% of the remaining time)
 * @param {StreamConsumerContext} context - the context to use
 * @returns {number} the number of milliseconds to wait
 */
function calculateTimeoutMs(timeoutAtPercentageOfRemainingTime, context) {
  const remainingTimeInMillis = context.awsContext.getRemainingTimeInMillis();
  return Math.round(remainingTimeInMillis * timeoutAtPercentageOfRemainingTime);
}

/**
 * Creates a promise that will timeout when the configured percentage or 90% (if not configured) of the remaining time
 * in millis is reached, which will give us hopefully enough time to finalise at least some of our message processing
 * before we run out of time to complete everything in this invocation.
 *
 * @param {Task} task - a task to be timed out if the timeout triggers
 * @param {number} timeoutMs - the number of milliseconds to wait before timing out
 * @param {Object|undefined|null} [timeoutCancellable] - an arbitrary object onto which a cancelTimeout method will be installed
 * @param {Cancellable|Object} processCancellable - the cancellable, which can be used to cancel the normal process flow if/when the new timeout promise times out
 * @param {StreamConsumerContext} context - the context to use
 * @return {Promise.<boolean>} a promise to return true if the timeout is triggered or false if not
 */
function createTimeoutPromise(task, timeoutMs, timeoutCancellable, processCancellable, context) {
  return Promises.delay(timeoutMs, timeoutCancellable).then(
    triggered => {
      if (triggered) {
        context.info(`Timed out while waiting for ${task.name}`);
        // Cancel/short-circuit the process tasks
        if (processCancellable && typeof processCancellable.cancel === 'function') {
          const completed = processCancellable.cancel();
          if (completed) {
            context.warn(`Timeout was triggered, but task ${task.name} process is also already completed!`);
            return false;
          }
        }
        const timeoutOpts = {overrideCompleted: false, overrideUnstarted: false, reverseAttempt: true};
        task.timeout(new TimeoutError(`Ran out of time to complete ${task.name}`), timeoutOpts, true);
      }
      return triggered;
    },
    err => {
      if (isInstanceOf(err, DelayCancelledError)) {
        // The timeout promise was cancelled, which means that the main completing promise must have completed and cancelled the timeout
        context.debug(`Timeout cancelled after ${task.name} completed - ${err.message}`);
        return false; // a DelayCancelledError will only be thrown if the timeout has NOT triggered
      } else {
        // The timeout promise rejected with an unexpected error!
        context.error(`Timeout failed with an unexpected error while waiting for ${task.name}`, err);
        throw err;
      }
    }
  );
}

/**
 * Build a completed promise that will only complete when the given completingPromise has completed.
 * @param {Task} task - a task to be completed or failed depending on the outcome of the promise and if the timeout has not triggered yet
 * @param {Promise} completingPromise - the promise that will complete when all processing has been completed for the current phase
 * @param {Batch} batch - the batch being processed
 * @param {Object} timeoutCancellable - a cancellable object that enables cancellation of the timeout promise on completion
 * @param {StreamConsumerContext} context - the context to use
 * @returns {Promise.<Outcomes>} a promise to return the outcomes of the completing promise when the completingPromise resolves
 */
function createCompletedPromise(task, completingPromise, batch, timeoutCancellable, context) {
  const mustResolve = false;

  return completingPromise.then(
    outcomes => {
      // The completing promise has completed
      // 1. Try to cancel the timeout with which the completing promise was racing
      const timedOut = timeoutCancellable.cancelTimeout(mustResolve);
      if (timedOut) {
        context.warn(`Timed out before ${task.name} completed for ${batch.describe(true)}`);
      } else {
        // 2. Mark the completing task as failed if any of its outcomes (or any of its sub-tasks' outcomes) is a Failure
        const failure = Try.findFailure(outcomes);
        if (failure) {
          task.fail(failure.error, false);
        } else {
          task.complete(outcomes, {overrideTimedOut: false}, false);
        }
        context.debug(`Completed ${task.name} for ${batch.describe(true)} - final state (${task.state})`);
      }
      return outcomes;
    },
    err => {
      if (isInstanceOf(err, CancelledError)) {
        // The completing promise was cancelled, which means the timeout must have triggered and cancelled it
        context.warn(`Cancelling ${task.name} for ${batch.describe(true)}`, err);
        const timeoutOpts = {overrideCompleted: false, overrideUnstarted: false, reverseAttempt: true};
        const timeoutError = new TimeoutError(`Ran out of time to complete ${task.name}`);
        if (!task.timedOut) {
          task.timeout(timeoutError, timeoutOpts, true);
        }
        // timeoutError.cancelledError = err;
        // throw timeoutError;
        throw err;

      } else {
        // The completing promise rejected with a non-cancelled error, so attempt to cancel the timeout promise
        const timedOut = timeoutCancellable.cancelTimeout(mustResolve);
        if (timedOut) {
          context.warn(`Timed out before failed ${task.name} for ${batch.describe(true)}`, err);
        } else {
          context.info(`Failed ${task.name} for ${batch.describe(true)}`, err);
          task.fail(err, false);
        }
        throw err;
      }
    }
  );
}

/**
 * Attempts to discard any and all of the given batch's unusable records using the previously configured discard one
 * unusable record task definitions and tasks.
 * @param {Batch} batch - the batch being processed
 * @param {Cancellable|Object|undefined} [cancellable] - a cancellable object onto which to install cancel functionality
 * @param {StreamConsumerContext} context - the context to use
 * @returns {Promise.<Outcomes>} a promise of a list of discard outcomes for the undiscarded, unusable records (if any)
 */
function discardUnusableRecords(batch, cancellable, context) {
  return batch.discardUnusableRecords(cancellable, context);
}

/**
 * Provides a hook for an optional pre-finalise batch function to be invoked after the batch has been successfully
 * processed and before the batch is finalised.
 * @param {Batch} batch
 * @param {StreamConsumerContext} context
 * @param {PreFinaliseBatch} [context.preFinaliseBatch] - an optional pre-finalise batch function to execute
 * @returns {Promise.<Batch|undefined>}
 */
function preFinaliseBatch(batch, context) {
  const task = this;

  // Look up the actual function to be used to do the pre-finalise batch logic (if any)
  const preFinaliseBatchFn = Settings.getPreFinaliseBatchFunction(context);

  if (!preFinaliseBatchFn) {
    if (context.traceEnabled) context.trace(`Skipping pre-finalise of ${batch.describe(false)}, since no preFinaliseBatch function configured`);
    return Promise.resolve(undefined);
  }

  // Create a task to be used to execute and track the configured preFinaliseBatch function
  const fnName = isNotBlank(preFinaliseBatchFn.name) ? preFinaliseBatchFn.name : 'preFinaliseBatch';
  const preFinaliseBatchTask = task.createSubTask(fnName, preFinaliseBatchFn, batchSettings, processTaskOpts);

  // Pre-finalise the batch
  const p = preFinaliseBatchTask.execute(batch, context);
  return whenDone(preFinaliseBatchTask, p, undefined, context).then(
    () => {
      if (context.traceEnabled) context.trace(`Pre-finalised ${batch.describe(false)}`);
      return batch;
    },
    err => {
      context.error(`Failed to pre-finalise ${batch.describe(false)}`, err);
      throw err;
    }
  );
}

/**
 * Attempts to finalise message processing (either after all processing completed successfully or after the configured
 * timeout expired to indicate this Lambda is running out of time) by first marking messages' incomplete tasks that have
 * exceeded the allowed number of attempts as discarded; then freezing all messages' tasks and then by handling all
 * still incomplete messages and discarding any rejected messages using the configured functions for both.
 * @this {FinaliseBatchTask} this is the main finalising task and this function is the `execute` function of the main finalising task
 * @param {Batch} batch - the batch being processed
 * @param {Outcomes} processOutcomes - all of the outcomes of the preceding process stage
 * @param {Cancellable|Object} cancellable - a cancellable object onto which to install cancel functionality
 * @param {StreamConsumerContext} context - the context to use
 * @returns {Promise.<Batch|BatchError>} a promise that will resolve with the batch processed or reject with an error
 */
function finaliseBatch(batch, processOutcomes, cancellable, context) {
  const task = this;

  const freezeProcessingTasksOutcome = freezeProcessingTasks(batch, context);

  function discard() {
    const discardTask = task.getSubTask(discardAnyRejectedMessages.name);
    const p = discardTask.execute(batch, cancellable, context);
    return whenDone(discardTask, p, cancellable, context);
  }

  // Discard any finalised messages that contain at least one rejected task
  const discardRejectedMessagesPromise = discard();

  // Freeze any finalising tasks after discarding (or at least attempting to discard) any rejected messages
  const freezeFinalisingTasksPromise = discardRejectedMessagesPromise
    .then(() => freezeFinalisingTasks(batch, context));

  function save() {
    const saveTask = task.getSubTask(saveBatchState.name);
    const p = saveTask.execute(batch, context);
    return whenDone(saveTask, p, cancellable, context);
  }

  // Save the current state of the batch after discarding (or at least attempting to discard) any rejected messages
  const saveBatchStatePromise = freezeFinalisingTasksPromise.then(save);

  // Create a finalised promise that will ONLY complete when every one of the other finalising promises resolve
  const finalisingPromise = Promises.every([freezeProcessingTasksOutcome, discardRejectedMessagesPromise,
    freezeFinalisingTasksPromise, saveBatchStatePromise], cancellable, context);

  const postFinaliseBatchTask = task.getSubTask(postFinaliseBatch.name);

  // Set a timeout to trigger at or within the last second, which will hopefully give us a enough time to
  // complete the finalisation of the batch before the Lambda runs out of time to execute
  const timeoutCancellable = {};
  // Use the greater of the configured or minimum finalise timeout at percentage
  const timeoutAtPercentage = Math.max(context.streamProcessing.timeoutAtPercentageOfRemainingTime,
    MIN_FINALISE_TIMEOUT_AT_PERCENTAGE);
  const timeoutAtLastSec = Math.max(context.awsContext.getRemainingTimeInMillis() - 1000, 0);
  const timeoutMs = Math.max(timeoutAtLastSec, calculateTimeoutMs(timeoutAtPercentage, context));

  context.debug(`Creating a finalise batch timeout to trigger after ${timeoutMs} ms (remaining time: ${context.awsContext.getRemainingTimeInMillis()} ms)`);

  const timeoutPromise = createTimeoutPromise(task, timeoutMs, timeoutCancellable, cancellable, context).then(
    timeoutTriggered => {
      if (timeoutTriggered) { // If the timeout triggered then
        // timeout any and all of the finalising tasks (reusing the timeout error set on the finalise batch task by createTimeoutPromise if any)
        const timeoutError = isInstanceOf(task.error, TimeoutError) ? task.error :
          new TimeoutError(`Ran out of time to complete ${task.name}`);
        batch.timeoutFinalisingTasks(timeoutError);
        return Promise.reject(timeoutError);
        // return new Failure(timeoutError);
      }
      return timeoutTriggered;
    }
  );

  const completedPromise = createCompletedPromise(task, finalisingPromise, batch, timeoutCancellable, context);

  const completedVsTimeoutPromise = Promise.race([completedPromise, timeoutPromise]);

  // Whichever finishes first, wrap up as best as possible
  return completedVsTimeoutPromise.then(
    outcomes => {
      const progress = batch.assessProgress();

      // Find the first finalising failure (if any) to use as the error with which to fail this Lambda
      const finaliseFailure = Try.findFailure(outcomes);

      // Throw an error to trigger replay of the entire batch if finalising failed
      if (finaliseFailure) {
        const finaliseError = finaliseFailure.error;
        context.error(`Triggering a replay of the entire batch with ${progress}, due to finalising failure`, finaliseError);
        throw finaliseError;
      }

      // Throw an error to trigger replay of the batch if the batch is NOT fully finalised yet (& sequencing is required)
      if (!batch.isFullyFinalised()) {
        // Find the first processing failure (if any)
        const processFailure = Try.findFailure(processOutcomes);

        // Log the previous processing failure (if any) as part of the reason for replaying the entire batch
        if (processFailure) {
          const processError = processFailure.error;
          context.error(`Triggering a replay of the entire batch with ${progress}, due to processing failure`, processError);
          throw processError;
        }

        const incompleteError = new Error(`Triggering a replay of the entire batch with ${progress}, since it is still incomplete!`);
        context.error(incompleteError);
        throw incompleteError;
      }

      if (Array.isArray(outcomes)) {
        // Finalisation must have completed
        // Batch is finally fully finalised & there was no finalising error, so batch is complete!
        context.info(`FINAL STATE: Batch (${batch.shardOrEventID}) with ${progress} is complete & fully finalised`);
        return batch;

      } else {
        // Finalisation probably timed out
        const error = task.error ? task.error : new Error(`Unexpected finalising outcome: ${stringify(outcomes)}`);
        context.error(`Triggering a replay of the entire batch with ${progress}, due to unexpected outcome`, error);
        throw error;
      }
    },
    err => {
      // Finalisation must have failed or timed out
      const progress = batch.assessProgress();

      // // Terminate the finalise batch task with EITHER the process phase's first Failure outcome (if any) ...
      // Try.flatten(processOutcomes);
      // // ... OR throw the finalise error
      // throw err;

      context.error(`Triggering a replay of the entire batch with ${progress}, due to finalise batch error`, err);
      throw err;
    })
    .then(
      batch => {
        // Post-finalise the batch after a successful finalise ...
        return postFinaliseBatchTask.execute(batch, context)
          .then(() => batch, () => batch); // ... and then return the finalise results
      },
      err => {
        // Post-finalise the batch after a failed finalise ...
        return postFinaliseBatchTask.execute(batch, context)
          .then(() => Promise.reject(err), () => Promise.reject(err)); // ... and then return the finalise error
      })
    .then(
      batch => {
        // Log the final results after a successful finalise ...
        logFinalResults(batch, undefined, context);
        return batch; // ... and then return the batch
      },
      err => {
        // Log the final results after a failed finalise ...
        logFinalResults(batch, err, context);
        throw err; // ... and then return the finalise error
      })
    .catch(err => {
      // // Terminate the finalise batch task with EITHER the process phase's first Failure outcome (if any) ...
      // Try.flatten(processOutcomes);
      // // ... OR throw the finalise error
      // throw err;

      // // Terminate the finalise batch task with EITHER the process phase's first Failure outcome (if any) ...
      // // Try.flatten(processOutcomes);

      return Promise.reject(err);
    });
}

/**
 * Provides a hook for an optional post-finalise batch function to be invoked after the batch has been successfully
 * finalised.
 * @param {Batch} batch
 * @param {StreamConsumerContext} context
 * @param {PostFinaliseBatch} [context.postFinaliseBatch] - an optional post-finalise batch function to execute
 * @returns {Promise.<Batch|undefined>}
 */
function postFinaliseBatch(batch, context) {
  const task = this;
  // const finalisingTask = task.parent;

  // Look up the actual function to be used to do the post-finalise batch logic (if any)
  const postFinaliseBatchFn = Settings.getPostFinaliseBatchFunction(context);

  if (!postFinaliseBatchFn) {
    if (context.traceEnabled) context.trace(`Skipping post-finalise of ${batch.describe()}, since no postFinaliseBatch function configured`);
    return Promise.resolve(undefined);
  }

  // Create a task to be used to execute and track the configured postFinaliseBatch function
  const fnName = isNotBlank(postFinaliseBatchFn.name) ? postFinaliseBatchFn.name : 'postFinaliseBatch';
  const postFinaliseBatchTask = task.createSubTask(fnName, postFinaliseBatchFn, batchSettings, finaliseTaskOpts);

  // Pre-finalise the batch
  const p = postFinaliseBatchTask.execute(batch, context);
  return whenDone(postFinaliseBatchTask, p, undefined, context).then(
    // return Promises.try(() => preFinaliseBatchTask.execute(batch, context)).then(
    () => {
      context.debug(`Post-finalised ${batch.describe()}`);
      return batch;
    },
    err => {
      context.error(`Failed to post-finalise ${batch.describe()}`, err);
      throw err;
    }
  );
}

function freezeProcessingTasks(batch, context) {
  try {
    // Abandon any "dead" unusable & unstarted tasks and sub-tasks that would otherwise block their fully finalised
    // and/or unusable root tasks from completing
    batch.abandonDeadProcessingTasks();

    // Mark any & all incomplete, over-attempted processing tasks as discarded
    const processingTasksDiscarded = batch.discardProcessingTasksIfOverAttempted(context);
    if (processingTasksDiscarded > 0) {
      context.debug(`Found & marked ${processingTasksDiscarded} over-attempted processing tasks as discarded`);
    }

    // Freeze all of the batch's processing tasks to prevent any further changes from the other promise that lost the timeout race
    batch.freezeProcessingTasks(context);

    return new Success(processingTasksDiscarded);

  } catch (err) {
    context.error(`Failed to find & mark over-attempted processing tasks as discarded`, err);
    return new Failure(err);
  }
}

function freezeFinalisingTasks(batch, context) {
  try {
    // Abandon any "dead" unusable & unstarted tasks and sub-tasks that would otherwise block their fully finalised
    // and/or unusable root tasks from completing
    batch.abandonDeadFinalisingTasks();

    // Mark any & all incomplete, over-attempted finalising tasks as discarded
    const finalisingTasksDiscarded = batch.discardFinalisingTasksIfOverAttempted(context);
    if (finalisingTasksDiscarded > 0) {
      context.debug(`Found & marked ${finalisingTasksDiscarded} over-attempted finalising tasks as discarded`);
    }

    // Freeze all of the batch's finalising tasks (i.e. discard rejected message tasks)
    batch.freezeFinalisingTasks(context);

    return new Success(finalisingTasksDiscarded);

  } catch (err) {
    context.error(`Failed to find & mark over-attempted finalising tasks as discarded`, err);
    return new Failure(err);
  }
}

/**
 * Summarizes and logs the final results of the given batch - converting lists of messages and records into counts.
 * @param {Batch} batch - the batch that was processed
 * @param {Error|undefined} [finalError] - the final error with which the batch was terminated (if unsuccessful)
 * @param {StreamConsumerContext} context - the context to use
 */
function logFinalResults(batch, finalError, context) {
  const summary = batch ? batch.summarizeFinalResults(finalError) : undefined;
  context.info(`Summarized final batch results: ${JSON.stringify(summary)}`);
}

function saveBatchState(batch, context) {
  const task = this;

  if (batch.messages.length <= 0 && batch.rejectedMessages.length <= 0 && batch.unusableRecords.length <= 0) {
    if (context.debugEnabled) context.debug(`No state to save for batch (${batch.shardOrEventID}), since ${batch.describeContents(true)}!`);
    return Promise.resolve(undefined);
  }

  // Get the configured saveBatchState function to be used to do the actual saving
  const saveBatchStateFn = Settings.getSaveBatchStateFunction(context);

  if (saveBatchStateFn) {
    // Create a task to be used to execute and track the configured saveBatchState function
    const fnName = isNotBlank(saveBatchStateFn.name) ? saveBatchStateFn.name : 'saveBatchState';
    const saveBatchStateTask = task.createSubTask(fnName, saveBatchStateFn, batchSettings, finaliseTaskOpts);

    // Trigger the configured saveBatchState function to do the actual saving
    const p = saveBatchStateTask.execute(batch, context);
    return whenDone(saveBatchStateTask, p, undefined, context);
  }
  else {
    const errMsg = `FATAL - Cannot save state of ${batch.describe(true)} WITHOUT a valid, configured saveBatchState function!`;
    context.error(errMsg);
    return Promise.reject(new FatalError(errMsg));
  }
}

/**
 * First finds all of the finalised, but rejected messages in the given batch's list of messages being processed and
 * then attempts to discard all of these rejected messages using the previously configured discard rejected message task
 * definition and tasks.
 * @param {Batch} batch - the batch being processed
 * @param {Cancellable|Object|undefined} [cancellable] - a cancellable object onto which to install cancel functionality
 * @param {StreamConsumerContext} context - the context to use
 * @returns {Promise.<*>} a promise that will complete when the configured discardRejectedMessage(s) function completes
 */
function discardAnyRejectedMessages(batch, cancellable, context) {
  return batch.discardRejectedMessages(cancellable, context);
}
