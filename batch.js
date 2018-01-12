/**
 * A Batch class that represents the current batch of records received in the Kinesis or DynamoDB stream event.
 * @module aws-stream-consumer-core/batch
 * @author Byron du Preez
 */
'use strict';

const any = require('core-functions/any');
const notDefined = any.notDefined;

const Strings = require('core-functions/strings');
const stringify = Strings.stringify;
const isBlank = Strings.isBlank;
const isNotBlank = Strings.isNotBlank;

// const copying = require('core-functions/copying');
// const copy = copying.copy;
// const deep = {deep: true};

const errors = require('core-functions/errors');
const FatalError = errors.FatalError;

const tries = require('core-functions/tries');
// const Success = tries.Success;
const Failure = tries.Failure;

const Promises = require('core-functions/promises');

const settings = require('./settings');
const isDynamoDBStreamType = settings.isDynamoDBStreamType;

const TaskDef = require('task-utils/task-defs');
const taskUtils = require('task-utils');
const getTasksAndSubTasks = taskUtils.getTasksAndSubTasks;
const isAnyTaskNotFullyFinalised = taskUtils.isAnyTaskNotFullyFinalised;
const ReturnMode = taskUtils.ReturnMode;
const Task = require("task-utils/tasks.js");

const tracking = require('./tracking');
const TaskName = tracking.TaskName;
const toCountString = tracking.toCountString;

const identify = require('./identify');
// const sequencing = require('./sequencing');
const taskDefSettings = require('./taskdef-settings');

// const stages = require('aws-core-utils/stages');
const streamEvents = require('aws-core-utils/stream-events');

const groupBy = require('lodash.groupby');
const uniq = require('lodash.uniq');

const push = Array.prototype.push;

const flattenOpts = {skipSimplifyOutcomes: false};

/**
 * A Batch class represents the current batch of records received in the Kinesis or DynamoDB stream event and keeps
 * track of any and all messages extracted from these records and any and all unusable records encountered and the state
 * of the current batch.
 * @property {Records} records - the entire batch of records received
 * @property {BatchKey|undefined} [key] - the key of the batch (if resolved); otherwise undefined
 * @property {string|undefined} [keyString] - a string version of the key of the batch (if resolved); otherwise Batch.MISSING_KEY_STRING
 * @property {string|undefined} [streamConsumerId] - the first part (i.e. stream name, consumer id & stage) of the key of the batch (if resolved); otherwise undefined
 * @property {string|undefined} [shardOrEventID] - the second part (i.e. shard ID or event ID) of the key of the batch (if resolved); otherwise undefined
 * @property {Message[]|undefined} [messages] - a list of zero or more successfully extracted messages
 * @property {UnusableRecord[]|undefined} [unusableRecords] - a list of zero or more unusable records encountered
 * @property {Message[]|undefined} [firstMessagesToProcess] - a list of the first messages to process (populated after messages have been sequenced)
 * @property {BatchTaskDefs|undefined} [taskDefs] - the task definitions of this batch
 * @property {Map.<TrackedItem, AnyTrackedState>} states - a map of the state being tracked for each of the items being processed keyed by the tracked item
 */
class Batch {

  /**
   * Constructs a new batch for the current batch of records received in the Kinesis or DynamoDB stream event.
   * @param {Record[]} records - the current batch of records received in the Kinesis or DynamoDB stream event
   * @param {ProcessOneTaskDef[]|undefined} [processOneTaskDefs] - a list of zero or more "process one at a time" task definitions that will be used to generate the tasks to be executed on each message independently
   * @param {ProcessAllTaskDef[]|undefined} [processAllTaskDefs] - a list of zero or more "process all at once" task definitions that will be used to generate the tasks to be executed on all messages in the batch collectively
   * @param {StreamConsumerContext} context - the context to use
   */
  constructor(records, processOneTaskDefs, processAllTaskDefs, context) {
    //super();
    const batchKey = Batch.resolveBatchKey(records, context);
    Object.defineProperty(this, 'key', {value: batchKey, enumerable: true, writable: false, configurable: false});

    const batchKeyString = batchKey ? `(${batchKey.streamConsumerId}, ${batchKey.shardOrEventID})` :
      Batch.MISSING_KEY_STRING;
    Object.defineProperty(this, 'keyString', {
      value: batchKeyString, enumerable: false, writable: false, configurable: false
    });

    const streamConsumerId = batchKey && batchKey.streamConsumerId;
    Object.defineProperty(this, 'streamConsumerId', {
      value: streamConsumerId, enumerable: false, writable: false, configurable: false
    });

    const shardOrEventID = batchKey && batchKey.shardOrEventID;
    Object.defineProperty(this, 'shardOrEventID', {
      value: shardOrEventID, enumerable: false, writable: false, configurable: false
    });

    Object.defineProperty(this, 'records', {value: records, enumerable: true, writable: false, configurable: false});
    Object.defineProperty(this, 'messages', {value: [], enumerable: true, writable: false, configurable: false});
    Object.defineProperty(this, 'rejectedMessages', {
      value: [],
      enumerable: true,
      writable: false,
      configurable: false
    });
    Object.defineProperty(this, 'unusableRecords', {value: [], enumerable: true, writable: false, configurable: false});
    Object.defineProperty(this, 'undiscardedUnusableRecordsBefore', {
      value: [], enumerable: false, writable: true, configurable: true
    });
    Object.defineProperty(this, 'undiscardedUnusableRecords', {
      value: [], enumerable: false, writable: true, configurable: true
    });

    Object.defineProperty(this, 'firstMessagesToProcess', {
      value: undefined, enumerable: false, writable: true, configurable: true
    });
    Object.defineProperty(this, 'undiscardedRejectedMessages', {
      value: [], enumerable: false, writable: true, configurable: true
    });
    Object.defineProperty(this, 'incompleteMessages', {
      value: [], enumerable: false, writable: true, configurable: true
    });

    Object.defineProperty(this, 'taskDefs', {value: {}, enumerable: false, writable: true, configurable: true});
    const taskDefs = this.taskDefs;

    Object.defineProperty(taskDefs, 'processOneTaskDefs', {
      value: processOneTaskDefs ? processOneTaskDefs : [], enumerable: false, writable: false, configurable: false
    });
    Object.defineProperty(taskDefs, 'processAllTaskDefs', {
      value: processAllTaskDefs ? processAllTaskDefs : [], enumerable: false, writable: false, configurable: false
    });

    Object.defineProperty(taskDefs, 'discardUnusableRecordTaskDef', {
      value: undefined, enumerable: false, writable: true, configurable: true
    });
    Object.defineProperty(taskDefs, 'discardRejectedMessageTaskDef', {
      value: undefined, enumerable: false, writable: true, configurable: true
    });

    // Define all of the discard one task definitions for the batch
    this.defineDiscardTasks(context);

    Object.defineProperty(taskDefs, 'initiateTaskDef', {
      value: undefined, enumerable: false, writable: true, configurable: true
    });
    Object.defineProperty(taskDefs, 'processTaskDef', {
      value: undefined, enumerable: false, writable: true, configurable: true
    });
    Object.defineProperty(taskDefs, 'finaliseTaskDef', {
      value: undefined, enumerable: false, writable: true, configurable: true
    });

    // Attach a map of all tracked states keyed by tracked object to this batch
    // /** @type {WeakMap.<TrackedItem, AnyTrackedState>} */
    // Object.defineProperty(this, 'states', {
    //   value: new WeakMap(), enumerable: false, writable: true, configurable: true
    // });
    /** @type {Map.<TrackedItem, AnyTrackedState>} */
    Object.defineProperty(this, 'states', {
      value: new Map(), enumerable: false, writable: true, configurable: true
    });

    /** @type {boolean|undefined} */
    Object.defineProperty(this, 'previouslySaved', {
      value: undefined, enumerable: false, writable: true, configurable: true
    });

    // Initialise the tracked state for this batch itself to an empty object
    this.states.set(this, {});

    // Add the batch to the context for convenient access ~ context.batch = this;
    Object.defineProperty(context, 'batch', {value: this, enumerable: false, writable: true, configurable: true});
  }

  /**
   * Returns a concatenated list of all of the batch's unique messages and rejected messages.
   * @return {Message[]} all of the batch's unique messages and rejected messages
   */
  allMessages() {
    return uniq(this.messages.concat(this.rejectedMessages));
  }

  /**
   * Returns the current state (if any) being tracked for the given item.
   * @param {TrackedItem} trackedItem
   * @returns {TrackedItemState} the state being tracked for the item
   */
  getState(trackedItem) {
    return this.states.get(trackedItem);
  }

  /**
   * Returns the current state (if any) being tracked for the given item or creates, caches & returns a new state object
   * for the given item.
   * @param {TrackedItem} trackedItem
   * @returns {TrackedItemState} the state being tracked for the item
   */
  getOrSetState(trackedItem) {
    let state = this.states.get(trackedItem);
    if (!state) {
      state = {};
      this.states.set(trackedItem, state);
    }
    return state;
  }

  /**
   * Resolves the key to be used for a batch of records. Returns undefined if no key could be resolved.
   * @param {Records} records - the entire batch of records
   * @param {StreamConsumerContext|StageAware|Logger} context - the context to use
   * @returns {BatchKey|undefined} the batch key to use (if resolved); otherwise undefined
   */
  static resolveBatchKey(records, context) {
    // Find the "first" usable record that has both an eventID and an eventSourceARN
    const record1 = records.find(r => r && typeof r === 'object' && isNotBlank(r.eventID) && isNotBlank(r.eventSourceARN));

    if (!record1) {
      context.error(`Failed to resolve a batch key to use, since failed to find any record with an eventID and eventSourceARN`);
      return undefined;
    }

    const dynamoDBStreamType = isDynamoDBStreamType(context);

    // Resolve the hash key to use:
    // For Kinesis: Extract the stream name from the "first" record's eventSourceARN
    // For DynamoDB: Extract the table name and stream timestamp from the "first" record's eventSourceARN and join them with a '/'
    const streamName = tracking.getSourceStreamName(record1, context);
    const consumerId = context.streamProcessing.consumerId;
    const streamConsumerId = dynamoDBStreamType ? `D|${streamName}|${consumerId}` : `K|${streamName}|${consumerId}`;

    // Resolve the range key to use:
    // For Kinesis, extract and use the shard id from the "first" record's eventID (if isBatchKeyedOnEventID is false)
    // For DynamoDB, use the eventID of the "first" usable DynamoDB record as a workaround, since it is NOT currently
    // possible to determine the DynamoDB stream's shard id from the event - have to instead use the "first" event ID as
    // an identifier for the batch of records. The only risk with this workaround is that the "first" record could be
    // trimmed at the TRIM_HORIZON and then we would not be able to load the batch's previous state
    const batchKeyedOnEventID = dynamoDBStreamType || context.streamProcessing.batchKeyedOnEventID;
    const shardId = !dynamoDBStreamType ? streamEvents.getKinesisShardId(record1) : undefined;
    const eventID1 = record1.eventID;
    const shardOrEventID = batchKeyedOnEventID ? `E|${eventID1}` : `S|${shardId}`;

    const batchKey = {
      streamConsumerId: streamConsumerId,
      shardOrEventID: shardOrEventID
    };

    const components = {
      streamName: streamName,
      consumerId: consumerId
    };
    if (shardId) components.shardId = shardId;
    if (eventID1) components.eventID = eventID1;
    Object.defineProperty(batchKey, 'components', {
        value: components, enumerable: false, writable: true, configurable: true
      }
    );

    return batchKey;
  }

  isKeyValid() {
    return this.key && isNotBlank(this.streamConsumerId) && isNotBlank(this.shardOrEventID);
  }

  /**
   * Adds the given userRecord (if valid) or the given record (if valid) or an empty object (if neither valid) as an
   * unusable record to this batch by:
   * - Caching a new tracked state object property with the given reason for the unusable record on this batch;
   * - Linking the given record (if any) and given user record (if any) to the unusable record via its cached state;
   * - Generating MD5 message digests for the given record & user record;
   * - Resolving the event id & event sequence numbers of the given record & user record;
   * - Updating the cached state with the generated MD5s and the resolved event ids & sequence numbers.
   * @param {Record|*} record - the AWS stream event record (or any other garbage sent)
   * @param {UserRecord|*|undefined} [userRecord] - the de-aggregated user record from which the message was extracted (if applicable)
   * @param {string|undefined} [reasonUnusable] - an optional reason for why the record was deemed unusable
   * @param {StreamConsumerContext} context - the context to use
   * @returns {UnusableRecord} the given userRecord (if valid) or the given record (if valid) or an empty object (if neither valid)
   */
  addUnusableRecord(record, userRecord, reasonUnusable, context) {
    const validRecord = typeof record === 'object' || notDefined(record) ? record : undefined;
    const validUserRecord = typeof userRecord === 'object' || notDefined(userRecord) ? userRecord : undefined;

    // Use the given user record (if valid) or the record (if valid) or an empty object (if neither valid) as the "unusable record"
    const unusableRecord = validUserRecord || validRecord || {};

    // Ensure tracked state exists for the record and fill in some of its details
    const state = this.getOrSetState(unusableRecord);

    // Link the new unusable record to its own state
    tracking.setUnusableRecord(state, unusableRecord);

    // Give each unusable record a link to the de-aggregated user record that it came from (if applicable) via its state
    if (userRecord) {
      tracking.setUserRecord(state, userRecord);
    }

    // Link the original record to the new unusable record's state
    tracking.setRecord(state, record);

    // Generate & set the unusable record's MD5 message digests on its state (if NOT previously generated)
    if (!state.md5s) {
      identify.generateAndSetMD5s(undefined, record, userRecord, state, context);
    }

    // Resolve & set the message's record's eventID, eventSeqNo & eventSubSeqNo on its state (if NOT previously resolved)
    if (isBlank(state.eventID) || isBlank(state.eventSeqNo)) {
      identify.resolveAndSetEventIdAndSeqNos(record, userRecord, state, context);
    }

    // Mark as unusable
    // state.unusable = true;

    if (reasonUnusable) {
      state.reasonUnusable = reasonUnusable;
    }

    // Add the unusable record to this batch's list of unusable records
    this.unusableRecords.push(unusableRecord);

    return unusableRecord;
  }

  /**
   * Adds the given message as itself (if valid & complete) or as a rejected message (if valid, but incomplete) or as an
   * unusable record (if invalid) to this batch by:
   * - Caching a new tracked state object property for the message on this batch;
   * - Linking the given record (if any) and given user record (if any) to the message via its cached state;
   * - Generating MD5 message digests for the given message;
   * - Resolving the event id & event sequence numbers of the given message;
   * - Resolving the ids, keys, sequence numbers & event id of the given message;
   * - Updating the cached state with the generated MD5s and the resolved ids, keys & sequence numbers.
   * @param {Object|Message} message - the message to update
   * @param {Record|undefined} [record] - the record from which the message was extracted
   * @param {UserRecord|undefined} [userRecord] - the de-aggregated user record from which the message was extracted (if applicable)
   * @param {StreamConsumerContext} context - the context to use
   * @returns {MsgOrUnusableRec} the message, rejected message or unusable record
   */
  addMessage(message, record, userRecord, context) {
    const validMessage = typeof message === 'object' || notDefined(message) ? message : undefined;

    if (!validMessage) {
      context.warn(`Adding invalid message (${message}) as an unusable record`);
      return {unusableRec: this.addUnusableRecord(record, userRecord, `invalid message (${message})`, context)};
    }

    // Ensure tracked state exists for the message
    const state = this.getOrSetState(message);

    // Link the message to its own state
    tracking.setMessage(state, message);

    // Give each message a link to the de-aggregated user record that it came from (if applicable) via its state
    if (userRecord) {
      tracking.setUserRecord(state, userRecord);
    }

    // Give each message a link to the stream event record that it came from (if provided) via its state
    tracking.setRecord(state, record);

    try {
      // Resolve the message's id(s), key(s) and sequence number(s) & cache them on its state
      identify.resolveAndSetMessageIdsAndSeqNos(message, state, context);

      // Add the message to this batch's list of messages
      this.messages.push(message);

      return {msg: message};

    } catch (err) {
      if (err instanceof FatalError) {
        throw err;
      }

      // Mark as rejected
      // state.rejected = true;

      // Add a reason for rejecting this message
      state.reasonRejected = isNotBlank(err.reason) ? err.reason : err.message;

      // Add the message to this batch's list of rejected messages
      this.rejectedMessages.push(message);

      return {rejectedMsg: message};
    }
  }

  /**
   * Defines the discard task definitions for this batch to be subsequently used to generate the discard unusable
   * records and discard rejected messages tasks. Updates the given batch with these new task definitions.
   * @param {StreamConsumerContext} context - the context to use
   */
  defineDiscardTasks(context) {
    // Get the configured discardUnusableRecord function to be used to do the actual discarding
    const discardUnusableRecord = context.streamProcessing.discardUnusableRecord;

    // Get the configured discardRejectedMessage function to be used to do the actual discarding
    const discardRejectedMessage = context.streamProcessing.discardRejectedMessage;

    // Validate that have at least one of the discard unusable record functions
    if (typeof discardUnusableRecord !== 'function') {
      const errMsg = `FATAL - The stream consumer must be configured with a discardUnusableRecord function!`;
      context.error(errMsg);
      throw new FatalError(errMsg);
    }

    // Validate that have at least one of the discard rejected message functions
    if (typeof discardRejectedMessage !== 'function') {
      const errMsg = `FATAL - The stream consumer must be configured with a discardRejectedMessage function!`;
      context.error(errMsg);
      throw new FatalError(errMsg);
    }

    const taskDefs = this.taskDefs;

    // Define the discard unusable record task definition to be subsequently used to handle & track the state of discarded records
    taskDefs.discardUnusableRecordTaskDef = TaskDef.defineTask(TaskName.discardUnusableRecord, discardUnusableRecord,
      taskDefSettings.unusableRecordSettings);

    // Define the discard rejected message task definition to be subsequently used to handle & track the state of discarded messages
    taskDefs.discardRejectedMessageTaskDef = TaskDef.defineTask(TaskName.discardRejectedMessage, discardRejectedMessage,
      taskDefSettings.rejectedMessageSettings);
  }

  /**
   * Revives all of the tasks of this batch and of all of this batch's messages and unusable records by re-incarnating
   * any prior task-like versions of these tasks as new task objects according to the configured task definitions.
   * @param {StreamConsumerContext} context - the context to use
   */
  reviveTasks(context) {
    const taskFactory = context.taskFactory;
    // const sequencingRequired = context.streamProcessing.sequencingRequired;
    const states = this.states;

    const createAllOpts = {onlyRecreateExisting: false, returnMode: ReturnMode.NORMAL};
    const onlyReviveOpts = {onlyRecreateExisting: true, returnMode: ReturnMode.NORMAL};

    // 1. Revive all of the batch's messages' tasks (if any)
    let messagesProcessAllTasks = [];
    const taskDefs = this.taskDefs;

    this.messages.forEach(message => {
      const state = states.get(message);

      // 1.1. Replace all of the old "process one" task-like objects on each message with new tasks created from the
      //      batch's processOneTaskDefs and update these new tasks with information from the old ones
      const processOneTasksByName = state.ones ? state.ones :
        taskDefs.processOneTaskDefs.length > 0 ? this.getOrSetProcessOneTasks(message) : undefined;
      if (processOneTasksByName) {
        taskUtils.reviveTasks(processOneTasksByName, taskDefs.processOneTaskDefs, taskFactory, createAllOpts);
      }

      // 1.2. Replace all of the old processAll task-like objects on each message with new tasks created from the
      //      batch's processAllTaskDefs and update these new tasks with information from the old ones
      const processAllTasksByName = state.alls ? state.alls :
        taskDefs.processAllTaskDefs.length > 0 ? this.getOrSetProcessAllTasks(message) : undefined;
      if (processAllTasksByName) {
        const revived = taskUtils.reviveTasks(processAllTasksByName, taskDefs.processAllTaskDefs, taskFactory, createAllOpts);
        const processAllTasks = revived[0];
        messagesProcessAllTasks = messagesProcessAllTasks.concat(processAllTasks);
      }

      // 1.3. Replace all of the old discardOne task-like objects on each message with new tasks created from the
      //      batch's discardRejectedMessageTaskDef and update these new tasks with information from the old ones
      const discardOneTasksByName = state.discards;
      if (discardOneTasksByName) {
        taskUtils.reviveTasks(discardOneTasksByName, [taskDefs.discardRejectedMessageTaskDef], taskFactory, onlyReviveOpts);
      }
    });

    // 2. Revive all of the batch's rejected messages' tasks (if any)
    // let messagesProcessAllTasks = [];
    this.rejectedMessages.forEach(rejectedMessage => {
      const state = states.get(rejectedMessage);

      // 2.1. Replace all of the old "process one" task-like objects on each message with new tasks created from the
      //      batch's processOneTaskDefs and update these new tasks with information from the old ones
      const processOneTasksByName = state.ones ? state.ones :
        taskDefs.processOneTaskDefs.length > 0 ? this.getOrSetProcessOneTasks(rejectedMessage) : undefined;
      if (processOneTasksByName) {
        taskUtils.reviveTasks(processOneTasksByName, taskDefs.processOneTaskDefs, taskFactory, onlyReviveOpts);
      }

      // 2.2. Replace all of the old processAll task-like objects on each message with new tasks created from the
      //      batch's processAllTaskDefs and update these new tasks with information from the old ones
      const processAllTasksByName = state.alls ? state.alls :
        taskDefs.processAllTaskDefs.length > 0 ? this.getOrSetProcessAllTasks(rejectedMessage) : undefined;
      if (processAllTasksByName) {
        const revived = taskUtils.reviveTasks(processAllTasksByName, taskDefs.processAllTaskDefs, taskFactory, onlyReviveOpts);
        const processAllTasks = revived[0];
        messagesProcessAllTasks = messagesProcessAllTasks.concat(processAllTasks);
      }

      // 2.3. Replace all of the old discardOne task-like objects on each message with new tasks created from the
      //      batch's discardRejectedMessageTaskDef and update these new tasks with information from the old ones
      const discardOneTasksByName = state.discards;
      if (discardOneTasksByName) {
        taskUtils.reviveTasks(discardOneTasksByName, [taskDefs.discardRejectedMessageTaskDef], taskFactory, onlyReviveOpts);
      }
    });

    // 3. Revive all of the batch's unusable records' discard one tasks (if any)
    this.unusableRecords.forEach(unusableRecord => {
      const state = states.get(unusableRecord);

      // 3.1. Replace all of the old discardOne task-like objects on each unusable record with new tasks created from
      //      the batch's discardUnusableRecordTaskDef and updates these new tasks with information from the old ones
      const discardOneTasksByName = state.discards ? state.discards : this.getOrSetDiscardOneTasks(unusableRecord);
      if (discardOneTasksByName) {
        taskUtils.reviveTasks(discardOneTasksByName, [taskDefs.discardUnusableRecordTaskDef], taskFactory, createAllOpts);
      }
    });

    // 4. Revive all of the batch's own tasks (if any):
    // const state = states.get(this);

    // 4.1. Replace all of the old "process all" task-like objects on the batch itself with new tasks created from the
    //      batch's processAllTaskDefs and update these new tasks with information from the old ones
    // const processAllTasksByName = state && state.alls ? state.alls :
    const processAllTasksByName = this.messages.length > 0 && taskDefs.processAllTaskDefs.length > 0 ?
      this.getOrSetProcessAllTasks(this) : undefined;

    if (processAllTasksByName) {
      const revived = taskUtils.reviveTasks(processAllTasksByName, taskDefs.processAllTaskDefs, taskFactory, createAllOpts);
      const processAllTasks = revived[0];

      // 3.1.1. Link the messages' process all tasks as slave tasks to the batch's corresponding process all tasks
      const messagesProcessAllTasksByName = groupBy(messagesProcessAllTasks, t => t.name);
      processAllTasks.forEach(masterTask => {
        const slaveTasks = messagesProcessAllTasksByName[masterTask.name];
        if (slaveTasks && slaveTasks.length > 0) {
          masterTask.setSlaveTasks(slaveTasks);
        }
      });
    }
  }

  /**
   * Discards any and all of this batch's unusable records, which have not been successfully discarded yet.
   * @param {Cancellable|Object|undefined} [cancellable] - a cancellable object onto which to install cancel functionality
   * @param {StreamConsumerContext} context - the context to use
   * @returns {Promise.<Outcomes>} a promise of a list of discard outcomes for the undiscarded, unusable records (if any)
   */
  discardUnusableRecords(cancellable, context) {
    const self = this;
    const r = this.records.length;
    const u = this.unusableRecords.length;
    const rs = `${r} record${r !== 1 ? 's' : ''}`;

    if (u <= 0) {
      // No records need to be discarded
      context.debug(`No unusable records to discard out of ${rs} for batch (${this.shardOrEventID})`);
      return Promise.resolve([]);
    }

    const us = `${u} unusable record${u !== 1 ? 's' : ''}`;
    const usOfRs = `${u} unusable of ${rs}`;

    // Ensure have a configured task definition to use to do the actual discarding
    if (!this.taskDefs.discardUnusableRecordTaskDef) { // should have caught this issue much earlier, but just in case ...
      const errMsg = `FATAL - Cannot discard ${usOfRs} for batch (${this.shardOrEventID}) without a valid, configured discard unusable record task definition - forced to trigger a replay!`;
      context.error(errMsg);
      return Promise.reject(new FatalError(errMsg));
    }

    // Ensure that we have a discard unusable record task for each unusable record (warn and fix if not) AND also
    // determine which of the unusable records still need to be discarded
    const states = this.states;
    const taskFactory = context.taskFactory;
    const taskOpts = {returnMode: ReturnMode.NORMAL};
    const undiscardedUnusableRecords = this.unusableRecords.filter(unusableRecord => {
      let discardTask = this.getDiscardUnusableRecordTask(unusableRecord);
      if (!discardTask) {
        context.warn(`LOGIC ERROR - Missing expected discard unusable record task for record (${states.get(unusableRecord).eventID}) for batch (${this.shardOrEventID})`);
        discardTask = taskFactory.createTask(this.taskDefs.discardUnusableRecordTaskDef, taskOpts);
        this.setDiscardUnusableRecordTask(unusableRecord, discardTask);
        return true;
      }
      return !discardTask.isFullyFinalised();
    });
    this.undiscardedUnusableRecordsBefore = undiscardedUnusableRecords;

    const n = undiscardedUnusableRecords.length;
    const nsOfUs = `${n} undiscarded of ${us}`;
    if (n <= 0) {
      context.debug(`Skipping discard of ${nsOfUs} for batch (${this.shardOrEventID}), since ALL of its records' discard tasks are already fully finalised`);
      return Promise.resolve([]); // Nothing to do, since all of the records have already been discarded/finalised
    }

    // Discard the unusable records that still need to be discarded
    const promises = undiscardedUnusableRecords.map(unusableRecord => {
      const discardTask = this.getDiscardUnusableRecordTask(unusableRecord);
      const p = Promises.try(() => discardTask.execute(unusableRecord, self, context));
      return whenDone(discardTask, p, cancellable, context)
        .catch(err => {
          context.error(`Failed to discard unusable record (${unusableRecord.eventID})`, err);
          throw err;
        });
    });

    return Promises.every(promises, cancellable, context).then(
      results => {
        const failures = results.filter(o => o instanceof Failure);
        if (failures.length > 0) {
          context.error(`Failed to discard ${failures.length} of ${nsOfUs} for batch (${this.shardOrEventID}) - failures: ${stringify(failures.map(f => `${f.error}`))}`);
        } else {
          context.info(`Discarded ${nsOfUs} for batch (${this.shardOrEventID})`);
        }
        return results;
      },
      err => {
        context.error(`Unexpected error during discard of ${nsOfUs}`, err);
        throw err;
      }
    );
  }

  /**
   * Abandons any "dead" unusable & unstarted processing phase tasks and sub-tasks that would otherwise block their
   * fully finalised and/or unusable root tasks from completing
   * @returns {Task[]} the abandoned tasks
   */
  abandonDeadProcessingTasks() {
    const abandonedTasks = [];

    this.messages.forEach(msg => {
      const state = this.states.get(msg);
      if (state) {
        // First check all of the message's process one tasks
        push.apply(abandonedTasks, abandonDeadTasks(state.ones));

        // Second check all of the message's process all tasks
        push.apply(abandonedTasks, abandonDeadTasks(state.alls));
      }
    });

    this.unusableRecords.forEach(uRec => {
      // Third check all of the unusable records's discard one tasks
      const state = this.states.get(uRec);
      if (state) push.apply(abandonedTasks, abandonDeadTasks(state.discards));
    });

    // Finally check all of the batch's process all tasks
    const state = this.states.get(this);
    if (state) push.apply(abandonedTasks, abandonDeadTasks(state.alls));

    return abandonedTasks;
  }

  /**
   * Abandons any "dead" unusable & unstarted finalising phase tasks and sub-tasks that would otherwise block their
   * fully finalised and/or unusable root tasks from completing
   * @returns {Task[]} the abandoned tasks
   */
  abandonDeadFinalisingTasks() {
    const abandonedTasks = [];
    this.undiscardedRejectedMessages.forEach(msg => {
      // Check all of the rejected message's discard one tasks
      const state = this.states.get(msg);
      push.apply(abandonedTasks, abandonDeadTasks(state && state.discards));
    });
    return abandonedTasks;
  }

  /**
   * Finds and marks every over-attempted processing task as discarded. An "over-attempted" processing task is any
   * incomplete processing task, which has no sub-tasks or ONLY fully finalised sub-tasks, that has reached or exceeded
   * the maximum number of attempts allowed.
   * @param {StreamConsumerContext} context - the context to use
   * @returns {number} the number of over-attempted tasks marked as discarded
   */
  discardProcessingTasksIfOverAttempted(context) {
    const maxNumberOfAttempts = context.streamProcessing.maxNumberOfAttempts;
    let overAttempted = 0;

    // Mark any over-attempted tasks as discarded
    function discardIfOverAttempted(tasksByName, count) {
      if (tasksByName) {
        taskUtils.getTasks(tasksByName).forEach(task => {
          const n = task.discardIfOverAttempted(maxNumberOfAttempts, true);
          if (count) overAttempted += n;
        });
      }
    }

    // Discard all incomplete tasks on each message that have reached or exceeded the maximum number of allowed attempts
    this.messages.forEach(msg => {
      const state = this.states.get(msg);

      // Discard the message's over-attempted processOne tasks (if any)
      discardIfOverAttempted(state.ones, true);

      // Discard the message's over-attempted processAll tasks (if any)
      discardIfOverAttempted(state.alls, true);
    });

    // Discard all discard tasks on each rejected message that have reached or exceeded the maximum number of allowed attempts
    this.rejectedMessages.forEach(rejectedMsg => {
      // Discard the rejected message's over-attempted discardOne tasks (if any)
      discardIfOverAttempted(this.states.get(rejectedMsg).discards, true);
    });

    // Discard all discard tasks on each unusable record that have reached or exceeded the maximum number of allowed attempts
    this.unusableRecords.forEach(uRec => {
      // Discard the unusable record's over-attempted discardOne tasks (if any)
      discardIfOverAttempted(this.states.get(uRec).discards, true);
    });

    // Find and mark the batch's over-attempted processAll tasks (if any) as discarded
    discardIfOverAttempted(this.states.get(this).alls, false);

    return overAttempted;
  }

  /**
   * Finds and marks every over-attempted finalising task as discarded. An "over-attempted" finalising task is any
   * incomplete finalising task, which has no sub-tasks or ONLY fully finalised sub-tasks, that has reached or exceeded
   * the maximum number of attempts allowed.
   * @param {StreamConsumerContext} context - the context to use
   * @returns {number} the number of over-attempted tasks marked as discarded
   */
  discardFinalisingTasksIfOverAttempted(context) {
    const maxNumberOfAttempts = context.streamProcessing.maxNumberOfAttempts;
    let overAttempted = 0;

    // Mark any over-attempted tasks as discarded
    function discardIfOverAttempted(tasksByName) {
      if (tasksByName) {
        taskUtils.getTasks(tasksByName).forEach(task => {
          overAttempted += task.discardIfOverAttempted(maxNumberOfAttempts, true);
        });
      }
    }

    // Discard any & all incomplete "discard one" (i.e. discardRejectedMessage) tasks on each message that have reached
    // or exceeded the maximum number of allowed attempts
    // this.messages.forEach(message => {
    this.undiscardedRejectedMessages.forEach(msg => {
      // Discard the message's over-attempted discardOne tasks (if any)
      const state = this.states.get(msg);
      discardIfOverAttempted(state.discards);
    });

    return overAttempted;
  }

  /**
   * Freezes all of this batch's processing tasks (if any), all of its messages' processing tasks and all of its
   * unusable records' discard tasks to prevent any further changes to these processing phase tasks (e.g. by the other
   * promise that lost the processing timeout race).
   * @param {StreamConsumerContext} context - the context with stream consumer configuration to use
   */
  freezeProcessingTasks(context) {
    context.debug(`FREEZING all processing tasks on batch (${this.shardOrEventID}) and on its ${this.messages.length} message(s) & ${this.unusableRecords.length} unusable record(s)`);

    // First freeze all the batch's process all tasks (if any)
    // const processAllTasksByName = this.getProcessAllTasks(this);
    const processAllTasks = taskUtils.getTasks(this.states.get(this).alls);
    processAllTasks.forEach(task => task.freeze());

    // Then freeze any and all of the batch's messages' process one and process all tasks
    this.messages.forEach(message => {
      const state = this.states.get(message);

      // Freeze any and all of the current message's process one tasks
      // const processOneTasksByName = this.getProcessOneTasks(message);
      const processOneTasks = taskUtils.getTasks(state.ones);
      processOneTasks.forEach(task => task.freeze());

      // Freeze any and all of the current message's process all tasks (if not already frozen via the batch's master tasks)
      // const processAllTasksByName = this.getProcessAllTasks(message);
      const processAllTasks = taskUtils.getTasks(state.alls);
      processAllTasks.forEach(task => {
        if (!task.isFrozen()) {
          task.freeze()
        }
      });
    });

    // Next freeze all of the batch's unusable records' discard one unusable record (if any)
    this.unusableRecords.forEach(unusableRecord => {
      // Freeze the unusable record's discardUnusableRecord task (if any)
      const discardOneTask = this.getDiscardUnusableRecordTask(unusableRecord);
      if (discardOneTask && !discardOneTask.isFrozen()) {
        discardOneTask.freeze();
      }
    });
  }

  /**
   * Freezes all of this batch's finalising phase tasks, i.e. any and all of its messages' discard tasks, to prevent any
   * further changes to these finalising tasks (e.g. by the other promise that lost the finalising timeout race).
   * @param {StreamConsumerContext} context - the context with stream consumer configuration to use
   */
  freezeFinalisingTasks(context) {
    context.debug(`FREEZING all finalising tasks on batch (${this.shardOrEventID}) and on its ${this.messages.length} message(s)`);

    // First freeze any and all of the batch's messages' discard rejected message tasks (if any)
    // this.messages.forEach(message => {
    this.undiscardedRejectedMessages.forEach(message => {
      // Freeze the message's discardRejectedMessage task (if any)
      const discardOneTask = this.getDiscardRejectedMessageTask(message);
      if (discardOneTask && !discardOneTask.isFrozen()) {
        discardOneTask.freeze();
      }
    });
  }

  /**
   * Times out any and all of the process all master tasks on this batch that have not finalised yet and also times out
   * any and all of this batch's messages' process one tasks and process all tasks that have not finalised yet.
   * @param {Error|undefined} [timeoutError] - the optional error that describes or that triggered the timeout
   */
  timeoutProcessingTasks(timeoutError) {
    const timeoutOpts = {overrideCompleted: false, overrideUnstarted: false, reverseAttempt: true};

    // Timeout any and all of this batch's process all tasks that have not finalised yet
    const processAllTasksByName = this.getProcessAllTasks(this);
    const processAllTasks = processAllTasksByName ? taskUtils.getTasks(processAllTasksByName) : [];
    processAllTasks.forEach(task => {
      task.timeout(timeoutError, timeoutOpts, true);
    });

    // Timeout any and all of this batch's messages' process one and process all tasks that have not finalised yet
    this.messages.forEach(message => {
      const state = this.states.get(message);

      // Timeout any and all of the message's process one tasks that have not finalised yet
      const processOneTasksByName = state && state.ones;
      const processOneTasks = processOneTasksByName ? taskUtils.getTasks(processOneTasksByName) : [];
      processOneTasks.forEach(task => {
        task.timeout(timeoutError, timeoutOpts, true);
      });

      // Timeout any and all of the message's process all tasks that have not finalised yet
      const processAllTasksByName = state && state.alls;
      const processAllTasks = processAllTasksByName ? taskUtils.getTasks(processAllTasksByName) : [];
      processAllTasks.forEach(task => {
        task.timeout(timeoutError, timeoutOpts, true);
      });
    });

    // Timeout any and all of this batch's unusable records' discard one unusable record tasks that have not finalised yet
    this.unusableRecords.forEach(unusableRecord => {
      // Timeout the unusable record's discard one unusable record task (if any & if not finalised yet)
      const discardOneTask = this.getDiscardUnusableRecordTask(unusableRecord);
      if (discardOneTask) {
        discardOneTask.timeout(timeoutError, timeoutOpts, true);
      }
    });
  }

  /**
   * Moves the given rejected message from this batch's list of active messages to its list of rejected messages.
   * @param {Message} rejectedMsg - the rejected message
   */
  moveMessageToRejected(rejectedMsg) {
    if (!this.rejectedMessages.includes(rejectedMsg)) {
      this.rejectedMessages.push(rejectedMsg);
    }
    const pos = this.messages.indexOf(rejectedMsg);
    if (pos !== -1) {
      this.messages.splice(pos, 1);
    }
  }

  /**
   * Discards any and all rejected messages, which have not been successfully discarded yet.
   * @param {Cancellable|Object|undefined} [cancellable] - a cancellable object onto which to install cancel functionality
   * @param {StreamConsumerContext} context - the context to use
   * @returns {*}
   */
  discardRejectedMessages(cancellable, context) {
    const self = this;
    const taskFactory = context.taskFactory;
    const taskOpts = {returnMode: ReturnMode.PROMISE};
    const states = this.states;

    try {
      const messages = this.messages;
      const rejectedMessages = this.rejectedMessages;

      const m = messages.length;
      const ms = `${m} message${m !== 1 ? 's' : ''}`;

      let r = rejectedMessages.length;
      let rs = `${r} rejected message${r !== 1 ? 's' : ''}`;

      if (m <= 0 && r <= 0) {
        context.debug(`No rejected messages to discard for batch (${this.shardOrEventID}), since ${ms} & ${rs}!`);
        return Promise.resolve([]);
      }

      // Move all messages that have been fully finalised, but that also contain at least one rejected task from the
      // batch's [usable] messages list to its rejected messages list
      const rejectedMsgs = this.messages.filter(message => this.isMessageFullyFinalisedButRejected(message));

      rejectedMsgs.forEach(rejectedMsg => this.moveMessageToRejected(rejectedMsg));

      r = rejectedMessages.length;
      rs = `${r} rejected message${r !== 1 ? 's' : ''}`;

      if (r <= 0) {
        // No messages need to be discarded
        context.debug(`No rejected messages to discard out of ${ms} & ${rs} for batch (${this.shardOrEventID})`);
        return Promise.resolve([]);
      }

      const discardRejectedMessageTaskDef = this.taskDefs.discardRejectedMessageTaskDef;

      // Determine which of the rejected messages still need to be discarded
      const undiscardedRejectedMessages = rejectedMessages.filter(rejectedMsg => {
        const state = states.get(rejectedMsg);
        // let discardTask = this.getDiscardRejectedMessageTask(rejectedMessage);
        let discardTask = state && state.discards && state.discards.discardRejectedMessage;

        if (!discardTask) {
          context.trace(`Creating new discard rejected message task for batch (${this.shardOrEventID}) message (${state.msgDesc})`);
          discardTask = taskFactory.createTask(discardRejectedMessageTaskDef, taskOpts);
          this.setDiscardRejectedMessageTask(rejectedMsg, discardTask);
          return true;
        }

        context.trace(`Found existing discard rejected message task for batch (${this.shardOrEventID}) message (${state.msgDesc})`);
        return discardTask && !discardTask.isFullyFinalised();
      });
      this.undiscardedRejectedMessages = undiscardedRejectedMessages;

      const u = undiscardedRejectedMessages.length;

      if (u <= 0) {
        if (context.debugEnabled) context.debug(`Skipping discard of ${u} undiscarded of ${rs} for batch (${this.shardOrEventID}), since ALL of its messages' discard rejected messages tasks are already fully finalised`);
        return Promise.resolve([]); // Nothing to do, since all of the messages have already been discarded/finalised
      }

      // Discard the rejected messages that still need to be discarded
      if (context.debugEnabled) context.debug(`Discarding ${u} undiscarded of ${rs} for batch (${this.shardOrEventID}) ...`);
      const promises = undiscardedRejectedMessages.map(rejectedMessage => {
        const task = this.getDiscardRejectedMessageTask(rejectedMessage);
        const promise = task.execute(rejectedMessage, self, context);
        return whenDone(task, promise, cancellable, context);
      });

      return Promises.every(promises, cancellable, context).then(
        outcomes => {
          const failures = outcomes.filter(o => o instanceof Failure);
          if (failures.length > 0) {
            context.error(`Failed to discard ${failures.length} of ${u} undiscarded of ${rs}`);
          }
          return outcomes;
        },
        err => {
          context.error(`Unexpected error during discard of ${u} undiscarded of ${rs}`, err);
          throw err;
        }
      );
    } catch (err) {
      context.error(`Failed to discard rejected message(s)`, err);
      return Promise.reject(err);
    }
  }

  /**
   * Times out any and all of the finalising tasks on this batch that have not finalised yet and also times out
   * any and all of this batch's messages' finalising tasks that have not finalised yet.
   * @param {Error|undefined} [timeoutError] - the optional error that describes or that triggered the timeout
   */
  timeoutFinalisingTasks(timeoutError) {
    const timeoutOpts = {overrideCompleted: false, overrideUnstarted: false, reverseAttempt: true};

    // Timeout any and all of this batch's discard one rejected message tasks that have not finalised yet
    this.undiscardedRejectedMessages.forEach(rejectedMessage => {
      // Timeout the rejected message's discard one rejected message task (if any & if not finalised yet)
      const discardOneTask = this.getDiscardRejectedMessageTask(rejectedMessage);
      if (discardOneTask) {
        discardOneTask.timeout(timeoutError, timeoutOpts, true);
      }
    });
  }

  /**
   * Returns true if this batch is full finalised; otherwise returns false.
   * @returns {boolean} true if incomplete; false otherwise
   */
  isFullyFinalised() {
    // Check if any of this batch's "process all" tasks are not fully finalised yet
    if (isAnyTaskNotFullyFinalised(this.getProcessAllTasks(this))) {
      return false;
    }

    // Check if any of this batch's messages' tasks are not fully finalised yet
    for (let message of this.messages) {
      // Check if any of the message's "process one", "discard one" or "process all" tasks are not finalised yet
      if (this.isMessageIncomplete(message)) {
        return false;
      }
    }

    // Check if any of this batch's unusable records' tasks are not fully finalised yet
    for (let unusableRecord of this.unusableRecords) {
      // Check if any of the unusable record's "discard one" tasks are not finalised yet
      if (isAnyTaskNotFullyFinalised(this.getDiscardOneTasks(unusableRecord))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Returns true if any of the given message's tasks have not fully finalised yet; otherwise returns false.
   * @param {Message} message - the message to check
   * @return {boolean} true if the message's tasks are still not fully finalised; false otherwise
   */
  isMessageIncomplete(message) {
    const state = this.states.get(message);
    return !state || isAnyTaskNotFullyFinalised(state.ones) || isAnyTaskNotFullyFinalised(state.alls) ||
      isAnyTaskNotFullyFinalised(state.discards);
  }

  /**
   * Returns true if any of the given rejected message's tasks have not fully finalised yet; otherwise returns false.
   * @param {Message} rejectedMessage - the rejected message to check
   * @return {boolean} true if the rejected message's tasks are still not fully finalised; false otherwise
   */
  isRejectedMessageIncomplete(rejectedMessage) {
    return this.isMessageIncomplete(rejectedMessage);
    // return isAnyTaskNotFullyFinalised(this.getDiscardOneTasks(rejectedMessage));
  }

  /**
   * Returns true if any of the given unusable record's tasks have not fully finalised yet; otherwise returns false.
   * @param {UnusableRecord} unusableRecord - the unusable record to check
   * @return {boolean} true if the unusable record's tasks are still not fully finalised; false otherwise
   */
  isUnusableRecordIncomplete(unusableRecord) {
    return isAnyTaskNotFullyFinalised(this.getDiscardOneTasks(unusableRecord));
  }

  isMessageFullyFinalisedButRejected(message) {
    const state = this.states.get(message);
    const tasksAndSubTasks = state ? getTasksAndSubTasks(state.ones).concat(getTasksAndSubTasks(state.alls)) : [];
    return tasksAndSubTasks.every(t => t.finalised) && tasksAndSubTasks.some(t => t.rejected);
  }

  findReasonRejected(message) {
    const state = this.states.get(message);
    if (isNotBlank(state.reasonRejected)) {
      return state.reasonRejected;
    }
    const tasksAndSubTasks = state ? getTasksAndSubTasks(state.ones).concat(getTasksAndSubTasks(state.alls)) : [];
    const rejectedTask = tasksAndSubTasks.find(task => task.rejected && isNotBlank(task.state.reason));
    return rejectedTask && rejectedTask.state.reason;
  }


  /**
   * Returns a description of the batch's current progress as:
   * "{i} incomplete messages of {m} messages and {u} undiscarded records of {r} records".
   * @return {string} a description of the progress
   */
  assessProgress() {
    const incompleteMessages = this.messages.filter(m => this.isMessageIncomplete(m));
    this.incompleteMessages = incompleteMessages;

    const undiscardedUnusableRecords = this.unusableRecords.filter(r => this.isUnusableRecordIncomplete(r));
    this.undiscardedUnusableRecords = undiscardedUnusableRecords;

    const undiscardedRejectedMessages = this.rejectedMessages.filter(m => this.isRejectedMessageIncomplete(m));
    this.undiscardedRejectedMessages = undiscardedRejectedMessages;

    const m = this.messages.length;
    const ms = toCountString(m, 'msg');

    const i = incompleteMessages ? incompleteMessages.length : 0;

    const rm = this.rejectedMessages.length;
    const rms = toCountString(rm, 'rejected msg');
    const urm = undiscardedRejectedMessages.length;

    const u = this.unusableRecords.length;
    const uu = undiscardedUnusableRecords.length;

    const r = this.records.length;
    const rs = toCountString(r, 'rec');

    return `${i} incomplete of ${ms}, ${rm > 0 || urm > 0 ? `${urm} undiscarded of ` : ''}${rms} & ${u > 0 || uu > 0 ? `${uu} undiscarded of `: ''}${u} unusable of ${rs}`;
  }

  /**
   * Summarizes the final results of this batch - converting lists of messages and records into counts.
   * @param {Error|undefined} [finalError] - the final error with which this batch was terminated (if unsuccessful)
   * @returns {SummarizedFinalBatchResults|undefined} the summarized final results of this batch
   */
  summarizeFinalResults(finalError) {

    function toLength(list) {
      return list ? list.length : undefined;
    }

    const batchState = this.states.get(this);

    return {
      key: this.key,
      records: this.records.length,
      messages: this.messages.length,
      unusableRecords: this.unusableRecords.length,
      undiscardedUnusableRecordsBefore: toLength(this.undiscardedUnusableRecordsBefore),
      undiscardedUnusableRecords: toLength(this.undiscardedUnusableRecords),
      firstMessagesToProcess: toLength(this.firstMessagesToProcess),
      rejectedMessages: this.rejectedMessages.length,
      undiscardedRejectedMessages: toLength(this.undiscardedRejectedMessages),
      incompleteMessages: toLength(this.incompleteMessages),
      initiating: batchState.initiating,
      processing: batchState.processing,
      finalising: batchState.finalising,
      finalError: finalError,
      partial: !!finalError,
      fullyFinalised: this.isFullyFinalised()
    };
  }

  // noinspection JSUnusedGlobalSymbols
  getRecord(trackedItem) {
    const state = this.states.get(trackedItem);
    return state ? state.record : undefined;
  }

  // noinspection JSUnusedGlobalSymbols
  setRecord(trackedItem, record) {
    const state = this.getOrSetState(trackedItem);
    tracking.setRecord(state, record);
  }

  // noinspection JSUnusedGlobalSymbols
  getUserRecord(trackedItem) {
    const state = this.states.get(trackedItem);
    return state ? state.userRecord : undefined;
  }

  // noinspection JSUnusedGlobalSymbols
  setUserRecord(trackedItem, userRecord) {
    const state = this.getOrSetState(trackedItem);
    tracking.setUserRecord(state, userRecord);
  }

  // =====================================================================================================================
  // Process one and process all tasks by name accessors
  // =====================================================================================================================

  /** Returns or creates the map of process one tasks by task name */
  getOrSetProcessOneTasks(message) {
    const state = this.getOrSetState(message);
    if (!state.ones) {
      state.ones = {};
    }
    return state.ones;
  }

  /**
   * Returns or creates the map of process all tasks by task name
   * @param {Message|Batch|undefined} [messageOrBatch] - the optional message or batch for which to get process all tasks
   * @returns {TasksByName} the map of process all tasks by name for the given message or batch (or this batch)
   */
  getOrSetProcessAllTasks(messageOrBatch) {
    const state = this.getOrSetState(messageOrBatch || this);
    if (!state.alls) {
      state.alls = {};
    }
    return state.alls;
  }

  /** Returns the map of process one tasks by task name */
  getProcessOneTasks(message) {
    const state = this.states.get(message);
    return state ? state.ones : undefined;
  }

  /**
   * Returns the map of process all tasks by task name for the given message or batch (if specified) or for this batch
   * (if not).
   * @param {Message|Batch|undefined} [messageOrBatch] - the optional message or batch for which to get process all tasks (defaults to this batch if omitted)
   * @returns {TasksByName} the map of process all tasks by name for the given message or batch (or this batch)
   */
  getProcessAllTasks(messageOrBatch) {
    const state = this.states.get(messageOrBatch || this);
    return state ? state.alls : undefined;
  }

  /** Returns the named process one task */
  getProcessOneTask(message, taskName) {
    const state = this.states.get(message);
    return state && state.ones ? state.ones[taskName] : undefined;
  }

  // noinspection JSUnusedGlobalSymbols
  setProcessOneTask(message, taskName, task) {
    const ones = this.getOrSetProcessOneTasks(message);
    ones[taskName] = task;
    return message;
  }

  /**
   * Returns the named process all task.
   * @param {Message|Batch|undefined} [messageOrBatch] - the optional message or batch for which to get process all tasks (defaults to this batch if omitted)
   * @param {string} taskName
   * @returns {TasksByName} the map of process all tasks by name for the given message or batch (or this batch)
   */
  getProcessAllTask(messageOrBatch, taskName) {
    const state = this.states.get(messageOrBatch || this);
    return state && state.alls ? state.alls[taskName] : undefined;
  }

  // noinspection JSUnusedGlobalSymbols
  setProcessAllTask(messageOrBatch, taskName, task) {
    const alls = this.getOrSetProcessAllTasks(messageOrBatch);
    alls[taskName] = task;
    return messageOrBatch;
  }

  // =====================================================================================================================
  // Discard one tasks by name accessors
  // =====================================================================================================================

  /** Returns or creates the map of discard one tasks by task name */
  getOrSetDiscardOneTasks(recordOrMessage) {
    const state = this.getOrSetState(recordOrMessage);
    if (!state.discards) {
      state.discards = {};
    }
    return state.discards;
  }

  /** Returns the map of discard one tasks by task name */
  getDiscardOneTasks(recordOrMessage) {
    const state = this.states.get(recordOrMessage);
    return state ? state.discards : undefined;
  }

  // noinspection JSUnusedGlobalSymbols
  /** Returns the named discard one task */
  getDiscardOneTask(recordOrMessage, taskName) {
    const state = this.states.get(recordOrMessage);
    return state && state.discards ? state.discards[taskName] : undefined;
  }

  // =====================================================================================================================
  // Specific discard tasks accessors
  // =====================================================================================================================

  getDiscardUnusableRecordTask(record) {
    const state = this.states.get(record);
    return state && state.discards ? state.discards.discardUnusableRecord : undefined;
  }

  setDiscardUnusableRecordTask(record, task) {
    const discards = this.getOrSetDiscardOneTasks(record);
    discards.discardUnusableRecord = task;
    return record;
  }

  getDiscardRejectedMessageTask(message) {
    const state = this.states.get(message);
    return state && state.discards ? state.discards.discardRejectedMessage : undefined;
  }

  setDiscardRejectedMessageTask(message, task) {
    const discards = this.getOrSetDiscardOneTasks(message);
    discards.discardRejectedMessage = task;
    return message;
  }

  // =====================================================================================================================
  // Phase tasks by name accessors
  // =====================================================================================================================

  /**
   * Returns the existing map or sets & returns a new map of initiating tasks by task name.
   * @returns {TasksByName} the existing or new map of initiating tasks by task name
   */
  getOrSetInitiatingTasks() {
    const state = this.getOrSetState(this);
    if (!state.initiating) {
      state.initiating = {};
    }
    return state.initiating;
  }

  /**
   * Returns the existing map or sets & returns a new map of processing tasks by task name.
   * @returns {TasksByName} the existing or new map of processing tasks by task name
   */
  getOrSetProcessingTasks() {
    const state = this.getOrSetState(this);
    if (!state.processing) {
      state.processing = {};
    }
    return state.processing;
  }

  /**
   * Returns the existing map or sets & returns a new map of finalising tasks by task name.
   * @returns {TasksByName} the existing or new map of finalising tasks by task name
   */
  getOrSetFinalisingTasks() {
    const state = this.getOrSetState(this);
    if (!state.finalising) {
      state.finalising = {};
    }
    return state.finalising;
  }

  // noinspection JSUnusedGlobalSymbols
  /**
   * Returns the map of initiating tasks by task name (if any).
   * @param {TrackedItem|undefined} [trackedItem] - the tracked item for which to get initiating tasks (defaults to this batch if omitted)
   * @returns {TasksByName|undefined} the map of initiating tasks by task name or undefined (if none)
   */
  getInitiatingTasks(trackedItem) {
    const state = this.states.get(trackedItem || this);
    return state ? state.initiating : undefined;
  }

  // noinspection JSUnusedGlobalSymbols
  /**
   * Returns the map of processing tasks by task name (if any).
   * @param {TrackedItem|undefined} [trackedItem] - the tracked item for which to get processing tasks (defaults to this batch if omitted)
   * @returns {TasksByName|undefined} the map of processing tasks by task name or undefined (if none)
   */
  getProcessingTasks(trackedItem) {
    const state = this.states.get(trackedItem || this);
    return state ? state.processing : undefined;
  }

  // noinspection JSUnusedGlobalSymbols
  /**
   * Returns the map of finalising tasks by task name (if any).
   * @param {TrackedItem|undefined} [trackedItem] - the tracked item for which to get finalising tasks (defaults to this batch if omitted)
   * @returns {TasksByName|undefined} the map of finalising tasks by task name or undefined (if none)
   */
  getFinalisingTasks(trackedItem) {
    const state = this.states.get(trackedItem || this);
    return state ? state.finalising : undefined;
  }

  /**
   * Generates a description of this batch's contents.
   * @param {boolean|undefined} [concise] - whether to return a more concise or more verbose description of this batch's contents
   * @return {string} a description of this batch's contents
   */
  describeContents(concise) {
    const m = this.messages.length;
    const rm = (this.rejectedMessages && this.rejectedMessages.length) || 0;
    const u = this.unusableRecords.length;
    const r = this.records.length;
    if (concise) {
      // More concise version
      return `${m} msg${m !== 1 ? 's' : ''}, ${rm} rejected msg${rm !== 1 ? 's' : ''} & ${u} unusable of ${r} rec${r !== 1 ? 's' : ''}`;
    }
    // More verbose version
    const urm = (this.undiscardedRejectedMessages && this.undiscardedRejectedMessages.length) || 0;
    const uu = (this.undiscardedUnusableRecords && this.undiscardedUnusableRecords.length) || 0;
    return `${m} msg${m !== 1 ? 's' : ''}, ${rm > 0 || urm > 0 ? `${urm} undiscarded of `: ''}${rm} rejected msg${rm !== 1 ? 's' : ''} & ${u > 0 || uu > 0 ? `${uu} undiscarded of ` : ''}${u} unusable of ${r} rec${r !== 1 ? 's' : ''}`;
  }

  /**
   * Generates a description of this batch.
   * @param {boolean|undefined} [concise] - whether to return a more concise or more verbose description of this batch's contents
   * @param {boolean|undefined} [capitalize] - whether or not to capitalize the first letter of the description or not
   * @return {string} a description of this batch
   */
  describe(concise, capitalize) {
    return `${capitalize ? 'Batch' : 'batch'} (${this.shardOrEventID}) with ${this.describeContents(concise)}`
  }

  // =====================================================================================================================
  // Identifiers
  // =====================================================================================================================

  // getEventID(messageOrRecord) {
  //   const state = this.states.get(messageOrRecord);
  //   return state ? state.eventID : undefined;
  // }
  //
  // getMessageIds(message) {
  //   const messageState = this.states.get(message);
  //   return messageState ? messageState.ids : undefined;
  // }
  //
  // getMessageId(message) {
  //   const messageState = this.states.get(message);
  //   return messageState ? messageState.id : undefined;
  // }
  //
  // getMessageKeys(message) {
  //   const messageState = this.states.get(message);
  //   return messageState ? messageState.keys : undefined;
  // }
  //
  // getMessageKey(message) {
  //   const messageState = this.states.get(message);
  //   return messageState ? messageState.key : undefined;
  // }
  //
  // getMessageSeqNos(message) {
  //   const messageState = this.states.get(message);
  //   return messageState ? messageState.seqNos : undefined;
  // }
  //
  // getPrevMessage(message) {
  //   const messageState = this.states.get(message);
  //   return messageState ? messageState.prevMessage : undefined;
  // }
  //
  // getNextMessage(message) {
  //   const messageState = this.states.get(message);
  //   return messageState ? messageState.nextMessage : undefined;
  // }
  //
  // getMessageKeySeqNoId(message) {
  //   const messageState = this.states.get(message);
  //   return messageState ? getMessageStateKeySeqNoId(messageState) : undefined;
  // }

}

Batch.MISSING_KEY_STRING = undefined;

function abandonDeadTasks(tasksByName) {
  if (tasksByName) {
    const tasksAndSubTasks = getTasksAndSubTasks(tasksByName);
    // Find any "dead" unusable & unstarted tasks and sub-tasks that would otherwise block their fully finalised and/or
    // unusable root tasks from completing
    const unusableTasks = tasksAndSubTasks.filter(t => t.unusable && t.unstarted);
    const deadTasks = unusableTasks.filter(t => {
      const rootTask = Task.getRootTask(t);
      return rootTask ? rootTask.isFullyFinalisedOrUnusable() : true;
    });

    // Abandon any "dead" unusable & unstarted tasks and sub-tasks that would otherwise block their fully finalised
    // and/or unusable root tasks from completing
    return deadTasks.filter(t => {
      if (!t.finalised) {
        const reason = `Abandoned dead ${t.unusable ? 'unusable' : 'usable'} ${t.parent ? 'sub-' : ''}task, since its root task is fully finalised and/or unusable`;
        t.abandon(reason, undefined, true);
        return true;
      }
      return false;
    });
  }
  return [];
}

// function getMessageStateKeySeqNoId(messageState) {
//   const key = messageState.key;
//   const seqNo = messageState.seqNo;
//   const id = messageState.id;
//   return `${key}, ${seqNo}${id !== `${key}|${seqNo}` ? `, ${id}` : ''}`;
// }

module.exports = Batch;

function whenDone(task, promise, cancellable, context) {
  return task.donePromise || Promises.flatten(promise, cancellable, flattenOpts, context);
}