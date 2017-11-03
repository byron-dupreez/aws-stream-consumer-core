'use strict';

const stages = require('aws-core-utils/stages');
const contexts = require('aws-core-utils/contexts');

const kinesisCache = require('aws-core-utils/kinesis-cache');
const lambdaCache = require('aws-core-utils/lambda-cache');

const tries = require('core-functions/tries');
const Try = tries.Try;

// const Promises = require('core-functions/promises');

const copying = require('core-functions/copying');
const copy = copying.copy;
const deep = {deep: true};

const errors = require('core-functions/errors');
const FatalError = errors.FatalError;

const Strings = require('core-functions/strings');
const isBlank = Strings.isBlank;
const isNotBlank = Strings.isNotBlank;
const trim = Strings.trim;

// Setting-related utilities
const Settings = require('./settings');

// Utilities for loading and saving tracked state of the current batch being processed from and to an external store
const persisting = require('./persisting');

const tracking = require('./tracking');

const esmCache = require('./esm-cache');

/**
 * Utilities for configuring stream processing, which configures and determines the processing behaviour of a stream
 * consumer.
 * @module aws-stream-consumer/stream-processing
 * @author Byron du Preez
 */
exports._ = '_'; //IDE workaround

// Stream processing configuration - configures and determines the processing behaviour of a stream consumer
exports.isStreamProcessingConfigured = isStreamProcessingConfigured;
exports.isStreamProcessingFullyConfigured = isStreamProcessingFullyConfigured;

exports.configureStreamProcessingWithSettings = configureStreamProcessingWithSettings;

exports.configureEventAwsContextAndStage = configureEventAwsContextAndStage;
exports.configureConsumerId = configureConsumerId;

exports.validateStreamProcessingConfiguration = validateStreamProcessingConfiguration;

// Default common Kinesis and DynamoDB stream processing functions
// ===============================================================

// Default loadBatchState function (re-exported for convenience)
exports.loadBatchStateFromDynamoDB = persisting.loadBatchStateFromDynamoDB;

// Default saveBatchState function (re-exported for convenience)
exports.saveBatchStateToDynamoDB = persisting.saveBatchStateToDynamoDB;

// Default discardUnusableRecord function
exports.discardUnusableRecordToDRQ = discardUnusableRecordToDRQ;

// Default discardRejectedMessage function
exports.discardRejectedMessageToDMQ = discardRejectedMessageToDMQ;

// Other default stream processing functions
exports.useStreamEventRecordAsMessage = useStreamEventRecordAsMessage;

exports.handleFatalError = handleFatalError;
exports.disableSourceStreamEventSourceMapping = disableSourceStreamEventSourceMapping;

// =====================================================================================================================
// Stream processing configuration - configures and determines the processing behaviour of a stream consumer
// =====================================================================================================================

/**
 * Returns true if stream processing is already configured on the given context; false otherwise.
 * @param {Object|StreamProcessing} context - the context to check
 * @returns {boolean} true if configured; false otherwise
 */
function isStreamProcessingConfigured(context) {
  return context && typeof context === 'object' && context.streamProcessing && typeof context.streamProcessing === 'object';
}

/**
 * Returns true if stream processing is fully configured on the given context; false otherwise.
 * @param {Object|StreamProcessing|StreamConsumerContext} context - the context to check
 * @returns {boolean} true if configured; false otherwise
 */
function isStreamProcessingFullyConfigured(context) {
  return isStreamProcessingConfigured(context) && context.event && context.awsContext;
}

/**
 * Configures the given context with the given stream processing settings, but only if stream processing is not already
 * configured on the given context OR if forceConfiguration is true, and with the given standard settings and options.
 *
 * Note that if either the given event or AWS context are undefined, then everything other than the event, AWS context &
 * stage will be configured. This missing configuration can be configured at a later point in your code by invoking
 * {@linkcode module:aws-stream-consumer/stream-processing#configureEventAwsContextAndStage}. This separation of configuration is
 * primarily useful for unit testing.
 *
 * @param {Object|StreamProcessing|StandardContext} context - the context onto which to configure the given stream processing settings and standard settings
 * @param {StreamProcessingSettings} settings - the stream processing settings to use
 * @param {StandardSettings|undefined} [standardSettings] - optional standard settings to use to configure dependencies
 * @param {StandardOptions|undefined} [standardOptions] - optional standard options to use to configure dependencies
 * @param {AWSEvent|undefined} [event] - the AWS event, which was passed to your lambda
 * @param {AWSContext|undefined} [awsContext] - the AWS context, which was passed to your lambda
 * @param {boolean|undefined} [forceConfiguration] - whether or not to force configuration of the given settings and
 * options, which will override any previously configured stream processing and stage handling settings on the given context
 * @param {function(context: StreamConsumerContext)|undefined} [validateConfiguration] - an optional function to use to further validate the configuration on the given context
 * @return {StreamProcessing} the context object configured with stream processing (either existing or new) and standard settings
 */
function configureStreamProcessingWithSettings(context, settings, standardSettings, standardOptions, event, awsContext,
  forceConfiguration, validateConfiguration) {

  // Configure all of the stream processing dependencies if not configured by configuring the given context as a
  // standard context with stage handling, logging, custom settings, an optional Kinesis instance and an optional
  // DynamoDB.DocumentClient instance using the given standard settings and standard options and ALSO optionally with
  // the current region, resolved stage and AWS context, if BOTH the optional given event and optional given AWS context
  // are defined
  contexts.configureStandardContext(context, standardSettings, standardOptions, event, awsContext, forceConfiguration);

  // If forceConfiguration is false check if the given context already has stream processing configured on it
  // and, if so, do nothing more and simply return the context as is (to prevent overriding an earlier configuration)
  if (!forceConfiguration && isStreamProcessingConfigured(context)) {
    // Configure the consumer id if possible
    configureConsumerId(context, awsContext);

    // Configure an AWS.Lambda instance for disabling of the Lambda's event source mapping if necessary
    lambdaCache.configureLambda(context);

    // Validate the stream processing configuration
    if (typeof validateConfiguration === 'function')
      validateConfiguration(context);
    else
      validateStreamProcessingConfiguration(context);

    return context;
  }

  // Configure stream processing with the given settings
  context.streamProcessing = settings;

  // Configure the consumer id if possible
  configureConsumerId(context, awsContext);

  // Configure an AWS.Lambda instance for disabling of the Lambda's event source mapping if necessary
  lambdaCache.configureLambda(context);

  // Validate the stream processing configuration
  if (typeof validateConfiguration === 'function')
    validateConfiguration(context);
  else
    validateStreamProcessingConfiguration(context);

  return context;
}

/**
 * Configures the given context with the given event, given AWS context and the resolved stage. In order to resolve the
 * stage, stage handling settings and logging must already be configured on the given context (see {@linkcode
 * stages#configureStageHandling} for details).
 * @param {StreamConsumerContext} context - the context to configure
 * @param {AWSEvent} event - the AWS event, which was passed to your lambda
 * @param {AWSContext} awsContext - the AWS context, which was passed to your lambda
 * @return {StreamConsumerContext} the given context configured with the given AWS event, AWS context & resolved stage
 * @throws {Error} if the resolved stage is blank
 */
function configureEventAwsContextAndStage(context, event, awsContext) {
  // Delegate most of the work to the `contexts` module's same named function
  contexts.configureEventAwsContextAndStage(context, event, awsContext);

  // Derive the consumerId to use and configure the given context with it
  configureConsumerId(context, awsContext);

  return context;
}

/**
 * Derives the consumer id for the given context and AWS context and then configures the given context with the derived
 * consumerId at `context.streamProcessing.consumerId` (if not already configured) .
 * @param {StreamConsumerContext} context - the context to configure
 * @param {AWSContext|undefined} [awsContext] - the AWS context that was passed to your lambda (defaults to `context.awsContext` if omitted)
 * @param {LambdaFunctionNameVersionAndAlias|undefined} [context.invokedLambda] - the name, version & alias of the invoked Lambda function
 * @param {string|undefined} [context.streamProcessing.consumerIdSuffix] - an optional suffix to append to the derived consumerId
 * @param {string|undefined} [context.streamProcessing.consumerId] - an optional, overriding, previously configured consumerId to use (if any)
 * @return {StreamConsumerContext} the given context configured with a consumer id
 */
function configureConsumerId(context, awsContext) {
  if (!context.streamProcessing.consumerId) {
    const invokedFunctionNameWithAliasOrVersion = tracking.getInvokedFunctionNameWithAliasOrVersion(context, awsContext);

    if (invokedFunctionNameWithAliasOrVersion) {
      // Resolve the suffix to use (if any)
      const suffix = trim(context.streamProcessing.consumerIdSuffix) || '';
      // Derive the consumer id from the invoked Lambda's function name (with alias or version, if any) & optional suffix
      const consumerId = suffix ? `${invokedFunctionNameWithAliasOrVersion}|${suffix}` : invokedFunctionNameWithAliasOrVersion;
      const msg = `Using consumerId (${consumerId})`;
      context.debug ? context.debug(msg) : console.log(msg);
      context.streamProcessing.consumerId = consumerId;
    }
  }
  return context;
}

function validateStreamProcessingConfiguration(context) {
  // const sequencingRequired = context.streamProcessing.sequencingRequired;
  const error = context.error || console.error;
  // const warn = context.warn || console.warn;

  if (context.awsContext) {
    if (isBlank(context.streamProcessing.consumerId)) {
      const errMsg = `FATAL - Cannot track any messages WITHOUT a consumerId. Fix defect and redeploy ASAP, since this issue is blocking all of your stream's shards!`;
      error(errMsg);
      throw new FatalError(errMsg);
    }
  }

  const resolveMessageIdsAndSeqNosFunction = Settings.getResolveMessageIdsAndSeqNosFunction(context);
  if (!resolveMessageIdsAndSeqNosFunction) {
    const errMsg = `FATAL - Cannot resolve identifiers & sequence numbers for any messages WITHOUT a resolveMessageIdsAndSeqNosFunction function. Fix by configuring one and redeploy ASAP, since this issue is blocking all of your stream's shards!`;
    error(errMsg);
    throw new FatalError(errMsg);
  }

  const extractMessagesFromRecordFunction = Settings.getExtractMessagesFromRecordFunction(context);
  if (!extractMessagesFromRecordFunction) {
    const errMsg = `FATAL - Cannot extract any messages from any stream event records WITHOUT an extractMessagesFromRecord function. Fix by configuring one and redeploy ASAP, since this issue is blocking all of your stream's shards!`;
    error(errMsg);
    throw new FatalError(errMsg);
  }

  if (!Settings.getLoadBatchStateFunction(context)) {
    const errMsg = `FATAL - Cannot load state for any messages WITHOUT a loadBatchState function. Fix by configuring one and redeploy ASAP, since this issue is blocking all of your stream's shards!`;
    error(errMsg);
    throw new FatalError(errMsg);
  }

  if (!Settings.getSaveBatchStateFunction(context)) {
    const errMsg = `FATAL - Cannot save state for any messages WITHOUT a saveBatchState function. Fix by configuring one and redeploy ASAP, since this issue is blocking all of your stream's shards!`;
    error(errMsg);
    throw new FatalError(errMsg);
  }

  if (!Settings.getDiscardUnusableRecordFunction(context)) {
    const errMsg = `FATAL - Cannot discard any unusable stream event record WITHOUT a discardUnusableRecord function. Fix by configuring one and redeploy ASAP, since this issue is blocking all of your stream's shards!`;
    error(errMsg);
    throw new FatalError(errMsg);
  }

  if (!Settings.getDiscardRejectedMessageFunction(context)) {
    const errMsg = `FATAL - Cannot discard any rejected message WITHOUT a discardRejectedMessage function. Fix by configuring one and redeploy ASAP, since this issue is blocking all of your stream's shards!`;
    error(errMsg);
    throw new FatalError(errMsg);
  }

}

// noinspection JSUnusedLocalSymbols
/**
 * A default `extractMessagesFromRecord` function that uses a copy of the given stream event record as the message
 * object & adds the message, rejected message or unusable record to the given batch.
 * @see ExtractMessagesFromRecord
 * @param {StreamEventRecord} record - a stream event record
 * @param {Batch} batch - the batch to which to add the extracted messages (or unusable records)
 * @param {undefined} [extractMessageFromRecord] - not used
 * @param {StreamProcessing} context - the context
 * @return {Promise.<MsgOrUnusableRec[]>} a promise of an array containing a message (copy of the record), a rejected message or an unusable record
 */
function useStreamEventRecordAsMessage(record, batch, extractMessageFromRecord, context) {
  if (!record || typeof record !== 'object') {
    context.warn(`Adding invalid record (${record}) as an unusable record`);
    return Promise.resolve([{unusableRec: batch.addUnusableRecord(record, undefined, `invalid record (${record})`, context)}]);
  }

  const messageOutcome = Try.try(() => copy(record, deep));

  // Add the "message" or unusable record to the batch
  return messageOutcome.map(
    message => {
      return [batch.addMessage(message, record, undefined, context)];
    },
    err => {
      return [{unusableRec: batch.addUnusableRecord(record, undefined, err.message, context)}];
    }
  ).toPromise();
}

/**
 * Discards the given unusable stream event record to the DRQ (i.e. Dead Record Queue).
 * Default implementation of a {@link DiscardUnusableRecord} function.
 * @param {UnusableRecord|Record} unusableRecord - the unusable record to discard
 * @param {Batch} batch - the batch being processed
 * @param {function(unusableRecord: UnusableRecord, batch: Batch, deadRecordQueueName: string, context: StreamConsumerContext): KinesisPutRecordRequest} toDRQPutRequest - the function to use to convert the dead record into a Kinesis PutRecord request
 * @param {StreamProcessing} context - the context to use
 * @return {Promise} a promise that will complete when the unusable record is discarded
 */
function discardUnusableRecordToDRQ(unusableRecord, batch, toDRQPutRequest, context) {
  if (!unusableRecord) {
    return Promise.resolve(undefined);
  }

  const eventID = unusableRecord.eventID;

  if (!batch || !batch.isKeyValid()) {
    const errMsg = `Missing valid batch key (${JSON.stringify(batch && batch.key)}) needed to discard unusable record (${eventID})`;
    context.warn(errMsg);
    return Promise.reject(new Error(errMsg));
  }

  const kinesis = getKinesis(context);

  // Get the stage-qualified version of the DRQ stream name
  const unqualifiedDeadRecordQueueName = context.streamProcessing.deadRecordQueueName;
  const deadRecordQueueName = stages.toStageQualifiedStreamName(unqualifiedDeadRecordQueueName, context.stage, context);

  // Discard the unusable record
  const request = toDRQPutRequest(unusableRecord, batch, deadRecordQueueName, context);

  return kinesis.putRecord(request).promise().then(
    result => {
      context.debug(`Discarded unusable record (${eventID}) to DRQ (${deadRecordQueueName})`);
      return result;
    },
    err => {
      context.error(`Failed to discard unusable record (${eventID}) to DRQ (${deadRecordQueueName})`, err);
      throw err;
    });
}

/**
 * Routes the given rejected message to the DMQ (i.e. Dead Message Queue).
 * Default implementation of a {@link DiscardRejectedMessage} function.
 * @param {Message} rejectedMessage - the rejected message to discard
 * @param {Batch} batch - the batch being processed
 * @param {function(rejectedMessage: Message, batch: Batch, deadMessageQueueName: string, context: StreamConsumerContext): KinesisPutRecordRequest} toDMQPutRequest - the function to use to convert the dead message into a Kinesis PutRecord request
 * @param {StreamProcessing} context the context to use
 * @return {Promise}
 */
function discardRejectedMessageToDMQ(rejectedMessage, batch, toDMQPutRequest, context) {
  if (!rejectedMessage) {
    return Promise.resolve(undefined);
  }

  const msgState = batch && batch.states.get(rejectedMessage);
  const id = msgState && msgState.id;

  if (!batch || !batch.isKeyValid()) {
    const error = new Error(`Missing valid batch key (${JSON.stringify(batch && batch.key)}) needed to discard rejected message (${id})`);
    context.error(error);
    return Promise.reject(error);
  }

  const kinesis = getKinesis(context);

  // Get the stage-qualified version of the DRQ stream name
  const unqualifiedDeadMessageQueueName = context.streamProcessing.deadMessageQueueName;
  const deadMessageQueueName = stages.toStageQualifiedStreamName(unqualifiedDeadMessageQueueName, context.stage, context);

  // Discard the rejected message to the DMQ
  const request = toDMQPutRequest(rejectedMessage, batch, deadMessageQueueName, context);
  return kinesis.putRecord(request).promise().then(
    result => {
      context.debug(`Discarded rejected message (${id}) to DMQ (${deadMessageQueueName})`);
      return result;
    })
    .catch(err => {
      context.error(`Failed to discard rejected message (${id}) to DMQ (${deadMessageQueueName})`, err);
      throw err;
    });
}

function getKinesis(context) {
  if (!context.kinesis) {
    // Configure a default Kinesis instance on context.kinesis if not already configured
    const kinesisOptions = require('./default-options.json').kinesisOptions;

    context.warn(`An AWS Kinesis instance was not configured on context.kinesis yet - configuring an instance with default options (${JSON.stringify(kinesisOptions)}). Preferably configure this beforehand by setting kinesisOptions in your stream consumer configuration settings/options`);
    kinesisCache.configureKinesis(context, kinesisOptions);
  }
  return context.kinesis;
}

/**
 * Handles the given fatal error by attempting to disable the event source mapping between this Lambda & its source
 * Kinesis/DynamoDB stream.
 * @param {FatalError} fatalError - the fatal error that occurred
 * @param {Batch} batch - the batch being processed
 * @param {StreamConsumerContext|LambdaAware} context - the context to use
 * @return {Promise.<FatalError>} a promise that will always fail with the given fatal error
 */
function handleFatalError(fatalError, batch, context) {
  const functionName = tracking.getInvokedFunctionNameWithAliasOrVersion(context);

  context.warn(`Disabling event source mapping for function (${functionName}) in response to: `, fatalError);

  const rethrowCause = () => {
    throw fatalError;// re-throw the original error
  };

  return disableSourceStreamEventSourceMapping(batch, context).then(rethrowCause, rethrowCause);
}

/**
 * Attempts to disable this Lambda's event source mapping to its source stream, which will disable this Lambda's trigger
 * and prevent it from processing any more messages until the issue is resolved and the trigger is manually re-enabled.
 * @param {Batch} batch
 * @param {LambdaAware|StreamConsumerContext} context
 * @returns {Promise.<*>|*}
 */
function disableSourceStreamEventSourceMapping(batch, context) {
  const functionName = tracking.getInvokedFunctionNameWithAliasOrVersion(context);

  const batchKey = batch.key;
  const sourceStreamName = (batchKey && batchKey.components && batchKey.components.streamName) ||
    (tracking.getSourceStreamNames(batch.records, context).find(n => isNotBlank(n)));

  if (isBlank(functionName) || isBlank(sourceStreamName)) {
    const errMsg = `FATAL - Cannot disable source stream event source mapping WITHOUT both function (${functionName}) & stream (${sourceStreamName})`;
    context.error(errMsg);
    return Promise.reject(new FatalError(errMsg));
  }

  const avoidEsmCache = context.streamProcessing && context.streamProcessing.avoidEsmCache;

  return esmCache.disableEventSourceMapping(functionName, sourceStreamName, avoidEsmCache, context);
}