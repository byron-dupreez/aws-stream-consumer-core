'use strict';

const Settings = require('./settings');
const isDynamoDBStreamType = Settings.isDynamoDBStreamType;

const streamEvents = require('aws-core-utils/stream-events');

const lambdas = require('aws-core-utils/lambdas');

/**
 * Common tracked state and tracking-related utilities and functions to be used by aws-stream-consumer-core modules.
 * @module aws-stream-consumer-core/tracking
 * @author Byron du Preez
 */
exports._$_ = '_$_'; //IDE workaround

// Tracked state-related accessors
exports.setMessage = setMessage;
exports.setUnusableRecord = setUnusableRecord;
exports.setUserRecord = setUserRecord;
exports.setRecord = setRecord;

exports.getInvokedFunctionNameWithAliasOrVersion = getInvokedFunctionNameWithAliasOrVersion;
exports.getSourceStreamName = getSourceStreamName;
exports.getSourceStreamNames = getSourceStreamNames;

exports.deleteLegacyState = deleteLegacyState;

// Miscellaneous utilities
exports.toCountString = toCountString;

// Common task names
const TaskName = {
  // Discard unusable record task name
  discardUnusableRecord: 'discardUnusableRecord',

  // Discard rejected message task name
  discardRejectedMessage: 'discardRejectedMessage',
};
exports.TaskName = TaskName;

// Names of the tasks-by-name maps
const TaskMapNames = {
  // Names of process one & all tasks-by-name maps
  ones: 'ones',
  alls: 'alls',

  // Names of discard one tasks-by-name map
  discards: 'discards',

  // Names of phase tasks-by-name maps
  initiating: 'initiating',
  processing: 'processing',
  finalising: 'finalising'
};
exports.TaskMapNames = TaskMapNames;

// =====================================================================================================================
// Tracked state-related utilities
// =====================================================================================================================

/**
 * Sets the given message on its given state that is being tracked for the message.
 * @param {MessageState} state - the tracked state to be updated
 * @param {Message} message - the message to which this state belongs
 */
function setMessage(state, message) {
  Object.defineProperty(state, 'message', {value: message, writable: true, configurable: true, enumerable: false});
}

/**
 * Sets the given unusable record on its given state that is being tracked for the unusable record.
 * @param {UnusableRecordState} state - the tracked state to be updated
 * @param {UnusableRecord} unusableRecord - the unusable record to which this state belongs
 */
function setUnusableRecord(state, unusableRecord) {
  Object.defineProperty(state, 'unusableRecord',
    {value: unusableRecord, writable: true, configurable: true, enumerable: false});
}

/**
 * Sets the given user record on the given state that is being tracked for a message.
 * @param {MessageState} state - the tracked state to be updated
 * @param {UserRecord} userRecord - the user record to link to the state
 */
function setUserRecord(state, userRecord) {
  Object.defineProperty(state, 'userRecord',
    {value: userRecord, writable: true, configurable: true, enumerable: false});
}

/**
 * Sets the given record on the given state that is being tracked for a message or unusable record.
 * @param {MessageState|UnusableRecordState} state - the tracked state to be updated
 * @param {Record} record - the record to link to the state
 */
function setRecord(state, record) {
  Object.defineProperty(state, 'record', {value: record, writable: true, configurable: true, enumerable: false});
}

/**
 * Returns the Lambda's invoked function name with the invoked alias or version (if any).
 * @param {StreamConsumerContext} context - the context to use
 * @param {AWSContext|undefined} [awsContext] - the AWS Lambda context (defaults to context.awsContext if not defined)
 * @return {string} the invoked function name with the invoked alias or version (if any)
 */
function getInvokedFunctionNameWithAliasOrVersion(context, awsContext) {
  awsContext = awsContext || context.awsContext;
  return (context.invokedLambda && context.invokedLambda.invoked) ||
    (awsContext && lambdas.getInvokedFunctionNameWithAliasOrVersion(awsContext));
}

/**
 * Extracts the source stream names from the given Kinesis or DynamoDB stream event record.
 * For Kinesis: Extract the stream name from the record's eventSourceARN
 * For DynamoDB: Extract the table name and stream timestamp from the record's eventSourceARN and join them with a '/'
 * @param {AnyStreamEventRecord} record - the record from which to extract
 * @param {StreamProcessing} context - the context from which to fetch the function
 * @return {string} the source stream name
 */
function getSourceStreamName(record, context) {
  if (isDynamoDBStreamType(context)) {
    const [tableName, streamTimestamp] = streamEvents.getDynamoDBEventSourceTableNameAndStreamTimestamp(record);
    return tableName && streamTimestamp ? `${tableName}/${streamTimestamp}` : '';
  } else {
    return streamEvents.getKinesisEventSourceStreamName(record);
  }
}

/**
 * Extracts the source stream names from the given Kinesis or DynamoDB stream event records.
 * For Kinesis: Extract the stream name from each of the records' eventSourceARNs
 * For DynamoDB: Extract the table name and stream timestamp from each of the records' eventSourceARNs and join them with a '/'
 * @param {AnyStreamEventRecord[]} records - the records from which to extract
 * @param {StreamProcessing} context - the context from which to fetch the function
 * @return {string[]} the source stream names
 */
function getSourceStreamNames(records, context) {
  const getSourceStreamName = isDynamoDBStreamType(context) ?
    record => {
      const [tableName, streamTimestamp] = streamEvents.getDynamoDBEventSourceTableNameAndStreamTimestamp(record);
      return tableName && streamTimestamp ? `${tableName}/${streamTimestamp}` : '';
    } : streamEvents.getKinesisEventSourceStreamName;

  return records.map(getSourceStreamName);
}

/**
 * Deletes any legacy task tracking state attached directly to the tracked item from previous runs of older versions of
 * aws-stream-consumer.
 * @param {TrackedItem} trackedItem - the tracked item from which to remove its legacy tracked state
 * @param {StreamConsumerContext} context - the context to use to resolve the name of the legacy task tracking state property
 * @return {boolean} whether the task tracking state property was deleted or not
 */
function deleteLegacyState(trackedItem, context) {
  const taskTrackingName = Settings.getLegacyTaskTrackingName(context);
  return taskTrackingName && trackedItem ? delete trackedItem[taskTrackingName] : true;
}

// =====================================================================================================================
// Miscellaneous utilities
// =====================================================================================================================

/**
 * Returns a string containing the count of the number of items followed by the singular name for an item appropriately
 * pluralised according to the count.
 * @param {number|Array.<*>} countOrArray - a count or an array of items to count
 * @param {string} singularName - the singular name for the items being counted
 */
function toCountString(countOrArray, singularName) {
  const n = Array.isArray(countOrArray) ? countOrArray.length : countOrArray;
  return `${n} ${singularName}${n !== 1 ? 's' : ''}`;
}