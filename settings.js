'use strict';

const Strings = require('core-functions/strings');
const isNotBlank = Strings.isNotBlank;
const trim = Strings.trim;

// Constants
const PROPERTY_NAME_SEPARATOR = ',';

/**
 * Common setting-related utilities and functions to be used by aws-stream-consumer-core modules.
 * @module aws-stream-consumer-core/settings
 * @author Byron du Preez
 */
exports._$_ = '_$_'; //IDE workaround

// Accessors for stream processing settings and functions
exports.getStreamProcessingSetting = getStreamProcessingSetting;
exports.getStreamProcessingFunction = getStreamProcessingFunction;

// Convenience accessors for specific stream processing settings
exports.getStreamType = getStreamType;
exports.isKinesisStreamType = isKinesisStreamType;
exports.isDynamoDBStreamType = isDynamoDBStreamType;
exports.isSequencingRequired = isSequencingRequired;
exports.isSequencingPerKey = isSequencingPerKey;
exports.isBatchKeyedOnEventID = isBatchKeyedOnEventID;
exports.isKplEncoded = isKplEncoded;
exports.getConsumerIdSuffix = getConsumerIdSuffix;
exports.getConsumerId = getConsumerId;
exports.getLegacyTaskTrackingName = getLegacyTaskTrackingName;
exports.getMaxNumberOfAttempts = getMaxNumberOfAttempts;
exports.getIdPropertyNames = getIdPropertyNames;
exports.getKeyPropertyNames = getKeyPropertyNames;
exports.getSeqNoPropertyNames = getSeqNoPropertyNames;

// Convenience accessors for specific batch initiating functions
exports.getGenerateMD5sFunction = getGenerateMD5sFunction;
exports.getResolveEventIdAndSeqNosFunction = getResolveEventIdAndSeqNosFunction;
exports.getResolveMessageIdsAndSeqNosFunction = getResolveMessageIdsAndSeqNosFunction;
exports.getExtractMessagesFromRecordFunction = getExtractMessagesFromRecordFunction;
exports.getExtractMessageFromRecordFunction = getExtractMessageFromRecordFunction;
exports.getLoadBatchStateFunction = getLoadBatchStateFunction;
exports.getPreProcessBatchFunction = getPreProcessBatchFunction;

// Convenience accessors for specific batch finalising functions
exports.getPreFinaliseBatchFunction = getPreFinaliseBatchFunction;
exports.getSaveBatchStateFunction = getSaveBatchStateFunction;
exports.getDiscardUnusableRecordFunction = getDiscardUnusableRecordFunction;
exports.getDiscardRejectedMessageFunction = getDiscardRejectedMessageFunction;
exports.getPostFinaliseBatchFunction = getPostFinaliseBatchFunction;

/**
 * An enum for the valid stream types currently supported.
 * @enum {string}
 * @readonly
 */
const StreamType = {
  kinesis: 'kinesis',
  dynamodb: 'dynamodb'
};
Object.freeze(StreamType);
exports.StreamType = StreamType;

/**
 * The names of the standard stream processing settings.
 * @namespace {StreamProcessingSettingNames} names
 */
const names = {
  // Common setting names
  streamType: 'streamType',
  sequencingRequired: 'sequencingRequired',
  sequencingPerKey: 'sequencingPerKey',
  batchKeyedOnEventID: 'batchKeyedOnEventID',
  kplEncoded: 'kplEncoded',
  consumerIdSuffix: 'consumerIdSuffix',
  consumerId: 'consumerId',
  timeoutAtPercentageOfRemainingTime: 'timeoutAtPercentageOfRemainingTime',
  maxNumberOfAttempts: 'maxNumberOfAttempts',
  idPropertyNames: 'idPropertyNames',
  keyPropertyNames: 'keyPropertyNames',
  seqNoPropertyNames: 'seqNoPropertyNames',

  // Function setting names
  extractMessagesFromRecord: 'extractMessagesFromRecord',
  extractMessageFromRecord: 'extractMessageFromRecord',
  generateMD5s: 'generateMD5s',
  resolveEventIdAndSeqNos: 'resolveEventIdAndSeqNos',
  resolveMessageIdsAndSeqNos: 'resolveMessageIdsAndSeqNos',
  loadBatchState: 'loadBatchState',
  preProcessBatch: 'preProcessBatch',
  discardUnusableRecord: 'discardUnusableRecord',
  preFinaliseBatch: 'preFinaliseBatch',
  saveBatchState: 'saveBatchState',
  discardRejectedMessage: 'discardRejectedMessage',
  postFinaliseBatch: 'postFinaliseBatch',

  // Specialised implementation setting names
  batchStateTableName: 'batchStateTableName',
  deadRecordQueueName: 'deadRecordQueueName',
  deadMessageQueueName: 'deadMessageQueueName'
};
exports.names = names;

// Default options
/**
 * The last-resort, default options to fallback to during configuration to fill in any missing settings
 * @namespace {StreamProcessingOptions} defaults
 */
const defaults = {
  timeoutAtPercentageOfRemainingTime: 0.9,
  maxNumberOfAttempts: 10,
  batchStateTableName: 'StreamConsumerBatchState',
  deadRecordQueueName: 'DeadRecordQueue',
  deadMessageQueueName: 'DeadMessageQueue',
};
exports.defaults = defaults;

// =====================================================================================================================
// Accessors for stream processing settings and functions
// =====================================================================================================================

/**
 * Returns the value of the named stream processing setting (if any) on the given context.
 * @param {StreamProcessing} context - the context from which to fetch the named setting's value
 * @param {string} settingName - the name of the stream processing setting
 * @returns {*|undefined} the value of the named setting (if any); otherwise undefined
 */
function getStreamProcessingSetting(context, settingName) {
  return context && context.streamProcessing && isNotBlank(settingName) ? context.streamProcessing[settingName] : undefined;
}

/**
 * Returns the function configured at the named stream processing setting on the given context (if any and if it's a
 * real function); otherwise returns undefined.
 * @param {StreamProcessing} context - the context from which to fetch the function
 * @param {string} settingName - the name of the stream processing setting
 * @returns {Function|undefined} the named function (if it's a function); otherwise undefined
 */
function getStreamProcessingFunction(context, settingName) {
  const fn = getStreamProcessingSetting(context, settingName);
  return typeof fn === 'function' ? fn : undefined;
}

// =====================================================================================================================
// Convenience accessors for specific stream processing settings
// =====================================================================================================================

/**
 * Returns the stream type configured on the given context.
 * @param {StreamProcessing} context - the context from which to fetch the stream type
 * @returns {string|undefined} the stream type (if any); otherwise undefined
 */
function getStreamType(context) {
  return context.streamProcessing.streamType;
}

/**
 * Returns true if the stream type configured on the given context is Kinesis; false otherwise.
 * @param {StreamProcessing} context - the context from which to fetch the stream type
 * @returns {boolean} true if Kinesis stream type; false otherwise
 */
function isKinesisStreamType(context) {
  return context.streamProcessing.streamType === StreamType.kinesis
}

/**
 * Returns true if the stream type configured on the given context is DynamoDB; false otherwise.
 * @param {StreamProcessing} context - the context from which to fetch the stream type
 * @returns {boolean} true if DynamoDB stream type; false otherwise
 */
function isDynamoDBStreamType(context) {
  return context.streamProcessing.streamType === StreamType.dynamodb;
}

/**
 * Returns whether message sequencing is required or not as configured on the given context.
 * @param {StreamProcessing} context - the context from which to fetch the maximum number of attempts
 * @returns {boolean} the maximum number of attempts (if any); otherwise undefined
 */
function isSequencingRequired(context) {
  return context.streamProcessing.sequencingRequired;
}

/**
 * Returns whether message sequencing is per key or not (i.e. per shard) as configured on the given context.
 * See notes on `StreamProcessingOptions` type definition for more details
 * @param {StreamProcessing} context - the context from which to fetch the sequencing per key setting
 * @returns {boolean} the sequencing per key setting (if any); otherwise undefined
 */
function isSequencingPerKey(context) {
  return context.streamProcessing.sequencingPerKey;
}

/**
 * Returns whether each batch must be keyed on its first usable record's event ID (if DynamoDB or true) or on its shard
 * ID (if Kinesis and false) as configured on the given context.
 * @param {StreamProcessing} context - the context from which to fetch the maximum number of attempts
 * @returns {boolean} whether each batch must be keyed on its first usable record's event ID or on its shard ID
 */
function isBatchKeyedOnEventID(context) {
  return context.streamProcessing.batchKeyedOnEventID;
}

/**
 * Returns whether the Kinesis stream event records are Kinesis Producer Library (KPL) encoded (with or without
 * aggregation) or not as configured on the given context.
 * @param {StreamProcessing} context - the context from which to fetch the KPL encoded option
 * @returns {boolean} whether the Kinesis stream event records are KPL encoded or not
 */
function isKplEncoded(context) {
  return context.streamProcessing.kplEncoded;
}

/**
 * Gets the optional suffix to be appended to the derived or configured consumer ID configured on the given context (if any).
 *
 * @param {StreamProcessing} context - the context to use to resolve the consumer ID suffix
 * @returns {string|undefined} the consumer ID suffix
 */
function getConsumerIdSuffix(context) {
  return context.streamProcessing.consumerIdSuffix;
}

/**
 * Gets the derived (or pre-configured) consumer ID configured on the given context.
 *
 * NB: The consumer ID acts as a subscription ID and MUST be unique amongst multiple different stream consumers that are
 * all consuming off of the same Kinesis or DynamoDB stream! The consumer ID defaults to a concatenation of the Lambda's
 * function name and its alias (if any) if NOT configured, which will meet the uniqueness requirement for all cases
 * except for the bizarre and most likely erroneous case of registering more than one event source mapping between the
 * SAME Lambda and the SAME Kinesis or DynamoDB stream, which in itself would cause duplicate delivery of all messages!
 * If the bizarre case really needs to be supported then EITHER use the consumerIdSuffix OR configure the two consumer
 * IDs explicitly to differentiate the two.
 *
 * @param {StreamProcessing} context - the context to use to resolve the consumer ID
 * @returns {string} the consumer ID
 */
function getConsumerId(context) {
  return context.streamProcessing.consumerId;
}

function getLegacyTaskTrackingName(context) {
  return context.streamProcessing.taskTrackingName;
}


/**
 * Returns the maximum number of attempts configured on the given context.
 * @param {StreamProcessing} context - the context from which to fetch the maximum number of attempts
 * @returns {number|undefined} the maximum number of attempts (if any); otherwise undefined
 */
function getMaxNumberOfAttempts(context) {
  return context.streamProcessing.maxNumberOfAttempts;
}

/**
 * Returns the the names of all of the message identifier properties configured on the given context, which are used to
 * extract a message's unique identifier(s), which uniquely identifies the message.
 * @param {StreamProcessing} context - the context from which to fetch the message id property names
 * @returns {string[]} the message id property names if any; otherwise an empty array
 */
function getIdPropertyNames(context) {
  const idPropertyNames = context.streamProcessing.idPropertyNames;
  return Array.isArray(idPropertyNames) ? idPropertyNames :
    typeof idPropertyNames === 'string' ? toPropertyNamesArray(idPropertyNames) : [];
}

/**
 * Returns the names of all of the message key properties configured on the given context, which are used to extract a
 * message's key(s), which uniquely identify the entity or subject of a message.
 * @param {StreamProcessing} context - the context from which to fetch the message key property names
 * @returns {string[]} the message key property names if any; otherwise an empty array
 */
function getKeyPropertyNames(context) {
  const keyPropertyNames = context.streamProcessing.keyPropertyNames;
  return Array.isArray(keyPropertyNames) ? keyPropertyNames :
    typeof keyPropertyNames === 'string' ? toPropertyNamesArray(keyPropertyNames) : [];
}

/**
 * Returns the names of all of the message sequence number properties configured on the given context, which are used to
 * extract a message's sequence number(s), which determine the order in which messages with identical keys must be
 * processed.
 * @param {StreamProcessing} context - the context from which to fetch the message sequence property names
 * @returns {string[]} the message sequence property names if any; otherwise an empty array
 */
function getSeqNoPropertyNames(context) {
  const seqNoPropertyNames = context.streamProcessing.seqNoPropertyNames;
  return Array.isArray(seqNoPropertyNames) ? seqNoPropertyNames :
    typeof seqNoPropertyNames === 'string' ? toPropertyNamesArray(seqNoPropertyNames) : [];
}

// =====================================================================================================================
// Convenience accessors for specific stream processing functions
// =====================================================================================================================

/**
 * Returns the `generateMD5s` function configured on the given context (if any and if it's a real function);
 * otherwise returns undefined.
 * @param {StreamProcessing} context - the context from which to fetch the function
 * @returns {GenerateMD5s|undefined} the generateMD5s function (if it's a function); otherwise undefined
 */
function getGenerateMD5sFunction(context) {
  const fn = context.streamProcessing.generateMD5s;
  return typeof fn === 'function' ? fn : undefined;
}

/**
 * Returns the `resolveEventIdAndSeqNos` function configured on the given context (if any and if it's a real function);
 * otherwise returns undefined.
 * @param {StreamProcessing} context - the context from which to fetch the function
 * @returns {ResolveEventIdAndSeqNos|undefined} the resolveEventIdAndSeqNos function (if it's a function); otherwise undefined
 */
function getResolveEventIdAndSeqNosFunction(context) {
  const fn = context.streamProcessing.resolveEventIdAndSeqNos;
  return typeof fn === 'function' ? fn : undefined;
}

/**
 * Returns the `resolveMessageIdsAndSeqNos` function configured on the given context (if any and if it's a real function);
 * otherwise returns undefined.
 * @param {StreamProcessing} context - the context from which to fetch the function
 * @returns {ResolveMessageIdsAndSeqNos|undefined} the resolveMessageIdsAndSeqNos function (if it's a function); otherwise undefined
 */
function getResolveMessageIdsAndSeqNosFunction(context) {
  const fn = context.streamProcessing.resolveMessageIdsAndSeqNos;
  return typeof fn === 'function' ? fn : undefined;
}

/**
 * Returns the `extractMessagesFromRecord` function configured on the given context (if any and if it's a real function);
 * otherwise returns undefined.
 * @param {StreamProcessing} context - the context from which to fetch the function
 * @returns {ExtractMessagesFromRecord|undefined} the extractMessagesFromRecord function (if it's a function); otherwise undefined
 */
function getExtractMessagesFromRecordFunction(context) {
  const fn = context.streamProcessing.extractMessagesFromRecord;
  return typeof fn === 'function' ? fn : undefined;
}

/**
 * Returns the `extractMessageFromRecord` function configured on the given context (if any and if it's a real function);
 * otherwise returns undefined.
 * @param {StreamProcessing} context - the context from which to fetch the function
 * @returns {ExtractMessageFromRecord|undefined} the extractMessageFromRecord function (if it's a function); otherwise undefined
 */
function getExtractMessageFromRecordFunction(context) {
  const fn = context.streamProcessing.extractMessageFromRecord;
  return typeof fn === 'function' ? fn : undefined;
}

/**
 * Returns the `loadBatchState` function configured on the given context (if any and if it's a real function);
 * otherwise returns undefined.
 * @param {StreamProcessing} context - the context from which to fetch the function
 * @returns {LoadBatchState|undefined} the loadBatchState function (if it's a function); otherwise undefined
 */
function getLoadBatchStateFunction(context) {
  const fn = context.streamProcessing.loadBatchState;
  return typeof fn === 'function' ? fn : undefined;
}

/**
 * Returns the `preProcessBatch` function configured on the given context (if any and if it's a real function);
 * otherwise returns undefined.
 * @param {StreamProcessing} context - the context from which to fetch the function
 * @returns {PreProcessBatch|undefined} the preProcessBatch function (if it's a function); otherwise undefined
 */
function getPreProcessBatchFunction(context) {
  const fn = context.streamProcessing.preProcessBatch;
  return typeof fn === 'function' ? fn : undefined;
}

/**
 * Returns the `discardUnusableRecord` function configured on the given context (if any and if it's a real function);
 * otherwise returns undefined.
 * @param {StreamProcessing} context - the context from which to fetch the function
 * @returns {DiscardUnusableRecord|undefined} the discardUnusableRecord function (if it's a function); otherwise undefined
 */
function getDiscardUnusableRecordFunction(context) {
  const fn = context.streamProcessing.discardUnusableRecord;
  return typeof fn === 'function' ? fn : undefined;
}

/**
 * Returns the `preFinaliseBatch` function configured on the given context (if any and if it's a real function);
 * otherwise returns undefined.
 * @param {StreamProcessing} context - the context from which to fetch the function
 * @returns {PreFinaliseBatch|undefined} the preFinaliseBatch function (if it's a function); otherwise undefined
 */
function getPreFinaliseBatchFunction(context) {
  const fn = context.streamProcessing.preFinaliseBatch;
  return typeof fn === 'function' ? fn : undefined;
}

/**
 * Returns the `saveBatchState` function configured on the given context (if any and if it's a real function);
 * otherwise returns undefined.
 * @param {StreamProcessing} context - the context from which to fetch the function
 * @returns {SaveBatchState|undefined} the saveBatchState function (if it's a function); otherwise undefined
 */
function getSaveBatchStateFunction(context) {
  const fn = context.streamProcessing.saveBatchState;
  return typeof fn === 'function' ? fn : undefined;
}

/**
 * Returns the `discardRejectedMessage` function configured on the given context (if any and if it's a real function);
 * otherwise returns undefined.
 * @param {StreamProcessing} context - the context from which to fetch the function
 * @returns {DiscardRejectedMessage|undefined} the discardRejectedMessage function (if it's a function); otherwise undefined
 */
function getDiscardRejectedMessageFunction(context) {
  const fn = context.streamProcessing.discardRejectedMessage;
  return typeof fn === 'function' ? fn : undefined;
}

/**
 * Returns the `postFinaliseBatch` function configured on the given context (if any and if it's a real function);
 * otherwise returns undefined.
 * @param {StreamProcessing} context - the context from which to fetch the function
 * @returns {PostFinaliseBatch|undefined} the postFinaliseBatch function (if it's a function); otherwise undefined
 */
function getPostFinaliseBatchFunction(context) {
  const fn = context.streamProcessing.postFinaliseBatch;
  return typeof fn === 'function' ? fn : undefined;
}

/**
 * Converts the given property names string into an array of property names
 * @param {string} propertyNamesString - a string of property name(s) separated by the given separator
 * @param {string|undefined} [separator] - the separator to use to split the property names (defaults to PROPERTY_NAME_SEPARATOR if omitted)
 * @returns {Array.<string>} an array of property names (if any)
 */
function toPropertyNamesArray(propertyNamesString, separator) {
  return propertyNamesString.split(separator || PROPERTY_NAME_SEPARATOR).map(s => trim(s)).filter(s => !!s);
}