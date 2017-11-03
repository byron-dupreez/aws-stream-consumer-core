'use strict';

const freeze = Object.freeze;

// TaskDef settings to use
const batchSettings = freeze({describeItem: describeBatch});
const batchAndCancellableSettings = freeze({describeItem: (batch, cancellable, context) => describeBatch(batch, context)});
const batchAndUnusableRecordsSettings = freeze({describeItem: (batch, cancellable, context) => describeBatchAndUnusableRecords(batch, context)});
const batchAndProcessOutcomesSettings = freeze({describeItem: (batch, processOutcomes, cancellable, context) => describeBatch(batch, context)});
const messageSettings = freeze({describeItem: describeMessage});
const batchAndIncompleteMessagesSettings = freeze({describeItem: describeBatchAndIncompleteMessages});

const unusableRecordSettings = freeze({describeItem: describeUnusableRecord});
const rejectedMessageSettings = freeze({describeItem: describeRejectedMessage});

/**
 * Common `TaskDef` settings & `describeItem` implementations to be used by stream consumer implementations.
 * @module aws-stream-consumer/taskdef-settings
 * @author Byron du Preez
 */
exports._ = '_'; //IDE workaround

// TaskDefSettings with a `describeItem` function for batch initiating, processing & finalising tasks
exports.batchSettings = batchSettings;
exports.batchAndCancellableSettings = batchAndCancellableSettings;
exports.batchAndUnusableRecordsSettings = batchAndUnusableRecordsSettings;
exports.batchAndProcessOutcomesSettings = batchAndProcessOutcomesSettings;

// TaskDefSettings with a `describeItem` function for process one tasks
exports.messageSettings = messageSettings;

// TaskDefSettings with a `describeItem` function for process all tasks
exports.batchAndIncompleteMessagesSettings = batchAndIncompleteMessagesSettings;

// TaskDefSettings with a `describeItem` function for discardUnusableRecord tasks
exports.unusableRecordSettings = unusableRecordSettings;

// TaskDefSettings with a `describeItem` function for discardRejectedMessage tasks
exports.rejectedMessageSettings = rejectedMessageSettings;

// =====================================================================================================================
// Task describeItem implementations
// =====================================================================================================================

function describeBatch(batch, context) {
  const b = batch || context.batch;
  return `batch (${b && b.shardOrEventID})`;
  // return b ? `batch (${b.shardOrEventID})${messages ? ` (${b.describeContents()})` : ''}` : '';
}

/**
 * A `describeItem` implementation that accepts and describes the arguments passed to a "process one message at a time" function.
 * @param {Message} message - the message being processed
 * @param {Batch} batch - the current batch
 * @param {StreamConsumerContext} context - the context to use
 * @returns {string} a short description of the message
 */
function describeMessage(message, batch, context) {
  const b = batch || context.batch;
  const state = b && b.states.get(message);
  return message ? `Message (${state && state.msgDesc})` : '';
}

function describeBatchAndIncompleteMessages(batch, incompleteMessages, context) {
  if (!incompleteMessages) return describeBatch(batch, context);

  const b = batch || context.batch;
  const i = incompleteMessages.length;
  const is = `${i} incomplete message${i !== 1 ? 's' : ''}`;
  const of = b && b.messages ? ` of ${b.messages.length}` : '';

  return b ? `batch (${b && b.shardOrEventID}) (${is}${of})` : '';
}

function describeBatchAndUnusableRecords(batch, context) {
  const b = batch || context.batch;
  const unusableRecords = b && b.unusableRecords;
  if (!unusableRecords) return describeBatch(b, context);

  const u = unusableRecords.length;
  const us = `${u} unusable record${u !== 1 ? 's' : ''}`;
  const of = b && b.records ? ` of ${b.records.length}` : '';

  return b ? `batch (${b && b.shardOrEventID}) (${us}${of})` : '';
}

/**
 * A `describeItem` implementation that accepts and describes the arguments passed to a `discardUnusableRecord` function.
 * @param {UnusableRecord} unusableRecord - the unusable record to be discarded
 * @param {Batch} batch - the batch being processed
 * @param {StreamConsumerContext} context - the context to use
 * @returns {string} a short description of the unusable record
 */
function describeUnusableRecord(unusableRecord, batch, context) {
  const b = batch || context.batch;
  const state = b && b.states.get(unusableRecord);
  return `Unusable record (${state && state.recDesc})`;
}

/**
 * A `describeItem` implementation that accepts and describes the arguments passed to a `discardRejectedMessage` function.
 * @param {Message} rejectedMessage - the rejected message to be discarded
 * @param {Batch} batch - the batch being processed
 * @param {StreamConsumerContext} context - the context to use
 * @returns {string} a short description of the rejected message
 */
function describeRejectedMessage(rejectedMessage, batch, context) {
  const b = batch || context.batch;
  const state = b && b.states.get(rejectedMessage);
  return `Rejected message (${state && (state.msgDesc || state.recDesc)})`;
}