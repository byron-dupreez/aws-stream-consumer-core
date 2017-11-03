'use strict';

const strings = require('core-functions/strings');
const isBlank = strings.isBlank;
const isNotBlank = strings.isNotBlank;
const trimOrEmpty = strings.trimOrEmpty;

const errors = require('core-functions/errors');
const FatalError = errors.FatalError;
const TransientError = errors.TransientError;

const tracking = require('./tracking');
const toCountString = tracking.toCountString;
const TaskMapNames = tracking.TaskMapNames;

const awsErrors = require('aws-core-utils/aws-errors');

const stages = require('aws-core-utils/stages');
const dynamoDBDocClientCache = require('aws-core-utils/dynamodb-doc-client-cache');
const dynamoDBUtils = require('aws-core-utils/dynamodb-utils');

const deepEqual = require('deep-equal');
// const strict = {strict: true};

/**
 * Utilities and functions to be used by the stream consumer to load its batch's previous state and save its batch's current state.
 * @module aws-stream-consumer-core/persisting
 * @author Byron du Preez
 */
exports._ = '_'; //IDE workaround

exports.saveBatchStateToDynamoDB = saveBatchStateToDynamoDB;
exports.loadBatchStateFromDynamoDB = loadBatchStateFromDynamoDB;

// Internal functions - only exposed for testing
exports.getBatchStateTableName = getBatchStateTableName;
exports.toBatchStateItem = toBatchStateItem;
exports.getDynamoDBDocClient = getDynamoDBDocClient;

exports.hasUnusableRecordIdentifier = hasUnusableRecordIdentifier;
exports.toUnusableRecordBFK = toUnusableRecordBFK;

exports.hasMessageIdentifier = hasMessageIdentifier;
exports.toMessageBFK = toMessageBFK;

/**
 * Saves the given batch's current state to the DynamoDB stream consumer batch state table.
 * @this {Task}
 * @param {Batch} batch - the batch to be saved
 * @param {StreamProcessing} context - the context to use
 * @returns {Promise.<*>} a promise that will resolve with the result returned by the database put or update request or reject with the error thrown
 */
function saveBatchStateToDynamoDB(batch, context) {
  const task = this;
  const startTime = Date.now();

  try {
    const streamConsumerId = batch.streamConsumerId || (batch.key && batch.key.streamConsumerId);
    const shardOrEventID = batch.shardOrEventID || (batch.key && batch.key.shardOrEventID);

    // Resolve the stage-qualified table name
    const unqualifiedTableName = getBatchStateTableName(context);
    const tableName = stages.toStageQualifiedResourceName(unqualifiedTableName, context.stage, context);

    if (isBlank(streamConsumerId) || isBlank(shardOrEventID)) {
      if (batch.messages.length <= 0 && batch.rejectedMessages.length <= 0 && batch.unusableRecords.length <= 0) {
        context.warn('Skipping save state of an empty batch - ', batch.describe(false));
        return Promise.resolve(undefined);
      }
      const errMsg = `Cannot save state of batch to ${tableName} WITHOUT a complete batch key - streamConsumerId (${streamConsumerId}) & shardOrEventID (${shardOrEventID})`;
      context.error(errMsg, '-', batch.describe(false));
      return Promise.reject(new Error(errMsg));
    }

    // Transform the batch into a batch state item to be saved
    const item = toBatchStateItem(batch, context);

    if (!item) {
      const errMsg = `Cannot save state of batch to ${tableName}, since no batch state item was resolved`;
      context.error(errMsg, '-', batch.describe(false));
      return Promise.reject(new Error(errMsg));
    }

    const dynamoDBDocClient = getDynamoDBDocClient(context);

    // Save a batch state item for the entire batch of messages, rejected messages and unusable records to DynamoDB
    return (batch.previouslySaved ?
        updateBatchState(dynamoDBDocClient, tableName, item, batch, context) :
        insertBatchState(dynamoDBDocClient, tableName, item, batch, context)
    ).then(
      result => {
        if (context.traceEnabled) {
          context.trace(`Saved state of batch to ${tableName} -`, batch.describe(false), took(startTime));
        }
        return result;
      },
      err => {
        if (awsErrors.isResourceNotFoundException(err)) {
          const errMsg = `FATAL - Cannot save state of batch, since missing DynamoDB table (${tableName}) - ${err.message}`;
          context.error(errMsg, '-', batch.describe(false), took(startTime), err);
          throw new FatalError(errMsg, err);
        }
        if (awsErrors.isRetryable(err)) {
          const errMsg = `TRANSIENT - Failed to save state of batch to ${tableName} - ${err.message}`;
          context.error(errMsg, '-', batch.describe(false), took(startTime), err);
          if (task) task.revertAttempts(true);
          throw new TransientError(errMsg, err);
        }
        context.error(`Failed to save state of batch to ${tableName} -`, batch.describe(false), took(startTime), err);
        throw err;
      }
    );

  } catch (err) {
    context.error('Failed to save state of ', batch.describe(false), took(startTime), err);
    return Promise.reject(err);
  }
}

/**
 * Loads the given batch's previous state from the DynamoDB stream consumer batch state table and restores the batch's
 * state to this previous state.
 * @this {Task}
 * @param {Batch} batch - the batch to be loaded
 * @param {StreamProcessing} context - the context to use
 * @returns {Promise.<*>} a promise that will resolve with the result returned by the database get request or reject with the error thrown
 */
function loadBatchStateFromDynamoDB(batch, context) {
  const task = this;

  const startTime = Date.now();

  try {
    const streamConsumerId = batch.streamConsumerId || (batch.key && batch.key.streamConsumerId);
    const shardOrEventID = batch.shardOrEventID || (batch.key && batch.key.shardOrEventID);

    const batchDesc = `batch (${shardOrEventID}) with ${batch.describeContents()}`;

    // Resolve the stage-qualified table name
    const unqualifiedTableName = getBatchStateTableName(context);
    const tableName = stages.toStageQualifiedResourceName(unqualifiedTableName, context.stage, context);

    const getRequest = {
      TableName: tableName,
      Key: {streamConsumerId: streamConsumerId, shardOrEventID: shardOrEventID},
      ConsistentRead: true,
      ReturnConsumedCapacity: "NONE", //"TOTAL" or "INDEXES" or "NONE"
      ProjectionExpression: ":streamConsumerId, :shardOrEventID, :messageStates, :rejectedMessageStates, :unusableRecordStates",
      ExpressionAttributeNames: {
        ":streamConsumerId": "streamConsumerId",
        ":shardOrEventID": "shardOrEventID",
        ":messageStates": "messageStates",
        ":rejectedMessageStates": "rejectedMessageStates",
        ":unusableRecordStates": "unusableRecordStates"
      }
    };

    if (context.traceEnabled) context.trace("get request: " + JSON.stringify(getRequest));

    const dynamoDBDocClient = getDynamoDBDocClient(context);

    // Load the batch state for the entire batch of messages, rejected messages and unusable records from DynamoDB
    return dynamoDBDocClient.get(getRequest).promise()
      .then(result => {
        const hasPreviouslySavedItem = !!(result && result.Item);
        if (context.traceEnabled) {
          let loadedDesc = 'found no previously saved state';
          if (hasPreviouslySavedItem) {
            const item = result.Item;
            const m = item.messageStates ? item.messageStates.length : 0;
            const r = item.rejectedMessageStates ? item.rejectedMessageStates.length : 0;
            const u = item.unusableRecordStates ? item.unusableRecordStates.length : 0;
            loadedDesc = `found ${toCountString(m, 'msg state')}, ${toCountString(r, 'rejected msg state')} & ${toCountString(u, 'unusable rec state')}`;
          }
          context.debug('Loaded state of', batchDesc, 'from', tableName, '-', loadedDesc, took(startTime));
        }

        batch.previouslySaved = hasPreviouslySavedItem;
        if (hasPreviouslySavedItem) {
          updateBatchWithPriorState(batch, result.Item, context);
        }
        return result;
      })
      .catch(err => {
        batch.previouslySaved = undefined;

        if (awsErrors.isResourceNotFoundException(err)) {
          const errMsg = `FATAL - Cannot load state of batch, since missing DynamoDB table (${tableName}) - ${err.message}`;
          context.error(errMsg, '-', batchDesc, took(startTime), err);
          throw new FatalError(errMsg, err);
        }
        if (awsErrors.isRetryable(err)) {
          const errMsg = `TRANSIENT - Failed to load state of batch from ${tableName} - ${err.message}`;
          context.error(errMsg, '-', batchDesc, took(startTime), err);
          if (task) task.revertAttempts(true);
          throw new TransientError(errMsg, err);
        }
        context.error('Failed to load state of', batchDesc, 'from', tableName, took(startTime), err);
        throw err;
      });

  } catch (err) {
    batch.previouslySaved = undefined;
    context.error('Failed to load state of batch', took(startTime), err);
    return Promise.reject(err);
  }
}

/**
 * Returns the name of the stream consumer's batch state table from which to load the previous state (if any) and to
 * which to save the current state of the {@link Batch} being processed.
 * @param {StreamProcessing} context - the context from which to fetch the stream consumer's batch state table name
 * @returns {string} the name of the stream consumer's batch state table
 */
function getBatchStateTableName(context) {
  return context.streamProcessing.batchStateTableName;
}

/**
 * Converts the given batch into a stream consumer batch state item to be subsequently persisted to DynamoDB.
 * @param {Batch} batch - the batch to be converted into a batch state item
 * @param {StreamProcessing} context - the context to use
 * @returns {BatchStateItem} the stream consumer batch state item
 */
function toBatchStateItem(batch, context) {
  const states = batch.states;
  // const messages = batch.allMessages();
  const messages = batch.messages;
  const rejectedMessages = batch.rejectedMessages;
  const unusableRecords = batch.unusableRecords;

  // Resolve the messages' states to be saved (if any)
  const messageStates = messages && messages.length > 0 ?
    messages.map(msg => toStorableMessageState(states.get(msg), msg, context)).filter(s => !!s) : [];

  // Resolve the rejected messages' states to be saved (if any)
  const rejectedMessageStates = rejectedMessages && rejectedMessages.length > 0 ?
    rejectedMessages.map(msg => toStorableMessageState(states.get(msg), msg, context)).filter(s => !!s) : [];

  // Resolve the unusable records' states to be saved (if any)
  const unusableRecordStates = unusableRecords && unusableRecords.length > 0 ?
    unusableRecords.map(uRec => toStorableUnusableRecordState(states.get(uRec), uRec, context)).filter(s => !!s) : [];

  const batchState = toStorableBatchState(states.get(batch), batch, context);

  return {
    streamConsumerId: batch.streamConsumerId, // hash key
    shardOrEventID: batch.shardOrEventID, // range key
    messageStates: messageStates,
    rejectedMessageStates: rejectedMessageStates,
    unusableRecordStates: unusableRecordStates,
    batchState: batchState || null
  };
}

/**
 * Converts the given message state into a storable version of itself.
 * @param {MessageState} messageState
 * @param {Message} message
 * @param {StreamProcessing} context
 * @return {MessageStateItem|undefined}
 */
function toStorableMessageState(messageState, message, context) {
  if (!messageState) {
    context.warn(`Skipping save of state for message, since it has no state - message (${JSON.stringify(message)})`);
    return undefined;
  }

  // Convert the message's state into a safely storable object to get a clean, simplified version of its state
  const state = dynamoDBUtils.toStorableObject(messageState);

  // If state has no message identifier then have no way to identify the message ... so attach a safely storable copy of
  // its original message, user record or record (if any) (i.e. without its legacy state, if any, for later matching)!
  if (!hasMessageIdentifier(messageState)) {
    message = message || messageState.message;

    if (message) {
      // Make a storable copy of the message & attach it to the storable state
      state.message = dynamoDBUtils.toStorableObject(message);
      // Remove the message copy's LEGACY state (if any) to get back to the original message
      tracking.deleteLegacyState(message, context);
    }
    else if (messageState.userRecord) {
      // If has no message, then attach a copy of its user record
      // Make a storable copy of the unusable user record & attach it to the storable state
      state.userRecord = dynamoDBUtils.toStorableObject(messageState.userRecord);
      // Remove the unusable user record copy's LEGACY state (if any) to get back to the original user record
      tracking.deleteLegacyState(state.userRecord, context);
    }
    else if (messageState.record) {
      // If has no message AND no user record, then attach a copy of its record
      // Make a storable copy of the unusable record & attach it to the storable state
      state.record = dynamoDBUtils.toStorableObject(messageState.record);
      // remove the unusable record copy's LEGACY state (if any) to get back to the original record
      tracking.deleteLegacyState(state.record, context);
    }
  }
  return state;
}

/**
 * Converts the given unusable record state into a storable version of itself.
 * @param {UnusableRecordState} unusableRecordState
 * @param {UnusableRecord|undefined} unusableRecord
 * @param {StreamProcessing} context
 * @return {UnusableRecordStateItem|undefined}
 */
function toStorableUnusableRecordState(unusableRecordState, unusableRecord, context) {
  if (!unusableRecordState) {
    context.warn(`Skipping save of state for unusable record, since it has no state - record (${JSON.stringify(unusableRecord)})`);
    return undefined;
  }

  // Convert the record's state into a safely storable object to get a clean, simplified version of its state
  const state = dynamoDBUtils.toStorableObject(unusableRecordState);

  // If state has no record identifier then have no way to identify the unusable record ... so attach a safely storable
  // copy of its original user record or record (if any) (without its legacy state, if any) for later matching!
  if (!hasUnusableRecordIdentifier(unusableRecordState)) {
    unusableRecord = unusableRecord || unusableRecordState.unusableRecord;

    if (unusableRecord) {
      // Make a storable copy of the unusable record & attach it to the storable state
      state.unusableRecord = dynamoDBUtils.toStorableObject(unusableRecord);
      // Remove the unusable record copy's LEGACY state (if any) to get back to the original unusable record
      tracking.deleteLegacyState(unusableRecord, context);
    }
    if (unusableRecordState.userRecord) {
      // Make a storable copy of the unusable record's user record & attach it to the storable state
      state.userRecord = dynamoDBUtils.toStorableObject(unusableRecordState.userRecord);
      // Remove the unusable record's user record copy's LEGACY state (if any) to get back to the original user record
      tracking.deleteLegacyState(state.userRecord, context);
    }
    else if (unusableRecordState.record) {
      // Make a storable copy of the unusable record's record & attach it to the storable state
      state.record = dynamoDBUtils.toStorableObject(unusableRecordState.record);
      // remove the unusable record's record copy's LEGACY state (if any) to get back to the original record
      tracking.deleteLegacyState(state.record, context);
    }
  }
  return state;
}

function toStorableBatchState(batchState, batch, context) {
  // If the given batch has no state then skip it
  if (!batchState) {
    context.warn(`Skipping save of own state for batch (${batch.shardOrEventID}), since it has no state`);
    return undefined;
  }
  // Convert the state into a safely storable object to get a clean, simplified version of the batch's state
  return dynamoDBUtils.toStorableObject(batchState);
}

function getDynamoDBDocClient(context) {
  if (!context.dynamoDBDocClient) {
    // Configure a default AWS DynamoDB.DocumentClient instance on context.dynamoDBDocClient if not already configured
    const defaultOptions = require('./default-options.json');
    const dynamoDBDocClientOptions = defaultOptions.dynamoDBDocClientOptions;
    context.trace(`An AWS DynamoDB.DocumentClient instance was not configured on context.dynamoDBDocClient yet - configuring an instance with default options (${JSON.stringify(dynamoDBDocClientOptions)}). Preferably configure this beforehand, using aws-core-utils/dynamodb-doc-client-cache#configureDynamoDBDocClient`);
    dynamoDBDocClientCache.configureDynamoDBDocClient(context, dynamoDBDocClientOptions);
  }
  return context.dynamoDBDocClient;
}

function insertBatchState(dynamoDBDocClient, tableName, item, batch, context) {
  const putRequest = {
    TableName: tableName,
    Item: item,
    ConditionExpression: 'attribute_not_exists(streamConsumerId) AND attribute_not_exists(shardOrEventID)'
  };

  if (context.traceEnabled) context.trace("put request: " + JSON.stringify(putRequest));

  const startTime = Date.now();

  return dynamoDBDocClient.put(putRequest).promise().then(
    result => {
      if (context.traceEnabled) {
        context.trace(`Inserted state of batch into ${tableName}`, took(startTime));
      }
      return result;
    },
    err => {
      if (awsErrors.isConditionalCheckFailed(err)) {
        const errMsg = `Cannot insert over EXISTING state of batch in ${tableName} - switching to update ${took(startTime)}`;
        batch.previouslySaved === false ? context.warn(errMsg) : context.trace(errMsg);
        return updateBatchState(dynamoDBDocClient, tableName, item, batch, context);
      }
      context.error(`Failed to insert state of batch into ${tableName}`, took(startTime), err);
      throw err;
    });
}

function updateBatchState(dynamoDBDocClient, tableName, item, batch, context) {
  const updateExpression = 'set #messageStates = :messageStates, #rejectedMessageStates = :rejectedMessageStates, #unusableRecordStates = :unusableRecordStates, #batchState = :batchState';
  const expressionAttributeNames = {
    '#messageStates': 'messageStates',
    '#rejectedMessageStates': 'rejectedMessageStates',
    '#unusableRecordStates': 'unusableRecordStates',
    '#batchState': 'batchState'
  };
  const expressionAttributeValues = {
    ':messageStates': item.messageStates || [],
    ':rejectedMessageStates': item.rejectedMessageStates || [],
    ':unusableRecordStates': item.unusableRecordStates || [],
    ':batchState': item.batchState || null
  };

  const updateRequest = {
    TableName: tableName,
    Key: {streamConsumerId: item.streamConsumerId, shardOrEventID: item.shardOrEventID},
    UpdateExpression: updateExpression,
    ConditionExpression: 'attribute_exists(streamConsumerId) AND attribute_exists(shardOrEventID)',
    ExpressionAttributeNames: expressionAttributeNames,
    ExpressionAttributeValues: expressionAttributeValues,
    ReturnValues: 'ALL_NEW'
  };

  const startTime = Date.now();

  return dynamoDBDocClient.update(updateRequest).promise().then(
    result => {
      if (context.traceEnabled) {
        context.trace(`Updated state of batch to ${tableName}`, took(startTime));
      }
      return result;
    },
    err => {
      if (awsErrors.isConditionalCheckFailed(err)) {
        const errMsg = `Cannot update NON-EXISTENT state of batch in ${tableName} - switching to insert ${took(startTime)}`;
        batch.previouslySaved === true ? context.warn(errMsg) : context.trace(errMsg);
        return insertBatchState(dynamoDBDocClient, tableName, item, batch, context);
      }
      context.error(`Failed to update state of batch to ${tableName}`, took(startTime), err);
      throw err;
    }
  );
}

/**
 * Updates the given batch with the previous message states, previous unusable record states and previous batch state on
 * the given item, which was loaded from the database.
 * @param {Batch} batch - the batch to update
 * @param {BatchStateItem} item - a previously loaded batch state item
 * @param context
 */
function updateBatchWithPriorState(batch, item, context) {
  restoreMessageAndRejectedMessageStates(batch, item, context);
  restoreUnusableRecordStates(batch, item, context);
}

/**
 * Returns true if the given message state has at least one non-blank identifier; otherwise false.
 * @param {MessageState} msgState - the tracked state of a message
 * @return {boolean}
 */
function hasMessageIdentifier(msgState) {
  const md5s = msgState.md5s;
  return isNotBlank(msgState.eventID) || // isNotBlank(msgState.eventSeqNo) || isNotBlank(msgState.eventSubSeqNo) ||
    (msgState.idVals && msgState.idVals.some(isNotBlank)) ||
    ((msgState.keyVals && msgState.keyVals.some(isNotBlank)) &&
      (msgState.seqNoVals && msgState.seqNoVals.some(isNotBlank))) ||
    (md5s && (isNotBlank(md5s.msg) || isNotBlank(md5s.userRec) || isNotBlank(md5s.rec) || isNotBlank(md5s.data)));
}

/**
 * Creates a BFK (i.e. Big Fat Key) for the given message state using EVERY available id, key and sequence number property.
 * @param {MessageState} messageState - the tracked state of a message
 * @returns {string|undefined} an "over-the-top" key for matching purposes
 */
function toMessageBFK(messageState) {
  // if (!hasMessageIdentifier(messageState)) {
  //   return undefined;
  // }
  const md5s = messageState.md5s;
  return [
    trimOrEmpty(messageState.eventID), trimOrEmpty(messageState.eventSeqNo), trimOrEmpty(messageState.eventSubSeqNo),
    trimOrEmpty(messageState.id), trimOrEmpty(messageState.key), trimOrEmpty(messageState.seqNo),
    trimOrEmpty(md5s && md5s.msg), trimOrEmpty(md5s && md5s.userRec), trimOrEmpty(md5s && md5s.rec),
    trimOrEmpty(md5s && md5s.data)
  ].join('|');
}

/**
 * Returns true if the given unusable record state has at least one non-blank identifier; otherwise false.
 * @param {UnusableRecordState} recState - the tracked state of an unusable record
 * @return {boolean}
 */
function hasUnusableRecordIdentifier(recState) {
  const md5s = recState.md5s;
  return isNotBlank(recState.eventID) || // isNotBlank(recState.eventSeqNo) || isNotBlank(recState.eventSubSeqNo) ||
    (md5s && (isNotBlank(md5s.userRec) || isNotBlank(md5s.rec) || isNotBlank(md5s.data)));
}

/**
 * Creates a BFK (i.e. Big Fat Key) for the given unusable record state using EVERY available id, key and
 * sequence number property.
 * @param {UnusableRecordState} recordState - the tracked state of an unusable record
 * @returns {string|undefined} an "over-the-top key" for matching purposes
 */
function toUnusableRecordBFK(recordState) {
  // if (!hasUnusableRecordIdentifier(recordState)) {
  //   return undefined;
  // }
  const md5s = recordState.md5s;
  return [
    trimOrEmpty(recordState.eventID), trimOrEmpty(recordState.eventSeqNo), trimOrEmpty(recordState.eventSubSeqNo),
    trimOrEmpty(md5s && md5s.userRec), trimOrEmpty(md5s && md5s.rec), trimOrEmpty(md5s && md5s.data)
  ].join('|');
}

function setStatePropertyToPrevious(currState, propertyName, prevState, targetDesc, context) {
  const currStateProperty = currState[propertyName];
  const prevStateProperty = prevState[propertyName];
  if (context.traceEnabled) {
    if (currStateProperty) {
      context.trace(`Replacing ${targetDesc} existing "${propertyName}" state ${JSON.stringify(currStateProperty)} with previous state ${JSON.stringify(prevStateProperty)}`);
    } else {
      context.trace(`Setting ${targetDesc} "${propertyName}" state to previous state ${JSON.stringify(prevStateProperty)}`);
    }
  }
  currState[propertyName] = prevStateProperty;
}

function restoreMessageAndRejectedMessageStates(batch, item, context) {
  // Collect all of the batch's messages and rejected messages into a single list of unique messages
  const allMessages = batch.allMessages();

  // No messages for which to restore their state
  if (allMessages.length <= 0) return;

  const states = batch.states;

  const [prevMsgStatesByBfk, prevMsgStatesByMsg] = cachePrevStatesByBfkOrMsg(item.messageStates, allMessages, states, context);
  const [prevRejMsgStatesByBfk, prevRejMsgStatesByMsg] = cachePrevStatesByBfkOrMsg(item.rejectedMessageStates, allMessages, states, context);

  // Restore message's states (if any)
  const messages = batch.messages.concat([]); // copy messages
  messages.forEach(msg => {
    const currState = states.get(msg);

    const bfk = hasMessageIdentifier(currState) ? toMessageBFK(currState) : undefined;

    const prevMsgState = bfk ? prevMsgStatesByBfk.get(bfk) : prevMsgStatesByMsg.get(msg);
    const prevRejMsgState = bfk ? prevRejMsgStatesByBfk.get(bfk) : prevRejMsgStatesByMsg.get(msg);
    const prevState = prevMsgState || prevRejMsgState;

    const targetDesc = `${prevRejMsgState ? 'previously rejected ' : ''}message (${currState.msgDesc})`;

    if (prevState) {
      // If the message was previously rejected, then move it from the batch's [active] messages to its rejected messages
      if (prevRejMsgState) {
        batch.moveMessageToRejected(msg);
      }
      // Update the message's current state with its matched prior message or rejected message state
      if (context.traceEnabled) context.trace(`Matched ${targetDesc} to previous state (${JSON.stringify(prevState)}) with matching ${bfk ? 'BFK' : 'message, user record or record'}`);
      setMessageStateToPrev(currState, prevState, targetDesc, context);
    } else {
      if (context.traceEnabled) context.trace(`Could NOT match ${targetDesc} to any previous state using ${bfk ? `BFK (${JSON.stringify(bfk)})` : 'message, user record or record'}\n- current state (${JSON.stringify(currState)})\n- previous msg states ${JSON.stringify(item.messageStates)}\n- previous rejected msg states ${JSON.stringify(item.rejectedMessageStates)}`);
    }
  });

  // Restore rejected messages' states (if any)
  const rejectedMessages = batch.rejectedMessages.concat([]); // copy rejected messages
  rejectedMessages.forEach(msg => {
    const currState = states.get(msg);

    const bfk = hasMessageIdentifier(currState) ? toMessageBFK(currState) : undefined;

    const prevRejMsgState = bfk ? prevRejMsgStatesByBfk.get(bfk) : prevRejMsgStatesByMsg.get(msg);
    const prevMsgState = bfk ? prevMsgStatesByBfk.get(bfk) : prevMsgStatesByMsg.get(msg);
    const prevState = prevRejMsgState || prevMsgState;

    const targetDesc = `${!prevRejMsgState ? 'newly ' : ''}rejected message (${currState.msgDesc})`;

    if (prevState) {
      // Update the message's current state with its matched prior message or rejected message state
      if (context.traceEnabled) context.trace(`Matched ${targetDesc} to previous state (${JSON.stringify(prevState)}) with matching ${bfk ? 'BFK' : 'message, user record or record'}`);
      setMessageStateToPrev(currState, prevState, targetDesc, context);
    } else {
      if (context.traceEnabled) context.trace(`Could NOT match ${targetDesc} to any previous state using ${bfk ? `BFK (${JSON.stringify(bfk)})` : 'message, user record or record'}\n- current state (${JSON.stringify(currState)})\n- previous rejected msg states ${JSON.stringify(item.rejectedMessageStates)}\n- previous msg states ${JSON.stringify(item.messageStates)}`);
    }
  });
}

function cachePrevStatesByBfkOrMsg(prevMsgStates, messages, states, context) {
  const prevStatesByBfk = new Map();
  const prevStatesByMsg = new Map();

  // Check if any previous message states were actually loaded or not
  if (prevMsgStates && prevMsgStates.length > 0) {
    // Cache the previous message states by their BFKs ("Big Fat Keys") or by their matching message, user record or record
    prevMsgStates.forEach((prevState, i) => {
      if (hasMessageIdentifier(prevState)) {
        const bfk = toMessageBFK(prevState);
        context.trace(`###### prev BFK [${i}] = "${bfk}"`);
        prevStatesByBfk.set(bfk, prevState);
      }
      else if (prevState.message) {
        const msg = messages.find(m => deepEqual(m, prevState.message));
        if (msg) prevStatesByMsg.set(msg, prevState);
      }
      else if (prevState.userRecord) {
        const msg = messages.find(m => deepEqual(states.get(m).userRecord, prevState.userRecord));
        if (msg) prevStatesByMsg.set(msg, prevState);
      }
      else if (prevState.record) {
        const msg = messages.find(m => deepEqual(states.get(m).record, prevState.record));
        if (msg) prevStatesByMsg.set(msg, prevState);
      }
    });
  }

  return [prevStatesByBfk, prevStatesByMsg];
}

function setMessageStateToPrev(currState, prevState, targetDesc, context) {
  setStatePropertyToPrevious(currState, TaskMapNames.ones, prevState, targetDesc, context);
  setStatePropertyToPrevious(currState, TaskMapNames.alls, prevState, targetDesc, context);
  setStatePropertyToPrevious(currState, TaskMapNames.discards, prevState, targetDesc, context);
}

function restoreUnusableRecordStates(batch, item, context) {
  const unusableRecords = batch.unusableRecords;

  // No unusable records for which to restore their state
  if (!unusableRecords || unusableRecords.length <= 0) return;

  const states = batch.states;
  const prevStates = item.unusableRecordStates;

  // Check if any previous unusable record states were actually loaded or not
  if (prevStates && prevStates.length > 0) {
    const [prevStatesByBfk, prevStatesByUnusableRec] = cachePrevURecStateByBfkOrURec(prevStates, unusableRecords, states);

    // Restore previous unusable record states (if any)
    unusableRecords.forEach(unusableRecord => {
      const currState = states.get(unusableRecord);
      const targetDesc = `unusable record (${currState.recDesc})`;

      const bfk = hasUnusableRecordIdentifier(currState) ? toUnusableRecordBFK(currState) : undefined;
      const prevState = bfk ? prevStatesByBfk.get(bfk) : prevStatesByUnusableRec.get(unusableRecord);

      if (prevState) {
        if (context.traceEnabled) context.trace(`Matched ${targetDesc} to previous state (${JSON.stringify(prevState)}) with matching ${bfk ? `BFK` : `user record or record`}`);
        setStatePropertyToPrevious(currState, TaskMapNames.discards, prevState, targetDesc, context);
      } else {
        context.trace(`Could NOT match ${targetDesc} to any previous states\n- current state (${JSON.stringify(currState)}) \n- previous states ${JSON.stringify(item.unusableRecordStates)}`);
      }
    });
  }
}

function cachePrevURecStateByBfkOrURec(prevStates, unusableRecords, states) {
  const prevStatesByBfk = new Map();
  const prevStatesByUnusableRec = new Map();

  // Cache the previous unusable record states by their BFKs ("Big Fat Keys") and/or by their matching unusable record
  prevStates.forEach(prevState => {
    if (hasUnusableRecordIdentifier(prevState)) {
      const bfk = toUnusableRecordBFK(prevState);
      prevStatesByBfk.set(bfk, prevState);
    }
    else if (prevState.unusableRecord) {
      const unusableRec = unusableRecords.find(ur => deepEqual(ur, prevState.unusableRecord));
      if (unusableRec) {
        prevStatesByUnusableRec.set(unusableRec, prevState);
      }
    }
    else if (prevState.userRecord) {
      const unusableRec = unusableRecords.find(ur => deepEqual(states.get(ur).userRecord, prevState.userRecord));
      if (unusableRec) {
        prevStatesByUnusableRec.set(unusableRec, prevState);
      }
    }
    else if (prevState.record) {
      const unusableRec = unusableRecords.find(ur => deepEqual(states.get(ur).record, prevState.record));
      if (unusableRec) {
        prevStatesByUnusableRec.set(unusableRec, prevState);
      }
    }
  });

  return [prevStatesByBfk, prevStatesByUnusableRec];
}

// function restoreBatchState(batch, item, context) {
//   const targetDesc = `batch (${batch.shardOrEventID})`;
//   const states = batch.states;
//   const prevState = item.batchState;
//
//   let currState = states.get(batch);
//   if (!currState) {
//     context.warn(`Setting ${targetDesc} missing current state to new empty object`);
//     currState = {};
//     states.set(batch, currState);
//   }
//
//   // Restore previous batch state (if any)
//   if (prevState) {
//     setStatePropertyToPrevious(currState, TaskMapNames.alls, prevState, targetDesc, context);
//   } else {
//     context.trace(`Could NOT update ${targetDesc} state, since no previous batch state for current state (${JSON.stringify(currState)})`);
//   }
// }

function took(startTimeInMs) {
  return `- Took ${(Date.now() - startTimeInMs)} ms`;
}
