'use strict';

const strings = require('core-functions/strings');
const stringifyKeyValuePairs = strings.stringifyKeyValuePairs;
const stringify = strings.stringify;
const isNotBlank = strings.isNotBlank;

const any = require('core-functions/any');
const defined = any.defined;

const settings = require('./settings');

const defaultStringifyKeyValuePairsOpts = {keyValueSeparator: ':', pairSeparator: '|'};

const FatalError = require('core-functions/errors').FatalError;

/**
 * Utilities and functions to be used by a stream consumer to identify messages, which is necessary for sequencing, persisting & idempotency.
 * @module aws-stream-consumer/sequencing
 * @author Byron du Preez
 */
exports._ = '_'; //IDE workaround

exports.generateAndSetMD5s = generateAndSetMD5s;
exports.resolveAndSetEventIdAndSeqNos = resolveAndSetEventIdAndSeqNos;
exports.resolveAndSetMessageIdsAndSeqNos = resolveAndSetMessageIdsAndSeqNos;

exports.setPrevMessage = setPrevMessage;
exports.setNextMessage = setNextMessage;

exports.getMessageStateKeySeqNoId = getMessageStateKeySeqNoId;

exports.getEventIdAndSeqNos = getEventIdAndSeqNos;

/**
 * Default values to use during sequencing
 * @namespace {SequencingDefaults} defaults
 */
const defaults = {
  stringifyIdsOpts: defaultStringifyKeyValuePairsOpts,
  stringifyKeysOpts: defaultStringifyKeyValuePairsOpts,
  stringifySeqNosOpts: defaultStringifyKeyValuePairsOpts
};
exports.defaults = defaults;

/**
 * Generates MD5 message digest(s) for the given message, its record and its optional user record using the configured
 * `generateMD5s` function and then updates the given message's or unusable record's state with the generate MD5s.
 * @param {Message} message - the message for which to generate MD5 message digest(s)
 * @param {Record} record - the record for which to generate MD5 message digest(s)
 * @param {UserRecord|undefined} [userRecord] - the optional user record (if applicable) for which to generate MD5 message digest(s)
 * @param {MessageState|UnusableRecordState} state - the message's or unusable record's tracked state to update with the generated MD5s
 * @param {StreamConsumerContext} context - the context to use
 * @param {GenerateMD5s} [context.streamProcessing.generateMD5s]
 * @return {MD5s} the generated MD5 message digest(s)
 */
function generateAndSetMD5s(message, record, userRecord, state, context) {
  // Get the generateMD5s function to use
  const generateMD5s = settings.getGenerateMD5sFunction(context);

  if (!generateMD5s) {
    const errMsg = `FATAL - Cannot generate MD5s, since no 'generateMD5s' function is configured. Configure one & redeploy ASAP!`;
    context.error(errMsg);
    throw new FatalError(errMsg);
  }

  // Generate the MD5 message digest(s)
  const md5s = generateMD5s(message, record, userRecord) || {};

  // Set the generated MD5 message digest(s) on the given message's state or unusable record's state
  setMD5s(state, md5s);

  return md5s;
}

/**
 * Resolves the eventID, eventSeqNo and optional eventSubSeqNo for the given record & optional user record using the
 * configured `resolveEventIdAndSeqNos` function and then updates the given message or unusable record state with the
 * resolved values.
 * @param {Record} record - the record from which to resolve its eventID and eventSeqNo
 * @param {UserRecord|undefined} [userRecord] - the optional user record (if applicable) from which to resolve its eventSubSeqNo
 * @param {MessageState|UnusableRecordState} state - the message's or unusable record's tracked state to update with the resolved event id and sequence numbers
 * @param {StreamConsumerContext} context - the context to use
 * @param {ResolveEventIdAndSeqNos} [context.streamProcessing.resolveEventIdAndSeqNos]
 * @return {EventIdAndSeqNos} the resolved event id & sequence number(s)
 */
function resolveAndSetEventIdAndSeqNos(record, userRecord, state, context) {
  // Get the resolveEventIdAndSeqNos function to use
  const resolveEventIdAndSeqNos = settings.getResolveEventIdAndSeqNosFunction(context);

  if (!resolveEventIdAndSeqNos) {
    const errMsg = `FATAL - Cannot resolve event id & sequence numbers, since no 'resolveEventIdAndSeqNos' function is configured. Configure one & redeploy ASAP!`;
    context.error(errMsg);
    throw new FatalError(errMsg);
  }

  try {
    // Resolve the event id & sequence number(s)
    const eventIdAndSeqNos = resolveEventIdAndSeqNos(record, userRecord) || {};

    // Set the event id & sequence number(s) on the given message's state or unusable record's state
    setEventIdAndSeqNos(state, eventIdAndSeqNos);

    return eventIdAndSeqNos;

  } finally {
    // Cache a description of the record for logging purposes
    resolveAndSetRecordDesc(state);
  }
}

/**
 * Resolves & caches the id(s), key(s) & sequence number(s) for the given message on its given state using the
 * configured `resolveMessageIdsAndSeqNos` function, but only after ensuring that the message's eventID, eventSeqNo,
 * optional eventSubSeqNo & MD5s are first resolved/generated & cached on its state.
 * @param {Message} message - the message for which to resolve its ids, keys, sequence numbers, eventID and eventSeqNo
 * @param {MessageState} state - the message's tracked state to update with the resolved ids and sequence numbers
 * @param {StreamConsumerContext} context - the context to use
 * @param {ResolveMessageIdsAndSeqNos} [context.streamProcessing.resolveMessageIdsAndSeqNos]
 * @returns {MessageIdsAndSeqNos}
 */
function resolveAndSetMessageIdsAndSeqNos(message, state, context) {
  // Get the resolveMessageIdsAndSeqNos function to use
  const resolveMessageIdsAndSeqNos = settings.getResolveMessageIdsAndSeqNosFunction(context);

  if (!resolveMessageIdsAndSeqNos) {
    const errMsg = `FATAL - Cannot resolve message ids & sequence numbers, since no 'resolveMessageIdsAndSeqNos' function is configured. Configure one & redeploy ASAP!`;
    context.error(errMsg);
    throw new FatalError(errMsg);
  }

  const record = state.record;
  const userRecord = state.userRecord;

  // Get or generate & set the message's & its record's MD5 message digests on its state (if NOT previously generated)
  const md5s = state.md5s ? state.md5s : generateAndSetMD5s(message, record, userRecord, state, context);

  // Get or resolve & set the message's record's eventID, eventSeqNo & eventSubSeqNo on its state (if NOT previously resolved)
  const eventIdAndSeqNos = isNotBlank(state.eventID) && isNotBlank(state.eventSeqNo) ?
    getEventIdAndSeqNos(state) : resolveAndSetEventIdAndSeqNos(record, userRecord, state, context);

  try {
    // Resolve the message's ids, keys & sequence numbers
    const messageIdsAndSeqNos = resolveMessageIdsAndSeqNos(message, record, userRecord, eventIdAndSeqNos, md5s, context);

    // Cache the message's ids, keys & sequence numbers on its state
    setMessageIdsAndSeqNos(state, messageIdsAndSeqNos);

    return messageIdsAndSeqNos;

  } finally {
    // Cache a description of the message for logging purposes
    resolveAndSetMessageDesc(state);

    // Clear the message state's previous and next message (if any) to prepare for subsequent sorting
    setPrevMessage(state, undefined);
    setNextMessage(state, undefined);
  }
}

/**
 * Updates the given message's state or unusable record's state with the given message's record's event id and event
 * sequence number(s).
 * @param {MessageState|UnusableRecordState} state - the message's state or unusable record's state to be updated
 * @param {EventIdAndSeqNos} eventIdAndSeqNos - the resolved event id and sequence number(s)
 * @returns {MessageState|UnusableRecordState} the given state
 */
function setEventIdAndSeqNos(state, eventIdAndSeqNos) {
  const {eventID, eventSeqNo, eventSubSeqNo} = eventIdAndSeqNos || {};
  state.eventID = eventID;
  state.eventSeqNo = eventSeqNo;
  if (isNotBlank(eventSubSeqNo)) state.eventSubSeqNo = eventSubSeqNo;
  return state;
}

/**
 * Updates the given message's state or unusable record's state with the given MD5 message digest(s).
 * @param {MessageState|UnusableRecordState} state - the message's state or unusable record's state to be updated
 * @param {MD5s|undefined} [md5s] - the generated MD5 message digest(s)
 * @returns {MessageState|UnusableRecordState} the given state
 */
function setMD5s(state, md5s) {
  state.md5s = md5s || {};
  return state;
}

/**
 * Updates the given message state with the given message ids, keys & sequence numbers & event id & sequence numbers.
 * @param {MessageState} state - the message state to be updated
 * @param {MessageIdsAndSeqNos} messageIdsAndSeqNos
 */
function setMessageIdsAndSeqNos(state, messageIdsAndSeqNos) {
  const {ids, keys, seqNos} = messageIdsAndSeqNos || {};

  setIds(state, ids);
  setKeys(state, keys);
  setSeqNos(state, seqNos);

  return state;
}

function setIds(messageState, ids) {
  Object.defineProperty(messageState, 'ids', {value: ids, enumerable: false, writable: true, configurable: true});

  const idValues = ids ? ids.map(kv => kv[1]) : [];
  Object.defineProperty(messageState, 'idValues',
    {value: idValues, enumerable: false, writable: true, configurable: true});

  const idVals = idValues.map(v => defined(v) ? stringify(v) : v);
  Object.defineProperty(messageState, 'idVals',
    {value: idVals, enumerable: false, writable: true, configurable: true});

  messageState.id = stringifyKeyValuePairs(ids, defaults.stringifyIdsOpts);
}

function setKeys(messageState, keys) {
  Object.defineProperty(messageState, 'keys', {value: keys, enumerable: false, writable: true, configurable: true});

  const keyValues = keys ? keys.map(kv => kv[1]) : [];
  Object.defineProperty(messageState, 'keyValues',
    {value: keyValues, enumerable: false, writable: true, configurable: true});

  const keyVals = keyValues.map(v => defined(v) ? stringify(v) : v);
  Object.defineProperty(messageState, 'keyVals',
    {value: keyVals, enumerable: false, writable: true, configurable: true});

  messageState.key = stringifyKeyValuePairs(keys, defaults.stringifyKeysOpts);
}

function setSeqNos(messageState, seqNos) {
  Object.defineProperty(messageState, 'seqNos', {value: seqNos, enumerable: false, writable: true, configurable: true});

  const seqNoValues = seqNos ? seqNos.map(kv => kv[1]) : [];
  Object.defineProperty(messageState, 'seqNoValues',
    {value: seqNoValues, enumerable: false, writable: true, configurable: true});

  const seqNoVals = seqNoValues.map(v => defined(v) ? stringify(v) : v);
  Object.defineProperty(messageState, 'seqNoVals',
    {value: seqNoVals, enumerable: false, writable: true, configurable: true});

  messageState.seqNo = stringifyKeyValuePairs(seqNos, defaults.stringifySeqNosOpts);
}

function setPrevMessage(messageState, prevMessage) {
  Object.defineProperty(messageState, 'prevMessage', {
    value: prevMessage, enumerable: false, writable: true, configurable: true
  });
}

function setNextMessage(messageState, nextMessage) {
  Object.defineProperty(messageState, 'nextMessage', {
    value: nextMessage, enumerable: false, writable: true, configurable: true
  });
}

function getMessageStateKeySeqNoId(messageState) {
  const key = messageState.key;
  const seqNo = messageState.seqNo;
  const id = messageState.id;
  return `${key}, ${seqNo}${id !== `${key}|${seqNo}` ? `, ${id}` : ''}`;
}

function getEventIdAndSeqNos(state) {
  return state ? {eventID: state.eventID, eventSeqNo: state.eventSeqNo, eventSubSeqNo: state.eventSubSeqNo} : {};
}

function resolveAndSetRecordDesc(state) {
  const desc = isNotBlank(state.eventID) ? resolveEventDesc(state) :
    state.md5s ? resolveMD5Desc(state.md5s) : undefined;
  Object.defineProperty(state, 'recDesc', {value: desc, enumerable: false, writable: true, configurable: true});
}

function resolveAndSetMessageDesc(state) {
  const desc = state.ids && state.ids.length > 0 ? `id:${state.ids.map(kv => kv[1]).join(';')}` :
    // state.keys && state.keys.length > 0 && state.seqNos && state.seqNos.length > 0 ?
    //   `key:${state.keys.map(kv => kv[1]).join(';')},seq#:${state.seqNos.map(kv => kv[1]).join(';')}` :
    isNotBlank(state.eventID) ? resolveEventDesc(state) :
      state.md5s ? resolveMD5Desc(state.md5s) : undefined;
  Object.defineProperty(state, 'msgDesc', {value: desc, enumerable: false, writable: true, configurable: true});
}

function resolveEventDesc(state) {
  const eventID = state.eventID;
  const seqNo = state.eventSeqNo;
  const pos = eventID.indexOf(':');
  const seqNoOrNone = isNotBlank(seqNo) && (pos === -1 || eventID.substring(pos + 1) !== seqNo) ? `,seq#:${seqNo}` : '';
  return `eventID:${eventID}${seqNoOrNone}${isNotBlank(state.eventSubSeqNo) ? `,subSeq#:${state.eventSubSeqNo}` : ''}`;
}

function resolveMD5Desc(md5s) {
  return isNotBlank(md5s.msg) ? `msgMD5:${md5s.msg}` :
    isNotBlank(md5s.userRec) ? `userRecMD5:${md5s.userRec}` :
      isNotBlank(md5s.rec) ? `recMD5:${md5s.rec}` :
        isNotBlank(md5s.data) ? `dataMD5:${md5s.data}` : undefined;
}