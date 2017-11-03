'use strict';

const strings = require('core-functions/strings');
const stringify = strings.stringify;

const sorting = require('core-functions/sorting');

const identify = require('./identify');
const setPrevMessage = identify.setPrevMessage;
const setNextMessage = identify.setNextMessage;
const getMessageStateKeySeqNoId = identify.getMessageStateKeySeqNoId;

const groupBy = require('lodash.groupby');
const uniq = require('lodash.uniq');

const deepEqual = require('deep-equal');
// const strict = {strict: true};

/**
 * Utilities and functions to be used by stream consumer to sequence messages.
 * @module aws-stream-consumer/sequencing
 * @author Byron du Preez
 */
exports._ = '_'; //IDE workaround

exports.prepareMessagesForSequencing = prepareMessagesForSequencing;
exports.compareSameKeyMessages = compareSameKeyMessages;
exports.compareAnyKeyMessages = compareAnyKeyMessages;
exports.sequenceMessages = sequenceMessages;

/**
 * Default values to use during sequencing
 * @namespace {SequencingDefaults} defaults
 */
const defaults = {
  keyCompareOpts: {ignoreCase: false},
  valueCompareOpts: {ignoreCase: true}
};
exports.defaults = defaults;

/**
 * Prepares and updates the given entire batch of messages for sequencing by: first normalizing all of their sequence
 * numbers parts' values against each other; and then by resolving the sortable object, which includes the sort type,
 * compare function and sortable values, for each distinct type of part; and finally by replacing the messages' sequence
 * numbers' parts' values with their corresponding sortable values and attaching their original values and corresponding
 * sortable objects as extra properties on each message's sequence numbers' part array.
 * Precondition: resolveAndSetMessageIdsAndSeqNos has already been invoked on each of the given messages
 * @param {Message[]} messages - the entire batch of messages for which to normalize
 * @param {WeakMap.<TrackedItem, AnyTrackedState>} states - a map of the state being tracked for each of the items being processed keyed by the tracked item
 * @param {StreamConsumerContext} context - the context to use
 */
function prepareMessagesForSequencing(messages, states, context) {
  if (messages.length < 2) return;

  const sequencingRequired = context.streamProcessing.sequencingRequired;

  const allSeqNos = messages.map(msg => states.get(msg).seqNos);

  // Find the maximum number of sequence number parts across all of the messages' sequence numbers
  const maxParts = allSeqNos.map(e => e.length).reduce((a, b) => Math.max(a, b));

  if (!allSeqNos.every(sns => sns.length === maxParts)) {
    context.warn(`NOT all of the messages' sequence numbers have the same number of parts (${maxParts}) - all sequence numbers ${stringify(allSeqNos)}`);
  }
  //const seqNoPartsSortablesByKey = new Array(maxParts);

  // Loop through each of the different sequence number parts
  for (let p = 0; p < maxParts; ++p) {
    // Extract all of the messages' keys for the current sequence number part
    const keys = allSeqNos.map(sns => p < sns.length ? sns[p][0] : undefined);
    // const origValues = allSeqNos.map(sns => p < sns.length ? sns[p][1] : undefined);
    const uniqueKeys = uniq(keys);
    // Drop any and all undefined or null keys
    const cleanUniqueKeys = uniqueKeys.filter(k => !!k);
    context.trace(`Seq # part[${p}]: ${cleanUniqueKeys.length} clean of ${uniqueKeys.length} unique of ${keys.length} key${keys.length !== 1 ? 's' : ''} - ${stringify(cleanUniqueKeys)}`);

    if (uniqueKeys.length > 1) {
      throwErrorOrWarn(`NOT all of the messages have the same key at sequence number part[${p}]: - keys ${stringify(keys)}; all sequence numbers ${stringify(allSeqNos)}`,
        sequencingRequired, context);
    }
    const sortablesByKey = new Map();
    //seqNoPartsSortablesByKey[p] = sortablesByKey;

    for (let k = 0; k < cleanUniqueKeys.length; ++k) {
      const key = cleanUniqueKeys[k];

      // Extract all of the messages' values for the current key & sequence number part (using undefined for any different keys' values)
      const values = allSeqNos.map(sns => p < sns.length && sns[p][0] === key ? sns[p][1] : undefined);

      // Resolve a sortable (sort type, compare function & sortable values) for the current key and part's values
      const sortable = sorting.toSortable(values, true);
      sortablesByKey.set(key, sortable);
      context.trace(`Seq # part[${p}] & key (${key}): sort type (${sortable.sortType}); sample sortable values ${stringify(sortable.sortableValues.slice(0, 3))}`); // old values ${stringify(origValues)}`);
    }

    // Set each of the sortable values on their corresponding message's sequence number part (and attach the old
    // value and sortable information to the part)
    for (let m = 0; m < allSeqNos.length; ++m) {
      const seqNo = allSeqNos[m];
      if (p < seqNo.length) {
        const seqNoPart = seqNo[p];
        const key = seqNoPart[0];

        Object.defineProperty(seqNoPart, 'oldValue', {value: seqNoPart[1], writable: true, enumerable: false});

        const sortable = sortablesByKey.get(key);
        Object.defineProperty(seqNoPart, 'sortable', {value: sortable, writable: true, enumerable: false});

        seqNoPart[1] = sortable ? sortable.sortableValues[m] : undefined;

        if (context.traceEnabled && !deepEqual(seqNoPart[1], seqNoPart.oldValue)) {
          context.trace(`Seq # part[${p}] & key (${key}) & message (${m}): sortable value (${stringify(seqNoPart[1])}); old value (${stringify(seqNoPart.oldValue)}); sortable.sortType (${sortable ? sortable.sortType : undefined})`);
        }
      }
    }
  }
  //return seqNoPartsSortablesByKey
}

/**
 * Compares the given two messages, which must have the same keys, to determine the sequence in which they must be
 * processed. Returns -1 if msg1 has the lower sequence number and hence must be processed before msg2; +1 if msg2 has
 * the lower sequence number and must be processed before msg1; otherwise 0 if msg1 and msg2 have the same sequence
 * number and can be processed in either sequence (same sequence numbers probably indicate duplicate messages too).
 * @param {Message} msg1 - the first message to compare
 * @param {Message} msg2 - the second message to compare
 * @param {WeakMap.<TrackedItem, AnyTrackedState>} states - a map of the state being tracked for each of the items being processed keyed by the tracked item
 * @param {StreamConsumerContext} context - the context to use
 * @returns {number} -1 if msg1 < msg2; +1 if msg1 > msg2; 0 if msg1 = msg2
 */
function compareSameKeyMessages(msg1, msg2, states, context) {
  const msg1State = states.get(msg1);
  const msg2State = states.get(msg2);

  const msg1Keys = msg1State.keys;
  const msg2Keys = msg2State.keys;
  const msg1Key = msg1State.key;
  const msg2Key = msg2State.key;
  if (!deepEqual(msg1Keys, msg2Keys)) {
    throw new Error(`Cannot compare sequence numbers of messages with different keys - ${msg1Key} vs ${msg2Key}`)
  }
  const msg1SeqNos = msg1State.seqNos;
  const msg2SeqNos = msg2State.seqNos;
  const msg1SeqNo = msg1State.seqNo;
  const msg2SeqNo = msg2State.seqNo;
  const len1 = msg1SeqNos.length;
  const len2 = msg2SeqNos.length;
  const maxParts = Math.max(len1, len2);
  if (len1 < maxParts) {
    context.warn(`Message 1 with key ${msg1Key} has a sequence # ${msg1SeqNo} with too few parts vs message 2 with sequence # ${msg2SeqNo} - sorting message 1 AFTER message 2`);
    return +1;
  }
  if (len2 < maxParts) {
    context.warn(`Message 2 with key ${msg2Key} has a sequence # ${msg2SeqNo} with too few parts vs message 1 with sequence # ${msg1SeqNo} - sorting message 2 AFTER message 1`);
    return -1;
  }

  // Compare each sequence number part
  for (let p = 0; p < maxParts; ++p) {
    const msg1SeqNoPart = msg1SeqNos[p];
    const msg2SeqNoPart = msg2SeqNos[p];

    // Messages current sequence # part keys must be the same, but just in case ...
    const msg1PartKey = msg1SeqNoPart[0];
    const msg2PartKey = msg2SeqNoPart[0];
    const keyComparison = sorting.compareStrings(msg1PartKey, msg2PartKey, defaults.keyCompareOpts);
    if (keyComparison !== 0) {
      context.warn(`Message 1 with key ${msg1Key} has a different sequence # part ${p} key (${msg1PartKey}) vs message 2 (${msg2PartKey}) - sorting messages based on these part keys, i.e. message 1 ${msg1SeqNo} ${keyComparison < 0 ? '<' : '>'} message 2 ${msg2SeqNo}`);
      return keyComparison;
    }

    // Messages current sequence # part keys are the same, so compare their part values
    const sortable1 = msg1SeqNoPart.sortable;
    const sortable2 = msg2SeqNoPart.sortable;

    const sortType1 = sortable1 ? sortable1.sortType : undefined;
    const sortType2 = sortable2 ? sortable2.sortType : undefined;

    if (sortType1 !== sortType2) {
      throwErrorOrWarn(`Message 1 with key ${msg1Key} has a different sequence # part ${p} key (${msg1PartKey}) sort type (${sortType1}) vs message 2 (${sortType2}) - message 1 ${msg1SeqNo} vs message 2 ${msg2SeqNo}`,
        true, context);
    }
    const sortable = sortable1 ? sortable1 : sortable2;

    const compare = sortable ? sortable.compare : undefined;
    if (!compare) {
      throwErrorOrWarn(`Missing compare function for comparison of message 1 with key ${msg1Key} sequence # part ${p} key (${msg1PartKey}) & sort type (${sortType1}) vs message 2 (${sortType2}) - message 1 ${msg1SeqNo} vs message 2 ${msg2SeqNo}`,
        true, context);
    }
    const msg1PartVal = msg1SeqNoPart[1];
    const msg2PartVal = msg2SeqNoPart[1];
    const valueComparison = compare(msg1PartVal, msg2PartVal, defaults.valueCompareOpts);
    if (valueComparison < 0) return -1;
    if (valueComparison > 0) return +1;
    // values are the same, so continue
  }
  return 0;
}

/**
 * Compares the given two messages, which can have any keys, to determine the sequence in which they must be processed.
 * Returns -1 if msg1 has the lower sequence number and hence must be processed before msg2; +1 if msg2 has
 * the lower sequence number and must be processed before msg1; otherwise 0 if msg1 and msg2 have the same sequence
 * number and can be processed in either sequence (same sequence numbers may indicate duplicate messages).
 * @param {Message} msg1 - the first message to compare
 * @param {Message} msg2 - the second message to compare
 * @param {WeakMap.<TrackedItem, AnyTrackedState>} states - a map of the state being tracked for each of the items being processed keyed by the tracked item
 * @param {StreamConsumerContext} context - the context to use
 * @returns {number} -1 if msg1 < msg2; +1 if msg1 > msg2; 0 if msg1 = msg2
 */
function compareAnyKeyMessages(msg1, msg2, states, context) {
  const msg1State = states.get(msg1);
  const msg2State = states.get(msg2);

  const msg1Key = msg1State.key;
  const msg2Key = msg2State.key;

  const msg1SeqNos = msg1State.seqNos;
  const msg2SeqNos = msg2State.seqNos;
  const msg1SeqNo = msg1State.seqNo;
  const msg2SeqNo = msg2State.seqNo;
  const len1 = msg1SeqNos.length;
  const len2 = msg2SeqNos.length;
  const maxParts = Math.max(len1, len2);
  if (len1 < maxParts) {
    context.warn(`Message 1 with key ${msg1Key} has a sequence # ${msg1SeqNo} with too few parts vs message 2 with key ${msg2Key} & sequence # ${msg2SeqNo} - sorting message 1 AFTER message 2`);
    return +1;
  }
  if (len2 < maxParts) {
    context.warn(`Message 2 with key ${msg2Key} has a sequence # ${msg2SeqNo} with too few parts vs message 1 with key ${msg1Key} & sequence # ${msg1SeqNo} - sorting message 2 AFTER message 1`);
    return -1;
  }

  // Compare each sequence number part
  for (let p = 0; p < maxParts; ++p) {
    const msg1SeqNoPart = msg1SeqNos[p];
    const msg2SeqNoPart = msg2SeqNos[p];

    // Messages current sequence # part keys must be the same, but just in case ...
    const msg1PartKey = msg1SeqNoPart[0];
    const msg2PartKey = msg2SeqNoPart[0];
    const keyComparison = sorting.compareStrings(msg1PartKey, msg2PartKey, defaults.keyCompareOpts);
    if (keyComparison !== 0) {
      context.warn(`Message 1 with key ${msg1Key} has a different sequence # part ${p} key (${msg1PartKey}) vs message 2 with key ${msg2Key} & part ${p} key (${msg2PartKey}) - sorting messages based on these part keys, i.e. message 1 ${msg1SeqNo} ${keyComparison < 0 ? '<' : '>'} message 2 ${msg2SeqNo}`);
      return keyComparison;
    }

    // Messages current sequence # part keys are the same, so compare their part values
    const sortable1 = msg1SeqNoPart.sortable;
    const sortable2 = msg2SeqNoPart.sortable;

    const sortType1 = sortable1 ? sortable1.sortType : undefined;
    const sortType2 = sortable2 ? sortable2.sortType : undefined;

    if (sortType1 !== sortType2) {
      throwErrorOrWarn(`Message 1 with key ${msg1Key} has a different sequence # part ${p} key (${msg1PartKey}) sort type (${sortType1}) vs message 2 with key ${msg2Key} & sort type(${sortType2}) - message 1 ${msg1SeqNo} vs message 2 ${msg2SeqNo}`,
        true, context);
    }
    const sortable = sortable1 ? sortable1 : sortable2;

    const compare = sortable ? sortable.compare : undefined;
    if (!compare) {
      throwErrorOrWarn(`Missing compare function for comparison of message 1 with key ${msg1Key} sequence # part ${p} key (${msg1PartKey}) & sort type (${sortType1}) vs message 2 with key ${msg2Key} & sort type (${sortType2}) - message 1 ${msg1SeqNo} vs message 2 ${msg2SeqNo}`,
        true, context);
    }
    const msg1PartVal = msg1SeqNoPart[1];
    const msg2PartVal = msg2SeqNoPart[1];
    const valueComparison = compare(msg1PartVal, msg2PartVal, defaults.valueCompareOpts);
    if (valueComparison < 0) return -1;
    if (valueComparison > 0) return +1;
    // values are the same, so continue
  }
  return 0;
}

/**
 * Sequences the given entire batch of messages by linking messages with the same keys to each other via their
 * nextMessage and prevMessage properties in sequences determined by their sequence numbers.
 * @param {Batch} batch - the batch containing the messages to be sequenced
 * @param {StreamConsumerContext} context - the context to use
 * @returns {Message[]} the first messages to be processed (i.e. all of the messages that do NOT have a defined prevMessage
 */
function sequenceMessages(batch, context) {
  const states = batch.states;
  const messages = batch.messages;

  if (messages.length < 2 || !context.streamProcessing.sequencingRequired) {
    return messages;
  }

  // First prepare the messages for sequencing by normalizing all of the messages' sequence numbers
  prepareMessagesForSequencing(messages, states, context);

  const firstMessagesToProcess = [];

  const sequencingPerKey = context.streamProcessing.sequencingPerKey;
  const comparator = sequencingPerKey ?
    (m1, m2) => compareSameKeyMessages(m1, m2, states, context):
    (m1, m2) => compareAnyKeyMessages(m1, m2, states, context);

  // Group all of the messages by the stringified versions of their keys (to avoid strict equal failures)
  const messagesByKeyString = sequencingPerKey ?
    groupBy(messages, msg => {
      const msgState = states.get(msg);
      const keys = msgState.keys;
      return keys && keys.length > 0 ? msgState.key : '?';
    }) : {'*': messages};

  // Sort all of the messages that share the same keys by their sequence numbers and then link them to each other via
  // their prevMessage and nextMessage state properties
  const keyStrings = Object.getOwnPropertyNames(messagesByKeyString);

  for (let i = 0; i < keyStrings.length; ++i) {
    const keyString = keyStrings[i];
    const msgs = messagesByKeyString[keyString];

    if (context.traceEnabled && msgs.length > 1) context.trace(`BEFORE sorting (${keyString}): ${stringify(msgs.map(m => states.get(m).seqNo))}`);

    msgs.sort(comparator);

    if (context.traceEnabled) context.trace(` AFTER sorting (${keyString}): ${stringify(msgs.map(m => states.get(m).seqNo))}`);

    let prevMessage = undefined;
    let prevMessageState = undefined;

    firstMessagesToProcess.push(msgs[0]);

    for (let m = 0; m < msgs.length; ++m) {
      const nextMessage = msgs[m];
      const nextMessageState = states.get(nextMessage);

      setPrevMessage(nextMessageState, prevMessage);

      if (prevMessageState) {
        setNextMessage(prevMessageState, nextMessage);
      }
      prevMessage = nextMessage;
      prevMessageState = nextMessageState;
    }
    if (prevMessageState) {
      setNextMessage(prevMessageState, undefined);
    }
  }

  if (context.debugEnabled) {
    const firstMessagesDetails = firstMessagesToProcess.map(m => `(${getMessageStateKeySeqNoId(states.get(m))})`).join(", ");
    context.debug(`Finished sequencing messages - found ${firstMessagesToProcess.length} first message${firstMessagesToProcess.length !== 1 ? 's' : ''} to process out of ${messages.length} message${messages.length !== 1 ? 's' : ''} - first [${firstMessagesDetails}]`);
  }

  return firstMessagesToProcess;
}

function throwErrorOrWarn(errMsg, throwError, context) {
  if (throwError) {
    const err = new Error(errMsg);
    context.error(err);
    throw err;
  } else {
    context.warn(errMsg);
  }
}