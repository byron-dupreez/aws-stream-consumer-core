'use strict';

/**
 * Unit tests for aws-stream-consumer-core/identify.js
 * @author Byron du Preez
 */

const test = require('tape');

// The test subject
const identify = require('../identify');

const samples = require('./samples');
const sampleKinesisMessageAndRecord = samples.sampleKinesisMessageAndRecord;

const samplesAsc = require('./samples-asc');
const generateSampleMD5s = samplesAsc.generateSampleMD5s;
const resolveSampleEventIdAndSeqNos = samplesAsc.resolveSampleEventIdAndSeqNos;
const sampleMessageIdsAndSeqNos = samplesAsc.sampleMessageIdsAndSeqNos;
const sampleResolveMessageIdsAndSeqNos = samplesAsc.sampleResolveMessageIdsAndSeqNos;

const strings = require('core-functions/strings');
const stringify = strings.stringify;
const stringifyKeyValuePairs = strings.stringifyKeyValuePairs;

const logging = require('logging-utils');
const LogLevel = logging.LogLevel;

const eventSourceARN = samples.sampleKinesisEventSourceArn('us-west-2', 'TestStream_DEV');

function createContext(streamType, idPropertyNames, keyPropertyNames, seqNoPropertyNames) {
  const context = {
    streamProcessing: {
      streamType: streamType,
      sequencingRequired: true,
      sequencingPerKey: true,
      consumerIdSuffix: undefined,
      idPropertyNames: idPropertyNames,
      keyPropertyNames: keyPropertyNames,
      seqNoPropertyNames: seqNoPropertyNames,
      generateMD5s: generateSampleMD5s,
      resolveEventIdAndSeqNos: resolveSampleEventIdAndSeqNos
    }
  };
  logging.configureLogging(context, {logLevel: LogLevel.TRACE});
  return context;
}

// function checkMD5s(t, md5s, expectedMD5s) {
//   t.deepEqual(md5s, expectedMD5s, `md5s must be '${expectedMD5s}'`);
// }

function checkEventIdAndSeqNos(t, state, expected) {
  t.equal(state.eventID, expected.eventID, `state.eventID must be '${expected.eventID}'`);
  t.equal(state.eventSeqNo, expected.eventSeqNo, `state.eventSeqNo must be '${expected.eventSeqNo}'`);
  t.equal(state.eventSubSeqNo, expected.eventSubSeqNo, `state.eventSubSeqNo must be '${expected.eventSubSeqNo}'`);
}

function checkMsgIdsKeysAndSeqNos(t, state, expected) {
  t.deepEqual(state.ids, expected.ids, `state.ids must be ${stringify(expected.ids)}`);

  const expectedId = expected.ids.length > 0 ? stringifyKeyValuePairs(expected.ids, identify.defaults.stringifyIdsOpts) : '';
  t.deepEqual(state.id, expectedId, `state.id must be '${expectedId}'`);

  t.deepEqual(state.keys, expected.keys, `state.keys must be ${stringify(expected.keys)}`);

  const expectedKey = expected.keys.length > 0 ? stringifyKeyValuePairs(expected.keys, identify.defaults.stringifyKeysOpts) : '';
  t.deepEqual(state.key, expectedKey, `state.key must be '${expectedKey}'`);

  t.deepEqual(state.seqNos, expected.seqNos, `state.seqNos must be ${stringify(expected.seqNos)}`);

  const expectedSeqNo = expected.seqNos.length > 0 ? stringifyKeyValuePairs(expected.seqNos, identify.defaults.stringifySeqNosOpts) : '';
  t.deepEqual(state.seqNo, expectedSeqNo, `state.seqNo must be '${expectedSeqNo}'`);
}

// =====================================================================================================================
// generateAndSetMD5s - kinesis
// =====================================================================================================================

test(`generateAndSetMD5s with NO generateMD5s function configured`, t => {
  const context = createContext('identity', ['id1', 'id2'], ['k1', 'k2'], ['n1', 'n2', 'n3']);

  const shardId = 'shardId-000000000000';
  const eventSeqNo = '49545115243490985018280067714973144582180062593244200961';

  const [msg, rec] = sampleKinesisMessageAndRecord(shardId, eventSeqNo, eventSourceARN, '123', '456', 'ABC', 10, 1, 2, 3);
  const userRec = {a: 1, b: 2, c: {d: 4}, subSequenceNumber: '007'}; // Dummy user record
  const state = {record: rec, userRecord: userRec};

  context.streamProcessing.generateMD5s = undefined;

  t.throws(() => identify.generateAndSetMD5s(msg, rec, userRec, state, context), /since no 'generateMD5s' function is configured/, `no generateMD5s function configured must throw error`);

  t.end();
});

test(`generateAndSetMD5s with a generateMD5s function configured`, t => {
  const context = createContext('identity', ['id1', 'id2'], ['k1', 'k2'], ['n1', 'n2', 'n3']);

  const shardId = 'shardId-000000000000';
  const eventSeqNo = '49545115243490985018280067714973144582180062593244200961';

  const [msg, rec] = sampleKinesisMessageAndRecord(shardId, eventSeqNo, eventSourceARN, '123', '456', 'ABC', 10, 1, 2, 3);
  const userRec = {a: 1, b: 2, c: {d: 4}, subSequenceNumber: '007'}; // Dummy user record
  const state = {record: rec, userRecord: userRec};

  const expectedMD5s = generateSampleMD5s(msg, rec, userRec);

  const md5s = identify.generateAndSetMD5s(msg, rec, userRec, state, context);

  t.deepEqual(md5s, expectedMD5s, `md5s must be ${JSON.stringify(expectedMD5s)}`);
  t.deepEqual(state.md5s, expectedMD5s, `state.md5s must be ${JSON.stringify(expectedMD5s)}`);

  t.end();
});

test(`generateAndSetMD5s with a generateMD5s function configured that throws an error`, t => {
  const context = createContext('identity', ['id1', 'id2'], ['k1', 'k2'], ['n1', 'n2', 'n3']);

  const shardId = 'shardId-000000000000';
  const eventSeqNo = '49545115243490985018280067714973144582180062593244200961';

  const [msg, rec] = sampleKinesisMessageAndRecord(shardId, eventSeqNo, eventSourceARN, '123', '456', 'ABC', 10, 1, 2, 3);
  const userRec = {a: 1, b: 2, c: {d: 4}, subSequenceNumber: '007'}; // Dummy user record
  const state = {record: rec, userRecord: userRec};

  context.streamProcessing.generateMD5s = () => { throw new Error('Boom'); };

  t.throws(() => identify.generateAndSetMD5s(msg, rec, userRec, state, context), /Boom/, `generateAndSetMD5s must throw error`);

  t.end();
});

// =====================================================================================================================
// resolveAndSetEventIdAndSeqNos - kinesis
// =====================================================================================================================

test(`resolveAndSetEventIdAndSeqNos with NO resolveEventIdAndSeqNos function configured`, t => {
  const context = createContext('identity', ['id1', 'id2'], ['k1', 'k2'], ['n1', 'n2', 'n3']);

  const shardId = 'shardId-000000000000';
  const eventSeqNo = '49545115243490985018280067714973144582180062593244200961';

  const [, rec] = sampleKinesisMessageAndRecord(shardId, eventSeqNo, eventSourceARN, '123', '456', 'ABC', 10, 1, 2, 3);
  const userRec = {a: 1, b: 2, c: {d: 4}, subSequenceNumber: '007'}; // Dummy user record
  const state = {record: rec, userRecord: userRec};

  context.streamProcessing.resolveEventIdAndSeqNos = undefined;

  t.throws(() => identify.resolveAndSetEventIdAndSeqNos(rec, userRec, state, context), /since no 'resolveEventIdAndSeqNos' function is configured/, `no resolveEventIdAndSeqNos function configured must throw error`);

  t.end();
});

test(`resolveAndSetEventIdAndSeqNos with resolveEventIdAndSeqNos function configured`, t => {
  const context = createContext('identity', ['id1', 'id2'], ['k1', 'k2'], ['n1', 'n2', 'n3']);

  const shardId = 'shardId-000000000000';
  const eventSeqNo = '49545115243490985018280067714973144582180062593244200961';

  const [, rec] = sampleKinesisMessageAndRecord(shardId, eventSeqNo, eventSourceARN, '123', '456', 'ABC', 10, 1, 2, 3);
  const eventSubSeqNo = '007';
  const userRec = {a: 1, b: 2, c: {d: 4}, subSequenceNumber: eventSubSeqNo}; // Dummy user record
  const state = {record: rec, userRecord: userRec};

  const expected = resolveSampleEventIdAndSeqNos(rec, userRec);
  // context.streamProcessing.resolveEventIdAndSeqNos = sampleResolveEventIdAndSeqNos(expected);

  const eventIdAndSeqNos = identify.resolveAndSetEventIdAndSeqNos(rec, userRec, state, context);

  t.deepEqual(eventIdAndSeqNos, expected, `eventIdAndSeqNos must be ${JSON.stringify(expected)}`);
  checkEventIdAndSeqNos(t, state, expected);

  t.end();
});

test(`resolveAndSetEventIdAndSeqNos with a resolveEventIdAndSeqNos function configured that throws an error`, t => {
  const context = createContext('identity', ['id1', 'id2'], ['k1', 'k2'], ['n1', 'n2', 'n3']);

  const shardId = 'shardId-000000000000';
  const eventSeqNo = '49545115243490985018280067714973144582180062593244200961';

  const [, rec] = sampleKinesisMessageAndRecord(shardId, eventSeqNo, eventSourceARN, '123', '456', 'ABC', 10, 1, 2, 3);
  const userRec = {a: 1, b: 2, c: {d: 4}, subSequenceNumber: '007'}; // Dummy user record
  const state = {record: rec, userRecord: userRec};

  context.streamProcessing.resolveEventIdAndSeqNos = () => { throw new Error('Bang')};

  t.throws(() => identify.resolveAndSetEventIdAndSeqNos(rec, userRec, state, context), /Bang/, `resolveAndSetEventIdAndSeqNos must throw error`);

  t.end();
});

// =====================================================================================================================
// resolveAndSetMessageIdsAndSeqNos - kinesis
// =====================================================================================================================

test(`resolveAndSetMessageIdsAndSeqNos with NO resolveMessageIdsAndSeqNos function configured`, t => {
  const context = createContext('identity', ['id1', 'id2'], ['k1', 'k2'], ['n1', 'n2', 'n3']);

  const shardId = 'shardId-000000000000';
  const eventSeqNo = '49545115243490985018280067714973144582180062593244200961';

  const [msg, rec] = sampleKinesisMessageAndRecord(shardId, eventSeqNo, eventSourceARN, '123', '456', 'ABC', 10, 1, 2, 3);
  const state = {record: rec};

  context.streamProcessing.resolveMessageIdsAndSeqNos = undefined;

  t.throws(() => identify.resolveAndSetMessageIdsAndSeqNos(msg, state, context), /since no 'resolveMessageIdsAndSeqNos' function is configured/, `no resolveMessageIdsAndSeqNos function configured must throw error`);

  t.end();
});

test(`resolveAndSetMessageIdsAndSeqNos for kinesis with all property names configured & all properties present`, t => {
  const context = createContext('identity', ['id1', 'id2'], ['k1', 'k2'], ['n1', 'n2', 'n3']);

  const shardId = 'shardId-000000000000';
  const eventSeqNo = '49545115243490985018280067714973144582180062593244200961';

  const [msg, rec] = sampleKinesisMessageAndRecord(shardId, eventSeqNo, eventSourceARN, '123', '456', 'ABC', 10, 1, 2, 3);
  const state = {record: rec};

  const expectedIds = [['id1', '123'], ['id2', '456']];
  const expectedKeys = [['k1', 'ABC'], ['k2', 10]];
  const expectedSeqNos = [['n1', 1], ['n2', 2], ['n3', 3]];
  const expected = sampleMessageIdsAndSeqNos(expectedIds, expectedKeys, expectedSeqNos);
  context.streamProcessing.resolveMessageIdsAndSeqNos = sampleResolveMessageIdsAndSeqNos(expected);

  const actual = identify.resolveAndSetMessageIdsAndSeqNos(msg, state, context);

  t.deepEqual(actual, expected, `messageIdsAndSeqNos must be ${JSON.stringify(expected)}`);
  checkMsgIdsKeysAndSeqNos(t, state, expected);

  t.end();
});

test(`resolveAndSetMessageIdsAndSeqNos for kinesis with idPropertyNames and seqNoPropertyNames NOT configured`, t => {
  const context = createContext('identity', [], ['k2'], []);

  const shardId = 'shardId-000000000000';
  const eventSeqNo = '49545115243490985018280067714973144582180062593244200961';

  const [msg, rec] = sampleKinesisMessageAndRecord(shardId, eventSeqNo, eventSourceARN, '123', 456, 'ABC', 10, 1, 2, 3);
  const state = {record: rec};

  const expectedIds = [['k2', 10], ['eventSeqNo', '49545115243490985018280067714973144582180062593244200961']];
  const expectedKeys = [['k2', 10]];
  const expectedSeqNos = [['eventSeqNo', eventSeqNo]];
  const expected = sampleMessageIdsAndSeqNos(expectedIds, expectedKeys, expectedSeqNos);
  context.streamProcessing.resolveMessageIdsAndSeqNos = sampleResolveMessageIdsAndSeqNos(expected);

  const actual = identify.resolveAndSetMessageIdsAndSeqNos(msg, state, context);

  t.deepEqual(actual, expected, `messageIdsAndSeqNos must be ${JSON.stringify(expected)}`);
  checkMsgIdsKeysAndSeqNos(t, state, expected);

  t.end();
});

test(`resolveAndSetMessageIdsAndSeqNos for kinesis with keyPropertyNames, seqNoPropertyNames & idPropertyNames NOT configured and message sequencing required`, t => {
  const context = createContext('identity', [], [], []);

  const shardId = 'shardId-000000000000';
  const eventSeqNo = '49545115243490985018280067714973144582180062593244200961';

  const [msg, rec] = sampleKinesisMessageAndRecord(shardId, eventSeqNo, eventSourceARN, '123', '457', 'ABC', 10, 1, 2, 3);
  const state = {record: rec};

  const expectedIds = [];
  const expectedKeys = [];
  const expectedSeqNos = [['eventSeqNo', eventSeqNo]];
  const expected = sampleMessageIdsAndSeqNos(expectedIds, expectedKeys, expectedSeqNos);
  context.streamProcessing.resolveMessageIdsAndSeqNos = sampleResolveMessageIdsAndSeqNos(expected);

  // Now allowed to have NO keyPropertyNames i.e. NO keys, which will force every message to be sequenced together
  // t.throws(() => identify.resolveAndSetMessageIdsAndSeqNos(msg, state, context), /Failed to resolve any keys/, `no keys must throw error`);
  const actual = identify.resolveAndSetMessageIdsAndSeqNos(msg, state, context);

  t.deepEqual(actual, expected, `messageIdsAndSeqNos must be ${JSON.stringify(expected)}`);
  checkMsgIdsKeysAndSeqNos(t, state, expected);

  t.end();
});

test(`resolveAndSetMessageIdsAndSeqNos for kinesis with keyPropertyNames, seqNoPropertyNames & idPropertyNames NOT configured and message sequencing NOT required`, t => {
  const context = createContext('identity', [], [], []);

  context.streamProcessing.sequencingRequired = false;
  const shardId = 'shardId-000000000000';
  const eventSeqNo = '49545115243490985018280067714973144582180062593244200961';

  const [msg, rec] = sampleKinesisMessageAndRecord(shardId, eventSeqNo, eventSourceARN, '123', '458', 'ABC', 10, 1, 2, 3);
  const state = {record: rec};

  const expectedIds = [];
  const expectedKeys = [];
  const expectedSeqNos = [['eventSeqNo', eventSeqNo]];
  const expected = sampleMessageIdsAndSeqNos(expectedIds, expectedKeys, expectedSeqNos);
  context.streamProcessing.resolveMessageIdsAndSeqNos = sampleResolveMessageIdsAndSeqNos(expected);

  const actual = identify.resolveAndSetMessageIdsAndSeqNos(msg, state, context);

  t.deepEqual(actual, expected, `messageIdsAndSeqNos must be ${JSON.stringify(expected)}`);
  checkMsgIdsKeysAndSeqNos(t, state, expected);

  t.end();
});

test(`resolveAndSetMessageIdsAndSeqNos for kinesis with a key property undefined and message sequencing required with a resolve error`, t => {
  const context = createContext('identity', ['id1', 'id2'], ['k1', 'k2'], ['n1', 'n2', 'n3']);

  const shardId = 'shardId-000000000000';
  const eventSeqNo = '49545115243490985018280067714973144582180062593244200961';

  const [msg, rec] = sampleKinesisMessageAndRecord(shardId, eventSeqNo, eventSourceARN, '456', '789', 'ABC', undefined, 1, 2, 3);
  const state = {record: rec};

  context.streamProcessing.resolveMessageIdsAndSeqNos = () => { throw new Error('Baddoom'); };

  t.throws(() => identify.resolveAndSetMessageIdsAndSeqNos(msg, state, context), /Baddoom/, `resolveAndSetMessageIdsAndSeqNos must throw error`);

  t.end();
});

test(`resolveAndSetMessageIdsAndSeqNos for kinesis with a key property undefined and message sequencing required, but sequencingPerKey false`, t => {
  const context = createContext('identity', ['id1', 'id2'], ['k1', 'k2'], ['n1', 'n2', 'n3']);
  context.streamProcessing.sequencingPerKey = false;

  const shardId = 'shardId-000000000000';
  const eventSeqNo = '49545115243490985018280067714973144582180062593244200961';

  const [msg, rec] = sampleKinesisMessageAndRecord(shardId, eventSeqNo, eventSourceARN, '456', '789', 'ABC', undefined, 1, 2, 3);
  const state = {record: rec};

  const expectedIds = [['id1', '123'], ['id2', '456']];
  const expectedKeys = [['k1', 'ABC'], ['k2', undefined]];
  const expectedSeqNos = [['n1', 1], ['n2', 2], ['n3', 3]];
  const expected = sampleMessageIdsAndSeqNos(expectedIds, expectedKeys, expectedSeqNos);
  context.streamProcessing.resolveMessageIdsAndSeqNos = sampleResolveMessageIdsAndSeqNos(expected);

  // t.throws(() => identify.resolveAndSetMessageIdsAndSeqNos(msg, state, context), /Failed to resolve usable keys/, `any undefined key must throw error`);
  const actual = identify.resolveAndSetMessageIdsAndSeqNos(msg, state, context);

  t.deepEqual(actual, expected, `messageIdsAndSeqNos must be ${JSON.stringify(expected)}`);
  checkMsgIdsKeysAndSeqNos(t, state, expected);

  t.end();
});

test(`resolveAndSetMessageIdsAndSeqNos for kinesis with a key property undefined and message sequencing NOT required`, t => {
  const context = createContext('identity', ['id2', 'id1'], ['k2', 'k1'], ['n3', 'n2', 'n1']);
  context.streamProcessing.sequencingRequired = false;

  const shardId = 'shardId-000000000000';
  const eventSeqNo = '49545115243490985018280067714973144582180062593244200961';

  const [msg, rec] = sampleKinesisMessageAndRecord(shardId, eventSeqNo, eventSourceARN, '444', '999', 'XYZ', undefined, -1, -2, -3);
  const state = {record: rec};

  const expectedIds = [['id2', '999'], ['id1', '444']];
  const expectedKeys = [['k2', undefined], ['k1', 'XYZ']];
  const expectedSeqNos = [['n3', -3], ['n2', -2], ['n1', -1]];
  const expected = sampleMessageIdsAndSeqNos(expectedIds, expectedKeys, expectedSeqNos);
  context.streamProcessing.resolveMessageIdsAndSeqNos = sampleResolveMessageIdsAndSeqNos(expected);

  const actual = identify.resolveAndSetMessageIdsAndSeqNos(msg, state, context);

  t.deepEqual(actual, expected, `messageIdsAndSeqNos must be ${JSON.stringify(expected)}`);
  checkMsgIdsKeysAndSeqNos(t, state, expected);

  t.end();
});

test(`resolveAndSetMessageIdsAndSeqNos for kinesis with an id property undefined and message sequencing required`, t => {
  const context = createContext('identity', ['id1', 'id2'], ['k1', 'k2'], ['n1', 'n2', 'n3']);

  const shardId = 'shardId-000000000000';
  const eventSeqNo = '49545115243490985018280067714973144582180062593244200961';

  const [msg, rec] = sampleKinesisMessageAndRecord(shardId, eventSeqNo, eventSourceARN, '123', undefined, 'ABC', 10, 1, 2, 3);
  const state = {record: rec};

  const expectedIds = [['id1', '123'], ['id2', undefined]];
  const expectedKeys = [['k1', 'ABC'], ['k2', 10]];
  const expectedSeqNos = [['n1', 1], ['n2', 2], ['n3', 3]];
  const expected = sampleMessageIdsAndSeqNos(expectedIds, expectedKeys, expectedSeqNos);
  context.streamProcessing.resolveMessageIdsAndSeqNos = sampleResolveMessageIdsAndSeqNos(expected);

  // t.throws(() => identify.resolveAndSetMessageIdsAndSeqNos(msg, state, context), /Failed to resolve usable ids/, `any undefined id must throw error`);
  const actual = identify.resolveAndSetMessageIdsAndSeqNos(msg, state, context);

  t.deepEqual(actual, expected, `messageIdsAndSeqNos must be ${JSON.stringify(expected)}`);
  checkMsgIdsKeysAndSeqNos(t, state, expected);

  t.end();
});

test(`resolveAndSetMessageIdsAndSeqNos for kinesis with a sequence number property undefined and message sequencing required`, t => {
  const context = createContext('identity', ['id1', 'id2'], ['k1', 'k2'], ['n1', 'n2', 'n3']);

  const shardId = 'shardId-000000000000';
  const eventSeqNo = '49545115243490985018280067714973144582180062593244200961';

  const [msg, rec] = sampleKinesisMessageAndRecord(shardId, eventSeqNo, eventSourceARN, '123', '555', 'ABC', '10', '1', '2', undefined);
  const state = {record: rec};

  const expectedIds = [['id1', '123'], ['id2', '555']];
  const expectedKeys = [['k1', 'ABC'], ['k2', '10']];
  const expectedSeqNos = [['n1', 1], ['n2', 2], ['n3', undefined]];
  const expected = sampleMessageIdsAndSeqNos(expectedIds, expectedKeys, expectedSeqNos);
  context.streamProcessing.resolveMessageIdsAndSeqNos = sampleResolveMessageIdsAndSeqNos(expected);

  // t.throws(() => identify.resolveAndSetMessageIdsAndSeqNos(msg, state, context), /Failed to resolve usable sequence numbers/, `any undefined sequence number must throw error`);
  const actual = identify.resolveAndSetMessageIdsAndSeqNos(msg, state, context);

  t.deepEqual(actual, expected, `messageIdsAndSeqNos must be ${JSON.stringify(expected)}`);
  checkMsgIdsKeysAndSeqNos(t, state, expected);

  t.end();
});

test(`resolveAndSetMessageIdsAndSeqNos for kinesis with no idPropertyNames defined`, t => {
  const context = createContext('identity', ['id1', 'id2'], ['k1', 'k2'], ['n1', 'n2', 'n3']);

  const shardId = 'shardId-000000000000';
  const eventSeqNo = '49545115243490985018280067714973144582180062593244200961';

  const [msg, rec] = sampleKinesisMessageAndRecord(shardId, eventSeqNo, eventSourceARN, '123', '777', 'PQR', 11, 10, 20, 30);
  const state = {record: rec};

  // Expect ids to contain a concatenation of keys and sequence numbers when no idPropertyNames are defined
  const expectedIds = [['k1', 'PQR'], ['k2', 11], ['n1', 10], ['n2', 20], ['n3', 30]];
  const expectedKeys = [['k1', 'PQR'], ['k2', 11]];
  const expectedSeqNos = [['n1', 10], ['n2', 20], ['n3', 30]];
  const expected = sampleMessageIdsAndSeqNos(expectedIds, expectedKeys, expectedSeqNos);
  context.streamProcessing.resolveMessageIdsAndSeqNos = sampleResolveMessageIdsAndSeqNos(expected);

  const actual = identify.resolveAndSetMessageIdsAndSeqNos(msg, state, context);

  t.deepEqual(actual, expected, `messageIdsAndSeqNos must be ${JSON.stringify(expected)}`);
  checkMsgIdsKeysAndSeqNos(t, state, expected);

  t.end();
});