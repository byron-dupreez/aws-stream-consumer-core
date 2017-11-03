'use strict';

/**
 * Utilities for generating samples of various AWS stream consumer artifacts for testing.
 * @author Byron du Preez
 */
exports._ = '_'; //IDE workaround

// For configuration
exports.generateSampleMD5s = generateSampleMD5s;
exports.resolveSampleEventIdAndSeqNos = resolveSampleEventIdAndSeqNos;
exports.sampleMessageIdsAndSeqNos = sampleMessageIdsAndSeqNos;
exports.sampleResolveMessageIdsAndSeqNos = sampleResolveMessageIdsAndSeqNos;

// =====================================================================================================================
// For configuration
// =====================================================================================================================

const crypto = require('crypto');

function md5Sum(data) {
  return crypto.createHash('md5').update(data).digest("hex");
}

function generateSampleMD5s(msg, rec, userRec) {
  const data = rec && rec.kinesis && rec.kinesis.data;
  return {
    msg: (msg && md5Sum(JSON.stringify(msg))) || undefined,
    rec: (rec && md5Sum(JSON.stringify(rec))) || undefined,
    userRec: (userRec && md5Sum(JSON.stringify(userRec))) || undefined,
    data: (data && md5Sum(data)) || undefined
  };
}

function resolveSampleEventIdAndSeqNos(rec, userRec) {
  const eventSeqNo = rec && ((rec.kinesis && rec.kinesis.sequenceNumber) || (rec.dynamodb && rec.dynamodb.SequenceNumber));
  return {
    eventID: (rec && rec.eventID) || undefined,
    eventSeqNo: eventSeqNo || undefined,
    userRec: (userRec && userRec.subSequenceNumber) || undefined
  };
}

function sampleMessageIdsAndSeqNos(ids, keys, seqNos) {
  return {ids: ids, keys: keys, seqNos: seqNos};
}

function sampleResolveMessageIdsAndSeqNos(messageIdsAndSeqNos) {
  const isArray = Array.isArray(messageIdsAndSeqNos);
  let i = 0;
  // noinspection JSUnusedLocalSymbols
  return (message, record, userRecord, eventIdAndSeqNos, md5s, context) => {
    return isArray ? messageIdsAndSeqNos[i++] : messageIdsAndSeqNos;
  }
}