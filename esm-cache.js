'use strict';

const lambdaUtils = require('aws-core-utils/lambda-utils');

/**
 * A cache of event source mapping keys by a concatenation of the Lambda's invoked function name & its source stream name (separated by "|")
 * @type {Map.<string,EventSourceMappingKey>}
 */
const eventSourceMappingKeysByFunctionAndStream = new Map();

/**
 * A cache of event source mapping promises by their event source mapping keys
 * @type {WeakMap.<EventSourceMappingKey,Promise.<EventSourceMapping>>}
 */
const eventSourceMappingPromisesByKey = new WeakMap();

/**
 * Utilities for getting, caching & disabling event source mappings.
 * @module aws-stream-consumer/esm-cache
 * @author Byron du Preez
 */
// exports.getEventSourceMapping = getEventSourceMapping;
// exports.getEventSourceMappingViaCache = getEventSourceMappingViaCache;
exports.disableEventSourceMapping = disableEventSourceMapping;
exports.clearCache = clearCache;

/**
 * Attempts to find the event source mapping between the named Lambda function and the named Kinesis/DynamoDB stream.
 * @param {string} functionName - the invoked name of the Lambda function (including alias if applicable)
 * @param {string} streamName - the name of the Kinesis or DynamoDB stream
 * @param {LambdaAware|StreamConsumerContext} context
 * @returns {Promise.<EventSourceMapping|undefined>}
 */
function getEventSourceMapping(functionName, streamName, context) {
  const params = {FunctionName: functionName};
  const desc = `event source mapping between function (${functionName}) & stream (${streamName})`;

  return lambdaUtils.listEventSourceMappings(context.lambda, params, context).then(
    result => {
      const mappings = result.EventSourceMappings;
      const mapping = mappings.find(m => m.EventSourceArn.indexOf(streamName) !== -1);
      if (mapping) {
        if (context.traceEnabled) context.trace(`Found ${desc} - mapping: ${JSON.stringify(mapping)}`);
      } else {
        context.error(`Failed to find ${desc} - existing mappings: ${JSON.stringify(mappings)}`);
      }
      return mapping;
    },
    err => {
      context.error(`Failed to find ${desc}`, err);
      throw err;
    }
  );
}

/**
 * Attempts to resolve the event source mapping between the named Lambda function & the named Kinesis/DynamoDB stream
 * using a cache.
 * @param {string} functionName - the function name
 * @param {string} streamName - the stream name
 * @param {StreamConsumerContext} context - the context to use
 * @return {Promise.<EventSourceMapping|undefined>} a promise of the event source mapping
 */
function getEventSourceMappingViaCache(functionName, streamName, context) {
  const functionAndStream = `${functionName}|${streamName}`;

  let key = eventSourceMappingKeysByFunctionAndStream.get(functionAndStream);
  if (!key) {
    key = {key: functionAndStream};
    eventSourceMappingKeysByFunctionAndStream.set(functionAndStream, key);
  }

  const esmPromiseCached = eventSourceMappingPromisesByKey.get(key);

  const esmPromise = esmPromiseCached ?
    esmPromiseCached.catch(err => getEventSourceMapping(functionName, streamName, context)) :
    getEventSourceMapping(functionName, streamName, context);

  eventSourceMappingPromisesByKey.set(key, esmPromise);

  return esmPromise;
}

/**
 * Attempts to disable the event source mapping between the named Lambda function and the named Kinesis/DynamoDB stream.
 * @param {string} functionName - the invoked name of the Lambda function (including alias if applicable)
 * @param {string} streamName - the name of the Kinesis or DynamoDB stream
 * @param {boolean|undefined} [avoidCache] - whether to avoid using the event source mapping cache or not
 * @param {LambdaAware|StreamConsumerContext} context - the context to use
 * @returns {Promise.<boolean>} whether the event source mapping was disabled or not
 */
function disableEventSourceMapping(functionName, streamName, avoidCache, context) {
  const desc = `event source mapping between function (${functionName}) & stream (${streamName})`;

  return (avoidCache ? getEventSourceMapping : getEventSourceMappingViaCache)(functionName, streamName, context).then(
    mapping => {
      if (mapping) {
        return lambdaUtils.disableEventSourceMapping(context.lambda, functionName, mapping.UUID, context)
          .then(() => true, () => false);
      }
      context.error(`Failed to disable ${desc}, since mapping does not exit`);
      return false;
    },
    err => {
      context.error(`Failed to disable ${desc}, since failed to get mapping`, err);
      return false;
    }
  );
}

/**
 * Clears the event source mapping key and promise caches.
 * @return {number} the number of promises removed from the cache
 */
function clearCache() {
  let deletedCount = 0;

  const iter = eventSourceMappingKeysByFunctionAndStream.entries();
  let next = iter.next();
  while (!next.done) {
    const [k, key] = next.value;

    const deleted = eventSourceMappingPromisesByKey.delete(key);
    eventSourceMappingKeysByFunctionAndStream.delete(k);

    if (deleted) {
      ++deletedCount;
    }

    next = iter.next();
  }

  return deletedCount;
}