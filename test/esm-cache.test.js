'use strict';

/**
 * Unit tests for aws-stream-consumer-core/esm-cache.js
 * @author Byron du Preez
 */

const test = require('tape');

// The test subject
const esmCache = require('../esm-cache');
const clearCache = esmCache.clearCache;

const samples = require('./samples');

const streamProcessing = require("../stream-processing");

function noop() {
}

function generateMockLambda(err, responsesByFunction, ms) {
  return {
    listEventSourceMappings(params, callback) {
      console.log(`Simulating AWS.Lambda listEventSourceMappings with ${JSON.stringify(params)})`);
      setTimeout(() => {
        if (err)
          callback(err, null);
        else
          callback(null, responsesByFunction['listEventSourceMappings']);
      }, ms);
    },

    updateEventSourceMapping(params, callback) {
      console.log(`Simulating AWS.Lambda updateEventSourceMapping with ${JSON.stringify(params)})`);
      setTimeout(() => {
        if (err)
          callback(err, null);
        else
          callback(null, responsesByFunction['updateEventSourceMapping']);
      }, ms);

    }
  };
}

test('disableEventSourceMapping with avoidEsmCache false', t => {
  const avoidEsmCache = false;

  const functionName = 'test-function';
  const streamName = 'TestStream_DEV';
  const region = 'eu-west-1';

  const eventSourceArn = samples.sampleKinesisEventSourceArn(region, streamName);
  const event = samples.sampleKinesisEventWithSampleRecord(undefined, undefined, undefined, {a: 1}, eventSourceArn, region);

  const alias = 'dev';
  const invokedFunctionArn = samples.sampleInvokedFunctionArn(region, functionName, alias);
  const invokedFunctionName = `${functionName}:${alias}`;
  const awsContext = samples.sampleAwsContext(functionName, '12', invokedFunctionArn, 2000);

  const stdOptions = require('../default-options.json');
  const streamProcessingOptions = {streamType: "kinesis"};

  const context = streamProcessing.configureStreamProcessingWithSettings({}, {}, streamProcessingOptions,
    stdOptions, event, awsContext, false, noop);

  const esm = {
    UUID: '363532fe-404a-42ef-bf02-d4912ab3f6c3',
    EventSourceArn: eventSourceArn
  };
  const mappings = {EventSourceMappings: [esm]};

  context.lambda = generateMockLambda(null, {listEventSourceMappings: mappings, updateEventSourceMapping: {}}, 1);

  clearCache();
  esmCache.disableEventSourceMapping(invokedFunctionName, streamName, avoidEsmCache, context).then(
    disabled => {
      t.equals(disabled, true, `event source mapping must be disabled`);
      t.equals(clearCache(), 1, `clearCache() must have deleted 1 cached promises`);
      t.end();
    },
    err => {
      clearCache();
      t.end(err);
    }
  );
});

test('disableEventSourceMapping with avoidEsmCache false and no mappings', t => {
  const avoidEsmCache = false;

  const functionName = 'test-function';
  const streamName = 'TestStream_DEV';
  const region = 'eu-west-1';

  const eventSourceArn = samples.sampleKinesisEventSourceArn(region, streamName);
  const event = samples.sampleKinesisEventWithSampleRecord(undefined, undefined, undefined, {a: 1}, eventSourceArn, region);

  const alias = 'dev';
  const invokedFunctionArn = samples.sampleInvokedFunctionArn(region, functionName, alias);
  const invokedFunctionName = `${functionName}:${alias}`;
  const awsContext = samples.sampleAwsContext(functionName, '12', invokedFunctionArn, 2000);

  const stdOptions = require('../default-options.json');
  const streamProcessingOptions = {streamType: "kinesis"};

  const context = streamProcessing.configureStreamProcessingWithSettings({}, {}, streamProcessingOptions, stdOptions,
    event, awsContext, false, noop);

  const mappings = {EventSourceMappings: []};

  context.lambda = generateMockLambda(null, {listEventSourceMappings: mappings, updateEventSourceMapping: {}}, 1);

  clearCache();
  esmCache.disableEventSourceMapping(invokedFunctionName, streamName, avoidEsmCache, context).then(
    disabled => {
      t.equals(disabled, false, `non-existent event source mapping must NOT be disabled`);
      t.equals(clearCache(), 1, `clearCache() must have deleted 1 cached promises`);
      t.end();
    },
    err => {
      clearCache();
      t.end(err);
    }
  );
});

test('disableEventSourceMapping with avoidEsmCache true', t => {
  const avoidEsmCache = true;

  const functionName = 'test-function';
  const streamName = 'TestStream_DEV';
  const region = 'eu-west-1';

  const eventSourceArn = samples.sampleKinesisEventSourceArn(region, streamName);
  const event = samples.sampleKinesisEventWithSampleRecord(undefined, undefined, undefined, {a: 1}, eventSourceArn, region);

  const alias = 'dev';
  const invokedFunctionArn = samples.sampleInvokedFunctionArn(region, functionName, alias);
  const invokedFunctionName = `${functionName}:${alias}`;
  const awsContext = samples.sampleAwsContext(functionName, '12', invokedFunctionArn, 2000);

  const stdOptions = require('../default-options.json');
  const streamProcessingOptions = {streamType: "kinesis"};

  const context = streamProcessing.configureStreamProcessingWithSettings({}, {}, streamProcessingOptions,
    stdOptions, event, awsContext, false, noop);

  const esm = {
    UUID: '363532fe-404a-42ef-bf02-d4912ab3f6c3',
    EventSourceArn: eventSourceArn
  };
  const mappings = {EventSourceMappings: [esm]};

  context.lambda = generateMockLambda(null, {listEventSourceMappings: mappings, updateEventSourceMapping: {}}, 1);

  clearCache();
  esmCache.disableEventSourceMapping(invokedFunctionName, streamName, avoidEsmCache, context).then(
    disabled => {
      t.equals(disabled, true, `event source mapping must be disabled`);
      t.equals(clearCache(), 0, `clearCache() must have deleted 0 cached promises`);
      t.end();
    },
    err => {
      t.equals(clearCache(), 0, `clearCache() must have deleted 0 cached promises`);
      t.end(err);
    }
  );
});

test('disableEventSourceMapping with avoidEsmCache true and no mappings', t => {
  const avoidEsmCache = true;

  const functionName = 'test-function';
  const streamName = 'TestStream_DEV';
  const region = 'eu-west-1';

  const eventSourceArn = samples.sampleKinesisEventSourceArn(region, streamName);
  const event = samples.sampleKinesisEventWithSampleRecord(undefined, undefined, undefined, {a: 1}, eventSourceArn, region);

  const alias = 'dev';
  const invokedFunctionArn = samples.sampleInvokedFunctionArn(region, functionName, alias);
  const invokedFunctionName = `${functionName}:${alias}`;
  const awsContext = samples.sampleAwsContext(functionName, '12', invokedFunctionArn, 2000);

  const stdOptions = require('../default-options.json');
  const streamProcessingOptions = {streamType: "kinesis"};

  const context = streamProcessing.configureStreamProcessingWithSettings({}, {}, streamProcessingOptions, stdOptions,
    event, awsContext, false, noop);

  const mappings = {EventSourceMappings: []};

  context.lambda = generateMockLambda(null, {listEventSourceMappings: mappings, updateEventSourceMapping: {}}, 1);

  clearCache();
  esmCache.disableEventSourceMapping(invokedFunctionName, streamName, avoidEsmCache, context).then(
    disabled => {
      t.equals(disabled, false, `non-existent event source mapping must NOT be disabled`);
      t.equals(clearCache(), 0, `clearCache() must have deleted 0 cached promises`);
      t.end();
    },
    err => {
      t.equals(clearCache(), 0, `clearCache() must have deleted 0 cached promises`);
      t.end(err);
    }
  );
});