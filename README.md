# aws-stream-consumer-core v2.1.9

Core utilities used by both `kinesis-stream-consumer` and `dynamodb-stream-consumer` for building robust AWS Lambda 
consumers of stream events from Amazon Web Services (AWS) Kinesis and DynamoDB streams respectively.

This shared library originated from a copy of the 1.0.0-beta.17 version of `aws-stream-consumer` code at commit                    
`c18e97dfeea95589e2fceda07984a267b65692f8` (dated 15 Dec 2016), which was updated with the fixes and changes made to 
`aws-stream-consumer` in the interim up to and including version `5.1.0`.

## Modules:
- `stream-consumer.js` module
  - Utilities and functions to be used to configure and robustly consume messages from an AWS Kinesis or DynamoDB stream event
- `stream-processing.js` module 
  - Utilities for configuring stream processing, which configures and determines the processing behaviour of a stream consumer
- `batch` module
  - A `Batch` class that represents and tracks the state of the current batch of records received in a Kinesis or DynamoDB stream event
- `identify` module
  - Utilities and functions to be used by a stream consumer to identify messages and records by extracting their ids, 
    sort keys, sequence numbers and MD5s, which are needed for sequencing, persisting & idempotency
- `sequencing` module
  - Utilities and functions to be used by a stream consumer to sort messages into the correct processing sequence
- `persisting` module
  - Utilities and functions to be used by a stream consumer to load its current batch's previous state (if any) from
    and save its batch's current state to DynamoDB
- `settings` module
  - Common setting-related utilities and functions to be used by a stream consumer
- `taskdef-settings` module
  - Common `TaskDef` settings & `describeItem` implementations to be used by a stream consumer
- `tracking` module
  - Common tracked state and tracking-related utilities and functions to be used by a stream consumer
- `esm-cache` module
  - An event source mapping cache used to cache an AWS Lambda's own event source mapping, which is needed in the event 
    that the Lambda needs to disable its own trigger after a fatal processing failure

## Usage
- **DO NOT USE!** These modules are **NOT** meant to be used directly and **ONLY** serve as shared core supporting 
  modules for the `kinesis-stream-consumer` and `dynamodb-stream-consumer` modules

## Installation
This module is exported as a [Node.js](https://nodejs.org) module.

Using npm:
```bash
$ {sudo -H} npm i -g npm
$ npm i aws-stream-consumer-core  --save
```

## Unit tests
This module's unit tests were developed with and must be run with [tape](https://www.npmjs.com/package/tape). The unit tests have been tested on [Node.js v6.10.3](https://nodejs.org/en/blog/release/v6.10.3).  

Install tape globally if you want to run multiple tests at once:
```bash
$ npm install tape -g
```

Run all unit tests with:
```bash
$ npm test
```
or with tape:
```bash
$ tape test/*.js
```

See the [package source](https://github.com/byron-dupreez/aws-core-utils) for more details.

## Changes
See [CHANGES.md](CHANGES.md)

## Background

The goal of an AWS stream consumer implementation is to make the process of consuming records from an AWS Kinesis or 
DynamoDB stream more robust for an AWS Lambda stream consumer by providing solutions to and workarounds for common AWS 
stream consumption issues. 

#### Common AWS stream consumption issues
1. The fundamental issue is that either all of a stream event's records must be processed successfully or an error must 
   be thrown back to AWS Lambda to trigger a replay of all of the event's records again (assuming that you don't want to
   lose any of the records). This course-grained error handling makes no distinction between persistent and transient 
   errors and does not provide a way to only reprocess unsuccessful records.
    
2. The fact that AWS stream event records should always be processed in batches from the AWS stream (to increase 
   throughput and reduce the risk of slow consumption ultimately leading to message loss), both increases the complexity
   and the chance of failures. For example, while processing a batch of 100 messages, if processing fails on only 1 
   message with a transient error, then ideally we would want to only replay that 1 failed message, but the only replay
   option is to throw an error that will trigger a replay of the entire batch of messages.
   
3. Any persistent error encountered, which is unhandled, is fatal, because any record that cannot be processed due to a 
   persistent error will block the shard from which it came (and all the records behind it), since the stream will 
   continuously redeliver this record until it expires 24 hours to 7 days later (depending on your stream retention 
   configuration). At expiry, the record will be lost and the records behind it with similar ages are also at risk of 
   being lost.
      
4. A "poisonous" record that always causes an error to be thrown back to AWS Lambda when an attempt is made to parse it 
   into a message, will block the shard from which it came until it expires.
   
5. A successfully parsed, but still invalid message that can NEVER be successfully processed also blocks its shard until
   it expires.
   
6. Tasks/functions, which are executed on a message or batch of messages, that fail "indefinitely" will similarly block 
   the shard from which the message(s) originated.
   
7. Each AWS Lambda invocation has a configurable, but limited number of seconds that it is allowed to run and if a batch 
   of messages cannot be fully processed within that time, then the invocation will be timed out and an error will be 
   thrown back to AWS Lambda, which will cause the same batch of messages to be replayed again and, in the worst case
   scenario, continue to time out and replay indefinitely until the batch of messages expires.
   
#### Solutions to and workarounds for the above issues provided by stream consumer implementations such as `kinesis-stream-consumer` & `dynamodb-stream-consumer`:
1. Any and all errors encountered during processing of a batch of records and their extracted message(s) are logged and 
   handled by discarding any unusable records, discarding any explicitly rejected and/or over-attempted messages and
   then by EITHER completing the run normally if every record/message in the batch has been finalised (i.e. completely 
   processed or successfully discarded) OR by throwing an error to trigger a replay of the entire batch of records again 
   if any record/message is NOT finalised yet. 
   
2. A `Batch` instance, which represents the batch of records/messages currently being processed, tracks the state of 
   each and every task to be executed on each message in the current batch. A "task" is an object, with a configured 
   custom function to be EITHER executed on each message individually (a process "one" task) OR on the entire batch of 
   messages collectively (a process "all" task), that tracks the state of that execution for a single message. These 
   tasks enable the stream consumer to determine which messages are completely processed and which messages are still 
   incomplete. The stream consumer attempts to save this state for the entire batch to the configured 
   `StreamConsumerBatchState` table and to subsequently reload this state when the Lambda is invoked again in order to 
   track the progress of the batch across multiple runs. This enables more fine-grained error handling, since it enables 
   the stream consumer to ignore any and all: successfully discarded unusable records; successfully discarded rejected 
   messages; and completely processed messages, and to only attempt to reprocess the incomplete tasks of any incomplete, 
   non-over-attempted messages during a replay of a previously failed batch.
   
3. Persistent or "permanent" errors can be dealt with by:
   - Explicitly rejecting tasks that will consistently or permanently fail by marking them as 'Rejected' within their 
     custom execute functions (Note: this is the responsibility of the developer of the custom task execute function); or 
   - Implicitly "discarding" such a message when all of its failing tasks have reached the maximum number of allowed 
     attempts.
     
   The explicit approach is preferred over the implicit one, because it eliminates un-processable messages much faster
   and because it differentiates rejected un-processable messages from discarded over-attempted messages.
   
4. Any error thrown during the extraction of a message from an event record, will be caught and logged and the record
   will be then treated as an "unusable" record. Any such unusable record will be "discarded" by passing it to the 
   configurable `discardUnusableRecord` function to be dealt with. The default `discardUnusableRecordToDRQ` function, 
   routes these unusable records to a Kinesis "Dead Record Queue (DRQ)" stream.
   
5. Invalid messages that can never be successfully processed should ideally be identified and their failing task(s) 
   should be rejected, which marks them as 'Rejected', within the custom task execute function. If this is not done, 
   then invalid messages will be indistinguishable from valid messages that could not be successfully processed within 
   the allowed number of attempts.
   
6. Task tracking includes tracking the number of attempts at each task on each message, which enables the stream 
   consumer to "discard" a message when all of its failing tasks have reached the maximum number of allowed attempts by
   discarding these tasks, which marks them as 'Discarded', and then passing the message to the configurable 
   `discardRejectedMessage` function to be dealt with. The default `discardRejectedMessageToDMQ` function, routes 
   these rejected messages to a Kinesis "Dead Message Queue (DMQ)" stream. 
   
7. The stream consumer attempts to deal with the issue of AWS Lambda time outs by setting up its own time out at a 
   configurable percentage of the remaining time that the AWS Lambda invocation has to execute. This time out races 
   against the completion of all processing tasks on all of the messages in the batch. If the time out triggers before
   processing has completed, the stream consumer finalises message processing prematurely with the current state of the 
   messages' tasks with the view that its better to preserve at least some of the state of each message than none. The 
   stream consumer finalises message processing in both the time out case and the completely processed case by:
   - Freezing all of the messages' tasks, which prevents subsequent updates by any still in progress tasks in the 
     timeout case; 
   - Ensuring that any and all unusable records have been successfully discarded;
   - Ensuring that any finalised message, which contains one or more tasks that were rejected (explicitly by custom task 
     execute functions) or discarded (due to exceeded attempts) or abandoned (if code changes make previous task 
     definitions obsolete), have been successfully discarded; and
   - Replaying any incomplete messages by throwing an error back to the stream. 
   If the stream consumer is unable to finalise message processing due to an error, then it is unfortunately left with 
   no choice, but to throw the error back to AWS Lambda to trigger a replay of the entire batch of records to prevent 
   message loss.