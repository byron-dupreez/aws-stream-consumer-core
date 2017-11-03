'use strict';

/**
 * @typedef {StreamProcessing|TaskFactoryAware|StandardContext|EventAWSContextAndStageAware|StageHandling|Logger} StreamConsumerContext - a stream consumer context
 * object configured with stream processing, a task factory and all of the standard context configuration, including
 * stage handling, logging, custom settings, an optional Kinesis instance, an optional DynamoDB.DocumentClient & the
 * current region and also, when provided, with the current AWS event, the AWS context and the resolved stage.
 * @property {string|undefined} [region] - the name of the AWS region to use
 * @property {string|undefined} [stage] - the configured stage to use
 * @property {AWSEvent|undefined} [event] - the AWS event passed to your Lambda function on invocation
 * @property {AWSContext|undefined} [awsContext] - the AWS context passed to your Lambda function on invocation
 * @property {LambdaFunctionNameVersionAndAlias|undefined} [invokedLambda] - the name, version & alias of the invoked Lambda function
 * @property {TaskFactory} taskFactory - the task factory to use to create Tasks
 * @property {Batch|undefined} [batch] - the current batch being processed
 */

/**
 * @typedef {StandardSettings} StreamConsumerSettings - settings to be used to configure a stream consumer context with
 * stream processing settings and all of the standard settings
 * @property {StreamProcessingSettings|undefined} [streamProcessingSettings] - optional stream processing settings to use to configure stream processing
 * @property {TaskFactorySettings|undefined} [taskFactorySettings] - optional task factory settings to use to construct and configure a task factory
 */

/**
 * @typedef {StandardOptions} StreamConsumerOptions - options to be used to configure a stream consumer context with
 * stream processing options and all of the standard options
 * @property {StreamProcessingOptions|undefined} [streamProcessingOptions] - optional stream processing options to use to configure stream processing
 * @property {TaskFactoryOptions|undefined} [taskFactoryOptions] - optional task factory options to use to construct and configure a task factory
 */

/**
 * @typedef {StandardContext} StreamProcessing - an object configured with stream processing, stage handling, logging,
 * custom settings, an optional AWS.Kinesis instance and an optional AWS.DynamoDB.DocumentClient and also OPTIONALLY
 * with the current region, the resolved stage and the AWS context
 * @property {StreamProcessingSettings} streamProcessing - the configured stream processing settings to use
 */

/**
 * StreamProcessingSettings - stream processing settings which configure and determine the processing behaviour of an
 * AWS stream consumer.
 *
 * @typedef {StreamProcessingOptions} StreamProcessingSettings
 * @property {ExtractMessagesFromRecord} extractMessagesFromRecord - a function that will be used to extract one or more messages from a given stream event record
 * @property {ExtractMessageFromRecord} extractMessageFromRecord - a function that will be used to extract a messages from a given record or from each of the record's user records (if any)
 * @property {GenerateMD5s} generateMD5s - a function that will be used to generate MD5(s) for a message, its record & its other sources
 * @property {ResolveEventIdAndSeqNos} resolveEventIdAndSeqNos - a function that will be used to resolve a record's eventID, eventSeqNo & optional eventSubSeqNo
 * @property {ResolveMessageIdsAndSeqNos} resolveMessageIdsAndSeqNos - a function that will be used to resolve a message's ids, keys, seqNos, eventID, eventSeqNo & optional eventSubSeqNo
 * @property {LoadBatchState} loadBatchState - a function that will be used to load the previous state of the stream consumer batch (if any)
 * @property {PreProcessBatch|undefined} [preProcessBatch] - an optional pre-process function that will be invoked after the batch has been successfully initiated and before the batch is processed
 * @property {DiscardUnusableRecord} discardUnusableRecord - a function that will be used to discard any unusable record
 * @property {PreFinaliseBatch|undefined} [preFinaliseBatch] - an optional pre-finalise function that will be invoked after the batch has been successfully processed and before the batch is finalised
 * @property {SaveBatchState} saveBatchState - a function that will be used to save the current state of the stream consumer batch
 * @property {DiscardRejectedMessage} discardRejectedMessage - a function that will be used to discard any rejected message
 * @property {PostFinaliseBatch|undefined} [postFinaliseBatch] - an optional post-finalise function that will be invoked after the batch has been successfully finalised
 */

/**
 * KinesisStreamProcessingSettings - stream processing settings which configure and determine the processing behaviour
 * of an AWS Kinesis stream consumer.
 * @typedef {StreamProcessingSettings} KinesisStreamProcessingSettings
 * @property {ExtractMessageFromRecord|undefined} [extractMessageFromRecord] - a synchronous function that will be used to extract a message from a given Kinesis stream event record or user record
 */

/**
 * DynamoDBStreamProcessingSettings - stream processing settings which configure and determine the processing behaviour
 * of an AWS DynamoDB stream consumer.
 * @typedef {StreamProcessingSettings} DynamoDBStreamProcessingSettings
 * @property {ExtractMessageFromRecord|undefined} [extractMessageFromRecord] - a synchronous function that will be used to extract a message from a given DynamoDB stream event record
 */

/**
 * @typedef {string} MD5 - an MD5 message digest (which looks like a UUID without its 'hyphens')
 */

/**
 * @typedef {Object} MD5s - the MD5 message digest(s) generated from a message, its record and/or its user record (if any)
 * @property {MD5|undefined} [msg] - an MD5 message digest generated from the JSON-stringified version of the message
 * @property {MD5|undefined} [rec] - an MD5 message digest generated from the JSON-stringified version of the record
 * @property {MD5|undefined} [userRec] - an MD5 message digest generated from the JSON-stringified version of the user record (if any)
 * @property {MD5|undefined} [data] - an MD5 message digest generated from a Kinesis record's data string (if applicable)
 */

/**
 * @typedef {Object} EventIdAndSeqNos - a record's: eventID; eventSeqNo; optional eventSubSeqNo; & MD5 message digest(s)
 * @property {string|undefined} [eventID]
 * @property {string|undefined} [eventSeqNo]
 * @property {string|undefined} [eventSubSeqNo]
 */

/**
 * @typedef {Object} MessageIdsAndSeqNos - a message's: id(s); key(s); and sequence number(s)
 * @property {KeyValuePair[]} ids
 * @property {KeyValuePair[]} keys
 * @property {KeyValuePair[]} seqNos
 */

/**
 * @typedef {KinesisEventRecord|DynamoDBEventRecord} Record - represents an AWS Kinesis or DynamoDB stream event record
 * (alias for AnyStreamEventRecord)
 * @property {string} eventID - the record's event ID
 */

/**
 * @typedef {Object} Message - represents any kind of message object extracted from an AWS stream event record
 */

/**
 * @typedef {KinesisEventRecord[]|DynamoDBEventRecord[]} Records - an array of AWS Kinesis or DynamoDB stream event records
 */

/**
 * @typedef {Record} UnusableRecord - represents an unusable AWS stream event record
 * @property {string} eventID - the event ID, which should uniquely identify the record
 */

/**
 * GenerateMD5s function type
 * @typedef {function(message: (Message|undefined), record: Record, userRecord: (UserRecord|undefined)): MD5s} GenerateMD5s -
 * a function that will be used to generate MD5 message digest(s) for the given message, its record & its optional user record.
 */

/**
 * ResolveEventIdAndSeqNos function type
 * @typedef {function(record: Record, userRecord: (UserRecord|undefined)): EventIdAndSeqNos} ResolveEventIdAndSeqNos -
 * a function that will be used to resolve the eventID, event sequence number and optional event sub-sequence number of the given record & optional user record.
 */

/**
 * ResolveMessageIdsAndSeqNos function type
 * @typedef {function(message: Message, record: Record, eventIdAndSeqNos: EventIdAndSeqNos, md5s: MD5s, context: StreamConsumerContext): MessageIdsAndSeqNos} ResolveMessageIdsAndSeqNos -
 * a function that will be used to resolve the ids, keys and sequence numbers of the given message.
 */

/**
 * @typedef {Object} MsgOrUnusableRec - an object containing either a successfully extracted message, or an extracted, but rejected message or an unusable record
 * @property {Message|undefined} [msg] - a successfully extracted (& accepted) message (if any)
 * @property {Message|undefined} [rejectedMsg] - an extracted, but rejected message (if any)
 * @property {UnusableRecord|undefined} [unusableRec] - an unusable record (if any)
 */

/**
 * ExtractMessagesFromRecord function type
 * @typedef {function(record: Record, batch: Batch, extractMessageFromRecord: (ExtractMessageFromRecord|undefined), context: StreamConsumerContext): Promise.<ExtractMessageResult[]>} ExtractMessagesFromRecord -
 * a function that will be used to extract one or more messages from a given stream event record, which must accept: the
 * record from which to extract; the current batch to which to add the extracted message(s) or unusable record(s); an
 * optional function to use to extract a message from the record or from each user record (if any); and the context to
 * use, and return a promise of one or more extract message results.
 */

/**
 * ExtractMessageFromRecord function type
 * @typedef {function(record: Record, userRecord: (UserRecord|undefined), context: StreamConsumerContext): Message} ExtractMessageFromRecord -
 * a synchronous function that will be used to extract a single message from a given stream event record, which must accept: the Kinesis/DynamoDB stream event record from which to extract; the UserRecord from which
 * to extract (if any); and the context to use, and return the successfully extracted message or throw an error if
 * a message cannot be extracted from the record.
 */

/**
 * LoadBatchState function type
 * @typedef {function(batch: Batch, context: StreamConsumerContext): Promise.<*>} LoadBatchState - a function that will
 * be used to load the previous state of the stream consumer's current batch and that must accept: the current batch
 * being processed; and the context to use, and return a promise that will complete when the batch's previous state (if
 * any) has been restored.
 */

/**
 * PreProcessBatch function type
 * @typedef {function(batch: Batch, context: StreamConsumerContext): Promise.<*>} PreProcessBatch - an optional, custom pre-process function
 * that will be invoked after the batch has been successfully initiated and before the batch is processed.
 */

/**
 * SaveBatchState function type
 * @typedef {function(batch: Batch, context: StreamConsumerContext): Promise.<*>} SaveBatchState - a function that will
 * be used to save the current state of the stream consumer's current batch and that must accept: the current batch
 * being processed; and the context to use, and return a promise that will complete when the batch's current state has
 * been stored.
 */

/**
 * HandleIncompleteMessages function type
 * @typedef {function(batch: Batch, incompleteMessages: Message[], context: StreamConsumerContext): Promise.<*>} HandleIncompleteMessages -
 * a function that will be used to handle any incomplete messages and that must accept: the current batch being processed;
 * a list of incomplete messages; and the context to use, and return a promise that will only complete when any and all
 * incomplete messages have been "handled" by ensuring that they will be replayed again later.
 */

/**
 * DiscardUnusableRecord function type
 * @typedef {function(unusableRecord: UnusableRecord, batch: Batch, context: StreamConsumerContext): Promise.<*>} DiscardUnusableRecord -
 * a function that will be used to discard an unusable record and that must accept: an unusable record to be discarded;
 * the current batch being processed; and the context to use, and return a promise that will only complete when the
 * unusable record has been "discarded" (e.g. by being sent to the DRQ Kinesis stream).
 */

/**
 * DiscardRejectedMessage function type
 * @typedef {function(rejectedMessage: Message, batch: Batch, context: StreamConsumerContext): Promise.<*>} DiscardRejectedMessage -
 * a function that will be used to discard a rejected message and that must accept: a rejected message to be discarded;
 * the current batch being processed; and the context to use, and return a promise that will only complete when the
 * rejected message has been "discarded" (e.g. by being sent to the DMQ Kinesis stream).
 */

/**
 * PreFinaliseBatch function type
 * @typedef {function(batch: Batch, context: StreamConsumerContext): Promise.<*>} PreFinaliseBatch - an optional, custom pre-finalise function
 * that will be invoked after the batch has been successfully processed and before the batch is finalised.
 */

/**
 * PostFinaliseBatch function type
 * @typedef {function(batch: Batch, context: StreamConsumerContext): Promise.<*>} PostFinaliseBatch - an optional, custom post-finalise function
 * that will be invoked after the batch has been successfully finalised.
 */


/**
 * StreamProcessingOptions
 * @typedef {Object} StreamProcessingOptions - stream processing options which configure ONLY the property (i.e.
 * non-function) settings of an AWS stream consumer and are a subset of the full StreamProcessingSettings
 * @property {string} streamType - the type of stream being processed - valid values are "kinesis" or "dynamodb"
 * @property {boolean|undefined} [sequencingRequired] - whether sequencing is required or not (defaults to true if not specified)
 * @property {boolean|undefined} [sequencingPerKey] - whether message sequencing is per key or not (i.e. per shard) - see notes below for more detail
 * @property {boolean|undefined} [batchKeyedOnEventID] - whether each batch must be keyed on its first usable record's event ID (if DynamoDB or true) or on its shard ID (if Kinesis and false)
 * @property {boolean|undefined} [kplEncoded] - whether the incoming records are Kinesis Producer Library (KPL) encoded records (with or without Aggregation) or not (defaults to false if not specified)
 * @property {string|undefined} [consumerIdSuffix] - an optional suffix to append to the derived consumer ID of the stream consumer (ONLY used if consumerId is NOT explicitly configured)
 * @property {string|undefined} [consumerId] - an optional pre-configured or derived consumer ID to use (if any)
 * @property {number} timeoutAtPercentageOfRemainingTime - the percentage of the remaining time at which to timeout processing (expressed as a number between 0.0 and 1.0, e.g. 0.9 would mean timeout at 90% of the remaining time)
 * @property {number} maxNumberOfAttempts - the maximum number of attempts on each of a message's tasks that are allowed before discarding the message and routing it to the Dead Message Queue. Note that if a message has multiple tasks, it will only be discarded when all of its tasks have reached this maximum
 * @property {string[]} idPropertyNames - the names of all of the identifier properties of a message, which are used to extract a message's unique identifier(s), which uniquely identifies the message.
 * @property {string[]} keyPropertyNames - the names of all of the key properties of a message, which are used to extract a message's key(s), which uniquely identify the entity or subject of a message
 * @property {string[]} seqNoPropertyNames - the names of all of the sequence properties of a message, which are used to extract a message's sequence number(s), which determine the order in which messages with identical keys must be processed
 * @property {string} batchStateTableName - the unqualified name of the stream consumer batch state table from which to load and/or to which to save the state of the batch being processed
 * @property {string} deadRecordQueueName - the unqualified stream name of the Dead Record Queue to which to discard unusable records
 * @property {string} deadMessageQueueName - the unqualified stream name of the Dead Message Queue to which to discard rejected messages
 * @property {boolean|undefined} [avoidEsmCache] - whether to avoid using the event source mapping cache or not
 *
 * Notes:
 * 1. Setting `sequencingPerKey` to true means that messages with distinct keys will be sequenced separately, which will
 *    allow them to be processed in parallel from a shard and which will ONLY preserve sequence between messages with
 *    identical keys.
 * 2. Setting `sequencingPerKey` to false means that all messages will be sequenced together, which will ignore keys and
 *    force sequential processing of all messages from a shard in sequence number order.
 *
 * NB: The consumer ID acts as a subscription ID and MUST be unique amongst multiple different stream consumers that are
 * all consuming off of the same Kinesis or DynamoDB stream! The consumer ID defaults to a concatenation of the Lambda's
 * function name and its alias (if any) if NOT configured, which will meet the uniqueness requirement for all cases
 * except for the bizarre and most likely erroneous case of registering more than one event source mapping between the
 * SAME Lambda and the SAME Kinesis or DynamoDB stream, which in itself would cause duplicate delivery of all messages!
 * If the bizarre case really needs to be supported then EITHER use the consumerIdSuffix OR configure the two consumer
 * IDs explicitly to differentiate the two.
 *
 * NB: For Kinesis stream consumer configuration:
 * 1. idPropertyNames - Optional, but RECOMMENDED - otherwise ids are derived from a concatenation of derived keys and seqNos if not specified
 * 2. keyPropertyNames - MANDATORY if sequencing is required; otherwise OPTIONAL, but RECOMMENDED anyway to assist with identification & discarding to DMQ
 * 3. seqNoPropertyNames - Optional, but RECOMMENDED - otherwise seqNos are derived from kinesis.sequenceNumber if not specified
 *
 * NB: For DynamoDB stream consumer configuration:
 * 1. idPropertyNames - Optional, but RECOMMENDED - otherwise ids are derived from a concatenation of derived keys and seqNos if not specified
 * 2. keyPropertyNames - Optional, but NOT recommended - since keys are derived from dynamodb.Keys if NOT specified
 * 3. seqNoPropertyNames - Optional, but NOT recommended - since seqNos are derived from dynamodb.SequenceNumber if NOT specified
 */
/**
 * @typedef {Object} SummarizedFinalBatchResults - the summarized stream consumer results
 * @property {string} key - the key of the batch
 * @property {number} records - the number of records in the batch
 * @property {number} messages - the number of successfully extracted message objects
 * @property {number} unusableRecords - the number of unusable records
 * @property {number} undiscardedUnusableRecordsBefore - the number of undiscarded unusable records at the start of a batch run from prior runs
 * @property {number} undiscardedUnusableRecords - the number of unusable records that still need to be discarded
 * @property {number} firstMessagesToProcess - the number of messages to be processed first in the sequence
 * @property {number} rejectedMessages - the number of rejected messages
 * @property {number} undiscardedRejectedMessages - the number of rejected messages that still need to be discarded
 * @property {number} incompleteMessages - the number of incomplete messages that still need to be processed
 * @property {Task} initiating - a task that tracks the state of the initiating phase of a run
 * @property {Task|undefined} [processing] - a task that tracks the state of the processing phase of a run
 * @property {Task|undefined} [finalising] - a task that tracks the state of the finalising phase of a run
 * @property {Error|undefined} [finalError] - the final error with which the batch failed if it failed
 * @property {boolean} partial - whether the run was only partially or fully completed, i.e. whether these results are partial (i.e. not all available yet) or full results
 * @property {boolean} fullyFinalised - whether the run was only fully finalised or not
 */

/**
 * @typedef {Error} BatchError - the final error returned via a rejected promise when the batch processing fails or times out
 * @property {Batch|undefined} [batch] - the batch being processed
 */

/**
 * @typedef {Object} BatchTaskDefs - contains all of the batch's task definitions
 * @property {TaskDef[]} processOneTaskDefs - the process one task definitions (if any)
 * @property {TaskDef[]} processAllTaskDefs - the process all task definitions (if any)
 * @property {TaskDef|undefined} [discardUnusableRecordTaskDef] - the discard one unusable record task definition (if any)
 * @property {TaskDef|undefined} [discardRejectedMessageTaskDef] - the discard one rejected message task definition (if any)
 * @property {TaskDef|undefined} [initiateTaskDef] - the initiate batch task definition
 * @property {TaskDef|undefined} [processTaskDef] - the process batch task definition
 * @property {TaskDef|undefined} [finaliseTaskDef] - the finalise batch task definition
 */

/**
 * @typedef {Object} BatchKey - the key of the batch & its components
 * @property {string} streamConsumerId - the first part of the batch key, which is a concatenation of: 'K' (for Kinesis) or 'D' (for DynamoDB); the Kinesis or DynamoDB stream name; and the derived consumer ID, all joined by '|' separators
 * @property {string} shardOrEventID - the second part of the batch key, which is a concatenation of either: 'S' and the Kinesis shard ID; or 'E' and the first usable Kinesis or DynamoDB event ID, joined by '|' separators
 * @property {BatchKeyComponents|undefined} [components] - the non-enumerable components of the batch key, included for convenience
 *
 * Notes:
 * 1. A DynamoDB stream name is derived from its DynamoDB table name suffixed with its stream timestamp (separated by '/').
 * 2. A consumerId is derived from its invoked Lambda's function name & alias (if any) suffixed with an optional, configurable consumerIdSuffix
 */

/**
 * @typedef {Object} BatchKeyComponents  - the NON-ENUMERABLE components of the batch key
 * @property {string} streamName - the Kinesis or DynamoDB stream name component
 * @property {string} consumerId - the derived consumer ID component
 * @property {string|undefined} [shardId] - the Kinesis shard ID component (if applicable & if any)
 * @property {string|undefined} [eventID] - the first usable Kinesis or DynamoDB event ID component (if any)
 */

/**
 * @typedef {Message|UnusableRecord|Batch} TrackedItem - represents an item (i.e. a message or unusable record or the
 * batch itself) for which state is being tracked as it is processed during stream consumption
 */

/**
 * @typedef {Object} TrackedState - represents the state being tracked for a message or an unusable record or the batch
 * being processed during stream consumption
 * @see MessageState
 * @see UnusableRecordState
 * @see BatchState
 */

/**
 * @typedef {MessageState|UnusableRecordState|BatchState} AnyTrackedState - represents the state being tracked for any
 * of the objects (i.e. message or unusable record or the batch itself) being processed during stream consumption
 */

/**
 * @typedef {Object} TasksByName - a map-like object that maps each of the Tasks that it manages by the task's name
 */

/**
 * @typedef {TrackedState} MessageState - represents the state being tracked for an individual message being processed during stream consumption.
 * @property {Message|undefined} [message] - the message to which this state belongs
 * @property {UserRecord|undefined} [userRecord] - the user record (if any) from which the message was extracted
 * @property {Record|undefined} [record] - the record from which the message was extracted
 * @property {KeyValuePair[]} ids - the message's id(s), which uniquely identifies the message
 * @property {KeyValuePair[]} keys - the message's key(s), which uniquely identifies the entity or subject of the message
 * @property {KeyValuePair[]} seqNos - the message's sequence number(s), which determines the sequence in which messages with the same key must be processed
 * @property {Array.<*>} idValues - only the values of the message's id(s)
 * @property {Array.<*>} keyValues - only the values of the message's key(s)
 * @property {Array.<*>} seqNoValues - only the values of the message's sequence number(s)
 * @property {Array.<string|undefined|null>} idVals - only the "stringified" (or non-defined) values of the message's id(s)
 * @property {Array.<string|undefined|null>} keyVals - only the "stringified" (or non-defined) values of the message's key(s)
 * @property {Array.<string|undefined|null>} seqNoVals - only the "stringified" (or non-defined) values of the message's sequence number(s)
 * @property {string|undefined} [id] - the message's id, which is simply a string version of the message's id(s) (if any) primarily used for display purposes
 * @property {string|undefined} [key] - the message's key, which is simply a string version of the message's key(s) (if any) primarily used for display purposes
 * @property {string|undefined} [seqNo] - the message's sequence number, which is simply a string version of the message's sequence number(s) (if any) primarily used for display purposes
 * @property {string} eventID - the eventID of the message's record, which should uniquely identify the message's record and the message if the record is NOT an aggregate Kinesis record
 * @property {string} eventSeqNo - the sequence number of the message's record, which can also be used to determine the sequence in which messages should be processed
 * @property {number|undefined} [eventSubSeqNo] - the optional sub-sequence number of the message within an aggregate Kinesis record's batch of user records / messages, which together with the eventID should uniquely identify the message (only applicable if the record is an aggregate record)
 * @property {MD5s|undefined} [md5s] - the MD5 message digest(s) generated from the message, its record and/or its user record (if any)
 * @property {TasksByName} [ones] - a map-like object that maps each process one message at a time Task (if any) by its name
 * @property {TasksByName} [alls] - a map-like object that maps each process all messages at once "slave" Task (if any) by its name
 * @property {TasksByName} [discards] - a map-like object that maps each discard one rejected message Task (if any) by its name
 */

/**
 * @typedef {TrackedState} UnusableRecordState - represents the state being tracked for an individual unusable record being processed during stream consumption
 * @property {UnusableRecord|undefined} [unusableRecord] - the unusable record to which this state belongs
 * @property {UserRecord|undefined} [userRecord] - the user record (if any) from which the unusable record was extracted
 * @property {Record|undefined} [record] - the record from which the unusable record was extracted
 * @property {boolean} unusable - whether the record was deemed unusable or not (always true)
 * @property {string|undefined} [reasonUnusable] - an optional reason for why the record was deemed unusable
 * @property {string|undefined} [eventID] - the eventID (if any) of the unusable record, which should uniquely identify the record
 * @property {string|undefined} [eventSeqNo] - the sequence number (if any) of the unusable record
 * @property {number|undefined} [eventSubSeqNo] - the optional sub-sequence number of a user record within an aggregate Kinesis record's batch of user records, which together with the eventID should uniquely identify the user record (only applicable if the record is an aggregate record)
 * @property {MD5s|undefined} [md5s] - the MD5 message digest(s) generated from the unusable record and/or its user record (if any)
 * @property {TasksByName} [discards] - a map-like object that maps each discard one unusable record Task (if any) by its name
 */

/**
 * @typedef {TrackedState} BatchState - represents the state being tracked for the {@link Batch} itself that is currently being processed during stream consumption.
 * Note that the tasks tracked on this state object's "alls" properties are master Tasks, which mirror and delegate
 * changes down to their corresponding slave Tasks on the individual messages within the batch.
 * @property {TasksByName} [alls] - a map-like object that maps each process all messages at once "master" Task (if any) by its name
 * @property {TasksByName} [initiatingTasks] - a map-like object that maps each initiating phase Task by its name (non-persistent!)
 * @property {TasksByName} [processingTasks] - a map-like object that maps each processing phase Task by its name (non-persistent!)
 * @property {TasksByName} [finalisingTasks] - a map-like object that maps each finalising phase Task by its name (non-persistent!)
 */

/**
 * @typedef {Object} BatchStateItem - the stream consumer batch state item structure stored in DynamoDB, which stores the state of the entire current batch
 * @property {string} streamConsumerId - a concatenation of: 'K' (for Kinesis) or 'D' (for DynamoDB); the stream name (with '/' stream timestamp if DynamoDB); and the derived consumer ID, all joined by '|' separators
 * @property {string} shardOrEventID - a concatenation of either: 'S' and the Kinesis shard ID; or 'E' and the DynamoDB event ID, joined by '|' separators
 * @property {MessageStateItem[]|undefined} [messageStates] - the tracked states of all of the messages (if any) in the batch
 * @property {MessageStateItem[]|undefined} [rejectedMessageStates] - the tracked states of all of the rejected messages (if any) in the batch
 * @property {UnusableRecordStateItem[]|undefined} [unusableRecordStates] - the tracked states of all of unusable records(if any) in the batch
 * @property {BatchState|undefined} [batchState] - the tracked state (if any) of the batch itself
 */

/**
 * @typedef {TasksByName} PhaseTasks - a map-like object that maps each of the phase Tasks that it manages by the task's name
 * @property {Task} processing - the processing phase Task
 * @property {Task} [finalising] - the finalising phase Task
 */

/**
 * @typedef {Object} MessageOutcomes - a result object with a reference to a Message and the list of Success and/or Failure outcomes collected from execution of its incomplete "process one" tasks
 * @param {Message} message - the message that was processed
 * @param {Outcomes} outcomes - the list of outcomes collected from execution of the message's incomplete "process one" tasks
 * @param {boolean} finalised - whether all of the message's tasks are now fully finalised or not
 */

/**
 * @typedef {Object} StreamProcessingSettingNames - The names of the standard stream processing settings
 * @property {string} streamType - the name of the streamType setting
 * @property {string} sequencingRequired - the name of the sequencingRequired setting
 * @property {string} sequencingPerKey - the name of the sequencingPerKey setting
 * @property {string} batchKeyedOnEventID - the name of the batchKeyedOnEventID setting
 * @property {string} consumerIdSuffix - the name of the consumerIdSuffix setting
 * @property {string} consumerId - the name of the consumerId setting
 * @property {string} timeoutAtPercentageOfRemainingTime - the name of the timeoutAtPercentageOfRemainingTime setting
 * @property {string} maxNumberOfAttempts - the name of the maxNumberOfAttempts setting
 * @property {string} idPropertyNames - the name of the idPropertyNames setting
 * @property {string} keyPropertyNames - the name of the keyPropertyNames setting
 * @property {string} seqNoPropertyNames - the name of the seqNoPropertyNames setting
 * @property {string} extractMessagesFromRecord - the name of the extractMessagesFromRecord function setting
 * @property {string} extractMessageFromRecord - the name of the extractMessageFromRecord function setting
 * @property {string} generateMD5s - the name of the generateMD5s function setting
 * @property {string} resolveEventIdAndSeqNos - the name of the resolveEventIdAndSeqNos function setting
 * @property {string} resolveMessageIdsAndSeqNos - the name of the resolveMessageIdsAndSeqNos function setting
 * @property {string} loadBatchState - the name of the loadBatchState function setting
 * @property {string} preProcessBatch - the name of the preProcessBatch function setting
 * @property {string} discardUnusableRecord - the name of the discardUnusableRecord function setting
 * @property {string} preFinaliseBatch - the name of the preFinaliseBatch function setting
 * @property {string} saveBatchState - the name of the saveBatchState function setting
 * @property {string} discardRejectedMessage - the name of the discardRejectedMessage function setting
 * @property {string} postFinaliseBatch - the name of the postFinaliseBatch function setting
 * @property {string} batchStateTableName - the name of the batchStateTableName setting
 * @property {string} deadRecordQueueName - the name of the deadRecordQueueName setting
 * @property {string} deadMessageQueueName - the name of the deadMessageQueueName setting
 */

/**
 * @typedef {Object} SequencingDefaults - defaults used by the sequencing module, which can be overridden to change behaviour
 * @property {CompareOpts|undefined} [keyCompareOpts] - optional options to use to compare keys
 * @property {CompareOpts|undefined} [valueCompareOpts] - optional options to use to compare values
 * @property {StringifyKeyValuePairsOpts|undefined} [stringifyIdsOpts] - optional options to control how the message ids get stringified
 * @property {StringifyKeyValuePairsOpts|undefined} [stringifyKeysOpts] - optional options to control how the message keys get stringified
 * @property {StringifyKeyValuePairsOpts|undefined} [stringifySeqNosOpts] - optional options to control how the message seqNos get stringified
 */

/**
 * @typedef {Object} EventSourceMappingKey
 * @property {string} functionName
 * @property {string} streamName
 */