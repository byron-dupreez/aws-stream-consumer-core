## Changes

### 2.1.9
- Fixed search for `FinaliseError` failures amongst the processing outcomes to survive non-Array outcomes

### 2.1.8
- Fixed search for `FinaliseError` failures amongst the processing outcomes to avoid non-Try/undefined/null outcomes

### 2.1.7
- Changes to `stream-consumer` module:
  - Removed the leaky registration of an `unhandledRejection` event handler on the global `process` object
  - Added rethrow of any `FinalisedError` failure amongst the processing outcomes as a `FatalError`
  - Added calls to `Promises.ignoreUnhandledRejection` to avoid spurious unhandled rejection warnings when ending with an error
- Updated dependencies

### 2.1.6
- Updated dependencies

### 2.1.5
- Updated dependencies

### 2.1.4
- Updated `aws-core-utils` dependency

### 2.1.3
- Updated dependencies

### 2.1.2
- Added `.npmignore`
- Renamed `release_notes.md` to `CHANGES.md`
- Updated dependencies

### 2.1.1
- Updated `aws-core-utils` dependency to version 7.2.0

### 2.1.0
- Updated `aws-core-utils` dependency to version 7.1.1

### 2.0.6
- Changes to `batch` and `stream-consumer` modules:
  - Replaced all of the `instanceof` checks against custom classes with calls to `isInstanceOf`
- Updated `aws-core-utils` dependency to version 7.0.12
- Updated `core-functions` dependency to version 3.0.22
- Updated `logging-utils` dependency to version 4.0.22
- Updated `task-utils` dependency to version 7.0.4
- Updated `aws-sdk` dev dependency to version 2.190.0

### 2.0.5
- Minor fixes to type definitions & JsDoc comments

### 2.0.4
- Updated `aws-core-utils` dependency to version 7.0.11
- Updated `core-functions` dependency to version 3.0.20
- Updated `logging-utils` dependency to version 4.0.20
- Updated `task-utils` dependency to version 7.0.3
- Updated `aws-sdk` dev dependency to version 2.161.0
- Updated `aws-core-test-utils` dev dependency to version 3.0.7

### 2.0.3
- Renamed dummy first exports (`exports._ = '_'; //IDE workaround`) of most modules to (`exports._$_ = '_$_';`) to avoid 
  potential future collisions with `lodash` & `underscore`
- Updated `core-functions` dependency to version 3.0.19
- Updated `logging-utils` dependency to version 4.0.19
- Updated `task-utils` dependency to version 7.0.2
- Updated `aws-core-utils` dependency to version 7.0.10
- Updated `aws-sdk` dev dependency to version 2.143.0
- Updated `aws-core-test-utils` dev dependency to version 3.0.6

### 2.0.2
- Upgraded `aws-core-utils` dependency to 7.0.9 (to facilitate use of AWS XRay)
- Upgraded `aws-sdk` dev dependency to 2.143.0

### 2.0.1
- Patched `persisting` module:
  - Fixed invalid `ProjectionExpression` & `ExpressionAttributeNames` clauses in `loadBatchStateFromDynamoDB` function

### 2.0.0
- Commit of a completely overhauled version of `aws-stream-consumer`, which is now only a set of core modules containing 
  utilities that are shared and used by the new `kinesis-stream-consumer` and `dynamodb-stream-consumer` modules

### 1.0.0-beta.17
- Initial commit of a clean copy of version `aws-stream-consumer` code from commit `c18e97dfeea95589e2fceda07984a267b65692f8` 
  of Dec 15 2016 that was used as the original base for this module (only committed for baseline comparisons)