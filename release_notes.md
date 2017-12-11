## Changes

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