## Changes

### 2.0.1
- Patched `persisting` module:
  - Fixed invalid `ProjectionExpression` & `ExpressionAttributeNames` clauses in `loadBatchStateFromDynamoDB` function

### 2.0.0
- Commit of a completely overhauled version of `aws-stream-consumer`, which is now only a set of core modules containing 
  utilities that are shared and used by the new `kinesis-stream-consumer` and `dynamodb-stream-consumer` modules

### 1.0.0-beta.17
- Initial commit of a clean copy of version `aws-stream-consumer` code from commit `c18e97dfeea95589e2fceda07984a267b65692f8` 
  of Dec 15 2016 that was used as the original base for this module (only committed for baseline comparisons)