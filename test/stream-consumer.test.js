'use strict';

/**
 * Unit tests for aws-stream-consumer/stream-consumer.js
 * @author Byron du Preez
 */

const test = require('tape');

// The test subject
const streamConsumer = require('../stream-consumer');

const TaskDef = require('task-utils/task-defs');

const strings = require('core-functions/strings');
const stringify = strings.stringify;

const Arrays = require('core-functions/arrays');

const logging = require('logging-utils');

function execute1() {
  console.log(`Executing execute1 on task (${this.name})`);
}
function execute2() {
  console.log(`Executing execute2 on task (${this.name})`);
}
function execute3() {
  console.log(`Executing execute3 on task (${this.name})`);
}
function execute4() {
  console.log(`Executing execute4 on task (${this.name})`);
}

// =====================================================================================================================
// validateTaskDefinitions
// =====================================================================================================================

test('validateTaskDefinitions', t => {
  function check(processOneTaskDefs, processAllTaskDefs, mustPass) {
    const arg1 = stringify(Arrays.isArrayOfType(processOneTaskDefs, TaskDef) ? processOneTaskDefs.map(d => d.name) : processOneTaskDefs);
    const arg2 = stringify(Arrays.isArrayOfType(processAllTaskDefs, TaskDef) ? processAllTaskDefs.map(d => d.name) : processAllTaskDefs);
    const prefix = `validateTaskDefinitions(${arg1}, ${arg2})`;
    try {
      streamConsumer.validateTaskDefinitions(processOneTaskDefs, processAllTaskDefs, context);
      if (mustPass) {
        t.pass(`${prefix} should have passed`);
      } else {
        t.fail(`${prefix} should NOT have passed`);
      }
    } catch (err) {
      if (mustPass) {
        t.fail(`${prefix} should NOT have failed (${err})`);
      } else {
        t.pass(`${prefix} should have failed`);
      }
    }
  }

  const context = {};
  logging.configureLogging(context);

  // Create task definitions
  const taskDef1 = TaskDef.defineTask('Task1', execute1);
  const taskDef2 = TaskDef.defineTask('Task2', execute2);
  const taskDef3 = TaskDef.defineTask('Task3', execute3);
  const taskDef4 = TaskDef.defineTask('Task4', execute4);

  const taskDef1Copy = TaskDef.defineTask('Task1', execute1);
  const taskDef2Copy = TaskDef.defineTask('Task2', execute2);
  const taskDef3Copy = TaskDef.defineTask('Task3', execute1);
  const taskDef4Copy = TaskDef.defineTask('Task4', execute4);

  // Not enough task defs
  check(undefined, undefined, false);
  check(undefined, [], false);
  check([], undefined, false);
  check([], [], false);

  // Invalid task definitions (not TaskDef)
  check(['"TaskDef1 string"'], undefined, false);
  check(undefined, [{
    name: 'TaskDef2', execute: () => {
    }
  }], false);

  // Validate that real TaskDef task defintions are valid
  check([taskDef1], undefined, true);
  check(undefined, [taskDef2], true);
  check([taskDef1], [taskDef2], true);
  check([taskDef1, taskDef2], undefined, true);
  check(undefined, [taskDef3, taskDef4], true);
  check([taskDef1, taskDef2], [taskDef3], true);
  check([taskDef1], [taskDef3, taskDef4], true);
  check([taskDef1, taskDef2], [taskDef3, taskDef4], true);

  // Create subtask definitions
  const subTaskDef1 = taskDef1.defineSubTask('SubTask1');
  const subTaskDef2 = taskDef2.defineSubTask('SubTask2');
  const subTaskDef3 = taskDef3.defineSubTask('SubTask3');
  const subTaskDef4 = taskDef4.defineSubTask('SubTask4');

  // Re-validate same set as before (now that the previous TaskDefs have become complex ones (each with a sub-task)
  check([taskDef1], undefined, true);
  check(undefined, [taskDef2], true);
  check([taskDef1], [taskDef2], true);
  check([taskDef1, taskDef2], undefined, true);
  check(undefined, [taskDef3, taskDef4], true);
  check([taskDef1, taskDef2], [taskDef3], true);
  check([taskDef1], [taskDef3, taskDef4], true);
  check([taskDef1, taskDef2], [taskDef3, taskDef4], true);

  // Invalid subtask definitions
  check([subTaskDef1], undefined, false);
  check(undefined, [subTaskDef2], false);
  check([subTaskDef1], [subTaskDef2], false);
  check([subTaskDef1, subTaskDef2], undefined, false);
  check(undefined, [subTaskDef3, subTaskDef4], false);
  check([subTaskDef1, subTaskDef2], [subTaskDef3], false);
  check([subTaskDef1], [subTaskDef3, subTaskDef4], false);
  check([subTaskDef1, subTaskDef2], [subTaskDef3, subTaskDef4], false);

  // Some valid task definitions with some invalid subtask definitions
  check([taskDef1], [subTaskDef2], false);
  check([subTaskDef1], [taskDef2], false);
  check([taskDef1, subTaskDef2], [taskDef3], false);
  check([taskDef1], [taskDef3, subTaskDef4], false);
  check([taskDef1, taskDef2], [taskDef3, subTaskDef4], false);
  check([taskDef1, subTaskDef2], [taskDef3, taskDef4], false);

  // All task definitions' names within each list must be unique
  check([taskDef1, taskDef2, taskDef3, taskDef1], undefined, false);
  check([taskDef1, taskDef2, taskDef3, taskDef2], [], false);
  check([taskDef1, taskDef2, taskDef3, taskDef3], [taskDef4], false);

  check(undefined, [taskDef2, taskDef3, taskDef4, taskDef2], false);
  check([], [taskDef2, taskDef3, taskDef4, taskDef3], false);
  check([taskDef1], [taskDef2, taskDef3, taskDef4, taskDef4], false);

  check([taskDef1, taskDef2, taskDef1, taskDef2], [taskDef3, taskDef4, taskDef3, taskDef4], false);

  // Copies with same names, but different instances
  check([taskDef1, taskDef1Copy], [taskDef3, taskDef3Copy], false); // names non-unique within each list

  // Instances not unique within each list (will fail name uniqueness check)
  check([taskDef1, taskDef1], [taskDef3, taskDef3], false); // names non-unique within each list

  // Invalid, since both lists share task definitions!
  check([taskDef1], [taskDef2, taskDef1], false);
  check([taskDef1, taskDef3], [taskDef3], false); // names non-unique within each list
  check([taskDef1, taskDef4], [taskDef3, taskDef4], false); // names non-unique within each list

  // names unique within each list and instances "unique" too
  check([taskDef1, taskDef3Copy], [taskDef1Copy, taskDef3], true);
  check([taskDef1, taskDef2, taskDef3Copy, taskDef4Copy], [taskDef1Copy, taskDef2Copy, taskDef3, taskDef4], true);

  t.end();
});