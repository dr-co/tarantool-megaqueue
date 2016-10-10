# Queue API description

A Tarantool instance can serve as a Queue Manager, along
with any other database work necessary.

A single properly configured Tarantool space can store any
number of queues.

Queues support task priority. Priority value lays in the range
[0, 1000], with default value being 0. A higher value means higher
priority, lower value - lower priority.

Each queue has one (currently) associated *fiber* taking care of
it. The fiber is started upon first access to the queue. The job
of the fiber is to monitor orphaned tasks, as well as prune and
clean the queue from obsolete tasks.

The script creates some spaces for processing:

1. `MegaQueue` - main data space
1. `MegaQueueStats` - statistics


```lua

    queue = requre('megaqueue')
    queue:init({ ... defaults ... })


    queue:put('queue1', { ... }, task_data)

    task = queue:take('queue1', 0.5)


```

## Terminology

* *Consumer* - a process, taking and executing tasks
* *Producer* - a process adding new tasks

### Arguments of queue API functions

* `tube` (string) - queue name,
* `domain` (string) - task's domain: only one task in the domain can
  be processed simultaneously,
* `delay` (number) - a delay between the moment a task is queued
  and is executed, in seconds
* `ttl` (number) - task time to live, in seconds. If `delay` is
  given along with `ttl`, the effective task time to live is
  increased by the amount of `delay`,
* `ttr` (number) - task time to run, the maximal time allotted
  to a consumer to execute a task, in seconds,
* `pri` (number) - task priority [0..1000],
* `id` (string) - task id,
* `timeout` (number) - timeout in seconds for the Queue API function.

### Task states

* `ready` - a task is ready for execution,
* `delayed` - a task is awaiting task `delay` to expire, after
   which it will become `ready`,
* `work` - a task is taken by a consumer and is being executed,
* `wait` - a task wait for worker processing the other tash with the
   same `domain`
* `buried` - a task is neither ready nor taken nor complete, it's
   excluded (perhaps temporarily) from the list of tasks for
   execution, but not deleted.

### The format of task tuple

Queue API functions, such as `put`, `take`, return a task.
The task consists of the following fields:



1. `ID` (num64) - task identifier
1. `TUBE` (str) - queue identifier 
1. `PRI` (unsigned) - task priority
1. `DOMAIN` (str) - task domain
1. `STATUS` (str) - task status
1. `EVENT` (double) - next task event timestamp
1. `CLIENT` (unsigned) - reserved
1. `OPTIONS` (map) - other info about task
1. `DATA` - user's task data


## API

### Producer

#### queue:put(tube, opts, data)

Enqueue a task. Returns a tuple, representing the new task.
The list of fields with task data ('...')is optional.


### Consumer

#### queue:take(tube, timeout)

If there are tasks in the queue `ready` for execution,
take the highest-priority task.
Otherwise, wait for a `ready` task to appear in the queue, and, as
soon as it appears, mark it as `taken` and return to the consumer.
If there is a `timeout`, and the task doesn't appear until the
timeout expires, return 'nil' (a timeout of 0 returns immediately).
If timeout is not given or negative, wait indefinitely until a task
appears.

All the time while the consumer is working on a task, it must keep
the connection to the server open. If a connection disappears while
the consumer is still working on a task, the task is put back on the
`ready` list.

#### queue:ack(id)

Confirm completion of a task. Before marking a task as complete,
this function verifies that:

* the task is `work` and
* the consumer that is confirming the task is the one which took it.

Consumer identity is established using a session identifier. In
other words, the task must be confirmed by the same connection
which took it. If verification fails, the function returns an
error.

On success, delete the task from the queue.

#### queue:release(id[, delay])

Return a task back to the queue: the task is not executed.
Task's TTL will be prolonged.


#### queue:bury(id)

Mark a task as `buried`. This special status excludes
the task from the active list, until it's `dug up`.
This function is useful when several attempts to execute a task
lead to a failure. Buried tasks can be monitored by the queue
owner, and treated specially.


### Common functions (neither producer nor consumer).

#### queue:kick(tube [, count] )

'Dig up' `count` tasks in a queue. If `count` is not given,
digs up just one buried task.


#### queue:statistics()

Return queue module statistics accumulated since server start.
The statistics is broken down by queue id. Only queues on which
there was some activity are included in the output.

The format of the statistics is a sequence of rows, where each
odd row is the name of a statistical parameter, and the
next even row is the value.


