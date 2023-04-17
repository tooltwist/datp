# Strategy - How DATP uses REDIS


## Caching and Archiving transaction states
>
> Note that under all circumstances, the transaction state can only ever be missing from the cache when the transaction is either sleeping,
> waiting for a switch, or the transaction is complete.
>

### LOGIC - Transaction is Queued

- (_luaEnqueue_startStep, luaEnqueue_pipelineEnd, luaEnqueue_continue => datp_enqueue_)
- The state is cached.  

- Short queues are rarely a problem, but long queues with huge numbers of transactions may result in transactions
    remaining queued for extended periods.
    Shifting transaction states from REDIS memory to the archive would reduce REDIS memory consumption in such "overloaded"
    situations, but the archiving process might impose further load on the system, and actually result in transactions
    remaining in the queue even longer.  

    So, in the tradeoff between keeping REDIS memory usage down, and keeping the queues as short as possible, the
    queues take precedence, as our prime concern is to keep throughput high. So...

- We suspend archiving while a transaction is queued.  

### LOGIC - Processing

- While a pipeline is processing, the state is being changed in memory by the code.  
- While the state is cached, it may be invalid.
- The cache will get updated when either (a) the transaction is re-queued to return to another pipeline, or (b) the transaction completes,
or (c) the step enters sleep prior to a retry.
- We suspend archiving while a trasaction is processing.

### LOGIC - Checkpoint step

- Not implemented yet.
- The cache is updated.
- Queued for archiving but not removal.
- This takes a snapshot of the transaction at the start of the step.

### LOGIC - Long sleep

- The transaction is scheduled for archiving and removal from the cache.
- The SleepProcessor periodically call a LUA script to requeue transactions ready to run.
- Where the state is cached, the archiving is un-scheduled and the Lua script re-queues the step.
- Where the state is not cached, Lua returns the transaction IDs to Javascript, which loads the
state from the archive and recalls the Lua script to cache the state and re-queue the step.
- In each case, we suspend archiving because the transactions are queued.
- The above are bulk operations, working on multiple transactions each time.

### LOGIC - Wait for switch

- The transaction is scheduled for archiving and removal from the cache.
- When a switch gets set, the `setSwitch` lua script checks whether the state is cached.
- If it is cached, the state is updated with the switch value. If there is a wait on the switch then the
Lua script re-queues the step.
- If the state is not cached, Lua returns the transactionId to Javascript, which loads the state from the
archive and recalls Lua to cache the state and if necessary re-queue the step. We call this _resurrecting the state_.
- Lua is normally safe because it is single threaded, but in this operation we need to consider concurrency issues
if two places try to set switches at the same time. During resurrection step the second Lua script must check whether the missing
cache state is now in the cache. If it is, it should be used, as it is newer than the state just loaded from the archive.
- In each case, if the step is requeued, we schedule archiving of the state, _without_ removal from the cache.
If the step is not requeued, we schedule archiving of the state _with_ removal from the cache.

### LOGIC - Re-queueing

- If a pipeline was started from another pipeline (ie. is a child pipeline) then the return to the parent pipeline
is queued.
- At this time the cached state is updated.
- The transaction is scheduled for archiving, but not removal from the cache.
- This takes a snapshot of the transaction at the completion of the pipeline.

### LOGIC - Transaction completion

- When the transaction completes, irrespective of success/abort/failed/etc status, we schedule it for archiving.
- When the archiving is done, a TTL is placed on the cached state, so it remains around while long polling or webhook return completes.

### LOGIC - Getting State

- _getState() => datp_getState_
- This can occur at any time, and is completely independant of the processing flow of the transaction.
- Lua script `datp_getState` which returns the state if it is cached.
If the state is not cached, it returns null to the Javascript, which then _resurrects_ the transaction state from the archive
and re-calls the Lua script to cache the status.
- As with waiting for a switch (#5 above) potentially has a concurrency problem, as it goes into and out of Lua twice, which is solved
as follows...
- When Lua is called the second time, to cache the status, the Lua script re-checks if the state is cached. If it is then the newly cached
status is returned. If it is still not cached the Lua script saves the just-resurrected state and sets the TTL so it will be removed
after a while.
- Note that this two step process cannot be the case while the transaction is queued or is processing, because the state _will_ be cached.


---
The following table details the various modes and mode changes.


| Phase                      | Queued  | StateH & externalId | TTL     | toArchiveZ | ProcessingZ | SleepingZ | WebhooksZ | Notes |
|----------------------------|--------:|---------------------|---------|------------|-------------|-----------|-----------|-------|
| pipeline start  | add    | **YES** | _confirm not set_ | _confirm not set_ | _confirm not set_ |          | Add, no TTL   |
| **WAITING IN QUEUE**       | **YES** | **IN CACHE**        | **NO**  | **NO**     | **NO**      | **NO**    | **NO**    |
| dequeued        | remove | YES | **Add**           | _confirm not set_ | _confirm not set_ |          | no TTL         |            | resurrect if no state
| **RUNNING PIPELINE**          | **YES** | **IN CACHE**     | **NO**  | **NO**     | **YES**     | **NO**    | **NO**    |
| pipeline end    | add    | YES | **Remove**        | _confirm not set_ | _confirm not set_ |          | Update, no TTL | Set        |
| tx complete     | -          | until archived | **Remove**        | **YES**       | (1)     | Update   |          |            |
| **WAITING TO BE ARCHIVED** | **NO**  | **IN CACHE**        | **NO**   | **YES**  | **NO**      | **NO**    | **NO**    |
| archiving       | -          | until TTL | no | Remove    | **NO** |          | **Set TTL**  | **Set TTL**    |
| **ARCHIVED**               | **NO**  | **NO**              | **-**   | **NO**    | **NO**      | **NO**    | **NO** |
| resurrect       | -          | YES | _confirm not set_ | **Add**   | _confirm not set_ |          | **No TTL**  | **No TTL**    |
|
| **SLEEPING 1**             | **NO**  | **IN CACHE**        | **NO**  | **YES**    | **NO**     | **YES**    | **NO** | initially |
| **SLEEPING 2**             | **NO**  | **NO**              | **NO**  | **NO**     | **NO**     | **YES**    | **NO** | if archived |
|
| **WAITING FOR SWITCH 1**  | **NO**  | **IN CACHE**        | **NO**  | **YES**    | **NO**     | **YES**    | **NO** | initially |
| **WAITING FOR SWITCH 2**  | **NO**  | **NO**              | **NO**  | **NO**     | **NO**     | **YES**    | **NO** | if archived |
|
| start long sleep| -          | YES | **Remove**        | **Add**       |          | Add      | Update   |            |
| wake from sleep | **Add**    | YES | _confirm not set_ | (1)       |          | Remove   | Update   |            |
| wait for switch | -          | YES | _confirm not set_ | **Add**       |          | (1)      | Update   |            |
| set switch      | **Add**    | YES | _confirm not set_ | _confirm not set_ | _confirm not set_ |          | update | no TTL | |
|**RESTORED BUT NOT DIRTY**  | **NO**  | **IN CACHE**      | **YES**   | **NO**     | **NO**     | **YES**    | **NO** | after  while |
|
| **AFTER DIRTYING**         | **NO**  | **IN CACHE**      | **NO**    | **YES**     | **NO**     | **YES**    | **NO** | if archived |
|
| | |


## Transaction State
The state of each transaction is stored in a REDIS Hash named ___stateH:&lt;transactionId&gt;___.

Most of the transaction state information is stored as JSON in a field named ***stateJSON***,
but some fields are used by the Lua scripts. Rather than parsing the JSON repeatedly, these values
are pulled out ("externalized") and stored as individual fields in the REDIS Hash for the transaction.

Details of these fields can be found in README-REDIS-externalization.md

### Archiving and TTL
- While a pipeline is being executed, the most recent version of the state is considered to be in memory.

- Any time the transaction state is set, it is (a) queued for archiving, (b) TTL is removed.
- Any time the transaction state is accessed
- When it gets archived, the TTL is set.
- If an operation requires the state, but it is not in the cache, it is presumed to be archived and will be 'resurrected' and the TTL set.
- If the state is subsequently changed the archive flag will again be set and TTL removed.


In queue: state may be archived
Processing: 



### Expiring / Time to Live
In order to conserve REDIS memory we set a TTL (Time To Live) value on the REDIS hash
for each transaction state. This causes the transaction state to be automatically removed
after the alloted time. Each time the state is accessed we reset the TTL to extend the
time till it is removed.

_However_, whenever the state is updated, we add the transaction to the list of
states that need to be archived, and remove the TTL. Once the transaction state has been
archived, the TTL is again set. This sequence causes the state to remain cached for
a while, for inquiry by MONDAT or client polling, of use by the webhook processing.

## Archiving
Once a transaction has completed, we need to persist it to long term storage (ie. the database) and remove it from REDIS. Database operations a relatively slow, compared to REDIS, so we want to do this processing independantly of
processing the transaction that has just completed, so the worker thread can move on to the next task as soon
as possible.

In the LUA script that handles transaction completion (_datp_completeTransaction_) we write a record to the archiving REDIS list.

|Prefix|Suffix|Contains|enqueue|dequeue|getState|sleep|completed|
|------:|---|-------|------|-----|------|-----|---|
|toArchiveZ:|-|List of transactions that need to be<br>persisted to the database.||  
|toArchiveLock1<br>toArchiveLock2|-|nodeId currently assigned the role<br>of persisting transaction states.|||  
||||

Any node can be used to perform the archiving process, and will read items off the archiving list,
persist them to the database, then remove the transaction's status record from REDIS.
In a simple low volume setup you might have everything, including archiving, performed by the master node.
In a high throughput setup you might configure a dedicated node to perform this task.

> Please Note:  
> Irrespective of your configuration, it is important that some node is configured to do the archiving.
> If archiving is not enabled somewhere then REDIS will ultimately run out of memory.
> You also run the risk of losing your transaction history if REDIS ever gets corrupted.

To configure a node to perform archive processing, set the `archiveBatchSize` for the node group to greater than zero. If there are multiple nodes in the group they will take turns archiving, periodically switching
from one node to another randomly. For further details on how these REDIS keys are used see the section _datp_transactionsToArchive_ in `README_REDIS_keys.md`.

ZZZZZZ Does the archive processing move to a second queue while the state is being saved? In case it crashes?

Once a transaction has been archived sucessfully, we re-set the TTL on the state, so it will be
removed after a period of time of not being accessed.



## Queueing to Pipelines
When a pipeline needs to be run, it is not started immediately.
Rather the request is placed as a PIPELINE_START ZZZ ___event___ in a queue for the Node Group where the pipeline is defined to be run.
All requests to all pipelines specified to using the same node group will be placed in the same queue.

If a step in a pipeline puts the pipeline to sleep using the _retry_  functionality, the pipeline will be
restarted by placing a PIPLINE_CONTINUE ZZZ event in this same queue.

One or more server instances are started to run the pipelines for a particular node group,
and each of those instances can run zero or more ___worker___ threads.
Once a node starts, it repeatedly pulls events off the queue for it's node group, to try to keep the workers busy. The queue to start pipelines is a REDIS List named ___qInL:&lt;nodegroup&gt;___.

If step in a pipeline invokes another "child" pipeline, that request is similarly placed as a PIPELINE_START ZZZ event into the
queue for the node group of the child pipeline. When the child pipline completes, it queues a PIPELINE_COMPLETE ZZZ event into a REDIS list named ___qOutL:&lt;nodegroup&gt;___.

There is another REDIS List named ___qAdminL:&lt;nodegroup&gt;___ used to queue out-of-band,
non-transactional requests. These events are relatively infrequent and are involved in the running and administration
pof the DAPT cluster, so they are queued seperately so they do not wait behind a potentially long list of waiting
pipeline requests.

The server instances check the status of their worker threads to determine how many events can be processed
by the instance. They first try to get these events from the admin queue, then the output queue, then the input queue for their node group. This approach ensures that admin events are processed first, and then out (pipeline complete) requests are processed before in (start pipeline) requests.

The analogy here is letting people get out of an elevator before more people try to get into the elevator.


## Running transactions
We keep track of which transactions are currently running using a REDIS sorted list named ___processingZ___.
When a PIPELINE_START ZZZ or PIPELINE_CONTINUE ZZZ event is pulled off a queue,
the transaction is added to this list.

The transactions are placed in the list using their transaction ID as the value, and the de-queueing time as the sort key. This information can be used to identify transactions that fail to complete in an acceptable timeframe,
and also to display long running transactions in MONDAT.

When the transaction is added to a queue, we can assume the transaction has either completed,
or is passing control to a child pipeline, so we removed the transaction from this list
(If it is dequeued in a child pipeline it will be re-added to this list, but with a different sort key).

## Long running transactions
ZZZZZZ

## Sleeping transactions
ZZZ Is this needed?
For short term sleeping transactions, we place the transaction ID in a sorted list named ___sleepingZ___.


## Metrics Collection
We collect metrics of various types, by collecting the data in 'time slots'.

For example, we can store the number of transactions that have been started for each minute, for the past hour.
And the total number of events queued for each minute,
and the number of each type of pipeline started each minute, etc.

In actuality, at the time of writing our time slots are 5 seconds, 100 seconds, and 1000 seconds,
and the retention periods are 5 minutes, 2 hours, and 24 hours. These values seem to work well
when displayed in MONDAT, but may be changed over time.

At the time a value is saved, we use the current time to find the start of the current timeslot
(e.g. now modulus 5 to find the start of the current five second time slot).
In other words, the start time for the 5 second metrics will always be a multiple of 5, etc.

A REDIS Hash is created for each of time slots, and an a REDIS EXPIRY is used to automatically remove
each hash after the required retention period.

The hashes use the keys  ___metric1H:&lt;timeslotStartTime&gt;___,  ___metric2H:&lt;timeslotStartTime&gt;___ and  ___metric3H:&lt;timeslotStartTime&gt;___.

## External IDs
Transaction Requests from a client may contain an ***externalId***.
This identifier can be used to reference the transaction in future API calls by the client, so must be
unique to that particular client.

To allow a uniqueness test before starting a transaction, we store external IDs in REDIS, and check the
key does not exist before starting the transaction.

|Prefix|Suffix|Contains|enqueue|dequeue|getState|sleep|completed|
|------:|---|-------|------|-----|------|-----|---|
|externalId:|key|txId|Set during first event for the transaction.||||
|

The key we store here is a composite value made up of the client ID and the external ID they provide.

To prevent REDIS overflowing, we only enforce this uniqueness check for a period of time, after which
we remove the record from REDIS, and the external ID may be reused by the client.
This time period is specified by a variable `EXTERNALID_UNIQUENESS_PERIOD` which is typically set to a day or two. 

> Please Note:  
> If you expect transactions to take very long times (as an example a KYC check that requires a background check),
> and the client wants to **poll** for transaction updates and status **using the external ID**,
> then you will need to increase the value of this variable to greater than the longest expected time for the
> transaction to complete.  
>  
> Alternatively, you can notify the client that they will have to use the transaction ID while polling for results.
>  
> If a webhook will be used to send statuses back to the client then you completely can disregard this requirement.

## Webhooks
If a webhook reply is specified for a transaction, then each time there is a progress report
or a status change the client needs to be notified.
As with archiving we want to do this offline so the worker thread can continue with it's transaction processing.

In the LUA script that handles transaction completion (_datp_completeTransaction_) we write a record to the webhooks REDIS list.

|Prefix|Suffix|Contains|enqueue|dequeue|getState|sleep|completed|
|------:|---|-------|------|-----|------|-----|---|
|webhooksZ|-|IDs of completed transactions<br>that need a reply via webhook.|||May be deleted by longpoll.||Add|
|webhookFail:|url|Count of failed attempts|||||
|

Similarly to the archive processing, any node can be used to perform the webhook processing - it will read
items off the archiving list and try to contact the webhook URL provided by the client.
If it succeeds the record will be removed from the webhook list.
If it fails we use the _webhookFail_ key to keep count of the number of failures.

ZZZZZZ Do we use a secondary list, while we process the webook?  
ZZZZZZ How do we handle the number of retries?

To configure a node to perform archive processing, set `webhookWorkers` for the node group to greater than zero.
All nodes in the node group will attempt to process webhooks.

A webhook reply can take an unspecified period of time, being dependant on network delays and the speed at
which the client system processes the message.
To prevent potential bottlenecks, multiple "worker" threads are used to process the webhooks.

In each node processing webhooks, it will go through a loop where it checks how many worker threads
are available, asks a LUA script for webhooks it can send, passes each webhook to a seperate thread,
and then pauses before restarting the loop.

The pause will depend on the how any webhook threads are busy, as specified by node group variables:

`webhookPauseBusy` - If all worker threads are busy, wait a bit longer so at least one time to clear out.  
`webhookPauseIdle` - If all the threads are unused, and LUA had no webhooks replies to send, we don't want to immediately call LUA again.
`webhookPause` - The default time before calling the LUA script for more webhooks.  

> Please Note:  
> Irrespective of your configuration, it is important that at least one node group is configured to process webhooks.  
> Failing to do so will result in no webhook replies will be sent to clients.

## Long Poll Replies
Long polling is where a client calls a DATP system asking for the completion status or a progress report of
a running transaction, but if the status is not available the server will wait for up to X seconds before
replying that no status is available.

This is a bit tricky, because a transaction's pipelines might jump across several different
nodes and machines, but the response must be sent using the request object in the exact Javascript process
that received the client's long poll request.

This is a multi-step process.

1. The process that receives the long poll API request 'registers' the request object, sleeps for the
configured long poll time, then de-registers the request object (if it is still there) and sends a reply
to the client that the status is not available. If the request object is not still registered, it does
nothing after the sleep.

2. The LUA script that handles transaction completion (__datp_completeTransaction_) uses REDIS PUB/SUB to
publish a notification to channel _datp_notify_ that the transaction has completed.

3. Each node that accepts long polls, including the node mentioned in step 1, subscribes to the notification
channel. Each time a transaction completes, it checks it's local registry of request objects to see if
there is a long poll waiting for the just-completed transaction.
If there is, the long poll replies, and the request object s removed from the registry.



|Prefix|Suffix|Contains|enqueue|dequeue|getState|sleep|completed|
|------:|---|-------|------|-----|------|-----|---|
datp-notify|-|Channel for notifications|
|

## Notifications
|Prefix|Suffix|Contains|enqueue|dequeue|getState|sleep|completed|
|------:|---|-------|------|-----|------|-----|---|
|datp-notify|-|Channel for notifications|
|

# Initial Processing

## Loading Lua Functions into REDIS

## Pipeline definitions
Pipeline definitions are initially stored in the database, but we don't want to go to the database to get the
definition every time a pipeline starts. Instead, we load pipeline definitions into a REDIS Hash named
___pipelineH:&lt;pipelineName&gt;___ every time a server instance starts.
An individual definition will also be loaded into REDIS if it's definition is changed using MONDAT.

## Nodegroup definitions
The configuration of each node group is also defined in the database, and uploaded into a REDIS hash named

## Node registration
Nodes are started up by the infrastructure, not according to any DATP internal configuration,
and the node's specific configuration and nodeGroup is provided by the infrastructure,
typically from a flat file during development and a secure store such as AWS Secrets Manager during production. 

In order for each node to know what other nodes are available, each node registers itself upon startup,
using a REDIS key named __datp:node-registration:RECORD:&lt;nodeGroup&gt;:nodeId-&lt;nodeId&gt;__.

The value stored is JSON containing build and version information, and a list of the step types
hard-coded into the executable.

For example:

```json
{
  "nodeGroup": "master",
  "nodeId": "nodeId-2ccd84e01e39e0f880758ae60ecfa24ebf058b62",
  "appVersion": "-",
  "datpVersion": "-",
  "buildTime": "-",
  "stepTypes": [
    {
      "name": "advance-ai/face-compare",
      "description": "Face Compare Check using Advance AI",
      "defaultDefinition": {
        "description": "Face Compare Check using Advance AI"
      }
    },
    ...
  ]
}
```
