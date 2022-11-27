# Keys used in REDIS


Here's a description of the various keys used to store data in REDIS.

We use a suffix to describe the type of key, which matches the initial letter of the REDIS commands associated with that type.

H - hash
L - list
Z - sorted list



|Prefix|Suffix|Contains|enqueue|dequeue|getState|sleep|completed|
|------:|---|-------|------|-----|------|-----|---|
|pipelineH:|Pipeline name|Pipeline definition|
|stateH:|txId|Transaction state|set||
|processingZ|-|List of txIds currently running|delete|add||delete|delete|
|qInL:|nodeGroup|Event to start pipeline|push|pop||||
|qOutL:|nodeGroup|Event to end pipeline|push|pop||||
|qAdminL:|nodeGroup|Admin event queue|||||
|externalId:|externalId|externalId->txId, dissappears after<br>EXTERNALID_UNIQUENESS_PERIOD|Set during first event for the transaction.||||
|stats1H:|time|Fine level statistics|||||
|stats2H:|time|Medium level statistics|||||
|stats3H:|time|Coarse level statistics|||||
|sleepingZ:|-|Short duration sleeping||||add||
|toArchiveZ|-|List of transactions that need to be<br>persisted to the database.||
|toArchiveLock1<br>toArchiveLock2|-|nodeId currently assigned the role<br>of persisting transaction states.|||
|webhooksZ|-|IDs of completed transactions<br>that need a reply via webhook.|||May be deleted by longpoll.||Add|
|webhookCounter:|url|Count of failed attempts|||||
|



# Lifecycle Steps / Lua Functions

## Enqueue
This has a combined function of saving the transaction state, and (optionally) putting the transaction
into a queue to be handled by a specific nodeGroup.

1. If an `externalIdKey` is provided, check that the externalId does not already exist in KEYSPACE_EXTERNALID, then add it, with an expiry time.

2. Check this txId does not already have a status of queued.

3. If a pipeline is specified, get `nodeGroup` from the pipeline definition.

4. Decide a queue based on the `queueType` and `nodeGroup`.

5. Save the state as JSON and a bunch of externalized fields.

6. If an event is provided, push it to the queue worked out above.

7. Remove the transaction from KEYSPACE_PROCESSING.

8. Update statistics for this pipeline/nodeGroup/owner.


## Dequeue
This function pulls events off the queues for a specified node group.

The three queues only contain the transaction IDs - the actual events are saved in the transaction's KEYSPACE_STATE.

First we get the requested number events (actually transaction IDs) from the queues (or as many as available),
giving priority to admin events, then outgoing events (completed pipelines), then incoming (start pipeline) events.

Then for each transaction ID:

1. Sanity check: Make sure there is a transaction state for the event.
1. Get the transaction JSON and externalized fields related to the event. ZZZZ
1. Check the transaction has not been cancelled while it was in the queue. ZZZZ
1. Sanity check: Make sure the transaction state is still `TX_STATUS_QUEUED`.
1. If the event specified a pipeline, load the definition of that pipeline from `KEYSPACE_PIPELINE`.
1. Set the transaction status to `TX_STATUS_RUNNING`.
1. Add the transaction Id to KEYSPACE_PROCESSING.


Note that when a transaction is removed from a queue we add it to the `KEYSPACE_PROCESSING` list, where it remains
until it is re-queued, or the transaction completes. The sort value in this list is the time it was added to the list,
and we can use this in MONDAT to find transactions that started, but don't appear to have completed.


## getState
1. If `markAsReplied` is true, this has been called by a longpoll about to reply.
We can set `notifiedTime` in the KEYSPACE_STATE hash for the transaction, knowing the reply will be sent as soon as this function returns.

1. Get the JSON and externalized fields for the transaction from KEYSPACE_STATE.

1. The `cancelWebhook` value will be set if the transaction was stated with a webhook, but specified
in the metadata that a longpoll should cancel the webhook, _and_ a longpoll reply is about to occur.
In this case we remove this transaction from the KEYSPACE_WEBHOOK_ZZZ list.

1. Return the transaction state.

## completeTransaction
1. Sanity check: Make sure the transaction already has it's state saved in `KEYSPACE_STATE`.
1. Sanity check: check the existing completion status. The transaction can only be completed if it is currently running.
1. Sanity check: check the _new_ completion status is valid.
1. Save the new transaction state in `KEYSPACE_STATE`, as JSON and externalized fields.
1. Remove any event specified for the transaction in `KEYSPACE_STATE`.
1. If the transaction was started with a webhook defined in the metadata, schedule the transaction completion
status to be send to the client using the webhook.
1. Schedule the transaction state to be archived after `DELAY_BEFORE_ARCHIVING` seconds. Archiving removes the
transaction from REDIS, so we don't do it immediately - the client might come back polling for the transaction status,
and it's faster if we don't have to pull it out of the archive.
1. We publish the transaction completion on `CHANNEL_NOTIFY`. Each node subscribes to this channel, and monitors
for the completion (or progress reports) of transactions that are currently long-polling on that node.

## transactionsToArchive
This function is called by the _ArchiveProcessor_ - the background task that moves trnsaction states from
REDIS to long term storage (e.g. a database).

When a bunch of transactions are passed to an ArchiveProcessor to archive, they are not actually removed from
the `KEYSPACE_ARCHIVE` list until the Archiveprocessor comes back and tells us the archiving has been done.
This is important, to ensure that we don't forget to archive a transaction in the case that a node crashes
in the middle or archiving.

So, as the ArchiveProcessor loops, it calls this function each time to notify
of the archiving it has just completed, and to ask for more transactions to archive.

These two steps are independant.
- The first time in, there won't be any transactions already persisted to remove from our list, and
- when the server is shutting down, the ArchiveProcessor won't be asking for any more work to do.

To avoid multiple nodes trying to persist the same transactions at the same time, we provide a simple locking mechanism
when only one node is granted the role of archiving for a period of time.
Multiple nodes may have ArchiveProcessors calling this function, but only one will be getting
a list of transactions to archive. The others will receive an empty list each time.

We perform the locking functioality using expiry of the keys `KEYSPACE_TOARCHIVE_LOCK1` and `KEYSPACE_TOARCHIVE_LOCK2` to indicate three states.

1. No node is currently granted the archiving role.
  Neither key exists - it is up for grabs, and the first to call this function will get the role.
  We saving the nodeId in `KEYSPACE_TOARCHIVE_LOCK1` with an expiry of `PERSISTING_PERMISSION_LOCK1_PERIOD` seconds, and set `KEYSPACE_TOARCHIVE_LOCK2` with a longer period of `PERSISTING_PERMISSION_LOCK2_PERIOD`.

1. A node has been granted the role, and can ask for more work.
  The node specified by `KEYSPACE_TOARCHIVE_LOCK1` is given work, until the key expired.

1. A grace period, where we won't give the node any more work.
  This is the time period where `KEYSPACE_TOARCHIVE_LOCK1` has expired, but `KEYSPACE_TOARCHIVE_LOCK2` has not expired.
  We use this pause to give the previously assigned node time to complete any archiving work and notify us,
  before giving the role to some other node.

At the time we return the transaction IDs from our list, we also return the JSON and externalised fields for those
transactions.


## webhooksToProcess
Returns a list of transactions where the client needs to be sent a notification using a webhook.

Transaction IDs are added to the `KEYSPACE_WEBHOOK` queue if a webhook was provided, and then either
a transaction `progressReport` is set, or the transaction completes.
This is a sorted list, where the sort value is the time at which the webhook should be attempted.

This initial retry time is set to the current time, so the first webhook attempt may occur immediately
if there are no other entries waiting in the queue.

At the time a webhook is removed from the queue, it is re-added to the queue with a new retry time,
which iss calculated with an exponential backoff. Successive attempts occur further and further apart.
If the webhook is called successfully, that entry gets removed from the queue. This approach ensures
that the webhook does not get forgotten if the node processing webhooks crashes.

If we reach `MAX_WEBHOOK_RETRIES` attempts and still fail, we stop trying.

Note: If a transaction is started with `cancelWebhook` specified in the metadata, then a longpoll that
returns the transaction status will also remove the transaction from this `KEYSPACE_WEBHOOK` queue.

Similar to `transactionsToArchive` above, this function is called repeatedly from a node's
`WebhookProcessor`.
On each call, the Webhookprocessor tells this function which webhooks it has attempted (and the result)
and asks for a new batch of webhook calls to try.
As with `transactionsToArchive`, this function uses a locking mechanism to grant just one node
the role of calling webhooks.


TODO:
1. When MAX_WEBHOOK_RETRIES are reached, send a notification to th administrator. ZZZZZ

1. Use `KEYSPACE_WEBHOOK_FAIL_COUNTER` to count the number of failed attempts to a specific URL. This counts
the failures of a specific webhook URL, rather than attempts to call the webhook for a specific transaction.
If a specific webhook URL fails to work for `KEYSPACE_WEBHOOK_FAIL_LIMIT` successive attempts within
`KEYSPACE_WEBHOOK_FAIL_PERIOD` seconds, then we reschedule any webhooks to that URL for
`KEYSPACE_WEBHOOK_DISABLE_PERIOD` seconds. ZZZZZ
And notify the administrator. ZZZZZ
