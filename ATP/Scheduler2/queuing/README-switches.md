# Switch Handling

_Switches_ can be set on a transaction, to convey whatever meaning you chose.

Each switch has a name, and a value, which can be set from within a step within the normal processing of the transaction, or it can be set externally via a seperate process, or application route.

Switches can be used to keep track of transaction statuses, but the most important purpose
is to allow a transaction to go sleep and wake when the switch value changes.


A common Use Case is that your transaction calls an external API that returns it's result via a webhook.
The best was to implement this is that the step calls the back end api, then goes to sleep using `stepInstance.retry()`. Your application can provide a route for the webhook, and call `TransactionState.setSwitch()`, which will cause the same step to be rerun.


## Handling within REDIS
This code needs to be treated carefully, to avoid concurrency issues.
In most cases DATP changes transaction state from within normal transaction processing, which is single threaded,
in that only one instance of step code will be working on a specific transaction at a time.
The current transaction state will either be in-memory while steps are being run,
or in REDIS while it is queued or sleeping, or will have been archived to the database.
It is never be active in more than one location.

In the case of switches however, `TransactionState.setSwitch()` can be called independantly from
the normal transaction processing, but unknown application code.
We won't know if the transaction state is in REDIS or in memory, or has been archived.
The solution is to ALWAYS store details switches and a transaction's wake switch in REDIS.

To ensure that the transaction state is actually in memory, we must load it from the archive
first if necessary.

## Concurrency

The normal sequence for waiting on a switch to change is this:

1. A step asks the current switch state.
2. Based on the result, the step decides to retry, and sets `retry_wakeSwitch` to that switch name.
The transaction state remains in REDIS, containing an event to restart the step, but it is not queued.
3. Some code changes the switch value, and LUA sees that the transaction is waiting on that switch, so
it adds the transaction to the processing queue. (The retry details are cleared to prevent possible double adding to the queue).

This process works well, except we will miss switch changes under this sequence of events:

1. The step asks the current switch state.
2. The switch is changed while the step is still running.
3. The step decides to retry, to wait on that switch, and sets the _retry_wakeSwitch_.

To handle this scenario, we need the concept of an "un-acknowledged switch value".

- We flag the switch value as unacknowledged when it's value changes.
- We clear the flag when the step asks for it's value.
- When `instance.retry()` is called:
  - if a switch value change is unacknowledged, immediately queue the step for retry.
  - if the unacknowledged flag is NOT set then we have no reason to think it's value has changed yet,
  so set _retry_wakeSwitch_ and go to sleep.
  - in either case clear the unacknowledged flags for ALL switches.

Note that the critical period is only between when the step asks for the value,
and when/if the step decides to wait for the switch using retry.
We care about whether the step has acknowledge the new switch value, but we don't care about
non-pipeline/step code checking the switch value.

# Phil TODO:

- separate out switches and retry values from stateH:

- getSwitchValue
  - need to specify for whether the step is asking
  - clear the unacknowleged flag if set.
  - only ask for one switch at a time. (getSwitch, not getSwitches)

- retry:
  - pass wakeSwitch to enqueue_stepStart
  - check/clear all switch flags, call immediately if necessary.

- setting switch, set unacknowledged flag.

- getting stateStatus should get switches and retrySwitch from REDIS






