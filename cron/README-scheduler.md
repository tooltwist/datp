# DATP Cron
This simplistic little taskscheduler is used to periodically initiate a few tasks, as described below. It gets started when DATP is initiated, and keeps
running in the background, popping up periodically (every 30 seconds?).


## Sleeping Steps
If the the invoke function in a step calls `StepInstance.sleep()` to put the step to sleep for a short duration, _setTimeout()_ is used the handle the delay.

Some steps however require a sleep of minutes or even days, and all sleeps
must survive server failures and restarts, so _setTimeout()_ is not
sufficient. To provide a more robust solution we create a database record
in the **datp_sleep** table, in the form of the event that should be queued at
the allotted time.

This little scheduler takes those events from the database after the
allotted time and drops them into the queue for the current node.

## Webhook callbacks
When a webhook is used to return the transaction status back to the API
client, we have a retry process to handle failures. These are stored in the
datp_webhook table.


## Houskeeping
Various cleanup tasks need to be performed periodically:

* Removing transactions from the transaction cache that have not been
accessed for a (relatively) long time.




