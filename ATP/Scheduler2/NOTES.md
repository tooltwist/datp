




## Pinging
ATP support several levels of "pinging" to help debug the system.

ping1 - with a transaction type of 'ping1' schedulerForThisNode.startTransaction() immediately calls the callback function.

ping2 - with a transaction type of 'ping2' schedulerForThisNode.startTransaction() returns via a normal TRANSACTION_COMPLETED_EVENT event, wih no processing.

ping3 - a step type of 'misc/ping3' causes the step to return via a normal STEP_COMPLETED_EVENT event, with no processing.
