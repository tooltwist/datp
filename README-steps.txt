


1. Step type
Pipelines, etc specify their steps by name.

A step type record is singular, irrespective of how many pipelines use it, or how many times it is invoked.


2. Step Invocation
At the time a step is invoked, it is passed the step options from the pipeline, and also the data from the transaction.

It is essential that a step does not store ANY pipeline or transaction related information in the step definition record.
