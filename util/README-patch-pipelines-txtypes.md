# Patching pipeline and transaction types


We originally had a mapping between transaction type and pipelines.
This is now changed such that transaction types and their matching pipelines
have the same name.

At the time of writing, there is a one-to-many relationship between the
transaction type and pipeline versions of the same name.

In the future we'll rename **atp_transaction_type** to **atp_pipeline_type**, which
more accurately reflects it's purpose.

This script does the conversion, renaming pipelines to match their corresponding transaction type.

1. Pipelines mapped to more than one transaction type.  
  Fatal error, requiring manual resolution.

1. Pipelines mapped to no transaction type.  
  Create a new transaction type.

1. Transaction type and it's pipeline name do not match.  
  Rename the pipeline.

1. Transaction type has no description.  
  Copy the description from one it it's pipelines.

