# Using DynamoDB from DATP

DynamoDB is a super fast NoSQL database provided by AWS. It's advantages are high scalability and low latency. It's disadvantages are that it works best when records are only accessed by their primary key, and that it becomes a nightmare if you exceed preallocated (and costed) capacity and throughtput limits.

For DATP, DynamoDB is ideal for our transaction journalling (our 'transaction deltas'). The transaction deltas are rarely (if ever) accessed, and when they are they are always accessed by the transaction ID. We can point our "firehose" of deltas at DynamoDB, and it can store them quickly with a simple primary key and with no need for indexes.

In DATP we ideally try to spread the risk around across storge mediums, such that if one medium fails then we can reconstruct the transaction details from another. For example:

1. Transaction summary -> Database
1. Transaction state -> REDIS
1. Transaction deltas -> DynamoDB

These options can be configured, to find a balance between performance, reliability, and ease of setup.

---
## Configuration
DynamoDB can be accessed via the AWS console, or can be deployed locally either as a Java application, or in a Docker container.

To install locally, see...

- https://hub.docker.com/r/amazon/dynamodb-local
- https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html
- https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.UsageNotes.html

At the time of writing (May 2022) there is a problem running locally on a M1 Mac, but it can be solved by downloading a dynamic library, renaming it to _native-libs/libsqlite4java-osx.dylib_, removing it's quarantine attributes using xattr, and passing an extra parameter to the JVM.

    java -Dsqlite4java.library.path=./native-libs -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -sharedDb -inMemory

See https://stackoverflow.com/questions/66635424/dynamodb-local-setup-on-m1-apple-silicon-mac

---
## Useful CLI commands

### Initial configuration
(Use foo, bar, local, json)


```bash
aws configure
```  

### Create the transaction_delta table:
```bash
aws dynamodb create-table \
    --table-name transaction_delta \
    --attribute-definitions AttributeName=transaction_id,AttributeType=S AttributeName=sequence_no,AttributeType=N \
    --key-schema AttributeName=transaction_id,KeyType=HASH AttributeName=sequence_no,KeyType=RANGE \
    --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5 \
    --endpoint-url http://localhost:8000
```

### List tables
```bash
aws dynamodb list-tables --endpoint-url http://localhost:8000
```

### List all items in a table

```bash
aws dynamodb scan --table-name transaction_delta  --endpoint-url http://localhost:8000
```

### Delete a table

```bash
aws dynamodb delete-table --table-name transaction_delta  --endpoint-url http://localhost:8000
```

---

## References

https://hub.docker.com/r/amazon/dynamodb-local
https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html

DynamoDB Local Usage Notes  
https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.UsageNotes.html

Cheat Sheet  
https://www.zuar.com/blog/amazon-dynamodb-cheat-sheet/

API Reference  
https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.Query.html


AWS SDK for JavaScript  
https://docs.aws.amazon.com/sdk-for-javascript/v2/developer-guide/dynamodb-example-query-scan.html