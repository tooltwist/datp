# Transactions and Persistence



## Caching

Database updates are the most resource intensive part of most DATP-based applications. As a transaction passes through multiple pipelines of steps, and if it sleeps, we need to keep track of the transaction state, and also of each of the pipeliness and steps, and also when queueing occurs. If we update the database every time a transaction moves from one step to another, a single transaction can have dozens of database accesses. To reduce this bottleneck we need to use multiple levels of caching.


## Layered Persistence

1. Status changes that need to be seen by the initiator of the transaction must be stored to the database. These include progress reports and the final status of trnsactions (ie. success, fail, etc)

2. State changes that need to be accessible by multiple DATP nodes must be saved to REDIS. For example, when a pipeline or step jumps to a different node.

3. Other transaction state changes can be saved using an in-memory cache.

4. The state of the transaction must be able to be re-constructed in the event of a server failure.

The important consideration is ensuring that the state escalates up the levels when required. For example, if a step on one node is invol=king a pipeline on another node, the in-memory cache will need to be written to the REDIS cache, and the in-memory cache invalidated.

Similarly, any state changes that should be visible to the initiator of the transaction should be copied through to the database.

## Redundancy
While the DATP-based application is normally run on a fault tolerant architecture with redundant hardware and backups, we need to be paranoid about the handling of financial information. In the rare case of a system failure, whether it be a DATP node, the database, or REDIS, it is essential that we can determine the one or two transactions that may have been incomplete at the time of failure.

In some cases automatic recovery is possible, but in others manual remediation may be required. To make this possible, we need to be able to identify any transactions that were in progress - specifically any steps that failed mid-way through their processing.




## Types of Transaction Information
Transaction information exists at several levels:


- Transaction Summary.  
  This is the transaction as seen _externally_, and reflects the status of the transaction and also any progress reports. It is updated whenever either of these things changes.

- Transaction State.  
  This is the _internal_ state of the transaction, and includes which step is currently running, the also the state of the various steps within the transaction. This information is used to coordinate the transaction processing.
  
  If a server crashes, this transaction state information can be used to determine which steps have and have not completed. Once a transaction has completed and the summary information updated, this transacrion state information is not strictly needed any more, however it is retained for logging and audit purposes.


- Transaction Deltas.  
  The DATP code updates a transaction's state by applying _deltas_. These are the individual changes to the transaction state. The DATP code processes these as instructions to update the state of the transaction at various stages of a pipeline running.
  
  (Optional) As a form of auxilliary backup, these deltas can be streamed to offline storage, similarly to database journaling. In an emergency situation, the deltas can be replayed to reconstruct the state of a transaction. Streaming in this way, rather than updating a database, can provide an additional last-resort transaction state backup with minimum impact on performance.
  
  See `README.dynamoDB.md` for more information.

> It's worth noting - the transaction state can be reconstructed from the transaction deltas, and the transaction summary can be reconstructed from the transaction state. It's not decided yet whether we want to automate this process, or just provide the information through MONDAT so that manual remediation is possible in the case of a system failure.


## Database storage of Transaction Summaries
Transaction summary information is persisted in the `atp_transaction2` database table. DATP is based on asynchronous transactions, which require not only initiating a transaction, but also enquiring on the status of the transaction. While we can optimise and cache the internal transaction states as a transaction progresses, the transaction summary must be updated immediately each time there is a change that may be of relevance to the API client.

Speed of access to this table directly impacts system throughput.

If neccessary, this table can be moved to a super-fast NoSQL database such as DynamoDB.



## In-memory caching of Transaction State
1. Deltas are always applied to the in-memory copy of the transaction state.
1. If a transaction's state cannot be found in the in-memory cache, it is loaded into the in-memory cache from the REDIS cache.
1. All the steps within a pipeline will run on the same server instance. This allows us to use in-memory caching between steps.
1. When a state change includes information relevant to the initiator of the transaction, it is written to the database (the atp_transaction table).
1. When a step goes to sleep the transaction state is written to REDIS, but not removed from memory.
1. If a step passes control to a pipeline on another node, the transaction state is written to REDIS, and IS removed from the in-memory cache.
1. When a transaction completes, the transaction state is saved to the database in the `atp-transaction-state` table.
1. After TX_MEMORY_CACHE_DURATION seconds (five minutes?) in inactivity, transaction state will be moved from in-memory to the REDIS cache. (Note: If the sleep and transaction complete functionality described above is working correctly, this is unlikely to ever happen)

## REDIS Caching of Transaction State
1. The REDIS cache is used for medium-term storage of transaction state, primarily when a step sleeps, or is passing control to another node.
1. After TX_REDIS_CACHE_DURATION seconds (30 minutes?) of inactivity the transaction is moved from REDIS to the `atp_transaction_state` database table.
1. The transaction state is saved using a regular key, and at the same time the transaction ID is saved in a sorted list with a timestamp, so DATP's cron process can determine transaction states that need to be moved to the database.

## Database storage of Transaction State
Transaction state are persisted to the database for long term storage in the `atp-transaction-state` table. Note that this is different to the transaction summary information. As mentioned above, this information is not strictly required for system functionality, but is retained for auditing and debugging purposes.

If necessary, this is information can be stored in a write-efficient database independant to the Transaction Summary database.
