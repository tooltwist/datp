# Testing archiving and Transaction modes

Our `test-fast` transaction:

- starts from the master node via MONDAT
- calls a pipeline in slave1, which
- calls a pipline in slave2, containing
- a step that complete in a few milliseconds.

Our `test-longrun` transaction:

- starts from the master node via MONDAT
- calls a pipeline in slave1, which
- calls a pipline in slave2, containing
- a step that takes 2 minutes to complete.

Start TX -> pipeline in slave1 -> pipline in slave2 -> step that takes 2 minutes to complete

This step does not use a DATP sleep or wait; it just takes teo minutes to complete using setTimeout.

During these tests, reduce `DELAY_BEFORE_ARCHIVING` to 1 minute. Don't forget to change it back afterwards!

## Easy Tests

### 1. Normal run

- Run the `test-fast` transaction with all nodes up, from MONDAT.
- The transaction will immediately complete. Long poll not required.
- The transaction should show in recent tab {cached=Y, archived=N, toArchive=Y}.
- Wait and should show {cached=Y, archived=Y, toArchive=N}. ZZZZ Skipped this step
- Wait and should show as {cached=N, archived=Y, toArchive=N}.
2023/3/23 - 

### 2. Long running transaction, via long poll

- Run the `test-longrun` transaction with all nodes up, from MONDAT.
- The transaction will complete after two minutes.
- The transaction should show in recent tab {cache=Y, archived=N, toArchive=Y}.
- Wait and should show {cached=Y, archived=Y, toArchive=N}.
- Wait and should show as {cached=N, archived=Y, toArchive=N}.

### 3. Long running transaction, via webhook

- Run the `test-longrun` transaction from Barage tester.
- The transaction will complete after two minutes.
- The transaction should show in recent tab {cache=Y, archived=N, toArchive=Y}.
- Wait and should show {cached=Y, archived=Y, toArchive=N}.
- Wait and should show as {cached=N, archived=Y, toArchive=N}.

## More complicated scenarios

### 10. Transaction with status "queued"

- Run the transction with node `slave1` shut down.
- The transaction should show as Queued, {cache=Y, archived=N, toArchive=Y}.
- Start slave1 and it should complete.
- The transaction should show in recent tab {cache=Y, archived=N, toArchive=Y}.
- Wait and should show {cached=Y, archived=Y, toArchive=N}.
- Wait and should show as {cached=N, archived=Y, toArchive=N}.

### 11. Transaction with status "processing" (or is it "running"?)

- Run the `test-fast` transaction with all nodes up.
- The transaction should show as Running.
- The transaction should show in the log running tab.
- Two minutes later it should show as complete, in the cache, not in the archive, and waiting to be archived.
- Wait and should show as in the cache and archived.
- Wait and should show as _not_ in the cache and archived.

### 12. Long poll after archived

- Start the `test-fast` transaction and let it complete.
- The state status screen should show it in the cache, but not archived.
- Wait a long while and it should show as archived.
- Select with 're-cache' set and it should end up in the cache and archived.
- Wait a while and it will disappear from the cache.

### 13. Webhook after archived

- Stop the webhook processing node.
- Start the `test-fast` transaction and let it complete.
- The state status screen should show it in the cache, not archived, and with an outstanding webhook.
- Wait until it shows as archived.
- Start the webhook processing node.
- Refresh the state status screen and it should be archived, in the cache, and the outstanding webhook should be cleared.
- Wait a while and it will disappear from the cache.

### 14. Transaction Recovery

- Run the `test-longrun` transaction with all nodes up.
- Kill node `slave2` within two minutes.
- After a while it should appear in the Long Running tab.
- ZZZZZ How do we recover?
