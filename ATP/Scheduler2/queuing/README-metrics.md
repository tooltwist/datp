# Metrics


## Series


## Stats

### Transaction

|Field|Description|
|-----|-----------|
|tx:::queued|When the initial transaction's pipeline is added to the queue|
|tx:::start|When the initial transaction's pipeline is removed from the queue|
|tx:::end|When the initial transaction's pipeline completes|
|||


### Pipeline
|Field|Description|
|-----|-----------|
|g:::[nodeGroup]|::queued|When pipeline is added to queue|
|g:::[nodeGroup]|::start|When pipeline is removed from queue|
|g:::[nodeGroup]|::end|When pipeline is queue to another queue, or upon completion|
|||

            local f_nodeGroup = 'g:::' .. nodeGroup
            local f_pipelineStart = 'p:::' .. pipeline .. ':::queued'
            local f_txPipelineStart = 'tx:::' .. pipeline .. ':::start'
            local f_ownerStart = 'o:::' .. owner .. ':::in'
            local f_ownerPipeline = 'o:::' .. owner .. ':::p:::' .. pipeline

### Node group


### User / Owner

## Queue lengths
Adjusted when an item is added to a queue, or removed from a queue.

|Field|Description|
|-----|-----------|
|iLen:::&lt;nodeGroup>|Updated when event is added to a queue|
|oLen:::&lt;nodeGroup&gt;|Updated when event is removed from a queue|
|||


-oOo-