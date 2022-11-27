# Datp v3 transaction states


Transaction state is primarily stored in REDIS, however it is only updated on certain occasions:

1. When an event is scheduled, that will require being queued (i.e. a pipeline)
2. When a child pipeline returns to it’s parent, if it ran on a different node group.
3. When a transaction completes.
4. When a retry (sleep) is scheduled.
5. (Future) When the step is designated as a “checkpoint”.

For (3), (4), (5), the state is scheduled to be persisted to long term storage.

For events processed by a Lua script, the state is stripped off the event and saved in REDIS.
We manage transaction state using the _Transaction_ class, but persist the state as JSON.

The following sections describe the data within the transaction state.


## Transaction information

**NOTE: Check these are actually used**

| Field | Description |
|------|----------------|  
| txId | Transaction ID |
| owner | The person who created the application |
| externalId | A unique identifier provided by the owner of the transaction |
| status | queued / running / success / failed / aborted / ... |
| progressReport | Information to be provided to the user, prior to transaction completion |
| transactionOutput | Output of the yransaction |
| completionTime | When the transation finished (irrespextive of status) |

Related to sleeping:

| Field | Description |
|------|----------------|  
| sleepingSince | Time of first sleep for the current step |
| sleepCounter | How many times we've tried to re-run this step |
| wakeTime | A time to re-run the latest step |
| wakeSwitch | "Switch" to wait for |
| wakeNodeGroup | Where to re-run the step |

Obsolete:  
These fields are made obsolete by 'flow':
sequenceOfUpdate,
wakeStepId,
deltaCounter

## transactionData

| Field | Description |
|------|----------------|  
| status | queued / running / success / failed / aborted / ... |

    "transactionData" : {
      "transactionType": "null",
      "nodeGroup": "master",
      "nodeId": "nodeId-5307a9b81ddcd928d56e5cb2f900a609f9c559df",
      "pipelineName": "null",
      "metadata": {
        "webhook": "http://yarp.com",
        "poll": "long",
        "reply": {}
      },
      "transactionInput": {
        "afield": "xyz"
      },
      "onComplete": {
        "nodeGroup": "master"
      },
      "nextStepId": "s-5b34516bc23ec295ae79fa10f4a15a499d71623e"
    },

Obsolete:



## steps

| Field | Description |
|------|----------------|  
| zzzz | Time of first sleep for the current step |
| stepDefinition | If a string, this is a pipeline name. Otherwise it is an object defining the step to be run. |
| zzzz | Time of first sleep for the current step |

``` json
    "steps": {
      "s-5b34516bc23ec295ae79fa10f4a15a499d71623e": {
        "vogPath": "a7ec7c=null",
        "level": 0,
        "fullSequence": "a7ec7c",
        "vogAddedBy": "Scheduler2.startTransaction()",
        "stepDefinition": "null",
        "parentStepId": "",
        "stepInput": {
          "afield": "xyz"
        },
        "status": "queued"
      },
      ...
    }
```

## flow
The flow records the sequence in which steps are called, and is an array
of objects with the following fields.

| Field | Description |
|------|----------------|  
| i | Index (simplifes debugging - may become obsolete) |
| p | Index of parent |
| vogPath | Comma separated string, indicating the hierarchy |
| ts1 | When the step was queued |
| ts2 | When the step started |
| ts3 | When the step completed |
| stepId | One of the steps in 'steps' |
| nodeId | Where the step was/is running |
| input | Data passed in to the step |
| onComplete.callback | A callback to run whwn the step finished |
| onComplete.nodeGroup | Where to run the callback |
| completionStatus | Final status of the step |
| output | Data returned from the step |
| note | A note associated with step completion |


## Example transaction state
``` json
{
  "txId": "tx-a7ec7c5ffbcd0f83bca31d1c80e25f6fb194c14d",
  "owner": "acme",
  "externalId": null,
  "transactionType": "null",
  "status": "running",
  "sequenceOfUpdate": 0,
  "progressReport": {},
  "transactionOutput": {},
  "completionTime": null,
  "sleepingSince": null,
  "sleepCounter": 0,
  "wakeTime": null,
  "wakeSwitch": null,
  "wakeNodeGroup": null,
  "wakeStepId": null,
  "deltaCounter": 6,
  "transactionData": {
    "status": "running",
    "transactionType": "null",
    "nodeGroup": "master",
    "nodeId": "nodeId-5307a9b81ddcd928d56e5cb2f900a609f9c559df",
    "pipelineName": "null",
    "metadata": {
      "webhook": "http://yarp.com",
      "poll": "long",
      "reply": {}
    },
    "transactionInput": {
      "afield": "xyz"
    },
    "onComplete": {
      "nodeGroup": "master"
    },
    "nextStepId": "s-5b34516bc23ec295ae79fa10f4a15a499d71623e"
  },
  "steps": {
    "s-5b34516bc23ec295ae79fa10f4a15a499d71623e": {
      "vogPath": "a7ec7c=null",
      "level": 0,
      "fullSequence": "a7ec7c",
      "vogAddedBy": "Scheduler2.startTransaction()",
      "stepDefinition": "null",
      "parentStepId": "",
      "stepInput": {
        "afield": "xyz"
      },
      "status": "queued"
    }
  },
  "flow": [
    {
      "i": 0,
      "vogPath": "a7ec7c=null",
      "ts1": 1665316515743,
      "ts2": 0,
      "ts3": 0,
      "stepId": "s-5b34516bc23ec295ae79fa10f4a15a499d71623e",
      "nodeId": null,
      "input": {
        "afield": "xyz"
      },
      "onComplete": {
        "nodeGroup": "master",
        "callback": "txComplete"
      },
      "note": null,
      "completionStatus": null,
      "output": null
    }
  ]
}
```

