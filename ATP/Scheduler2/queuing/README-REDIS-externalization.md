# How Transaction State is stored in REDIS


We store the transaction state in a REDIS "hash".

Most of the state information is stored as JSON in a field named `stateJSON`, however some information is extracted and saved as separate fields in the hash, so they can be manipulated, or accessed by the Lua scripts.

|In txState                            |REDIS hash              |Summary                  |Example                   |
|--------------------------------------|------------------------|-------------------------|--------------------------|
|Field|Example value|In state|In summary|
|owner                                 |**owner**               |metadata.owner           |*acme*|
|txId                                  |**txId**                |metadata.txId            |*tx-f3dd6aefe0c05c2...*|
|externalId                            |**externalId**          |metadata.externald       |*ABC12345Z94B-37224*|
|transactionData {||||
|&nbsp;&nbsp;&nbsp;&nbsp;transactionType|**transactionType**    |metadata.transactionType |*(type of the original transaction)*|
|&nbsp;&nbsp;&nbsp;&nbsp;status        |**status**              |metadata.status          |*success*|
|&nbsp;&nbsp;&nbsp;&nbsp;metadata {    |                        |                         |(When transaction was started)|
|&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;webhook|**webhook**|                     |*http://localhost:3030/webhook*|
|&nbsp;&nbsp;&nbsp;&nbsp;}             |                        |                         ||
|&nbsp;&nbsp;&nbsp;&nbsp;transactionInput |                     |                         |*(object)*|
|&nbsp;&nbsp;&nbsp;&nbsp;transactionOutput|**txOutput**         |metadata.transactionOutput|*(object)*|
|&nbsp;&nbsp;&nbsp;&nbsp;startTime|**startTime**      |metadata.startTime  |*2022-10-16T23:14:48.527Z*|
|&nbsp;&nbsp;&nbsp;&nbsp;completionTime|**completionTime**      |metadata.completionTime  |*2022-10-16T23:14:48.527Z*|
|&nbsp;&nbsp;&nbsp;&nbsp;completionNote|**completionNote**      |metadata.completionNote  |*"Transaction complete"*|
|&nbsp;&nbsp;&nbsp;&nbsp;lastUpdated   |**lastUpdated**         |metadata.lastUpdated     |*2022-10-16T23:14:48.000Z*|
|&nbsp;&nbsp;&nbsp;&nbsp;notifiedTime  |**notifiedTime**        |metadata.notifiedTime    |*2022-10-16T23:14:48.000Z*|
|}||
|webhook {|||// Used during the webhook reply.|
|&nbsp;&nbsp;&nbsp;&nbsp;type          |**webhook_type**        |                         |*complete* or *progresReport*|
|&nbsp;&nbsp;&nbsp;&nbsp;retryCount    |**webhook_retryCount**  |                         |*5*|
|&nbsp;&nbsp;&nbsp;&nbsp;status        |**webhook_status**      |                         |*delivered*|
|&nbsp;&nbsp;&nbsp;&nbsp;comment       |**webhook_comment**     |                         |*ECONNREFUSED*|
|&nbsp;&nbsp;&nbsp;&nbsp;initialAttempt|**webhook_initial**     |                         |*1666008833005*|
|&nbsp;&nbsp;&nbsp;&nbsp;latestAttempt |**webhook_latest**      |                         |*1666008833005*|
|&nbsp;&nbsp;&nbsp;&nbsp;nextAttempt   |**webhook_next**        |                         |*1666008833005*|
|}||
|retry {||
|&nbsp;&nbsp;&nbsp;&nbsp;sleepingSince |**retry_sleepingSince** |                         |*1666008833005*|
|&nbsp;&nbsp;&nbsp;&nbsp;counter       |**retry_counter**       |                         |*5*|
|&nbsp;&nbsp;&nbsp;&nbsp;wakeTime      |**retry_wakeTime**      |                         |*1666008833005*|
|&nbsp;&nbsp;&nbsp;&nbsp;wakeSwitch    |**retry_wakeSwitch**    |                         |*haveResult*|
|&nbsp;&nbsp;&nbsp;&nbsp;wakeNodeGroup |**retry_wakeNodeGroup** |                         |*slave-1*|
|&nbsp;&nbsp;&nbsp;&nbsp;wakeStepId    |**retry_wakeStepId**    |                         |*s-6aefe6aefe6aefe....*|
|}||
|progressReport                        |**progressReport**      |progressReport           | **&lt;JSON>** |
|                                      |**ts**                  |                         |*1665936047516*|
|                                      |**event**               |                         |*(the most recent event as JSON)*|
|                                      |**queue**               |                         |*(queue the most recent event was pulled from)*|
|                                      |**pipeline**            |                         |*(pipeline of the original transaction)ZZZ*|
|                                      |**nodeGroup**           |                         |*(where the pipeline runs ZZZ*|
|                                      |**webhook_delay**       |                         |*(for debugging)*|
|                                      |**state**               |                         |*(TX state as JSON with all the above fields removed)*|
|onComplete                            |                        |                         |*(callback details)*|
|onChange                              |                        |                         |*(callback details)*|
|--------------------------------------|------------------------|-------------------------|------------------------------------------------------|

<!--
    "owner": "acme",
    "txId": "tx-669a6efcc972318ef370974d030e9b32b6c35675",
    "externalId": null,
    "transactionType": "null",
    "status": "success",
    "sequenceOfUpdate": 24,
    "completionTime": "2022-10-16T23:14:48.527Z",
    "lastUpdated": "2022-10-16T23:14:48.000Z",
    "notifiedTime": "2022-10-16T23:14:48.000Z"
  },
  "progressReport": null,
  "data": {
    "afield": "xyz"
  }
-->
  
  





# Changes to Transaction State
## Changes to make:
Move from top level to transactionData:
- transactionType
- status
- completionTime


Remove:
- sequenceOfUpdate
- transactionData.nextStepId
- transactionData.onComplete
- deltaCounter


Move into 'retry':
- sleepingSince
- sleepCounter (rename to retryCounter?)
- wakeTime
- wakeSwitch
- wakeNodeGroup
- wakeStepId

Add into 'current':
- transactionData.nodeGroup
- transactionData.nodeId
- transactionData.pipelineName


## Example transaction state

```json
{
  "txId": "tx-f3dd6aefe0c05c25fbd697e85b170e3dff5665b7",
  "owner": "acme",
  "externalId": null,
  "transactionType": "null",
  "status": "success",
  "sequenceOfUpdate": 24,
  "progressReport": null,
  "transactionOutput": {
    "afield": "xyz"
  },
  "completionTime": "2022-10-17T12:13:49.858Z",
  "sleepingSince": null,
  "sleepCounter": 0,
  "wakeTime": null,
  "wakeSwitch": null,
  "wakeNodeGroup": null,
  "wakeStepId": null,
  "deltaCounter": 24,
  "transactionData": {
    "status": "success",
    "transactionType": "null",
    "nodeGroup": "master",
    "nodeId": "nodeId-5ef8e4bfdc35c053ddfc8213ca38a479fe309676",
    "pipelineName": "null",
    "metadata": {
      "webhook": "http://localhost:3030/webhook",
      "poll": "long",
      "reply": {}
    },
    "transactionInput": {
      "afield": "xyz"
    },
    "onComplete": {
      "nodeGroup": "master"
    },
    "nextStepId": "s-1e855ac18e103080d13c01e3483c2b8a8876c28c",
    "note": "1ms",
    "transactionOutput": {
      "afield": "xyz"
    },
    "completionTime": {}
  },
  "steps": {
    "s-a9beaa13f5d3364dd6c7b9ab5b17877365fc0836": {
      "vogPath": "f3dd6a=null",
      "vogI": 0,
      "vogP": null,
      "stepDefinition": "null",
      "level": 0,
      "fullSequence": "f3dd6a",
      "status": "running",
      "vogAddedBy": "Scheduler2.startTransaction()",
      "pipelineSteps": [
        {
          "definition": {
            "min": 0,
            "max": 1,
            "forceDeepSleep": false,
            "stepType": "util/delay",
            "description": "Sleep an insignificant amount of time"
          },
          "description": "Sleep an insignificant amount of time",
          "stepType": "util/delay"
        },
        {
          "definition": {
            "forceDeepSleep": false,
            "min": 1,
            "max": 2,
            "stepType": "util/delay",
            "description": "Delay a random period of time"
          },
          "description": "Delay a random period of time"
        }
      ],
      "childStepIds": [
        "s-8388e935189a79feb8df98b146761386db06d430",
        "s-1e855ac18e103080d13c01e3483c2b8a8876c28c"
      ]
    },
    "s-8388e935189a79feb8df98b146761386db06d430": {
      "vogPath": "f3dd6a=null,1=PC.util/delay",
      "vogI": 0,
      "vogP": "s-a9beaa13f5d3364dd6c7b9ab5b17877365fc0836",
      "level": 1,
      "fullSequence": "f3dd6a.1",
      "status": "success",
      "vogAddedBy": "PipelineStep.invoke()",
      "stepDefinition": {
        "min": 0,
        "max": 1,
        "forceDeepSleep": false,
        "stepType": "util/delay",
        "description": "Sleep an insignificant amount of time"
      }
    },
    "s-1e855ac18e103080d13c01e3483c2b8a8876c28c": {
      "vogPath": "f3dd6a=null,2=PC.util/delay",
      "vogI": 1,
      "vogP": "s-a9beaa13f5d3364dd6c7b9ab5b17877365fc0836",
      "level": 1,
      "fullSequence": "f3dd6a.2",
      "status": "success",
      "vogAddedBy": "PipelineStepCompletedCallback()",
      "stepDefinition": {
        "forceDeepSleep": false,
        "min": 1,
        "max": 2,
        "stepType": "util/delay",
        "description": "Delay a random period of time"
      }
    }
  },
  "flow": [
    {
      "i": 0,
      "_tmpPath": "f3dd6a=null",
      "ts1": 1666008829830,
      "ts2": 1666008829855,
      "ts3": 0,
      "stepId": "s-a9beaa13f5d3364dd6c7b9ab5b17877365fc0836",
      "nodeId": "nodeId-5ef8e4bfdc35c053ddfc8213ca38a479fe309676",
      "input": {
        "afield": "xyz"
      },
      "onComplete": {
        "nodeGroup": "master",
        "callback": "txComplete"
      },
      "note": "1ms",
      "completionStatus": "success",
      "output": {
        "afield": "xyz"
      },
      "vog_nodeGroup": "master",
      "vog_currentPipelineStep": 1
    },
    {
      "i": 1,
      "p": 0,
      "_tmpPath": "f3dd6a=null,1=PC.util/delay",
      "ts1": 1666008829856,
      "ts2": 1666008829856,
      "ts3": 1666008829857,
      "stepId": "s-8388e935189a79feb8df98b146761386db06d430",
      "nodeId": "nodeId-5ef8e4bfdc35c053ddfc8213ca38a479fe309676",
      "input": {
        "afield": "xyz"
      },
      "onComplete": {
        "nodeGroup": "master",
        "callback": "pipelineStepComplete"
      },
      "note": "0ms",
      "completionStatus": "success",
      "output": {
        "afield": "xyz"
      },
      "vog_nodeGroup": "master"
    },
    {
      "i": 2,
      "p": 0,
      "_tmpPath": "f3dd6a=null,2=PC.util/delay",
      "ts1": 1666008829857,
      "ts2": 1666008829858,
      "ts3": 1666008829858,
      "stepId": "s-1e855ac18e103080d13c01e3483c2b8a8876c28c",
      "nodeId": "nodeId-5ef8e4bfdc35c053ddfc8213ca38a479fe309676",
      "input": {
        "afield": "xyz"
      },
      "onComplete": {
        "nodeGroup": "master",
        "callback": "pipelineStepComplete"
      },
      "note": "1ms",
      "completionStatus": "success",
      "output": {
        "afield": "xyz"
      },
      "vog_nodeGroup": "master"
    }
  ]
}
```


{\"txId\":\"tx-cc84cbc28c65514567b4eaf5e76f409a880ed1b0\",\"owner\":\"acme\",\"externalId\":null,\"transactionType\":\"example\",\"status\":\"success\",\"sequenceOfUpdate\":17,\"progressReport\":null,\"transactionOutput\":{},\"completionTime\":\"2022-10-16T16:00:47.521Z\",\"sleepingSince\":null,\"sleepCounter\":0,\"wakeTime\":null,\"wakeSwitch\":null,\"wakeNodeGroup\":null,\"wakeStepId\":null,\"deltaCounter\":17,\"transactionData\":{\"status\":\"success\",\"transactionType\":\"example\",\"nodeGroup\":\"master\",\"nodeId\":\"nodeId-ad3566313b06f03df6c2bc736246d9dc3b179e9e\",\"pipelineName\":\"example\",\"metadata\":{\"poll\":\"short\",\"reply\":{}},\"transactionInput\":{},\"onComplete\":{\"nodeGroup\":\"master\"},\"nextStepId\":\"s-cfda9980fa0a396c5d4f76014bdaef6015ccfab0\",\"note\":\"2ms\",\"transactionOutput\":{},\"completionTime\":{}},\"steps\":{\"s-b0b68b9c95448b9feebdcac1b07fe43fe2dfee73\":{\"vogPath\":\"cc84cb=example\",\"vogI\":0,\"vogP\":null,\"stepDefinition\":\"example\",\"level\":0,\"fullSequence\":\"cc84cb\",\"status\":\"running\",\"vogAddedBy\":\"Scheduler2.startTransaction()\",\"pipelineSteps\":[{\"definition\":{\"min\":0,\"max\":5,\"stepType\":\"util/delay\",\"description\":\"Delay a random period of time\"},\"description\":\"Delay a random period of time\"}],\"childStepIds\":[\"s-cfda9980fa0a396c5d4f76014bdaef6015ccfab0\"]},\"s-cfda9980fa0a396c5d4f76014bdaef6015ccfab0\":{\"vogPath\":\"cc84cb=example,1=PC.util/delay\",\"vogI\":0,\"vogP\":\"s-b0b68b9c95448b9feebdcac1b07fe43fe2dfee73\",\"level\":1,\"fullSequence\":\"cc84cb.1\",\"status\":\"success\",\"vogAddedBy\":\"PipelineStep.invoke()\",\"stepDefinition\":{\"min\":0,\"max\":5,\"stepType\":\"util/delay\",\"description\":\"Delay a random period of time\"}}},\"flow\":[{\"i\":0,\"_tmpPath\":\"cc84cb=example\",\"ts1\":1665936047516,\"ts2\":1665936047518,\"ts3\":0,\"stepId\":\"s-b0b68b9c95448b9feebdcac1b07fe43fe2dfee73\",\"nodeId\":\"nodeId-9301bf7373960019642464e47edeee433df35f71\",\"input\":{},\"onComplete\":{\"nodeGroup\":\"master\",\"callback\":\"txComplete\"},\"note\":\"2ms\",\"completionStatus\":\"success\",\"output\":{},\"vog_nodeGroup\":\"master\",\"vog_currentPipelineStep\":0},{\"i\":1,\"p\":0,\"_tmpPath\":\"cc84cb=example,1=PC.util/delay\",\"ts1\":1665936047518,\"ts2\":1665936047518,\"ts3\":1665936047521,\"stepId\":\"s-cfda9980fa0a396c5d4f76014bdaef6015ccfab0\",\"nodeId\":\"nodeId-9301bf7373960019642464e47edeee433df35f71\",\"input\":{},\"onComplete\":{\"nodeGroup\":\"master\",\"callback\":\"pipelineStepComplete\"},\"note\":\"2ms\",\"completionStatus\":\"success\",\"output\":{},\"vog_nodeGroup\":\"master\"}]}"






## Transaction Sumary
The user of DATP receives the transaction result as a summary, similar to the following.


```javascript
{
  "metadata": {
    "owner": "acme",
    "txId": "tx-669a6efcc972318ef370974d030e9b32b6c35675",
    "externalId": null,
    "transactionType": "null",
    "status": "success",
    "sequenceOfUpdate": 24,
    "completionTime": "2022-10-16T23:14:48.527Z",
    "lastUpdated": "2022-10-16T23:14:48.000Z",
    "notifiedTime": "2022-10-16T23:14:48.000Z"
  },
  "progressReport": null,
  "data": {
    "afield": "xyz"
  }
}
```

All of this information is available from the transaction state.


