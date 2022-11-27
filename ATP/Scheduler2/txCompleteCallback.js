/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import { schedulerForThisNode } from "../.."
import { PIPELINES_VERBOSE } from "../hardcoded-steps/PipelineStep"
import Scheduler2 from "./Scheduler2"
import { GO_BACK_AND_RELEASE_WORKER } from "./Worker2"
import assert from "assert"
import { RedisQueue } from "./queuing/RedisQueue-ioredis"
import { FLOW_VERBOSE } from "./queuing/redis-lua"
import { flow2Msg, flowMsg } from "./flowMsg"


export const TX_COMPLETE_CALLBACK = `txComplete`

export async function txCompleteCallback (tx, flowIndex, f2i , nodeInfo, worker) {
  // if (FLOW_VERBOSE)
  flowMsg(tx, `Callback txCompleteCallback(flowIndex=${flowIndex})`, flowIndex)
  
  // Save the final step's output, status and note as the transaction's result
  const f2Status = tx.vf2_getStatus(f2i)
  const f2Note = tx.vf2_getNote(f2i)
  const f2Output = tx.vf2_getOutput(f2i)
  tx.vog_setToComplete(f2Status, f2Note, f2Output)
  
  // Update the root f2
  const rootF2 = tx.vf2_getF2(0)
  rootF2.ts3 = Date.now()

  // console.log(`

  

  // FROM HERE WE NEED TO EITHER SCHEDULE REPLY VIA...
  
  // - WEBHOOK IF REQUESTED, and
  
  // - POSSIBLY LONG POLL, VIA PUB/SUB
  
  // Once these are requested, we've finished processing the transaction.
  
  // Yay!
  
  

  // (finished ${Date.now() % 10000})
  
  // `.magenta)



  //VOGTX
  // console.log(`returnTxStatusCallbackZZZ tx=`, JSON.stringify(tx.asObject(), '', 2))
  const redisLua = await RedisQueue.getRedisLua()
  await redisLua.luaTransactionCompleted(tx)




  return GO_BACK_AND_RELEASE_WORKER

  // assert(typeof(flowIndex)==='number')

  // // Get the details of the root step
  // const txData = tx.txData()

  // // Get the step output and status
  // const stepData = tx.stepData(callbackContext.stepId)
  // const stepOutput = stepData.stepOutput
  // const stepNote = stepData.note ? stepData.note : null
  // const stepStatus = stepData.status

  // // Save this output and status as the transaction's result
  // await tx.delta(null, {
  //   status: stepStatus,
  //   note: stepNote,
  //   transactionOutput: stepOutput,
  //   "-progressReport": null,
  //   completionTime: new Date()
  // }, 'rootStepCompleteCallback()')
  
  // // Pass control back to the Transaction.
  // // This occurs as an event, because the transaction may have been initiated in a different node group.
  // const txInitNodeGroup = txData.nodeGroup
  // const txInitNodeId = txData.nodeId
  // // console.log(`txInitNodeGroup=`, txInitNodeGroup)
  // // console.log(`txInitNodeId=`, txInitNodeId)

  // // If the transaction was initiated in a different node group, we'll reply via the group
  // // queue for that nodeGroup. If it was initiated in the current node group, we'll
  // // run it in this current node, so it'll have access to the cached transaction.
  // // const myNodeGroup = schedulerForThisNode.getNodeGroup()
  // // const myNodeId = schedulerForThisNode.getNodeId()
  // // let queueName
  // // if (nodeGroupWhereTransactionWasInitiated === myNodeGroup) {
  // //   // Run the new pipeline in this node - put the event in this node's pipeline.
  // //   queueName = Scheduler2.nodeExpressQueueName(myNodeGroup, myNodeId)
  // // } else {
  // //   // The new pipeline will run in a different nodeGroup. Put the event in the group queue.
  // //   queueName = Scheduler2.groupQueueName(nodeGroupWhereTransactionWasInitiated)
  // // }

  // // const queueName = Scheduler2.nodeRegularQueueName(txData.nodeGroup, txData.nodeId)
  // // const queueName = Scheduler2.groupQueueName(txData.nodeGroup)

  // console.log(`FINAL tx.vog_getFlow()=`, tx.vog_getFlow())

  // const rv = await schedulerForThisNode.enqueue_TransactionCompleted(tx, txInitNodeGroup, txInitNodeId, worker, {
  //   txId: callbackContext.txId,
  // })
  // assert(rv === GO_BACK_AND_RELEASE_WORKER)
  // return GO_BACK_AND_RELEASE_WORKER
}
