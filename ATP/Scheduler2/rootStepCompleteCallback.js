/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import { schedulerForThisNode } from "../.."
import { PIPELINES_VERBOSE } from "../hardcoded-steps/PipelineStep"
import Scheduler2 from "./Scheduler2"
import TransactionCache from "./TransactionCache"


export const ROOT_STEP_COMPLETE_CALLBACK = `rootStepComplete`

export async function rootStepCompleteCallback (callbackContext, nodeInfo) {
  if (PIPELINES_VERBOSE) console.log(`==> Callback rootStepCompleteCallback()`, callbackContext, nodeInfo)

  // Get the details of the root step
  const tx = await TransactionCache.findTransaction(callbackContext.txId, true)
  // console.log(`tx=`, tx)
  // const obj = transaction.asObject()

  const txData = tx.txData()
  // console.log(`txData=`, txData)

  // Get the step output and status
  const stepData = tx.stepData(callbackContext.stepId)
  // console.log(`stepData=`, stepData)
  const stepOutput = stepData.stepOutput
  const stepNote = stepData.note ? stepData.note : null
  const stepStatus = stepData.status

  // Save this output and status as the transaction's result
  await tx.delta(null, {
    status: stepStatus,
    note: stepNote,
  })

    transactionOutput: stepOutput,
    "-progressReport": null,
    completionTime: new Date()
  // Pass control back to the Transaction.
  // This occurs as an event, because the transaction may have been initiated in a different node group.
  const nodeGroupWhereTransactionWasInitiated = txData.nodeGroup

  // If the transaction was initiated in a different node group, we'll reply via the group
  // queue for that nodeGroup. If it was initiated in the current node group, we'll
  // run it in this current node, so it'll have access to the cached transaction.
  const myNodeGroup = schedulerForThisNode.getNodeGroup()
  const myNodeId = schedulerForThisNode.getNodeId()
  let queueName
  if (nodeGroupWhereTransactionWasInitiated === myNodeGroup) {
    // Run the new pipeline in this node - put the event in this node's pipeline.
    queueName = Scheduler2.nodeExpressQueueName(myNodeGroup, myNodeId)
  } else {
    // The new pipeline will run in a different nodeGroup. Put the event in the group queue.
    queueName = Scheduler2.groupQueueName(nodeGroupWhereTransactionWasInitiated)
  }

  // const queueName = Scheduler2.nodeRegularQueueName(txData.nodeGroup, txData.nodeId)
  // const queueName = Scheduler2.groupQueueName(txData.nodeGroup)
  await schedulerForThisNode.enqueue_TransactionCompleted(queueName, {
    txId: callbackContext.txId,
  })
}
