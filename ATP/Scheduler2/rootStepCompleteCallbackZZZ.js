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


export const ROOT_STEP_COMPLETE_CALLBACK_ZZZ = `rootStepComplete`

export async function rootStepCompleteCallbackZZZ (tx, flowIndex, nodeInfo, worker) {
  // if (PIPELINES_VERBOSE)
  console.log(`Callback rootStepCompleteCallbackZZZ(flowIndex=${flowIndex})`.brightYellow)
  assert(false)

  assert(typeof(flowIndex)==='number')

  // Get the details of the root step
  const txData = tx.txData()

  // Get the step output and status
  const stepData = tx.stepData(callbackContext.stepId)
  const stepOutput = stepData.stepOutput
  const stepNote = stepData.note ? stepData.note : null
  const stepStatus = stepData.status

  // Save this output and status as the transaction's result
  // await tx.delta(null, {
  //   status: stepStatus,
  //   note: stepNote,
  //   transactionOutput: stepOutput,
  //   "-progressReport": null,
  //   completionTime: new Date()
  // }, 'rootStepCompleteCallbackZZZ()')

  tx.vog_setToComplete(stepStatus, note, transactionOutput)

  
  // Pass control back to the Transaction.
  // This occurs as an event, because the transaction may have been initiated in a different node group.
  const txInitNodeGroup = txData.nodeGroup
  const txInitNodeId = txData.nodeId
  // console.log(`txInitNodeGroup=`, txInitNodeGroup)
  // console.log(`txInitNodeId=`, txInitNodeId)

  // If the transaction was initiated in a different node group, we'll reply via the group
  // queue for that nodeGroup. If it was initiated in the current node group, we'll
  // run it in this current node, so it'll have access to the cached transaction.
  // const myNodeGroup = schedulerForThisNode.getNodeGroup()
  // const myNodeId = schedulerForThisNode.getNodeId()
  // let queueName
  // if (nodeGroupWhereTransactionWasInitiated === myNodeGroup) {
  //   // Run the new pipeline in this node - put the event in this node's pipeline.
  //   queueName = Scheduler2.nodeExpressQueueName(myNodeGroup, myNodeId)
  // } else {
  //   // The new pipeline will run in a different nodeGroup. Put the event in the group queue.
  //   queueName = Scheduler2.groupQueueName(nodeGroupWhereTransactionWasInitiated)
  // }

  // const queueName = Scheduler2.nodeRegularQueueName(txData.nodeGroup, txData.nodeId)
  // const queueName = Scheduler2.groupQueueName(txData.nodeGroup)

  console.log(`FINAL tx.vog_getFlow()=`, tx.vog_getFlow())

  const rv = await schedulerForThisNode.enqueue_TransactionCompleted(tx, txInitNodeGroup, txInitNodeId, worker, {
    txId: callbackContext.txId,
  })
  assert(rv === GO_BACK_AND_RELEASE_WORKER)
  return GO_BACK_AND_RELEASE_WORKER
}
