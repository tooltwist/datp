import Scheduler2, { DEFAULT_QUEUE } from "./Scheduler2"
import TransactionCache from "./TransactionCache"


export const ROOT_STEP_COMPLETE_CALLBACK = `rootStepComplete`

export async function rootStepCompleteCallback (callbackContext, nodeInfo) {
  // console.log(`==> Callback rootStepCompleteCallback()`, callbackContext, nodeInfo)

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
  const stepNote = stepData.note
  const stepStatus = stepData.status

  // Save this output and status as the transaction's result
  await tx.delta(null, {
    status: stepStatus,
    note: stepNote,
    transactionOutput: stepOutput
  })

  // Pass control back to the Transaction.
  // This occurs as an event, because the transaction may have been initiated on a different node.
  const queueName = Scheduler2.standardQueueName(txData.nodeId, DEFAULT_QUEUE)
  await Scheduler2.enqueue_TransactionCompleted(queueName, {
    txId: callbackContext.txId,
  })
}
