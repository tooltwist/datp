/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import assert from 'assert'
import TransactionCache from './TransactionCache'
import { STEP_FAILED } from '../Step'
import { ROUTERSTEP_VERBOSE } from '../hardcoded-steps/RouterStep'
import indentPrefix from '../../lib/indentPrefix'
import { PIPELINES_VERBOSE } from '../hardcoded-steps/PipelineStep'
import { schedulerForThisNode } from '../..'
import dbLogbook from '../../database/dbLogbook'
import { GO_BACK_AND_RELEASE_WORKER } from './Worker2'

export const CHILD_PIPELINE_COMPLETION_CALLBACK = 'childPipelineComplete'

/**
 * This callback is used by RouterStep, which is a step in a parent pipeline that passes control
 * to a child pipeline. When the child pipline completes, the RouterStep completes with the
 * status of the child pipeline it invoked.
 */
export async function childPipelineCompletionCallback (callbackContext, nodeInfo, worker) {
  if (PIPELINES_VERBOSE) console.log(`==> Callback childPipelineCompletionCallback()`, callbackContext, nodeInfo)
  // constructor() {
  //   super()
  // }
  // async haveResult(contextForCompletionHandler, status, note, response) {
    // assert(response instanceof XData)
    // console.log(`<<<<    ChildPipelineCompletionHandler.haveResult(${status}, ${note})  `.white.bgBlue.bold)

    // try {

      // Get the transaction details
      const txId = callbackContext.txId
      const tx = await TransactionCache.getTransactionState(txId)
      // const txData = tx.txData()

      // Update the transaction status
      // const txId = contextForCompletionHandler.txId

      const childStepId = callbackContext.childStepId
      if (ROUTERSTEP_VERBOSE) console.log(`FINISHED child ${childStepId}`)
      const childStep = tx.stepData(childStepId)
      assert(childStep)
      // console.log(`childStep=`, childStep)

      const parentStepId = callbackContext.parentStepId
      if (ROUTERSTEP_VERBOSE) console.log(`WHICH MEANS FINISHED parent ${parentStepId}`)
      const parentStep = tx.stepData(parentStepId)
      assert(parentStep)
      // console.log(`parentStep=`, parentStep)
      const indent = indentPrefix(parentStep.level)
      if (ROUTERSTEP_VERBOSE) console.log(indent + `==> Callback childPipelineCompletionCallback() context=`, callbackContext, nodeInfo)

      // Tell the transaction we're back from the child pipeline, back to the RouterStep.
      await tx.delta(null, {
        currentStepId: callbackContext.parentStepId
      }, 'childPipelineCompletionCallback()')


      const childStatus = childStep.status
      assert(childStatus !== STEP_FAILED) // Should not happen. Pipelines either succeed, rollback to success, or abort.
      // if (childStatus === STEP_SUCCESS || childStatus === STEP_ABORTED) {

      dbLogbook.bulkLogging(txId, parentStepId, [{
        level: dbLogbook.LOG_LEVEL_TRACE,
        source: dbLogbook.LOG_SOURCE_SYSTEM,
        message: `Child pipeline completed with status #${childStatus}`
      }])



        /*
        *  We've finished this pipeline - return the final respone
        */
        if (ROUTERSTEP_VERBOSE) console.log(indent + `<<<<    ROUTERSTEP'S PIPELINE HAS COMPLETED ${parentStepId}  `.black.bgGreen.bold)

        // Save the child status and output as our own
        await tx.delta(parentStepId, {
          stepOutput: childStep.stepOutput,
          note: childStep.note,
          status: childStep.status
        }, 'childPipelineCompletionCallback()')

        dbLogbook.bulkLogging(txId, parentStepId, [{
          level: dbLogbook.LOG_LEVEL_TRACE,
          source: dbLogbook.LOG_SOURCE_SYSTEM,
          message: `This step will complete with status ${childStep.status}`
        }])


        // Send the event back to whoever started this step
        const parentNodeGroup = parentStep.onComplete.nodeGroup
        const parentNodeId = parentStep.onComplete.nodeId ? parentStep.onComplete.nodeId : null

        const workerForShortcut = worker
        const rv = await schedulerForThisNode.schedule_StepCompleted(parentNodeGroup, parentNodeId, tx, {
        // const queueToParent = Scheduler2.groupQueueName(parentStep.onComplete.nodeGroup)
        // const rv = await schedulerForThisNode.enqueue_StepCompletedZZ(queueToParent, {
          txId,
          // parentStepId: '-',
          stepId: parentStepId,
          completionToken: parentStep.onComplete.completionToken
        }, workerForShortcut)
        assert(rv === GO_BACK_AND_RELEASE_WORKER)
        return GO_BACK_AND_RELEASE_WORKER
}
