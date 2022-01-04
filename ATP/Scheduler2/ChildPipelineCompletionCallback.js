/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import XData from '../XData'
import assert from 'assert'
import TransactionCache from './TransactionCache'
import Scheduler2, { DEFAULT_QUEUE } from './Scheduler2'
import { STEP_FAILED } from '../Step'
import { ROUTERSTEP_VERBOSE } from '../hardcoded-steps/RouterStep'
import indentPrefix from '../../lib/indentPrefix'
import { PIPELINES_VERBOSE } from '../hardcoded-steps/PipelineStep'

export const CHILD_PIPELINE_COMPLETION_CALLBACK = 'childPipelineComplete'

/**
 * This callback is used by RouterStep, which is a step in a parent pipeline that passes control
 * to a child pipeline. When the child pipline completes, the RouterStep completes with the
 * status of the child pipeline it invoked.
 */
export async function childPipelineCompletionCallback (callbackContext, nodeInfo) {
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
      const tx = await TransactionCache.findTransaction(txId, true)
      const txData = tx.txData()

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
      })


      const childStatus = childStep.status
      assert(childStatus !== STEP_FAILED) // Should not happen. Pipelines either succeed, rollback to success, or abort.
      // if (childStatus === STEP_SUCCESS || childStatus === STEP_ABORTED) {
        /*
        *  We've finished this pipeline - return the final respone
        */
        if (ROUTERSTEP_VERBOSE) console.log(indent + `<<<<    ROUTERSTEP'S PIPELINE HAS COMPLETED ${parentStepId}  `.black.bgGreen.bold)

        // Save the child status and output as our own
        await tx.delta(parentStepId, {
          stepOutput: childStep.stepOutput,
          note: childStep.note,
          status: childStep.status
        })

        // Send the event back to whoever started this step
        const queueToParent = Scheduler2.standardQueueName(parentStep.onComplete.nodeGroup, DEFAULT_QUEUE)
        await Scheduler2.enqueue_StepCompleted(queueToParent, {
          txId,
          // parentStepId: '-',
          stepId: parentStepId,
          completionToken: parentStep.onComplete.completionToken
        })
        return


/*

      // const parentStepId = callbackContext.parentStepId
      // const parentIndexEntry = await Scheduler.getStepEntry(parentStepId)
      // if (!parentIndexEntry) {
      //   throw new Error(`Internal error 827772: could not find transfer step in Scheduler (${parentStepId})`)
      // }
      // const parentInstance = await parentIndexEntry.getStepInstance()

      // Complete the parent step, based on what the child pipline returned.
      assert(status !== STEP_FAILED) // Should not happen. Pipelines either rollback or abort.
      switch (status) {
        case STEP_COMPLETED:
          console.log(`Child pipeline is COMPLETED`)
          return await parentInstance.succeeded(note, response)

        // case STEP_FAILED:
        //   console.log(`Child pipeline is FAILED`)
        //   return await parentInstance.failed(note, response)

        case STEP_ABORTED:
          console.log(`Child pipeline is ABORTED`)
          return await parentInstance.failed(note, response)

        default:
          console.log(`------- Step completed with an unrecognised status ${status}`)
          return await parentInstance.exceptionInStep(`Child transfer pipeline returned unknown status [${status}]`, { })
      }
*/
    // } catch (e) {
    //   console.trace(`Error in ChildPipelineCompletionHandler`, e)
    //   throw e
    // }
  // }//- haveResult
}

// async function register() {
//   await ResultReceiverRegister.register(CHILD_PIPELINE_COMPLETION_HANDLER_NAME, new ChildPipelineCompletionHandler())
// }//- register

// const myDef = {
//   register,
//   CHILD_PIPELINE_COMPLETION_HANDLER_NAME
// }
// export default myDef
