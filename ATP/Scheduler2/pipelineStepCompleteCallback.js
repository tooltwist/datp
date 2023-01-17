/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import assert from 'assert'
import { schedulerForThisNode } from '../..'
import indentPrefix from '../../lib/indentPrefix'
import { PIPELINES_VERBOSE } from '../hardcoded-steps/PipelineStep'
import { STEP_SUCCESS, STEP_ABORTED, STEP_FAILED, STEP_INTERNAL_ERROR } from '../Step'
import { STEP_DEFINITION, validateStandardObject } from './eventValidation'
import { flow2Msg } from './flowMsg'
import { FLOW_PARANOID, FLOW_VERBOSE } from './queuing/redis-lua'
import Scheduler2 from './Scheduler2'
import { F2ATTR_CALLBACK, F2ATTR_CURRENT_PIPELINE_STEP, F2ATTR_NODEGROUP, F2ATTR_SIBLING, F2ATTR_STEPID, F2_PIPELINE_CH, F2_STEP, F2_VERBOSE } from './TransactionState'
import { GO_BACK_AND_RELEASE_WORKER } from './Worker2'


export const PIPELINE_STEP_COMPLETE_CALLBACK = `pipelineStepComplete`

export async function pipelineStepCompleteCallback (tx, f2i, nodeInfo, worker) {
  if (FLOW_VERBOSE) flow2Msg(tx, `Callback pipelineStepCompleteCallback(f2i=${f2i})`, f2i)
  if (F2_VERBOSE) console.log(`F2: pipelineStepCompleteCallback: ${f2i}`)

  assert(typeof(f2i) === 'number')
  assert(f2i > 0)

  // Get the transaction details
  const txId = tx.getTxId()

  // Get the status from the previous flow entry
  const stepStatus = tx.vf2_getStatus(f2i - 1)

  // Get the flow entry for the original pipeline.
  const pipelineF2 = tx.vf2_getF2OrSibling(f2i)
  assert(typeof(pipelineF2[F2ATTR_CURRENT_PIPELINE_STEP]) === 'number')

  // Get the flow entry for the pipeline that called this step
  const parentStepId = tx.vf2_getStepId(f2i)

  const pipelineStep = tx.stepData(parentStepId)
  assert(pipelineStep)
  if (FLOW_PARANOID) {
    validateStandardObject('pipelineStepCompleteCallback pipelineStep 1', pipelineStep, STEP_DEFINITION)
  }

  // // Tell the transaction we're back from the child, back to this pipeline.
  // await tx.delta(null, {
  //   currentStepId: callbackContext.parentStepId
  // }, 'pipelineStepCompleteCallback()')

  // Prefix to make debug messages nice
  const indent = indentPrefix(pipelineStep.level)

  /*
   *  What we do next depends on how the step completed.
   *  - Do we continue to the next step?
   *  - Do we roll back previously run steps?
   */
  const pipelineStepId = pipelineStep.stepId
  const pipelineSteps = pipelineStep.pipelineSteps
  const childStepIds = pipelineStep.childStepIds

  if (stepStatus === STEP_SUCCESS) {



    // Do we have any steps left
    pipelineF2[F2ATTR_CURRENT_PIPELINE_STEP]++
    const nextStepNo = pipelineF2[F2ATTR_CURRENT_PIPELINE_STEP]
    // if (PIPELINES_VERBOSE) console.log(`Which step?  ${nextStepNo} of [0...${pipelineSteps.length - 1}]`)
    if (nextStepNo >= childStepIds.length) {


      /*************************************************************
       *
       * 
       *      The step SUCCEEDED, and that was THE LAST STEP.
       * 
       * 
       *************************************************************/

      /*
       *  We've finished this pipeline - return the final response
       */
      if (PIPELINES_VERBOSE) console.log(indent + `<<<<    PIPELINE COMPLETED ${pipelineStepId}  `.black.bgGreen.bold)
      // if (PIPELINES_VERBOSE) console.log(`pipelineStep.onComplete=`, pipelineStep.onComplete)
      // if (PIPELINES_VERBOSE) {
      //   dbLogbook.bulkLogging(txId, pipelineStepId, [{
      //     level: dbLogbook.LOG_LEVEL_TRACE,
      //     source: dbLogbook.LOG_SOURCE_SYSTEM,
      //     message: `Pipeline completed with status ${childStep.status}`,
      //     sequence: pipelineFullSequence,
      //     ts: Date.now()
      //   }])
      // }
      // console.log(`POINT VOG_83, childFlow=`.red, childFlow)

      // Save the child status as our own
      await tx.delta(pipelineStepId, {
        status: stepStatus
      }, 'pipelineStepCompleteCallback()')
      if (FLOW_PARANOID) {
        validateStandardObject('pipelineStepCompleteCallback pipelineStep 2', pipelineStep, STEP_DEFINITION)
      }

// dbLogbook.bulkLogging(txId, pipelineStepId, [{
//   level: dbLogbook.LOG_LEVEL_TRACE,
//   source: dbLogbook.LOG_SOURCE_SYSTEM,
//   message: `delta count YARP ${tx.getDeltaCounter()}`,
//   sequence: txId.substring(0, 6),
//   ts: Date.now()
// }])

      // Update the final time for the pipeline.
      pipelineF2.ts3 = Date.now()

      const nextF2i = f2i + 1
      if (F2_VERBOSE) console.log(`F2: pipelineStepCompleteCallback: Pipeline finished, go to ${nextF2i}`.bgBrightBlue.white)
      const completionToken = null
      const workerForShortcut = worker
      const rv = await schedulerForThisNode.enqueue_StepCompleted(tx, nextF2i, completionToken, workerForShortcut)
      assert(rv === GO_BACK_AND_RELEASE_WORKER)
      return GO_BACK_AND_RELEASE_WORKER

    } else {

      /*************************************************************
       *
       * 
       *      The step SUCCEEDED, and THERE ARE MORE STEPS.
       * 
       * 
       *************************************************************/

      if (PIPELINES_VERBOSE) console.log(indent + `----    ON TO THE NEXT PIPELINE STEP  `.black.bgGreen.bold)

      const childStepId = childStepIds[nextStepNo]
      const metadataForNewStep = tx.vog_getMetadata()
      const inputForNewStep = tx.vf2_getOutput(f2i)

      // The child will run in this node - same as this pipeline.
      // We keep the steps all running on the same node, so they all use the same
      // cached transaction. We only jump to another node when we are calling a
      // pipline that runs on another node.
      const myNodeGroup = schedulerForThisNode.getNodeGroup()

      //VOGGY
      if (FLOW_VERBOSE) {
        // console.log(`--------------------------------------`)
        flow2Msg(tx, `pipelineStepCompleteCallback`, f2i)
        // console.log(`--------------------------------------`)
      }

      //ZZZZZ Stuff to delete
      // await tx.delta(childStepId, {
      //   stepDefinition: pipelineSteps[nextStepNo].definition,
      // })


      // Add the next child to f2
      const myF2 = tx.vf2_getF2(f2i)
      assert(myF2)

      const { f2i:childF2i, f2:childF2} = tx.vf2_addF2child(f2i, F2_STEP, 'pipelineStepCompleteCallback')
      childF2[F2ATTR_STEPID] = childStepId
      childF2.ts1 = Date.now()
      childF2.ts2 = 0
      childF2.ts3 = 0
      const { f2i: completionF2i, f2:completionHandlerF2 } = tx.vf2_addF2sibling(f2i, F2_PIPELINE_CH, 'pipelineStepCompleteCallback')
      completionHandlerF2[F2ATTR_CALLBACK] = PIPELINE_STEP_COMPLETE_CALLBACK
      completionHandlerF2[F2ATTR_NODEGROUP] = schedulerForThisNode.getNodeGroup()

      const nextF2i = f2i + 1


      const event = {
        eventType: Scheduler2.STEP_START_EVENT,
        txId,
        parentNodeGroup: nodeInfo.nodeGroup,
        stepDefinition: pipelineSteps[nextStepNo].definition,
        metadata: metadataForNewStep,
        data: inputForNewStep,
        level: pipelineStep.level + 1,
        f2i: nextF2i,
      }
      const onComplete = {
        nodeGroup: myNodeGroup,
        callback: PIPELINE_STEP_COMPLETE_CALLBACK,
      }

      if (F2_VERBOSE) console.log(`F2: pipelineStepCompleteCallback: On to next step ${nextF2i}`.bgBrightBlue.white)

      const rv = await schedulerForThisNode.enqueue_StartStep(tx, childStepId, event, onComplete, worker)
      assert(rv === GO_BACK_AND_RELEASE_WORKER)
      return GO_BACK_AND_RELEASE_WORKER
    }//- initiate the next step

  } else if (
    stepStatus === STEP_FAILED
    || stepStatus === STEP_ABORTED // Should this be included? ZZZZ
    || stepStatus === STEP_INTERNAL_ERROR
  ) {

    /*************************************************************
     *
     *        The step DID NOT succeed. Need to try Rollback
     *
     *************************************************************/
    // We can't rollback yet, so abort instead.
    const pipelineStatus = (stepStatus === STEP_FAILED) ? STEP_ABORTED : stepStatus
    //ZZZZ Log this
    if (PIPELINES_VERBOSE) console.log(indent + `<<<<    PIPELINE DID NOT SUCCEED ${pipelineStepId}  `.white.bgRed)
//ZM    await tx.delta(pipelineStepId, {
//ZM      stepOutput: childStep.stepOutput,
//ZM      // note: childStep.note,
//ZM      // status: pipelineStatus
//ZM    }, 'pipelineStepCompleteCallback()')

    // Send the event back to whoever started this step
    const nextF2i = f2i + 1
    const completionToken = null
    const workerForShortcut = worker
    const rv = await schedulerForThisNode.enqueue_StepCompleted(tx, nextF2i, completionToken, workerForShortcut)
    assert(rv === GO_BACK_AND_RELEASE_WORKER)
    return GO_BACK_AND_RELEASE_WORKER

  } else {
    throw new Error(`Pipeline step returned status [${stepStatus}]`)
  }
}//- pipelineStepCompleteCallback

