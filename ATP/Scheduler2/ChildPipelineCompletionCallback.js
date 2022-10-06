/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import assert from 'assert'
import { STEP_FAILED } from '../Step'
import { ROUTERSTEP_VERBOSE } from '../hardcoded-steps/RouterStep'
import indentPrefix from '../../lib/indentPrefix'
import { PIPELINES_VERBOSE } from '../hardcoded-steps/PipelineStep'
import { schedulerForThisNode } from '../..'
import dbLogbook from '../../database/dbLogbook'
import { GO_BACK_AND_RELEASE_WORKER } from './Worker2'
import Scheduler2 from './Scheduler2'

export const CHILD_PIPELINE_COMPLETION_CALLBACK = 'childPipelineComplete'

/**
 * This callback is used by RouterStep, which is a step in a parent pipeline that passes control
 * to a child pipeline. When the child pipline completes, the RouterStep completes with the
 * status of the child pipeline it invoked.
 */
export async function childPipelineCompletionCallback (tx, flowIndex, nodeInfo, worker) {
  // if (PIPELINES_VERBOSE)
  console.log(`==> Callback childPipelineCompletionCallback(flowIndex=${flowIndex})`, nodeInfo)

  assert(typeof(flowIndex) === 'number')

  // Get the transaction details
  const txId = tx.getTxId()


  // Get the flow entry for the step that has just completed
  const childFlow = tx.vog_getFlowRecord(flowIndex)
  console.log(`childFlow=`.red, childFlow)
  const childStepId = childFlow.stepId
  const childStep = tx.stepData(childStepId)
  assert(childStep)

  // const childStepId = callbackContext.childStepId
  if (ROUTERSTEP_VERBOSE) console.log(`FINISHED child ${childStepId}`)
  // const childStep = tx.stepData(childStepId)
  // assert(childStep)
  // console.log(`childStep=`, childStep)
  const childStepFullSequence = childStep.fullSequence
  // console.log(`childStepFullSequence=`, childStepFullSequence)


  // Get the flow entry for the pipeline that called this step
  const parentFlowIndex = tx.vog_getParentFlowIndex(flowIndex)
  // console.log(`parentFlowIndex=`.red, parentFlowIndex)
  const parentFlow = tx.vog_getFlowRecord(parentFlowIndex)
  console.log(`parentFlow=`.red, parentFlow)

  const parentStepId = parentFlow.stepId
  if (ROUTERSTEP_VERBOSE) console.log(`WHICH MEANS FINISHED parent ${parentStepId}`)
  const parentStep = tx.stepData(parentStepId)
  assert(parentStep)
  const parentStepFullSequence = parentStep.fullSequence
  // console.log(`parentStepFullSequence=`, parentStepFullSequence)

  // Prefix to make debug messages nice
  const indent = indentPrefix(parentStep.level)
  // if (ROUTERSTEP_VERBOSE) console.log(indent + `==> Callback childPipelineCompletionCallback() context=`, callbackContext, nodeInfo)

  // Tell the transaction we're back from the child pipeline, back to the RouterStep.
  // await tx.delta(null, {
  //   currentStepId: callbackContext.parentStepId
  // }, 'childPipelineCompletionCallback()')


  const childStatus = childFlow.completionStatus
  assert(childStatus !== STEP_FAILED) // Should not happen. Pipelines either succeed, rollback to success, or abort.

  // if (ROUTERSTEP_VERBOSE) {
  //   dbLogbook.bulkLogging(txId, parentStepId, [{
  //     level: dbLogbook.LOG_LEVEL_TRACE,
  //     source: dbLogbook.LOG_SOURCE_SYSTEM,
  //     message: `Child pipeline completed with status ${childStatus}`,
  //     sequence: childStepFullSequence,
  //     ts: Date.now()
  //   }])
  // }

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

  parentFlow.note = childFlow.note
  parentFlow.completionStatus = childFlow.completionStatus
  parentFlow.output = childFlow.output

  console.log(`AFTER SETTING THE RESULT, parentFlow=`, parentFlow)


  // if (ROUTERSTEP_VERBOSE) {
  //   dbLogbook.bulkLogging(txId, parentStepId, [{
  //     level: dbLogbook.LOG_LEVEL_TRACE,
  //     source: dbLogbook.LOG_SOURCE_SYSTEM,
  //     message: `RouterStep completing with status ${childStep.status}`,
  //     sequence: parentStepFullSequence,
  //     ts: Date.now()
  //   }])
  // }

  // Send the event back to whoever started this step
  const parentNodeGroup = parentFlow.onComplete.nodeGroup
  console.log(`parentNodeGroup=`.red, parentNodeGroup)

  const workerForShortcut = worker
  const event = {
    eventType: Scheduler2.STEP_COMPLETED_EVENT,
    txId,
    // parentStepId: '-',
    // stepId: parentStepId,
    // completionToken: parentStep.onComplete.completionToken
    flowIndex: parentFlowIndex,
  }
  const rv = await schedulerForThisNode.schedule_StepCompleted(tx, parentNodeGroup, event, workerForShortcut)
  assert(rv === GO_BACK_AND_RELEASE_WORKER)
  return GO_BACK_AND_RELEASE_WORKER
}//- childPipelineCompletionCallback
