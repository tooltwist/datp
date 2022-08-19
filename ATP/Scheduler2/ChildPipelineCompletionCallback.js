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

export const CHILD_PIPELINE_COMPLETION_CALLBACK = 'childPipelineComplete'

/**
 * This callback is used by RouterStep, which is a step in a parent pipeline that passes control
 * to a child pipeline. When the child pipline completes, the RouterStep completes with the
 * status of the child pipeline it invoked.
 */
export async function childPipelineCompletionCallback (tx, callbackContext, nodeInfo, worker) {
  if (PIPELINES_VERBOSE) console.log(`==> Callback childPipelineCompletionCallback()`, callbackContext, nodeInfo)

  // Get the transaction details
  const txId = callbackContext.txId

  const childStepId = callbackContext.childStepId
  if (ROUTERSTEP_VERBOSE) console.log(`FINISHED child ${childStepId}`)
  const childStep = tx.stepData(childStepId)
  assert(childStep)
  // console.log(`childStep=`, childStep)
  const childStepFullSequence = childStep.fullSequence
  // console.log(`childStepFullSequence=`, childStepFullSequence)


  const parentStepId = callbackContext.parentStepId
  if (ROUTERSTEP_VERBOSE) console.log(`WHICH MEANS FINISHED parent ${parentStepId}`)
  const parentStep = tx.stepData(parentStepId)
  assert(parentStep)
  const parentStepFullSequence = parentStep.fullSequence
  // console.log(`parentStepFullSequence=`, parentStepFullSequence)
  const indent = indentPrefix(parentStep.level)
  if (ROUTERSTEP_VERBOSE) console.log(indent + `==> Callback childPipelineCompletionCallback() context=`, callbackContext, nodeInfo)

  // Tell the transaction we're back from the child pipeline, back to the RouterStep.
  await tx.delta(null, {
    currentStepId: callbackContext.parentStepId
  }, 'childPipelineCompletionCallback()')


  const childStatus = childStep.status
  assert(childStatus !== STEP_FAILED) // Should not happen. Pipelines either succeed, rollback to success, or abort.

  dbLogbook.bulkLogging(txId, parentStepId, [{
    level: dbLogbook.LOG_LEVEL_TRACE,
    source: dbLogbook.LOG_SOURCE_SYSTEM,
    message: `Child pipeline completed with status ${childStatus}`,
    sequence: childStepFullSequence,
    ts: Date.now()
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
    message: `RouterStep completing with status ${childStep.status}`,
    sequence: parentStepFullSequence,
    ts: Date.now()
  }])

  // Send the event back to whoever started this step
  const parentNodeGroup = parentStep.onComplete.nodeGroup
  const parentNodeId = parentStep.onComplete.nodeId ? parentStep.onComplete.nodeId : null

  const workerForShortcut = worker
  const rv = await schedulerForThisNode.schedule_StepCompleted(tx, parentNodeGroup, parentNodeId, {
    txId,
    // parentStepId: '-',
    stepId: parentStepId,
    completionToken: parentStep.onComplete.completionToken
  }, workerForShortcut)
  assert(rv === GO_BACK_AND_RELEASE_WORKER)
  return GO_BACK_AND_RELEASE_WORKER
}//- childPipelineCompletionCallback
