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
import { schedulerForThisNode } from '../..'
import { GO_BACK_AND_RELEASE_WORKER } from './Worker2'
import { FLOW_PARANOID, FLOW_VERBOSE } from './queuing/redis-lua'
import { STEP_DEFINITION, validateStandardObject } from './eventValidation'
import { flow2Msg } from './flowMsg'

export const CHILD_PIPELINE_COMPLETION_CALLBACK = 'childPipelineComplete'

/**
 * This callback is used by RouterStep, which is a step in a parent pipeline that passes control
 * to a child pipeline. When the child pipline completes, the RouterStep completes with the
 * status of the child pipeline it invoked.
 */
export async function childPipelineCompletionCallback (tx, f2i, nodeInfo, worker) {
  if (FLOW_VERBOSE) flow2Msg(tx, `Callback childPipelineCompletionCallback(f2i=${f2i})`, f2i)

  assert(typeof(f2i) === 'number')

  // Get the status from the previous flow entry
  assert(f2i > 0)
  const stepStatus = tx.vf2_getStatus(f2i - 1)

  // Get the flow entry for the original pipeline.
  const pipelineF2 = tx.vf2_getF2OrSibling(f2i)
  console.log(`pipelineF2=`, pipelineF2)


  // Get the flow entry for the step that has just completed
  const childStepId = tx.vf2_getStepId(f2i - 1)
  const childStep = tx.stepData(childStepId)
  if (FLOW_PARANOID) {
    validateStandardObject('childPipelineCompletionCallback childStep', childStep, STEP_DEFINITION)
  }
  if (ROUTERSTEP_VERBOSE) console.log(`FINISHED child ${childStepId}`)

  // Get the flow entry for the pipeline that called this step
  const parentStepId = tx.vf2_getStepId(f2i)
  if (ROUTERSTEP_VERBOSE) console.log(`WHICH MEANS FINISHED parent ${parentStepId}`)
  const parentStep = tx.stepData(parentStepId)
  assert(parentStep)

  if (FLOW_PARANOID) {
    validateStandardObject('childPipelineCompletionCallback parentStep 1', parentStep, STEP_DEFINITION)
  }

  // Prefix to make debug messages nice
  const indent = indentPrefix(parentStep.level)
  // if (ROUTERSTEP_VERBOSE) console.log(indent + `==> Callback childPipelineCompletionCallback() context=`, callbackContext, nodeInfo)

  assert(stepStatus !== STEP_FAILED) // Should not happen. Pipelines either succeed, rollback to success, or abort.

  // if (ROUTERSTEP_VERBOSE) {
  //   dbLogbook.bulkLogging(txId, parentStepId, [{
  //     level: dbLogbook.LOG_LEVEL_TRACE,
  //     source: dbLogbook.LOG_SOURCE_SYSTEM,
  //     message: `Child pipeline completed with status ${stepStatus}`,
  //     sequence: childStepFullSequence,
  //     ts: Date.now()
  //   }])
  // }

  /*
   *  We've finished this pipeline - return the final respone
   */
  if (FLOW_VERBOSE) flow2Msg(tx, indent + `<<<<    ROUTERSTEP'S PIPELINE HAS COMPLETED ${parentStepId}`, f2i)

  // Save the child status as our own
  await tx.delta(parentStepId, {
    status: stepStatus
  }, 'childPipelineCompletionCallback()')
  if (FLOW_PARANOID) {
    validateStandardObject('childPipelineCompletionCallback parentStep 2', parentStep, STEP_DEFINITION)
  }


  // if (ROUTERSTEP_VERBOSE) {
  //   dbLogbook.bulkLogging(txId, parentStepId, [{
  //     level: dbLogbook.LOG_LEVEL_TRACE,
  //     source: dbLogbook.LOG_SOURCE_SYSTEM,
  //     message: `RouterStep completing with status ${childStep.status}`,
  //     sequence: parentStepFullSequence,
  //     ts: Date.now()
  //   }])
  // }

  // Update the RouterStep F2
  pipelineF2.ts3 = Date.now()

  const nextF2i = f2i + 1
  const completionToken = null
  const workerForShortcut = worker
  const rv = await schedulerForThisNode.enqueue_StepCompleted(tx, nextF2i, completionToken, workerForShortcut)
  assert(rv === GO_BACK_AND_RELEASE_WORKER)
  return GO_BACK_AND_RELEASE_WORKER
}//- childPipelineCompletionCallback
