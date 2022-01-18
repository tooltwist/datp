/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import assert from 'assert'
import indentPrefix from '../../lib/indentPrefix'
import GenerateHash from '../GenerateHash'
import { PIPELINES_VERBOSE } from '../hardcoded-steps/PipelineStep'
import { STEP_SUCCESS, STEP_ABORTED, STEP_FAILED, STEP_INTERNAL_ERROR } from '../Step'
import Scheduler2, { DEFAULT_QUEUE } from './Scheduler2'
import Transaction from './Transaction'
import TransactionCache from './TransactionCache'


export const PIPELINE_STEP_COMPLETE_CALLBACK = `pipelineStepComplete`

export async function pipelineStepCompleteCallback (callbackContext, nodeInfo) {
  if (PIPELINES_VERBOSE) console.log(`==> Callback pipelineStepCompleteCallback() context=`, callbackContext, nodeInfo)

  // Get the transaction details
  const txId = callbackContext.txId
  const tx = await TransactionCache.findTransaction(txId, true)
  const txData = tx.txData()
  // console.log(`txData=`, txData)


  const pipelineStep = tx.stepData(callbackContext.parentStepId)
  assert(pipelineStep)
  // console.log(`pipelineStep=`, pipelineStep)
  const childStep = tx.stepData(callbackContext.childStepId)
  assert(childStep)
  // console.log(`childStep=`, childStep)

  // Tell the transaction we're back from the child, back to this pipeline.
  await tx.delta(null, {
    currentStepId: callbackContext.parentStepId
  })

  // Prefix to make debug messages nice
  const indent = indentPrefix(pipelineStep.level)

  /*
   *  What we do next depends on how the step completed.
   *  - Do we continue to the next step?
   *  - Do we roll back previously run steps?
   */
  const pipelineStepId = callbackContext.parentStepId
  const pipelineSteps = pipelineStep.pipelineSteps
  // console.log(`pipelineSteps=`, pipelineSteps)
  const childStepIds = pipelineStep.childStepIds
  // console.log(`childStepIds=`, childStepIds)
  const indexOfCurrentChildStep = pipelineStep.indexOfCurrentChildStep
  const childStatus = childStep.status

  Transaction.bulkLogging(txId, pipelineStepId, [{
    level: Transaction.LOG_LEVEL_TRACE,
    source: Transaction.LOG_SOURCE_SYSTEM,
    message: `Pipeline child #${indexOfCurrentChildStep} completed with status ${childStep.status}`
  }])


  if (childStatus === STEP_SUCCESS) {
    // Do we have any steps left
    // console.log(`yarp D - ${this.stepNo}`)
    const nextStepNo = indexOfCurrentChildStep + 1
    // // const currentStepNo = contextForCompletionHandler.stepNo
    // pipelineInstance.privateData.indexOfCurrentChildStep = nextStepNo

    // const stepNo = ++pipelineInstance.privateData.indexOfCurrentChildStep
    // console.log(`yarp E - ${this.stepNo}`)
    // if (nextStepNo >= pipelineInstance.privateData.numSteps) {
    // if (PIPELINES_VERBOSE) console.log(`Which step?  ${nextStepNo} of [0...${pipelineSteps.length - 1}]`)
    if (nextStepNo >= pipelineSteps.length) {
      /*
       *  We've finished this pipeline - return the final respone
       */
      if (PIPELINES_VERBOSE) console.log(indent + `<<<<    PIPELINE COMPLETED ${pipelineStepId}  `.black.bgGreen.bold)
      Transaction.bulkLogging(txId, pipelineStepId, [{
        level: Transaction.LOG_LEVEL_TRACE,
        source: Transaction.LOG_SOURCE_SYSTEM,
        message: `Pipeline completed with status ${childStep.status}`
      }])

      // Save the child status and output as our own
      await tx.delta(pipelineStepId, {
        stepOutput: childStep.stepOutput,
        note: childStep.note,
        status: childStep.status
      })

      // Send the event back to whoever started this step
      const queueToParentOfPipeline = Scheduler2.standardQueueName(pipelineStep.onComplete.nodeGroup, DEFAULT_QUEUE)
      await Scheduler2.enqueue_StepCompleted(queueToParentOfPipeline, {
        txId,
        // parentStepId: '-',
        stepId: pipelineStepId,
        completionToken: pipelineStep.onComplete.completionToken
      })
      return

    } else {
      // Initiate the next step
      if (PIPELINES_VERBOSE) console.log(indent + `----    ON TO THE NEXT PIPELINE STEP  `.black.bgGreen.bold)


      // Remember that we'ree moving on to the next step
      await tx.delta(pipelineStepId, {
        indexOfCurrentChildStep: nextStepNo,
      })
      Transaction.bulkLogging(txId, pipelineStepId, [{
        level: Transaction.LOG_LEVEL_TRACE,
        source: Transaction.LOG_SOURCE_SYSTEM,
        message: `Start pipeline child #${nextStepNo}`
      }])


      const childStepId = childStepIds[nextStepNo]
      const metadataForNewStep = txData.metadata
      const inputForNewStep = childStep.stepOutput
      const childFullSequence = `${pipelineStep.fullSequence}.${1 + nextStepNo}` // Start sequence at 1

      // The child will run in the same node as this pipeline.
      const queueToChild = Scheduler2.standardQueueName(nodeInfo.nodeGroup, DEFAULT_QUEUE)
      await Scheduler2.enqueue_StepStart(queueToChild, {
        txId,
        nodeGroup: nodeInfo.nodeGroup, // Child runs in same node as the pipeline step
        stepId: childStepId,
        parentNodeGroup: nodeInfo.nodeGroup,
        parentStepId: pipelineStepId,
        fullSequence: childFullSequence,
        stepDefinition: pipelineSteps[nextStepNo].definition,
        metadata: metadataForNewStep,
        data: inputForNewStep,
        level: pipelineStep.level + 1,
        onComplete: {
          nodeGroup: nodeInfo.nodeGroup,
          callback: PIPELINE_STEP_COMPLETE_CALLBACK,
          context: { txId, parentNodeGroup: nodeInfo.nodeGroup, parentStepId: pipelineStepId, childStepId }
        }
      })
    }

  } else if (
    childStatus === STEP_FAILED
    || childStatus === STEP_ABORTED || childStatus === STEP_INTERNAL_ERROR) {
    /*
      *  Need to try Rollback
      */
    // We can't rollback yet, so abort instead.
    const pipelineStatus = (childStatus === STEP_FAILED) ? STEP_ABORTED : childStatus
    //ZZZZ Log this
    if (PIPELINES_VERBOSE) console.log(indent + `<<<<    PIPELINE DID NOT SUCCEED ${pipelineStepId}  `.white.bgRed.bold)
    // console.log(``)
    // console.log(``)
    // return Scheduler.haveResult(pipelineStepId, pipelineInstance.getCompletionToken(), STEP_COMPLETED, stepOutput)
    // pipelineInstance.succeedeed(`Step ${currentStepNo} failed`, stepOutput)
    await tx.delta(pipelineStepId, {
      stepOutput: childStep.stepOutput,
      note: childStep.note,
      status: pipelineStatus
    })
    // console.log(`pipeline step is now`, tx.stepData(pipelineStepId))

    // Send the event back to whoever started this step
    const queueToParentOfPipeline = Scheduler2.standardQueueName(pipelineStep.onComplete.nodeGroup, DEFAULT_QUEUE)
    await Scheduler2.enqueue_StepCompleted(queueToParentOfPipeline, {
      txId,
      // parentStepId: '-',
      stepId: pipelineStepId,
      completionToken: pipelineStep.onComplete.completionToken
    })
    return

  } else {
    throw new Error(`Child has unknown status [${childStatus}]`)
  }
}//- pipelineStepCompleteCallback