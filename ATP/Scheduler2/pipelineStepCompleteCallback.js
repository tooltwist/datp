/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import assert from 'assert'
import { schedulerForThisNode } from '../..'
import dbLogbook from '../../database/dbLogbook'
import indentPrefix from '../../lib/indentPrefix'
import pause from '../../lib/pause'
import GenerateHash from '../GenerateHash'
import { PIPELINES_VERBOSE } from '../hardcoded-steps/PipelineStep'
import { STEP_SUCCESS, STEP_ABORTED, STEP_FAILED, STEP_INTERNAL_ERROR } from '../Step'
import Scheduler2 from './Scheduler2'
import { GO_BACK_AND_RELEASE_WORKER } from './Worker2'


export const PIPELINE_STEP_COMPLETE_CALLBACK = `pipelineStepComplete`

export async function pipelineStepCompleteCallback (tx, flowIndex, nodeInfo, worker) {
  // if (PIPELINES_VERBOSE)
  console.log(`==> Callback pipelineStepCompleteCallback(flowIndex=${flowIndex})`.blue, nodeInfo)

  assert(typeof(flowIndex) === 'number')

  // console.log(`callbackContext=`, callbackContext)

  // Get the transaction details
  // const txId = callbackContext.txId
  const txId = tx.getTxId()
  const txData = tx.txData()
  // console.log(`tx=`, tx)

  // Get the flow entry for the step that has just completed
  const childFlow = tx.vog_getFlowRecord(flowIndex)
  console.log(`childFlow=`.red, childFlow)
  const childStep = tx.stepData(childFlow.stepId)
  assert(childStep)
  // const childStepFullSequence = childStep.fullSequence
  // console.log(`childStep=`, childStep)
  // console.log(`childStepFullSequence=`, childStepFullSequence)

  // Get the flow entry for the pipeline that called this step
  const parentFlowIndex = tx.vog_getParentFlowIndex(flowIndex)
  // console.log(`parentFlowIndex=`.red, parentFlowIndex)
  const parentFlow = tx.vog_getFlowRecord(parentFlowIndex)
  console.log(`parentFlow=`.red, parentFlow)
  const pipelineStep = tx.stepData(parentFlow.stepId)
  console.log(`pipelineStep=`, pipelineStep)
  assert(pipelineStep)
  // const pipelineFullSequence = pipelineStep.fullSequence
  // console.log(`pipelineStep=`, pipelineStep)
  // console.log(`pipelineFullSequence=`, pipelineFullSequence)


  console.log(`POINT VOG_81, childFlow=`.red, childFlow)

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
  // console.log(`pipelineSteps=`, pipelineSteps)
  const childStepIds = pipelineStep.childStepIds
  // console.log(`childStepIds=`, childStepIds)
  const indexOfCurrentChildStep = pipelineStep.indexOfCurrentChildStep
  const childStatus = childFlow.completionStatus


  console.log(`indexOfCurrentChildStep=`.red, indexOfCurrentChildStep)
  console.log(`parentFlow.vogYarpYarp=`.red, parentFlow.vogYarpYarp)
  parentFlow.vogYarpYarp = indexOfCurrentChildStep
  // if (PIPELINES_VERBOSE) {
  //   dbLogbook.bulkLogging(txId, pipelineStepId, [{
  //     level: dbLogbook.LOG_LEVEL_TRACE,
  //     source: dbLogbook.LOG_SOURCE_SYSTEM,
  //     message: `Step #${indexOfCurrentChildStep+1} - end [${childStep.status}]`,
  //     sequence: pipelineFullSequence,
  //     ts: Date.now()
  //   }])
  // }
  console.log(`POINT VOG_82, childFlow=`.red, childFlow)


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
      console.log(`POINT VOG_83, childFlow=`.red, childFlow)

      // Save the child status and output as our own
      await tx.delta(pipelineStepId, {
        stepOutput: childStep.stepOutput,
        note: childStep.note,
        status: childStep.status
      }, 'pipelineStepCompleteCallback()')

      parentFlow.note = childFlow.note
      parentFlow.completionStatus = childFlow.completionStatus
      parentFlow.output = childFlow.output

      console.log(`AFTER SETTING THE RESULT, parentFlow=`, parentFlow)

// dbLogbook.bulkLogging(txId, pipelineStepId, [{
//   level: dbLogbook.LOG_LEVEL_TRACE,
//   source: dbLogbook.LOG_SOURCE_SYSTEM,
//   message: `delta count YARP ${tx.getDeltaCounter()}`,
//   sequence: txId.substring(0, 6),
//   ts: Date.now()
// }])

      // Send the event back to whoever started this step
      // const queueToParentOfPipeline = Scheduler2.groupQueueName(pipelineStep.onComplete.nodeGroup)
      const parentNodeGroup = parentFlow.onComplete.nodeGroup
      console.log(`parentNodeGroup=`.red, parentNodeGroup)
      const workerForShortcut = worker
      const event = {
        eventType: Scheduler2.STEP_COMPLETED_EVENT,
        txId,
        // parentStepId: '-',
        // stepId: pipelineStepId,
        // completionToken: pipelineStep.onComplete.completionToken
        flowIndex: parentFlowIndex,
      }

      const rv = await schedulerForThisNode.schedule_StepCompleted(tx, parentNodeGroup, event, workerForShortcut)
      assert(rv === GO_BACK_AND_RELEASE_WORKER)
      return GO_BACK_AND_RELEASE_WORKER

    } else {
      /*
       *  Initiate the next step
       */
      if (PIPELINES_VERBOSE) console.log(indent + `----    ON TO THE NEXT PIPELINE STEP  `.black.bgGreen.bold)


      // Remember that we're moving on to the next step
      await tx.delta(pipelineStepId, {
        indexOfCurrentChildStep: nextStepNo,
      }, 'pipelineStepCompleteCallback()')
      parentFlow.vogYarpYarp++
      console.log(`nextStepNo=`, nextStepNo)
      console.log(`  => parentFlow.vogYarpYarp=`.red, parentFlow.vogYarpYarp)

      // if (PIPELINES_VERBOSE) {
      //   dbLogbook.bulkLogging(txId, pipelineStepId, [{
      //     level: dbLogbook.LOG_LEVEL_TRACE,
      //     source: dbLogbook.LOG_SOURCE_SYSTEM,
      //     message: `Step #${nextStepNo+1} - begin`,
      //     sequence: pipelineFullSequence,
      //     ts: Date.now()
      //   }])
      // }

      const childStepId = childStepIds[nextStepNo]
      const metadataForNewStep = txData.metadata
      const inputForNewStep = childStep.stepOutput
      const childFullSequence = `${pipelineStep.fullSequence}.${1 + nextStepNo}` // Start sequence at 1
      const childVogPath = `${pipelineStep.vogPath},${1 + nextStepNo}=PC.${pipelineSteps[nextStepNo].definition.stepType}` // Start sequence at 1

      // The child will run in this node - same as this pipeline.
      // We keep the steps all running on the same node, so they all use the same
      // cached transaction. We only jump to another node when we are calling a
      // pipline that runs on another node.
      const myNodeGroup = schedulerForThisNode.getNodeGroup()
      const myNodeId = schedulerForThisNode.getNodeId()
      // const queueToChild = Scheduler2.nodeRegularQueueName(myNodeGroup, myNodeId)

      //VOGGY
      console.log(`--------------------------------------`)
      console.log(`VOGGY C - pipelineStepCompleteCallback`)
      console.log(`--------------------------------------`)

      //ZZZZZ Stuff to delete
      await tx.delta(childStepId, {
        stepDefinition: pipelineSteps[nextStepNo].definition,
      })


      await tx.delta(childStepId, { vogAddedBy: 'PipelineStepCompletedCallback()' }, 'pipelineStep.invoke()')/// Temporary - remove this

      const event = {
        eventType: Scheduler2.STEP_START_EVENT,
        // Need either a pipeline or a nodeGroup
        nodeGroup: nodeInfo.nodeGroup, // Child runs in same node as the pipeline step
        txId,
        stepId: childStepId,
        parentNodeGroup: nodeInfo.nodeGroup,
        parentStepId: pipelineStepId,
        fullSequence: childFullSequence,
        vogPath: childVogPath,
        stepDefinition: pipelineSteps[nextStepNo].definition,
        metadata: metadataForNewStep,
        data: inputForNewStep,
        level: pipelineStep.level + 1,
      }
      const onComplete = {
        nodeGroup: myNodeGroup,
        // nodeId: myNodeId,
        callback: PIPELINE_STEP_COMPLETE_CALLBACK,
        context: { txId, parentNodeGroup: nodeInfo.nodeGroup, parentStepId: pipelineStepId, childStepId }
      }
      // const parentFlowIndex = tx.vog_getParentFlowIndex(flowIndex)
      console.log(`parentFlowIndex=`, parentFlowIndex)
      const rv = await schedulerForThisNode.schedule_StepStart(tx,
        // myNodeGroup, myNodeId,
        worker, event, childStepId, onComplete, parentFlowIndex)
      assert(rv === GO_BACK_AND_RELEASE_WORKER)
      return GO_BACK_AND_RELEASE_WORKER
    }//- initiate the next step

  } else if (
    childStatus === STEP_FAILED
    || childStatus === STEP_ABORTED // Should this be included? ZZZZ
    || childStatus === STEP_INTERNAL_ERROR
  ) {

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
    }, 'pipelineStepCompleteCallback()')
    // console.log(`pipeline step is now`, tx.stepData(pipelineStepId))

    // Send the event back to whoever started this step
    const parentNodeGroup = pipelineStep.onComplete.nodeGroup
    const workerForShortcut = worker
    const rv = await schedulerForThisNode.schedule_StepCompleted(tx, parentNodeGroup, {
    // const queueToParentOfPipeline = Scheduler2.groupQueueName(pipelineStep.onComplete.nodeGroup)
    // const rv = await schedulerForThisNode.enqueue_StepCompletedZZZ(queueToParentOfPipeline, {
      txId,
      // parentStepId: '-',
      stepId: pipelineStepId,
      completionToken: pipelineStep.onComplete.completionToken
    }, workerForShortcut)
    assert(rv === GO_BACK_AND_RELEASE_WORKER)
    return GO_BACK_AND_RELEASE_WORKER

  } else {
    throw new Error(`Child has unknown status [${childStatus}]`)
  }
}//- pipelineStepCompleteCallback