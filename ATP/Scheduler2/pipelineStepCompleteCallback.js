import assert from 'assert'
import indentPrefix from '../../lib/indentPrefix'
import GenerateHash from '../GenerateHash'
import { PIPELINES_VERBOSE } from '../hardcoded-steps/PipelineStep'
import { STEP_SUCCESS, STEP_ABORTED, STEP_FAILED, STEP_INTERNAL_ERROR } from '../Step'
import Scheduler2, { DEFAULT_QUEUE } from './Scheduler2'
import TransactionCache from './TransactionCache'


export const PIPELINE_STEP_COMPLETE_CALLBACK = `pipelineStepComplete`

export async function pipelineStepCompleteCallback (callbackContext, nodeInfo) {
  // console.log(`==> Callback pipelineStepCompleteCallback() context=`, callbackContext, nodeInfo)

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
  if (childStatus === STEP_SUCCESS) {
    // Do we have any steps left
    // console.log(`yarp D - ${this.stepNo}`)
    const nextStepNo = indexOfCurrentChildStep + 1
    // // const currentStepNo = contextForCompletionHandler.stepNo
    // pipelineInstance.privateData.indexOfCurrentChildStep = nextStepNo

    // const stepNo = ++pipelineInstance.privateData.indexOfCurrentChildStep
    // console.log(`yarp E - ${this.stepNo}`)
    // if (nextStepNo >= pipelineInstance.privateData.numSteps) {
    if (PIPELINES_VERBOSE) console.log(`Which step?  ${nextStepNo} of [0...${pipelineSteps.length - 1}]`)
    if (nextStepNo >= pipelineSteps.length) {
      /*
       *  We've finished this pipeline - return the final respone
       */
      if (PIPELINES_VERBOSE) console.log(indent + `<<<<    PIPELINE COMPLETED ${pipelineStepId}  `.blue.bgGreen.bold)

      // Save the child status and output as our own
      await tx.delta(pipelineStepId, {
        stepOutput: childStep.stepOutput,
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
      if (PIPELINES_VERBOSE) console.log(indent + `----    ON TO THE NEXT PIPELINE STEP  `.blue.bgGreen.bold)


      // Remember that we'ree moving on to the next step
      await tx.delta(pipelineStepId, {
        indexOfCurrentChildStep: nextStepNo,
      })


      const childStepId = childStepIds[nextStepNo]
      const metadataForNewStep = txData.metadata
      const inputForNewStep = childStep.stepOutput
      // const childNodeId = nodeInfo.nodeGroup // Child runs in same node as the pipeline step

      // The child will run in the same node as this pipeline.
      const queueToChild = Scheduler2.standardQueueName(nodeInfo.nodeGroup, DEFAULT_QUEUE)
      await Scheduler2.enqueue_StepStart(queueToChild, {
        txId,
        nodeId: nodeInfo.nodeGroup, // Child runs in same node as the pipeline step
        stepId: childStepId,
        parentNodeId: nodeInfo.nodeGroup,
        parentStepId: pipelineStepId,
        sequenceYARP: txId.substring(txId.length - 8),/// Is this right?
        stepDefinition: pipelineSteps[nextStepNo].definition,
        metadata: metadataForNewStep,
        data: inputForNewStep,
        level: pipelineStep.level + 1,
        onComplete: {
          nodeGroup: nodeInfo.nodeGroup,
          callback: PIPELINE_STEP_COMPLETE_CALLBACK,
          context: { txId, parentNodeId: nodeInfo.nodeGroup, parentStepId: pipelineStepId, childStepId }
        }
      })


      // throw new Error(`BOMB EARLY`)
      // const txForNextStep = stepOutput
      // //ZZZZZ Should be cloned, to prevent previous step from secretly
      // // continuing to run and accessing the tx during the next step.
      // const pipelineObject = pipelineInstance.stepObject
      // await pipelineObject.initiateChildStep(pipelineInstance, stepNo, stepDefinition, txForNextStep)
    }

  } else if (childStatus === STEP_FAILED || childStatus === STEP_ABORTED || childStatus === STEP_INTERNAL_ERROR) {
    /*
      *  Need to try Rollback
      */
    // We can't rollback yet, so abort instead.
    //ZZZZ Log this
    if (PIPELINES_VERBOSE) console.log(indent + `<<<<    PIPELINE FAILED ${pipelineStepId}  `.white.bgRed.bold)
    // console.log(``)
    // console.log(``)
    // return Scheduler.haveResult(pipelineStepId, pipelineInstance.getCompletionToken(), STEP_COMPLETED, stepOutput)
    // pipelineInstance.finish(STEP_ABORTED, `Step ${currentStepNo} failed`, stepOutput)
    await tx.delta(pipelineStepId, {
      stepOutput: childStep.stepOutput,
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
    throw new Error(`Child has unknown status [${childStatus}]`)
  }
}//- pipelineStepCompleteCallback