/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import Step from '../Step'
import StepTypes from '../StepTypeRegister'
import assert from 'assert'
import StepInstance from '../StepInstance'
import Scheduler2 from '../Scheduler2/Scheduler2'
import GenerateHash from '../GenerateHash'
import TransactionCache from '../Scheduler2/TransactionCache'
import { PIPELINE_STEP_COMPLETE_CALLBACK } from '../Scheduler2/pipelineStepCompleteCallback'
import { schedulerForThisNode } from '../..'

// const STEP_COMPLETION_HANDLER = 'pipeline-step-completion-handler'
export const PIPELINES_VERBOSE = 0

class Pipeline extends Step {
  #stepIndex
  #steps

  constructor(definition) {
    super(definition)
    if (PIPELINES_VERBOSE) {
      console.log(`PipelineStepHandler.constructor(${definition.description})`)
    }
    // this.#definition = definition
    // this.sequencePrefix = parent ? parent.
    this.#stepIndex = definition.steps // { 0:{ id: 0, definition: {...} }, 1:... }
    // console.log(`this.#stepIndex=`, this.#stepIndex)

    this.#steps = Object.values(this.#stepIndex).sort((a,b) => {
      if (a.id < b.id) return -1
      if (a.id > b.id) return +1
      return 0
    })
    // this.initialized = false
    // console.log(`this.#steps=`, this.#steps)

    // Check the step types are valid before we start
    //ZZZZ
    // for (const stepDef of this.definition.steps) {
    //   console.log(`->`, stepDef)
    //   // const step =
    //   //ZZZZ
    // }

  }//- constructor


  async invoke(pipelineInstance) {
    assert(pipelineInstance instanceof StepInstance)
    if (PIPELINES_VERBOSE) pipelineInstance.trace(`>>>>    Pipeline.invoke (${pipelineInstance.getStepId()})  `.black.bgGreen.bold)

    // console.log(new Error(`IN PipelineStep.invoke()`).stack)

    //ZZZZ If there are no steps, return immediately
    if (this.#steps.length < 1) {
      throw new Error(`Pipeline contains no steps [${pipelineInstance.getStepId()}]`)
    }


    const indexOfCurrentChildStep = 0
    const childStepDefinition = this.#steps[0].definition

    const txId = pipelineInstance.getTransactionId()//ZZZZ rename
    // console.log(`txId=`, txId)
    const stepInput = await pipelineInstance.getTxData().getData()
    // console.log(`stepInput=`, stepInput)
    const metadata = await pipelineInstance.getMetadata()

    // Let the transaction know we are here
    const tx = await TransactionCache.findTransaction(txId, true)
    const txData = tx.txData()
    // console.log(`In invoke() tx=`, tx.asObject())
    const pipelineStepId = pipelineInstance.getStepId()
    // console.log(`pipelineStepId=`, pipelineStepId)
    const childStepIds = [ ]
    for (let i = 0; i < this.#steps.length; i++) {
      const childStepId = GenerateHash('s')
      childStepIds[i] = childStepId
    }
    await tx.delta(pipelineStepId, {
      pipelineSteps: this.#steps,
      indexOfCurrentChildStep,
      childStepIds,
      // metadata,
      // stepInput: txdata.getData()
    }, 'pipelineStep.invoke()')
    await tx.delta(null, {
      nextStepId: pipelineStepId,
    }, 'pipelineStep.invoke()')

//     // console.log(`tx.asObject()=`.cyan, tx.asObject())


//     // // We'll save the responses from the steps
//     // pipelineInstance.privateData.responses = [ ]
//     // pipelineInstance.privateData.numSteps = this.#steps.length
//     // pipelineInstance.privateData.indexOfCurrentChildStep = 0

//     //ZZZZ Should probably create a new TX object
//     this.initiateChildStep(pipelineInstance, indexOfCurrentChildStep, childStepDefinition, txdata, metadata)

//     // logbook.log(id, `DummyStep.invoke()`, {
//     //   level: dbLogbook.LOG_LEVEL_DEBUG,
//     //   data
//     // })
//   }//- invoke

// //ZZZZZ Join these together ^^^^^ vvvvv

//   async initiateChildStep(pipelineInstance, indexOfCurrentChildStep, childStepDefinition, txdata, metadata) {
//     assert(pipelineInstance instanceof StepInstance)
    // assert(txdata instanceof XData)
    // pipelineInstance.log(``)
    // const stepNo = pipelineInstance.privateData.indexOfCurrentChildStep
    if (PIPELINES_VERBOSE) {
      console.log(`PipelineStep.initiateChildStep()`)
      console.log(`********************************`)
      console.log(`Pipeline.initiateChildStep(${indexOfCurrentChildStep})`)
    }

    // pipelineInstance.trace(`Pipeline initiating child step #${indexOfCurrentChildStep}:`)
    // // console.log(`tx=`, tx)
    // // console.log(`pipelineInstance=`, pipelineInstance)
    // const stepDef =  this.#steps[stepNo]

    //     // Find the
    // const contextForCompletionHandler = {
    //   // context,
    //   pipelineId: pipelineInstance.getStepId(),
    //   stepNo: stepNo
    // }
    // const definition = stepDef.definition
    if (PIPELINES_VERBOSE) console.log(`childStepDefinition`, childStepDefinition)
    // const sequence = `${stepNo}`
    // const logbook = pipelineInstance.getLogbook()

    // await Scheduler2.invokeStep(pipelineInstance.getTransactionId(), pipelineInstance, sequence, definition, txdata, logbook, STEP_COMPLETION_HANDLER, contextForCompletionHandler)

    // Generate a new ID for this step
    // const txId = pipelineInstance.getTransactionId()//ZZZZ rename
    const parentStepId = pipelineInstance.getStepId()
    // console.log(`parentStepId=`, parentStepId)
    const myNodeGroup = schedulerForThisNode.getNodeGroup()
    const myNodeId = schedulerForThisNode.getNodeId()
    const parentNodeGroup = pipelineInstance.getNodeGroup()// Shouldn't this just be the current node group?
    // console.log(`parentNodeGroup=`, parentNodeGroup)
    // const childStepId = GenerateHash('s')
    const childStepId = childStepIds[0]
    const childNodeGroup = myNodeGroup // Step runs in same node as it's pipeline

    // The child will run in this node - same as this pipeline.
    // We keep the steps all running on the same node, so they all use the same
    // cached transaction. We only jump to another node when we are calling a
    // pipline that runs on another node.
    const queueToStep = Scheduler2.nodeRegularQueueName(myNodeGroup, myNodeId)

    // console.log(`Pipline invoking step with ${queueToStep}`)
    // const queueToPipelineNode = Scheduler2.groupQueueName(parentNodeGroup)
    // console.log(`parentNodeGroup=`, parentNodeGroup)
    // console.log(`queueToPipelineNode=`, queueToPipelineNode)

    const childFullSequence = `${pipelineInstance.getFullSequence()}.1` // Start sequence at 1

    pipelineInstance.trace(`Start pipeline child #0`)
    pipelineInstance.syncLogs()

    // console.log(`metadata=`, metadata)
    // console.log(`txdata=`, txdata)
    // console.log(`parentNodeGroup=`, parentNodeGroup)
    await schedulerForThisNode.enqueue_StepStart(queueToStep, {
      txId,
      nodeGroup: childNodeGroup,
      // nodeId: childNodeGroup,
      stepId: childStepId,
      // parentNodeId,
      parentStepId,
      fullSequence: childFullSequence,
      stepDefinition: childStepDefinition,
      metadata: metadata,
      data: stepInput,
      level: pipelineInstance.getLevel() + 1,
      onComplete: {
        nodeGroup: myNodeGroup,
        nodeId: myNodeId,
        callback: PIPELINE_STEP_COMPLETE_CALLBACK,
        context: { txId, parentNodeGroup, parentStepId, childStepId }
      }
    })

    //ZZZZ Handling of sync steps???

    //ZZZZ Perhaps we should get the new step ID above and double check it in the completion handler????

  }//- invoke
}


async function register() {
  // Note that our matching callback is a built-in, so doesn't need to be registered.



  await StepTypes.register(PipelineDef, 'hidden/pipeline', 'Pipeline')
  // await ResultReceiverRegister.register(STEP_COMPLETION_HANDLER, new PipelineChildStepCompletionHandler())

}

async function defaultDefinition() {
  return {
    children: [ ],
  }
}

async function factory(definition) {
  return new Pipeline(definition)
}

async function describe(definition) {
  const description = {
    stepType: definition.stepType,
    description: definition.description,
    children: [ ]
  }
  for (const step of definition.steps) {
    const childDescription = await Step.describe(step.definition)
    description.children.push(childDescription)
  }
  return description
}

const PipelineDef = {
  register,
  factory,
  describe,
  defaultDefinition,
}
export default PipelineDef
