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
import { PIPELINE_STEP_COMPLETE_CALLBACK } from '../Scheduler2/pipelineStepCompleteCallback'
import { schedulerForThisNode } from '../..'
import { GO_BACK_AND_RELEASE_WORKER } from '../Scheduler2/Worker2'

export const PIPELINES_VERBOSE = 0

class Pipeline extends Step {
  #stepIndex
  #steps

  constructor(definition) {
    super(definition)
    if (PIPELINES_VERBOSE) {
      console.log(`PipelineStepHandler.constructor(${definition.description})`)
    }
    this.#stepIndex = definition.steps // { 0:{ id: 0, definition: {...} }, 1:... }
    this.#steps = Object.values(this.#stepIndex).sort((a,b) => {
      if (a.id < b.id) return -1
      if (a.id > b.id) return +1
      return 0
    })
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
    const stepInput = await pipelineInstance.getTxData().getData()
    const metadata = await pipelineInstance.getMetadata()

    // This function gets the transaction status object. We want to keep this unpublished
    // and hard to notice - we don't want developers mucking with the internals of DATP.
    const tx = pipelineInstance._7agghtstrajj_37(txId)
    const pipelineStepId = pipelineInstance.getStepId()
    const childStepIds = [ ]
    for (let i = 0; i < this.#steps.length; i++) {
      const childStepId = GenerateHash('s')
      childStepIds[i] = childStepId
    }
    await tx.delta(pipelineStepId, {
      pipelineSteps: this.#steps,
      indexOfCurrentChildStep,
      childStepIds,
    }, 'pipelineStep.invoke()')
    await tx.delta(null, {
      nextStepId: pipelineStepId,
    }, 'pipelineStep.invoke()')

    if (PIPELINES_VERBOSE) {
      console.log(`PipelineStep.initiateChildStep()`)
      console.log(`********************************`)
      console.log(`Pipeline.initiateChildStep(${indexOfCurrentChildStep})`)
    }

    if (PIPELINES_VERBOSE) console.log(`childStepDefinition`, childStepDefinition)

    const parentStepId = pipelineInstance.getStepId()
    const myNodeGroup = schedulerForThisNode.getNodeGroup()
    const myNodeId = schedulerForThisNode.getNodeId()
    const parentNodeGroup = pipelineInstance.getNodeGroup()// Shouldn't this just be the current node group?
    const childStepId = childStepIds[0]
    const childNodeGroup = myNodeGroup // Step runs in same node as it's pipeline

    const childFullSequence = `${pipelineInstance.getFullSequence()}.1` // Start sequence at 1

    pipelineInstance.trace(`Step #1 - begin`)
    pipelineInstance.syncLogs()

    // The child will run in this node - same as this pipeline.
    // We keep the steps in a pipeline all running on the same node, so they all use
    // the same cached transaction state. We only jump to another node when we are
    // calling a pipline that runs on another node.
    const workerForShortcut = pipelineInstance.getWorker()
    const rv = await schedulerForThisNode.schedule_StepStart(tx, myNodeGroup, myNodeId, workerForShortcut, {
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
    assert(rv === GO_BACK_AND_RELEASE_WORKER)

    //ZZZZ Handling of sync steps???

    //ZZZZ Perhaps we should get the new step ID above and double check it in the completion handler????


    // We need to tell the instance that we are returning without calling succeeded(), failed(), etc.
    pipelineInstance.stepWillNotCallCompletionFunction()
    return GO_BACK_AND_RELEASE_WORKER

  }//- invoke
}


async function register() {
  // Note that our matching callback is a built-in, so doesn't need to be registered.
  await StepTypes.register(PipelineDef, 'hidden/pipeline', 'Pipeline')
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
