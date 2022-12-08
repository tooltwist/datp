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
import { FLOW_VERBOSE } from '../Scheduler2/queuing/redis-lua'
import { flow2Msg, flowMsg } from '../Scheduler2/flowMsg'
import { F2_PIPELINE_CH, F2_STEP, F2_VERBOSE } from '../Scheduler2/TransactionState'

export const PIPELINES_VERBOSE = 0

class Pipeline extends Step {
  #stepIndex
  #steps

  constructor(definition) {
    super(definition)
    if (PIPELINES_VERBOSE) {
      console.log(`Pipeline.constructor()`)
      console.log(`definition.steps=`.magenta, definition.steps)
    }
    this.#stepIndex = definition.steps // { 0:{ id: 0, definition: {...} }, 1:... }
    this.#steps = Object.values(this.#stepIndex).sort((a,b) => {
      if (a.id < b.id) return -1
      if (a.id > b.id) return +1
      return 0
    })
    // console.log(`Pipeline.constructor(). Definition=`, definition)

  }//- constructor


  async invoke(pipelineInstance) {
    assert(pipelineInstance instanceof StepInstance)

    // This function gets the transaction status object. We want to keep this unpublished
    // and hard to notice - we don't want developers mucking with the internals of DATP.
    const txId = pipelineInstance.getTransactionId()//ZZZZ rename
    const tx = pipelineInstance._7agghtstrajj_37(txId)

    if (FLOW_VERBOSE) flow2Msg(tx, `>>>>    Pipeline.invoke (${pipelineInstance.getStepId()})  `)

    //ZZZZ If there are no steps, return immediately
    if (this.#steps.length < 1) {
      throw new Error(`Pipeline contains no steps [${pipelineInstance.getStepId()}]`)
    }
    const childStepDefinition = this.#steps[0].definition
// console.log(`first childStepDefinition=`, childStepDefinition)

    const stepInput = await pipelineInstance.getTxData().getData()
    const metadata = await pipelineInstance.getMetadata()

    const pipelineStepId = pipelineInstance.getStepId()
    const pipelineStep = tx.stepData(pipelineStepId)
    const childStepIds = [ ]
    for (let i = 0; i < this.#steps.length; i++) {
      const childStepId = await tx.addChildStep(pipelineStepId, i)
      childStepIds[i] = childStepId

      const childStepDefinition = this.#steps[i].definition
      const childFullSequence = `${pipelineStep.fullSequence}.${i + 1}` // Start sequence at 1
      const childVogPath = `${pipelineStep.vogPath},${i + 1}=PC.${childStepDefinition.stepType}` // Start sequence at 1



      await tx.delta(childStepId, { vogAddedBy: 'PipelineStep.invoke()' }, 'pipelineStep.invoke()')/// Temporary - remove this
      await tx.delta(childStepId, {
        stepDefinition: childStepDefinition,
        fullSequence: childFullSequence,
        vogPath: childVogPath
      })

    }//- next child step

    await tx.delta(pipelineStepId, {
      "-vogStepDefinition": "",
      pipelineSteps: this.#steps,
//VOG812      indexOfCurrentChildStep,
      childStepIds,
    }, 'pipelineStep.invoke()')

    tx.vog_setNextStepId(pipelineStepId)

    if (PIPELINES_VERBOSE) {
      console.log(`PipelineStep.initiateChildStep()`)
      console.log(`********************************`)
//VOG812      console.log(`Pipeline.initiateChildStep(${indexOfCurrentChildStep})`)
    }

    if (PIPELINES_VERBOSE) console.log(`childStepDefinition`, childStepDefinition)

    const myNodeGroup = schedulerForThisNode.getNodeGroup()
    const parentNodeGroup = pipelineInstance.getNodeGroup()// Shouldn't this just be the current node group?
    const childStepId = childStepIds[0]

    if (PIPELINES_VERBOSE)  pipelineInstance.trace(`Step #1 - begin`)
    pipelineInstance.syncLogs()

    // Add all the steps to f2, with a completion handler after each.
    // if (F2_VERBOSE) console.log(`F2: PipelineStep.invoke - add ${childStepIds.length} steps to f2`.bgBrightYellow.blue)
    // if (F2_VERBOSE) console.log(`F2: Pipeline is ${childStepDefinition}`.bgBrightYellow.blue)

    // const f2i = pipelineInstance.vog_getF2i()
    // let firstChildF2i = -1
    // for (let i = 0; i < childStepIds.length; i++) {
    //   const { f2i:childF2i, f2:childF2} = tx.vf2_addF2child(f2i, F2_STEP, 'Pipeline.invoke')
    //   childF2.stepId = childStepId
    //   childF2.ts1 = Date.now()
    //   childF2.ts2 = 0
    //   childF2.ts3 = 0
    //   if (firstChildF2i < 0) {
    //     firstChildF2i = childF2i
    //   }
    //   const { f2:completionHandlerF2 } = tx.vf2_addF2child(f2i, F2_PIPELINE_CH, 'Pipeline.invoke')
    //   // const { f2:completionHandlerF2 } = tx.vf2_addF2sibling(f2i, F2_PIPELINE_CH, 'Pipeline.invoke')
    //   completionHandlerF2.callback = PIPELINE_STEP_COMPLETE_CALLBACK
    //   completionHandlerF2.nodeGroup = schedulerForThisNode.getNodeGroup()
    // }

    // Add the first child to f2
    const f2i = pipelineInstance.vog_getF2i()
    const { f2i:firstChildF2i, f2:childF2} = tx.vf2_addF2child(f2i, F2_STEP, 'Pipeline.invoke')
    childF2.stepId = childStepId
    childF2.ts1 = Date.now()
    childF2.ts2 = 0
    childF2.ts3 = 0
    // const { f2:completionHandlerF2 } = tx.vf2_addF2sibling(f2i, F2_PIPELINE_CH, 'Pipeline.invoke')
    // const { f2:completionHandlerF2 } = tx.vf2_addF2sibling(firstChildF2i, F2_PIPELINE_CH, 'Pipeline.invoke')
    const { f2:completionHandlerF2 } = tx.vf2_addF2sibling(f2i, F2_PIPELINE_CH, 'Pipeline.invoke')
    // const { f2:completionHandlerF2 } = tx.vf2_addF2child(f2i, F2_PIPELINE_CH, 'Pipeline.invoke')
    completionHandlerF2.callback = PIPELINE_STEP_COMPLETE_CALLBACK
    completionHandlerF2.nodeGroup = schedulerForThisNode.getNodeGroup()


    // The child will run in this node - same as this pipeline.
    // We keep the steps in a pipeline all running on the same node, so they all use
    // the same cached transaction state. We only jump to another node when we are
    // calling a pipline that runs on another node.
    const workerForShortcut = pipelineInstance.getWorker()
    //VOGGY
    if (FLOW_VERBOSE) {
      // console.log(`-----------------------------`)
      flow2Msg(tx, `PipelineStep.invoke`)
      // console.log(`-----------------------------`)
    }

    //ZZZZZ Stuff to delete
    await tx.delta(childStepId, {
      stepDefinition: childStepDefinition,
    })

    const event = {
      eventType: Scheduler2.STEP_START_EVENT,
      // Need either a pipeline or a nodeGroup
      // nodeGroup: childNodeGroup,
      txId,
      // nodeId: childNodeGroup,
      // stepId: childStepId,
      parentNodeGroup,
      // parentNodeId,
      // parentStepId,
      // fullSequence: childFullSequence,
      // vogPath: childVogPath,

      //ZZZZZ Why is this here?????
      stepDefinition: childStepDefinition,
      metadata: metadata,
      data: stepInput,
      level: pipelineInstance.getLevel() + 1,
      // parentFlowIndex: pipelineInstance.vog_getFlowIndex(),
      f2i: firstChildF2i,
    }
    const onComplete = {
      nodeGroup: myNodeGroup,
      // nodeId: myNodeId,
      callback: PIPELINE_STEP_COMPLETE_CALLBACK,
//VOG777      context: { txId, parentNodeGroup, parentStepId, childStepId }
    }

    const parentFlowIndex = pipelineInstance.vog_getFlowIndex()
    const parentFlow = tx.vog_getFlowRecord(parentFlowIndex)
    parentFlow.vog_currentPipelineStep = 0
    const rv = await schedulerForThisNode.enqueue_StartStep(tx, parentFlowIndex, childStepId, event, onComplete, workerForShortcut)
    assert(rv === GO_BACK_AND_RELEASE_WORKER)

    //ZZZZ Handling of sync steps???

    //ZZZZ Perhaps we should get the new step ID above and double check it in the completion handler????


    // We need to tell the instance that we are returning without calling succeeded(), failed(), etc.
    // After this step completes, PIPELINE_STEP_COMPLETE_CALLBACK will be called, and it
    // will wither start aother step, or call one of the completion functions.
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
