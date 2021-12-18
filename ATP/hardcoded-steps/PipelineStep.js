/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import Step from '../Step'
import Scheduler from "../Scheduler"
import StepTypes from '../StepTypeRegister'
import ResultReceiver from '../ResultReceiver'
import ResultReceiverRegister from '../ResultReceiverRegister'
import defaultATE from '../ATP'
import XData from '../XData'
import assert from 'assert'
import StepInstance from '../StepInstance'
import Scheduler2, { DEFAULT_QUEUE } from '../Scheduler2/Scheduler2'
import GenerateHash from '../GenerateHash'
import TransactionCache from '../Scheduler2/TransactionCache'
import { PIPELINE_STEP_COMPLETE_CALLBACK } from '../Scheduler2/pipelineStepCompleteCallback'

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
    // await this.initIfRequired()
    // pipelineInstance.console(`*****`)
    if (PIPELINES_VERBOSE) pipelineInstance.console(`>>>>    Pipeline.invoke (${pipelineInstance.getStepId()})  `.blue.bgGreen.bold)
    // console.log(`pipelineInstance=`, pipelineInstance)

    // pipelineInstance.console(`*****`)
    // console.log(`tx=`, tx)
    // console.log(`pipelineInstance=`, pipelineInstance)

    //ZZZZ If there are no steps, return immediately
    if (this.#steps.length < 1) {
      throw new Error(`Pipeline contains no steps [${pipelineInstance.getStepId()}]`)
    }
    const childStepIndex = 0
    const childStepDefinition = this.#steps[0].definition

    const txId = pipelineInstance.getTransactionId()//ZZZZ rename
    // console.log(`txId=`, txId)
    const stepInput = await pipelineInstance.getTxData().getData()
    // console.log(`stepInput=`, stepInput)
    const metadata = await pipelineInstance.getMetadata()
    // const parentNodeId = await pipelineInstance.getNodeId()

    // Let the transaction know we are here
    const tx = await TransactionCache.findTransaction(txId, true)
    const txData = tx.txData()
    // console.log(`In invoke() tx=`, tx.asObject())
    const stepId = pipelineInstance.getStepId()
    // console.log(`stepId=`, stepId)
    await tx.delta(stepId, {
      pipelineSteps: this.#steps,
      childStepIndex,
      // metadata,
      // stepInput: txdata.getData()
    })
    await tx.delta(null, {
      nextStepId: stepId,
    })

//     // console.log(`tx.asObject()=`.cyan, tx.asObject())


//     // // We'll save the responses from the steps
//     // pipelineInstance.privateData.responses = [ ]
//     // pipelineInstance.privateData.numSteps = this.#steps.length
//     // pipelineInstance.privateData.childStepIndex = 0

//     //ZZZZ Should probably create a new TX object
//     this.initiateChildStep(pipelineInstance, childStepIndex, childStepDefinition, txdata, metadata)

//     // logbook.log(id, `DummyStep.invoke()`, {
//     //   level: logbook.LEVEL_DEBUG,
//     //   data
//     // })
//   }//- invoke

// //ZZZZZ Join these together ^^^^^ vvvvv

//   async initiateChildStep(pipelineInstance, childStepIndex, childStepDefinition, txdata, metadata) {
//     assert(pipelineInstance instanceof StepInstance)
    // assert(txdata instanceof XData)
    pipelineInstance.log(``)
    // const stepNo = pipelineInstance.privateData.childStepIndex
    if (PIPELINES_VERBOSE) {
      console.log(`PipelineStep.initiateChildStep()`)
      console.log(`********************************`)
      console.log(`Pipeline.initiateChildStep(${childStepIndex})`)
    }

    pipelineInstance.console()
    pipelineInstance.console(`Pipeline initiating child step #${childStepIndex}:`)
    pipelineInstance.console()
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
      const parentNodeId = pipelineInstance.getNodeId()
      // console.log(`parentNodeId=`, parentNodeId)
      const childStepId = GenerateHash('s')
      const childNodeId = parentNodeId

      // The child will run in the same node as this pipeline.
      // const nodeGroupWherePipelineRuns = metadata.nodeId
      const queueToPipelineNode = Scheduler2.standardQueueName(parentNodeId, DEFAULT_QUEUE)
      // console.log(`parentNodeId=`, parentNodeId)
      // console.log(`queueToPipelineNode=`, queueToPipelineNode)

      // console.log(`metadata=`, metadata)
      // console.log(`txdata=`, txdata)
      // console.log(`parentNodeId=`, parentNodeId)
      await Scheduler2.enqueue_StepStart(queueToPipelineNode, {
        txId,
        nodeId: childNodeId,
        stepId: childStepId,
        // parentNodeId,
        parentStepId,
        sequenceYARP: txId.substring(txId.length - 8),/// Is this right?
        stepDefinition: childStepDefinition,
        metadata: metadata,
        data: stepInput,
        level: pipelineInstance.getLevel() + 1,
        onComplete: {
          nodeGroup: parentNodeId,
          callback: PIPELINE_STEP_COMPLETE_CALLBACK,
          context: { txId, parentNodeId, parentStepId, childStepId }
        }
      })




    //ZZZZ Handling of sync steps???

    //ZZZZ Perhaps we should get the new step ID above and souble check it in the completion handler????

  }//- initiateChildStep

}


// class PipelineChildStepCompletionHandler extends ResultReceiver {

//   constructor() {
//     super()
//   }
//   async haveResult(contextForCompletionHandler, status, note, newTx) {
//     assert(newTx instanceof XData)
//     // console.log(`PipelineChildStepCompletionHandler.haveResult(status=${status})`, newTx.toString())
//     // console.log(`PipelineChildStepCompletionHandler.haveResult(status=${status})`)
//     // console.log(`newTx=`, newTx)
//     // console.log(`contextForCompletionHandler=`, contextForCompletionHandler)

//     const pipelineStepId = contextForCompletionHandler.pipelineId
//     const pipelineIndexEntry = await Scheduler.getStepEntry(pipelineStepId)
//     if (!pipelineIndexEntry) {
//       throw new Error(`Internal error 827772: could not find pipeline in Scheduler (${pipelineStepId})`)
//     }
//     const pipelineInstance = await pipelineIndexEntry.getStepInstance()

//     // Double check the step number
//     if (contextForCompletionHandler.stepNo != pipelineInstance.privateData.childStepIndex) {
//       throw new Error(`Internal Error 882659: invalid step number {${contextForCompletionHandler.stepNo} vs ${pipelineInstance.privateData.childStepIndex}}`)
//     }

//     // Remember the reply
//     //ZZZZZ Really needed?
//     const currentStepNo = pipelineInstance.privateData.childStepIndex
//     pipelineInstance.privateData.responses[currentStepNo] = { status, newTx }
//     // console.log(`yarp C - ${this.stepNo}`)

//     if (status === STEP_COMPLETED) {
//       // Do we have any steps left
//       // console.log(`yarp D - ${this.stepNo}`)
//       const nextStepNo = currentStepNo + 1
//       // const currentStepNo = contextForCompletionHandler.stepNo
//       pipelineInstance.privateData.childStepIndex = nextStepNo

//       // const stepNo = ++pipelineInstance.privateData.childStepIndex
//       // console.log(`yarp E - ${this.stepNo}`)
//       if (nextStepNo >= pipelineInstance.privateData.numSteps) {
//         // We've finished this pipeline - return the final respone
//         // console.log(``)
//         // console.log(``)
//         // console.log(``)
//         console.log(`<<<<    PIPELINE COMPLETED ${pipelineStepId}  `.blue.bgGreen.bold)
//         // console.log(``)
//         // console.log(``)
//         // return Scheduler.haveResult(pipelineStepId, pipelineInstance.getCompletionToken(), STEP_COMPLETED, newTx)
//         pipelineInstance.finish(STEP_COMPLETED, 'Successful completion', newTx)

//       } else {
//         // Initiate the next step
//         console.log(`----    ON TO THE NEXT PIPELINE STEP  `.blue.bgGreen.bold)
//         const txForNextStep = newTx
//         //ZZZZZ Should be cloned, to prevent previous step from secretly
//         // continuing to run and accessing the tx during the next step.
//         const pipelineObject = pipelineInstance.stepObject
//         await pipelineObject.initiateChildStep(pipelineInstance, stepNo, stepDefinition, txForNextStep)
//       }

//     } else if (status === STEP_FAILED || status === STEP_ABORTED || status === STEP_INTERNAL_ERROR) {
//       /*
//        *  Need to try Rollback
//        */
//       // We can't rollback yet, so abort instead.
//       console.log(`<<<<    PIPELINE FAILED ${pipelineStepId}  `.white.bgRed.bold)
//       // console.log(``)
//       // console.log(``)
//       // return Scheduler.haveResult(pipelineStepId, pipelineInstance.getCompletionToken(), STEP_COMPLETED, newTx)
//       pipelineInstance.finish(STEP_ABORTED, `Step ${currentStepNo} failed`, newTx)

//     } else {
//       //ZZZZ
//       throw new Error(`Status ${status} not supported yet`)
//     }
//   }//- haveResult

// }//- class PipelineChildStepCompletionHandler


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
