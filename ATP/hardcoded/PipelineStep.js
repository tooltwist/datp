import Step from '../Step'
import Scheduler from "../Scheduler"
import StepTypes from '../StepTypeRegister'
import ResultReceiver from '../ResultReceiver'
import ResultReceiverRegister from '../ResultReceiverRegister'
import defaultATE from '../ATP'
import TxData from '../TxData'
import assert from 'assert'
import StepInstance from '../StepInstance'

const STEP_COMPLETION_HANDLER = 'pipeline-step-completion-handler'
const VERBOSE = false

class Pipeline extends Step {
  #steps

  constructor(definition) {
    super(definition)
    if (VERBOSE) {
      console.log(`PipelineStepHandler.constructor(${definition.description})`)
    }
    // this.#definition = definition
    // this.sequencePrefix = parent ? parent.
    this.#steps = definition.steps
    // this.initialized = false

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
    pipelineInstance.console(`>>>>    Pipeline.invoke (${pipelineInstance.getStepId()})  `.blue.bgGreen.bold)
    // pipelineInstance.console(`*****`)
    // console.log(`tx=`, tx)
    // console.log(`pipelineInstance=`, pipelineInstance)

    // We'll save the responses from the steps
    pipelineInstance.privateData.responses = [ ]
    pipelineInstance.privateData.numSteps = this.#steps.length
    pipelineInstance.privateData.childStepIndex = 0

    //ZZZZ Should probably create a new TX object
    const txdata = await pipelineInstance.getTxData()
    const metadata = await pipelineInstance.getMetadata()
    this.initiateChildStep(pipelineInstance, txdata)

    // logbook.log(id, `DummyStep.invoke()`, {
    //   level: logbook.LEVEL_DEBUG,
    //   data
    // })
  }//- invoke


  async initiateChildStep(pipelineInstance, txdata, metadata) {
    assert(pipelineInstance instanceof StepInstance)
    assert(txdata instanceof TxData)
    // console.log(`PipelineStep.initiateChildStep()`, data)
    pipelineInstance.log(``)
    const stepNo = pipelineInstance.privateData.childStepIndex
    // pipelineInstance.console(`********************************`)
    // pipelineInstance.console(`Pipeline.initiateChildStep(${stepNo})`)
    pipelineInstance.console()
    pipelineInstance.console(`Pipeline initiating child step #${stepNo}:`)
    pipelineInstance.console()
    // console.log(`tx=`, tx)
    // console.log(`pipelineInstance=`, pipelineInstance)
    const stepDef =  this.#steps[stepNo]

        // Find the
    const contextForCompletionHandler = {
      // context,
      pipelineId: pipelineInstance.getStepId(),
      stepNo: stepNo
    }
    const definition = stepDef.definition
    const sequence = `${stepNo}`
    const logbook = pipelineInstance.getLogbook()

    await Scheduler.invokeStep(pipelineInstance.getTransactionId(), pipelineInstance, sequence, definition, txdata, logbook, STEP_COMPLETION_HANDLER, contextForCompletionHandler)

    //ZZZZ Handling of sync steps???

    //ZZZZ Perhaps we should get the new step ID above and souble check it in the completion handler????

  }//- initiateChildStep

}


class PipelineChildStepCompletionHandler extends ResultReceiver {

  constructor() {
    super()
  }
  async haveResult(contextForCompletionHandler, status, note, newTx) {
    assert(newTx instanceof TxData)
    // console.log(`PipelineChildStepCompletionHandler.haveResult()`, newTx.toString())
    // console.log(`newTx=`, newTx)
    // console.log(`contextForCompletionHandler=`, contextForCompletionHandler)

    const pipelineStepId = contextForCompletionHandler.pipelineId
    const pipelineIndexEntry = await Scheduler.getStepEntry(pipelineStepId)
    if (!pipelineIndexEntry) {
      throw new Error(`Internal error 827772: could not find pipeline in Scheduler (${pipelineStepId})`)
    }
    const pipelineInstance = await pipelineIndexEntry.getStepInstance()

    // Double check the step number
    if (contextForCompletionHandler.stepNo != pipelineInstance.privateData.childStepIndex) {
      throw new Error(`Internal Error 882659: invalid step number {${contextForCompletionHandler.stepNo} vs ${pipelineInstance.privateData.childStepIndex}}`)
    }

    // Remember the reply
    //ZZZZZ Really needed?
    const currentStepNo = pipelineInstance.privateData.childStepIndex
    pipelineInstance.privateData.responses[currentStepNo] = { status, newTx }
    // console.log(`yarp C - ${this.stepNo}`)

    if (status === Step.COMPLETED) {
      // Do we have any steps left
      // console.log(`yarp D - ${this.stepNo}`)
      const nextStepNo = currentStepNo + 1
      // const currentStepNo = contextForCompletionHandler.stepNo
      pipelineInstance.privateData.childStepIndex = nextStepNo

      // const stepNo = ++pipelineInstance.privateData.childStepIndex
      // console.log(`yarp E - ${this.stepNo}`)
      if (nextStepNo >= pipelineInstance.privateData.numSteps) {
        // We've finished this pipeline - return the final respone
        // console.log(``)
        // console.log(``)
        // console.log(``)
        console.log(`<<<<    PIPELINE COMPLETED ${pipelineStepId}  `.blue.bgGreen.bold)
        // console.log(``)
        // console.log(``)
        // return Scheduler.haveResult(pipelineStepId, pipelineInstance.getCompletionToken(), Step.COMPLETED, newTx)
        pipelineInstance.finish(Step.COMPLETED, 'Successful completion', newTx)

      } else {
        // Initiate the next step
        console.log(`----    ON TO THE NEXT PIPELINE STEP  `.blue.bgGreen.bold)
        const txForNextStep = newTx
        //ZZZZZ Should be cloned, to prevent previous step from secretly
        // continuing to run and accessing the tx during the next step.
        const pipelineObject = pipelineInstance.stepObject
        await pipelineObject.initiateChildStep(pipelineInstance, txForNextStep)
      }

    } else if (status === Step.FAIL || status === Step.ABORT || status === Step.INTERNAL_ERROR) {
      /*
       *  Need to try Rollback
       */
      // We can't rollback yet, so abort instead.
      console.log(`<<<<    PIPELINE FAILED ${pipelineStepId}  `.white.bgRed.bold)
      // console.log(``)
      // console.log(``)
      // return Scheduler.haveResult(pipelineStepId, pipelineInstance.getCompletionToken(), Step.COMPLETED, newTx)
      pipelineInstance.finish(Step.ABORT, `Step ${currentStepNo} failed`, newTx)

    } else {
      //ZZZZ
      throw new Error(`Status ${status} not supported yet`)
    }
  }//- haveResult

}//- class PipelineChildStepCompletionHandler


async function register() {
  await StepTypes.register(PipelineDef, 'pipeline', 'Pipeline')
  await ResultReceiverRegister.register(STEP_COMPLETION_HANDLER, new PipelineChildStepCompletionHandler())
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
