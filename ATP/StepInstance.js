/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import Logbook from './Logbook'
import StepTypes from './StepTypeRegister'
import Step, { STEP_ABORTED, STEP_FAILED, STEP_INTERNAL_ERROR, STEP_RUNNING, STEP_SUCCESS } from './Step'
import dbStep from "../database/dbStep";
import dbPipelines from "../database/dbPipelines";
import XData from "./XData";
import dbArtifact from "../database/dbArtifact";
import { STEP_TYPE_PIPELINE } from './StepTypeRegister'
import Scheduler2, { DEFAULT_QUEUE } from "./Scheduler2/Scheduler2";
import TransactionCache from "./Scheduler2/TransactionCache";
import indentPrefix from '../lib/indentPrefix'
import assert from 'assert'

const VERBOSE = 0

export default class StepInstance {
  #txId
  #nodeId
  #nodeGroup
  #stepId
  // #parentNodeId
  #parentStepId
  #stepDefinition
  #logbook
  #txdata
  #metadata
  #logSequence
  #fullSequenceYARP

  #level
  #indent

  // What to do after the step completes {nodeId, completionToken}
  #onComplete

  constructor() {
    // console.log(`StepInstance.materialize()`, options)

    this.#nodeGroup = null
    this.#nodeId = null
    // Definition
    // this.#parentNodeId = null
    this.#parentStepId = null
    this.#stepId = null
    this.#stepDefinition = { stepType: null}
    // Transaction data
    this.#txdata = null
    // Private data for use within step
    // this.privateData = { }
    // Debug stuff
    this.#level = 0
    this.#fullSequenceYARP = ''
    // Step completion handling
    this.#onComplete = null
    // Children
    // this.childStep = 0

    this.#logSequence = 0

    // Note that this object is transitory - not persisted. It will be
    // recreated if we reload this stepInstance from persistant storage.
    this.stepObject = null


    this.TRACE = 'trace'
    this.WARNING = 'warning'
    this.ERROR = 'error'
    this.DEBUG = 'debug'
  }


  async materialize(options, tx) {
    // console.log(``)
// console.log(`StepInstance.materialize options=`, options)
    const txData = tx.txData()
    // console.log(`txData=`, txData)
    const stepData = tx.stepData(options.stepId)
    // console.log(`stepData=`, stepData)



    assert (typeof(options.txId) === 'string')
    assert (typeof(options.nodeGroup) === 'string')
    assert (typeof(options.nodeId) === 'string')
    assert (typeof(options.stepId) === 'string')
    // assert (typeof(options.parentNodeId) === 'string')
    assert (typeof(stepData.parentStepId) === 'string')
    assert (typeof(stepData.sequenceYARP) === 'string')
    assert (typeof(stepData.stepDefinition) !== 'undefined')
    assert (typeof(stepData.stepInput) === 'object')
    assert (typeof(txData.metadata) === 'object')
    assert (typeof(stepData.level) === 'number')

    assert (typeof(options.onComplete) === 'object')
    assert (typeof(options.onComplete.nodeGroup) === 'string')
    assert (typeof(options.onComplete.completionToken) === 'string')

    // const txData = tx.txData()
    this.#parentStepId = stepData.parentStepId


    // console.log(``)
    // console.log(`StepInstance.materialize()`)
    // console.log(`options.data=`, options.data)

    // this.pipeId = GenerateHash('pipe')
    // this.data = { }
    // this.pipelineStack = [ ]


    this.#txId = options.txId
    this.#nodeGroup = options.nodeGroup
    this.#nodeId = options.nodeId
    this.#stepId = options.stepId
    // this.#parentNodeId = options.parentNodeId
    this.#parentStepId = stepData.parentStepId
    //this.#stepDefinition set below
    this.#txdata = new XData(stepData.stepInput)
    this.#metadata = txData.metadata
    this.#level = stepData.level
    this.#fullSequenceYARP = stepData.sequenceYARP

    this.#onComplete = options.onComplete
    // this.#completionToken = options.completionToken

    // Log this step being materialized
    // this.#logbook = options.logbook
    //ZZZZZ
    this.#logbook = new Logbook.cls({
      transactionId: this.#txId,
      description: `Pipeline logbook`
    })
    await this.log(Logbook.LEVEL_TRACE, `Start step ${this.#stepId}`)


    // Prepare an indent string to prepend to messages
    this.#indent = indentPrefix(this.#level)


    /*
     *  Load the definition of the step (which is probably a pipeline)
     */
    // console.log(`typeof(options.definition)=`, typeof(options.definition))
    let jsonDefinition
    switch (typeof(stepData.stepDefinition)) {
      case 'string':
        // console.log(`Loading definition for ${options.stepDefinition}`)
        // jsonDefinition = fs.readFileSync(`./pipeline-definitions/${options.stepDefinition}.json`)
        const arr = stepData.stepDefinition.split(':')
        // console.log(`arr=`, arr)
        let pipelineName = arr[0]
        let version = (arr.length > 0) ? arr[1] : null
        const list = await dbPipelines.getPipelines(pipelineName, version)
        // console.log(`list=`, list)
        if (list.length < 1) {
          throw new Error(`Unknown pipeline (${stepData.stepDefinition})`)
        }
        const pipeline = list[list.length - 1]
        //ZZZZ Check that it is active
        const description = pipeline.description
        jsonDefinition = pipeline.stepsJson
        const steps = JSON.parse(jsonDefinition)
        // console.log(`jsonDefinition=`, jsonDefinition)

        this.#stepDefinition = {
          stepType: STEP_TYPE_PIPELINE,
          description,
          steps,
        }
        // console.log(`this.#stepDefinition=`, this.#stepDefinition)
        break

    case 'object':
      // console.log(`already have definition`)
      this.#stepDefinition = stepData.stepDefinition
      jsonDefinition = JSON.stringify(this.#stepDefinition, '', 2)
      break

    default:
      throw new Error(`Invalid value for parameter stepDefinition (${typeof(stepData.stepDefinition)})`)
    }

    // Instantiate the step object
    const stepType = this.#stepDefinition.stepType
    this.stepObject = await StepTypes.factory(stepType, this.#stepDefinition)
    if (!(this.stepObject instanceof Step)) {
      throw Error(`Factory for ${stepType} did not return a step`)
    }

    // console.log(`END OF MATRIALIZE, txdata IS ${this.#txdata.getJson()}`.magenta)
  }

  getTxId() {
    return this.#txId
  }

  // deprecate this
  getTransactionId() {
    return this.#txId
  }

  getNodeId() {
    return this.#nodeId
  }

  getStepId() {
    return this.#stepId
  }

  // getParentNodeId() {
  //   return this.#parentNodeId
  // }

  // getParentStepId() {
  //   return this.#parentStepId
  // }

  getStepType() {
    return this.#stepDefinition.stepType
  }

  getStepObject() {
    return this.stepObject
  }

  getLevel() {
    return this.#level
  }

  /**
   *
   * @returns XData
   */
  getTxData() {
    return this.#txdata
  }
  // getData() {
  //   return this.#txdata.getData()
  // }
  getDataAsObject() {
    return this.#txdata.getData()
  }
  getDataAsJson() {
    return this.#txdata.getJson()
  }

  getMetadata() {
    return this.#metadata
  }

  getSequence() {
    return this.fullSequenceYARP
  }

  /**
   * Persist a value of interest used by the step
   * @param {String} name
   * @param {String | Number | Object} value
   */
  async artifact(name, value) {
    // console.log(`StepInstance.artifact(${name}, ${value})`)

    switch (typeof(value)) {
      case 'string':
        break
      case 'number':
        value = value.toString()
        break;
      default:
        value = 'JSON:' + JSON.stringify(value, '', 2)
        break
    }
    await dbArtifact.saveArtifact(this.#stepId, name, value)
  }

  /*
   * Step return functions
   */

  // async finish(status, note, newTx) {
  //   // console.log(`StepInstance.finish(${status}, ${note}, newTx):`, newTx)
  //   // console.log(`StepInstance.finish(${status}, ${note}, newTx)`)
  //   // console.log(`StepInstance.finish(${status}, ${note}, newTx):`, newTx)
  //   if (!newTx) {
  //     newTx = this.getDataAsObject()
  //   }
  //   const myTx = new XData(newTx)
  //   // console.log('YARP', myTx)
  //   // console.log(`StepInstance.finish(${status}, ${note}). newTx:`, newTx)
  //   const response = myTx.getJson()
  //   await dbStep.saveExitStatus(this.#stepId, status, response)

  //   const tx = await TransactionCache.findTransaction(this.#txId)

  //   // Return the promise
  //   // console.log(`   -> stepId=${this.#stepId}, completionToken=${this.#completionToken}`)
  //   // return Scheduler.stepFinished(this.#stepId, this.#completionToken, status, note, myTx)

  //   const queueToParentNodeRunningParent = Scheduler2.standardQueueName(parentNodeId, '')
  //   await Scheduler2.enqueue_StepCompleted(queueToParentNodeRunningParent, {
  //     txId,
  //     stepId,
  //     status,
  //     stepOutput: { from: 'misc/ping3'}
  //   })

  // }

  _sanitizedOutput(stepOutput) {
    // Sanitize the output to an object (not TXData, not null)
    let myStepOutput // TtxData
    if (stepOutput === null || stepOutput === undefined) {
      // No output provided - use the input as the output
      stepOutput = this.#txdata
    }

    if (stepOutput === null || stepOutput === undefined) {
      myStepOutput = { }
    } else if (stepOutput instanceof XData) {
      myStepOutput = stepOutput.getData()
    } else if (typeof(stepOutput) === 'object') {
      // Use the provided output
      myStepOutput = stepOutput
    } else {
      throw new Error(`Invalid stepOutput parameter. Must be null, object or TXData`)
    }
    return myStepOutput
  }

  async succeeded(note, stepOutput) {
    if (VERBOSE) console.log(`StepInstance.succeeded(note=${note}, ${typeof stepOutput})`, stepOutput)

    const myStepOutput = this._sanitizedOutput(stepOutput)
    if (VERBOSE) console.log(`succeeded: step out is `.magenta, myStepOutput)


    // Quick sanity check - make sure this step is actually running, and has not already exited.
    // console.log(`yarp getting tx ${this.#txId}`)
    const tx = await TransactionCache.findTransaction(this.#txId, false)
    // console.log(`tx=`, tx)
    const stepData = tx.stepData(this.#stepId)
    if (stepData.status !== STEP_RUNNING) {
      //ZZZ Write to the log
      const description = `Step status is not ${STEP_RUNNING}. Has this step already exited? [${this.#stepId}]`
      console.log(description)
      throw new Error(description)
    }

    // Persist the result and new status
    await tx.delta(this.#stepId, {
      status: STEP_SUCCESS,
      note,
      stepOutput: myStepOutput
    })

    // Tell the parent we've completed.
    // console.log(`replying to `, this.#onComplete)
    const queueName = Scheduler2.standardQueueName(this.#onComplete.nodeGroup, DEFAULT_QUEUE)
    await Scheduler2.enqueue_StepCompleted(queueName, {
      txId: this.#txId,
      stepId: this.#stepId,
      completionToken: this.#onComplete.completionToken,
    })

  }

  async aborted(note, stepOutput) {
    if (VERBOSE) console.log(`StepInstance.aborted(note=${note}, ${typeof stepOutput})`, stepOutput)

    const myStepOutput = this._sanitizedOutput(stepOutput)
    if (VERBOSE) console.log(`aborted: step out is `.magenta, myStepOutput)


    // Quick sanity check - make sure this step is actually running, and has not already exited.
    // console.log(`yarp getting tx ${this.#txId}`)
    const tx = await TransactionCache.findTransaction(this.#txId, false)
    // console.log(`tx=`, tx)
    const stepData = tx.stepData(this.#stepId)
    if (stepData.status !== STEP_RUNNING) {
      //ZZZ Write to the log
      const description = `Step status is not ${STEP_RUNNING}. Has this step already exited? [${this.#stepId}]`
      console.log(description)
      throw new Error(description)
    }

    // Persist the result and new status
    await tx.delta(this.#stepId, {
      status: STEP_ABORTED,
      note,
      stepOutput: myStepOutput
    })

    // Tell the parent we've completed.
    // console.log(`replying to `, this.#onComplete)
    const queueName = Scheduler2.standardQueueName(this.#onComplete.nodeGroup, DEFAULT_QUEUE)
    await Scheduler2.enqueue_StepCompleted(queueName, {
      txId: this.#txId,
      stepId: this.#stepId,
      completionToken: this.#onComplete.completionToken,
    })
  }

  async failed(note, stepOutput) {
    if (VERBOSE) console.log(`StepInstance.failed(note=${note}, ${typeof stepOutput})`, stepOutput)

    const myStepOutput = this._sanitizedOutput(stepOutput)
    if (VERBOSE) console.log(`failed: step out is `.magenta, myStepOutput)


    // Quick sanity check - make sure this step is actually running, and has not already exited.
    // console.log(`yarp getting tx ${this.#txId}`)
    const tx = await TransactionCache.findTransaction(this.#txId, false)
    // console.log(`tx=`, tx)
    const stepData = tx.stepData(this.#stepId)
    if (stepData.status !== STEP_RUNNING) {
      //ZZZ Write to the log
      const description = `Step status is not ${STEP_RUNNING}. Has this step already exited? [${this.#stepId}]`
      console.log(description)
      throw new Error(description)
    }

    // Persist the result and new status
    await tx.delta(this.#stepId, {
      status: STEP_FAILED,
      note,
      stepOutput: myStepOutput
    })

    // Tell the parent we've completed.
    // console.log(`replying to `, this.#onComplete)
    const queueName = Scheduler2.standardQueueName(this.#onComplete.nodeGroup, DEFAULT_QUEUE)
    await Scheduler2.enqueue_StepCompleted(queueName, {
      txId: this.#txId,
      stepId: this.#stepId,
      completionToken: this.#onComplete.completionToken,
    })
  }

  async badDefinition(msg) {
    // console.log(`StepInstance.badDefinition(${msg})`)

    // Write this to the admin log.
    //ZZZZ

    // Write to the transaction / step
    this.console(msg)
    await this.log(Logbook.LEVEL_TRACE, `Step reported bad definition [${msg}]`)
    await this.artifact('badStepDefinition', this.#stepDefinition)

    // Finish the step
    const status = STEP_INTERNAL_ERROR
    const data = {
      error: `Internal error: bad pipeline definition. Please notify system administrator.`,
      transactionId: this.#txId,
      stepId: this.#stepId
    }
    await dbStep.saveExitStatus(this.#stepId, status, data)

    const note = `Bad step definition: ${msg}`
    return Scheduler.stepFinished(this.#stepId, this.#onComplete.completionToken, status, note, new XData(data))
  }

  async exceptionInStep(e) {
    // console.log(Logbook.LEVEL_TRACE, `StepInstance.exceptionInStep()`)

    // Write this to the admin log.
    //ZZZZ

    // Write to the transaction / step
    await this.console(`Exception in step: ${e.stack}`)
    await this.log(Logbook.LEVEL_TRACE, `Exception in step.`, e)
    // this.artifact('exception', { stacktrace: e.stack })

    // Trim down the stacktract and save it
    let trace = e.stack
    let pos = trace.indexOf('    at processTicksAndRejections')
    if (pos >= 0) {
      trace = trace.substring(0, pos)
    }
    await this.artifact('exception', trace)

    // Finish the step
    const status = STEP_INTERNAL_ERROR
    const data = {
      error: `Internal error: exception in step. Please notify system administrator.`,
      transactionId: this.#txId,
      stepId: this.#stepId
    }
    await dbStep.saveExitStatus(this.#stepId, status, data)

    const note = `Exception in step`
    return Scheduler.stepFinished(this.#stepId, this.#onComplete.completionToken, status, note, new XData(data))
  }


  console(msg, p2) {
    if (!msg) {
      msg = ''
    }
    if (p2) {
      console.log(`${this.#indent} ${msg}`, p2)
    } else {
      console.log(`${this.#indent} ${msg}`)
    }
  }

  dump(obj) {
    // this.console
    const type = obj.constructor.name
    switch (type) {
      case 'StepInstance':
        this.console(`Dump: ${type}`)
        this.console(`  pipeId: ${obj.pipeId}`)
        this.console(`  data: `, obj.data)
        this.console(`  ${obj.pipelineStack.length} pipelines deep`)
        break

      default:
        console.log(this.#indent, obj)
      }
  }

  log(level, msg) {
    const options = {
      level
    }
    this.#logbook.log(this.pipeId, options, msg)
  }

  getLogbook() {
    return this.#logbook
  }

  // getChildStep() {
  //   return this.childStep
  // }

  // setChildStep(index) {
  //   this.childStep = index
  // }

  // fullSequenceYARP() {
  //   if (this.parentContext) {
  //     return `${this.parentContext.sequenceNo()}.${this.sequence}`
  //   }
  //   return ''+this.stepNo
  // }

  // Add into to toString() when debugging
  // See https://stackoverflow.com/questions/42886953/whats-the-recommended-way-to-customize-tostring-using-symbol-tostringtag-or-ov
  get [Symbol.toStringTag]() {
    let s = `${this.#stepDefinition.stepType}, ${this.#stepId}`
    if (this.#parentStepId) {
      s += `, parent=${this.#parentStepId}`
    } else {
      s += `, no parent`
    }
    return s
  }
}
