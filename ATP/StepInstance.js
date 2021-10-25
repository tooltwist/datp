/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import GenerateHash from "./GenerateHash"
import Logbook from './Logbook'
import StepTypes from './StepTypeRegister'
import Step from './Step'
import StepTypeRegister from './StepTypeRegister'
import Scheduler from './Scheduler'
import dbStep from "../database/dbStep";
import dbPipelines from "../database/dbPipelines";
import TxData from "./TxData";
import dbArtifact from "../database/dbArtifact";
import { STEP_TYPE_PIPELINE } from './StepTypeRegister'


export default class StepInstance {
  #transactionId
  #parentId
  #stepId
  #definition
  #completionToken
  #logbook
  #txdata
  #metadata
  #logSequence

  constructor() {
    // Definition
    this.#parentId = null
    this.#stepId = null
    this.#definition = { stepType: null}
    // Transaction data
    this.#txdata = null
    // Private data for use within step
    this.privateData = { }
    // Debug stuff
    this.level = 0
    this.fullSequence = ''
    // Step completion handling
    this.#completionToken = null
    // Children
    // this.childStep = 0

    this.#logSequence = 0

    // Note that this object is transitory - not persisted. It will be
    // recreated if we reload this stepInstance from persistant storage.
    this.stepObject = null
  }


  async materialize(options) {
    // console.log(``)
    console.log(`-----------------------------------------------------------------------------------------------------`)
    // console.log(``)
    // console.log(`StepInstance.materialize()`)
    // console.log(`options.data=`, options.data)

    // this.pipeId = GenerateHash('pipe')
    // this.data = { }
    // this.pipelineStack = [ ]


    this.#transactionId = options.transactionId
    this.#parentId = options.parentId
    this.#stepId = GenerateHash('step')
    //this.#definition set below
    this.#txdata = new TxData(options.data)
    this.#metadata = options.metadata
    // this.privateData
    // this.indentLevel
    this.level = options.level
    this.fullSequence = options.fullSequence
    this.#completionToken = options.completionToken

    // Log this step being materialized
    this.#logbook = options.logbook
    await this.log(Logbook.LEVEL_TRACE, `Start step ${this.#stepId}`)


    // this.indentLevel = parentContext ? (parentContext.indentLevel + 1) : 1
    this.indentStr()
    // console.log(`StepInstance.constructor() 9`, this)

    // Remember the current step
    // this.sequence = 0

    // this.parentStepId = parentStepId

    /*
     *  Load the definition of the step (which is probably a pipeline)
     */
    // console.log(`typeof(options.definition)=`, typeof(options.definition))
    let jsonDefinition
    switch (typeof(options.definition)) {
      case 'string':
        // console.log(`Loading definition for ${options.definition}`)
        // jsonDefinition = fs.readFileSync(`./pipeline-definitions/${options.definition}.json`)
        const arr = options.definition.split(':')
        // console.log(`arr=`, arr)
        let pipelineName = arr[0]
        let version = (arr.length > 0) ? arr[1] : null
        const list = await dbPipelines.getPipelines(pipelineName, version)
        // console.log(`list=`, list)
        if (list.length < 1) {
          throw new Error(`Unknown pipeline (${options.definition})`)
        }
        const pipeline = list[list.length - 1]
        //ZZZZ Check that it is active
        const description = pipeline.description
        jsonDefinition = pipeline.stepsJson
        const steps = JSON.parse(jsonDefinition)
        // console.log(`jsonDefinition=`, jsonDefinition)

        this.#definition = {
          stepType: STEP_TYPE_PIPELINE,
          description,
          steps,
        }
        // console.log(`this.#definition=`, this.#definition)
        break

    case 'object':
      // console.log(`already have definition`)
      this.#definition = options.definition
      jsonDefinition = JSON.stringify(this.#definition, '', 2)
      break

    default:
      throw new Error(`Invalid value for parameter definition (${typeof(options.definition)})`)
    }
    // console.log(`this.#definition=`, this.#definition)

    // Instantiate the step object
    const stepType = this.#definition.stepType
    this.stepObject = await StepTypes.factory(stepType, this.#definition)
    if (!(this.stepObject instanceof Step)) {
      throw Error(`Factory for ${stepType} did not return a step`)
    }

    // Persist this step
    await dbStep.startStep(this.#stepId, this.#definition.stepType, this.#transactionId, this.#parentId, this.fullSequence, jsonDefinition)
  }

  getTransactionId() {
    return this.#transactionId
  }

  getStepId() {
    return this.#stepId
  }

  getStepType() {
    return this.#definition.stepType
  }

  getStepObject() {
    return this.stepObject
  }

  getLevel() {
    return this.level
  }

  /**
   *
   * @returns TxData
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
    return this.fullSequence
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

  async finish(status, note, newTx) {
    // console.log(`StepInstance.finish(${status}, ${note}, newTx):`, newTx)
    // console.log(`StepInstance.finish(${status}, ${note}, newTx)`)
    // console.log(`StepInstance.finish(${status}, ${note}, newTx):`, newTx)
    if (!newTx) {
      newTx = this.getDataAsObject()
    }
    const myTx = new TxData(newTx)
    // console.log('YARP', myTx)
    // console.log(`StepInstance.finish(${status}, ${note}). newTx:`, newTx)
    const response = myTx.getJson()
    await dbStep.saveExitStatus(this.#stepId, status, response)

    // Return the promise
    // console.log(`   -> stepId=${this.#stepId}, completionToken=${this.#completionToken}`)
    return Scheduler.stepFinished(this.#stepId, this.#completionToken, status, note, myTx)
  }

  async succeeded(note, newTx) {
    const myTx = new TxData(newTx)
    const response = myTx.getJson()
    // const response = JSON.stringify(newTx, '', 2)
    const status = Step.COMPLETED
    await dbStep.saveExitStatus(this.#stepId, status, response)

    // Return the promise
    // console.log(`   -> stepId=${this.stepId}, completionToken=${this.completionToken}`)
    return Scheduler.stepFinished(this.#stepId, this.#completionToken, status, note, myTx)
  }

  async failed(note, newTx) {
    const myTx = new TxData(newTx)
    const response = myTx.getJson()
    // const response = JSON.stringify(newTx, '', 2)
    this.console(`Step exiting with fail status [${note}]`)

    const status = Step.FAIL
    await dbStep.saveExitStatus(this.#stepId, status, response)

    return Scheduler.stepFinished(this.#stepId, this.#completionToken, status, note, myTx)
  }

  async badDefinition(msg) {
    // console.log(`StepInstance.badDefinition(${msg})`)

    // Write this to the admin log.
    //ZZZZ

    // Write to the transaction / step
    this.console(msg)
    await this.log(Logbook.LEVEL_TRACE, `Step reported bad definition [${msg}]`)
    await this.artifact('badStepDefinition', this.#definition)

    // Finish the step
    const status = Step.INTERNAL_ERROR
    const data = {
      error: `Internal error: bad pipeline definition. Please notify system administrator.`,
      transactionId: this.#transactionId,
      stepId: this.#stepId
    }
    await dbStep.saveExitStatus(this.#stepId, status, data)

    const note = `Bad step definition: ${msg}`
    return Scheduler.stepFinished(this.#stepId, this.#completionToken, status, note, new TxData(data))
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
    const status = Step.INTERNAL_ERROR
    const data = {
      error: `Internal error: exception in step. Please notify system administrator.`,
      transactionId: this.#transactionId,
      stepId: this.#stepId
    }
    await dbStep.saveExitStatus(this.#stepId, status, data)

    const note = `Exception in step`
    return Scheduler.stepFinished(this.#stepId, this.#completionToken, status, note, new TxData(data))
  }

  // pushPipeline (pipelineStep) {
  //   this.log(Logbook.LEVEL_TRACE, `Start pipeline ${pipelineStep.#stepId}`)

  //   this.pipelineStack.push({
  //     pipelineStep,
  //     threadId: GenerateHash('thread-')
  //   })
  //   this.indentLevel++
  //   this.indentStr()
  // }

  // currentPipeline () {
  //   return this.pipelineStack[0]
  // }

  // popPipeline () {
  //   this.log(Logbook.LEVEL_TRACE, `End pipeline`)
  //   this.pipelineStack.pop()
  //   this.indentLevel--
  //   this.indentStr()
  // }

  console(msg, p2) {
    if (!msg) {
      msg = ''
    }
    if (p2) {
      console.log(`${this.indent} ${msg}`, p2)
    } else {
      console.log(`${this.indent} ${msg}`)
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
        console.log(this.indent, obj)
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

  indentStr() {
    let s = ''
    // s += `${this.level}  `
    for (let i = 0; i < this.level; i++) {
      s += '    '
    }
    s += `${this.level}  `
// console.log(`----------------------->${s}`)
    this.indent = s
  }

  // getChildStep() {
  //   return this.childStep
  // }

  // setChildStep(index) {
  //   this.childStep = index
  // }

  // fullSequence() {
  //   if (this.parentContext) {
  //     return `${this.parentContext.sequenceNo()}.${this.sequence}`
  //   }
  //   return ''+this.stepNo
  // }

  // Add into to toString() when debugging
  // See https://stackoverflow.com/questions/42886953/whats-the-recommended-way-to-customize-tostring-using-symbol-tostringtag-or-ov
  get [Symbol.toStringTag]() {
    let s = `${this.#definition.stepType}, ${this.#stepId}`
    if (this.#parentId) {
      s += `, parent=${this.#parentId}`
    } else {
      s += `, no parent`
    }
    return s
  }
}
