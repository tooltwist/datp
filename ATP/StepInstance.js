import GenerateHash from "./GenerateHash"
import Logbook from './Logbook'
const fs = require('fs');
import StepTypes from './StepTypeRegister'
import Step from './Step'
import Scheduler from './Scheduler'
import dbStep from "../database/dbStep";
import dbPipelines from "../database/dbPipelines";
import TxData from "./TxData";


export default class StepInstance {
  #transactionId
  #parentId
  #stepId
  #definition
  #completionToken
  #logbook
  #txdata
  #metadata

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
    this.#logbook = new Logbook.cls({ description: `Test pipeline`})
    this.log(0, Logbook.LEVEL_TRACE, `Start step ${this.#stepId}`)


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
          stepType: 'pipeline',
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

  async finish(status, note, newTx) {
    // console.log(`StepInstance.finish(${status}, ${note}, newTx):`, newTx)
    if (!newTx) {
      newTx = this.getDataAsObject()
    }
    const myTx = new TxData(newTx)
    // console.log('YARP', myTx)
    // console.log(`StepInstance.finish(${status}, ${note}). newTx:`, newTx)
    const response = myTx.getJson()
    await dbStep.complete(this.#stepId, response)

    // Return the promise
    // console.log(`   -> stepId=${this.#stepId}, completionToken=${this.#completionToken}`)
    return Scheduler.stepFinished(this.#stepId, this.#completionToken, status, note, myTx)
  }

  async succeeded(newTx) {
    const myTx = new TxData(newTx)
    const response = myTx.getJson()
    // const response = JSON.stringify(newTx, '', 2)
    await dbStep.complete(this.#stepId, response)

    const status = Step.COMPLETED
    const note = ''

    // Return the promise
    // console.log(`   -> stepId=${this.stepId}, completionToken=${this.completionToken}`)
    return Scheduler.stepFinished(this.#stepId, this.#completionToken, status, note, myTx)
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

  log(msg, level) {
    this.#logbook.log(this.pipeId, msg, level)
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
