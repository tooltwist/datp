/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import Logbook from './Logbook'
import StepTypes from './StepTypeRegister'
import Step, { STEP_ABORTED, STEP_FAILED, STEP_INTERNAL_ERROR, STEP_RUNNING, STEP_SLEEPING, STEP_SUCCESS } from './Step'
import dbPipelines from "../database/dbPipelines";
import XData from "./XData";
import { STEP_TYPE_PIPELINE } from './StepTypeRegister'
import Scheduler2 from "./Scheduler2/Scheduler2";
import TransactionCache from "./Scheduler2/TransactionCache";
import indentPrefix from '../lib/indentPrefix'
import assert from 'assert'
import Transaction from './Scheduler2/Transaction';
import { schedulerForThisNode } from '..';

const VERBOSE = 0
export const DEEP_SLEEP_DURATION = 2 * 60 // Two minutes

export default class StepInstance {
  #txId
  #nodeGroup  // Cluster of redundant servers, performing the same role
  #nodeId     // Specific server
  #stepId
  // #parentNodeId
  #parentStepId
  #stepDefinition
  // #logbook
  #txdata
  #metadata
  // #logSequence
  #fullSequence

  #rollingBack
  #logBuffer

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
    this.#fullSequence = ''
    // Step completion handling
    this.#onComplete = null
    // Children
    // this.childStep = 0

    this.#rollingBack = false
    this.#logBuffer = [ ] // Temporary store of log entries

    // this.#logSequence = 0

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
    assert (typeof(stepData.fullSequence) === 'string')
    assert (typeof(stepData.stepDefinition) !== 'undefined')
    assert (typeof(stepData.stepInput) === 'object')
    assert (typeof(txData.metadata) === 'object')
    assert (typeof(stepData.level) === 'number')

    assert (typeof(stepData.onComplete) === 'object')
    assert (typeof(stepData.onComplete.nodeGroup) === 'string')
    assert (typeof(stepData.onComplete.completionToken) === 'string')

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
    this.#fullSequence = stepData.fullSequence

    this.#onComplete = stepData.onComplete
    // this.#completionToken = options.completionToken

    // Log this step being materialized
    // this.#logbook = options.logbook
    //ZZZZZ
    // this.#logbook = new Logbook.cls({
    //   transactionId: this.#txId,
    //   description: `Pipeline logbook`
    // })
    // this.trace(`Materialize stepInstance ${this.#stepId}`, Transaction.LOG_SOURCE_SYSTEM)


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

  getNodeGroup() {
    return this.#nodeGroup
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

  getFullSequence() {
    return this.#fullSequence
  }

  // /**
  //  * Persist a value of interest used by the step
  //  * @param {String} name
  //  * @param {String | Number | Object} value
  //  */
  // async artifact(name, value) {
  //   // console.log(`StepInstance.artifact(${name}, ${value})`)

  //   switch (typeof(value)) {
  //     case 'string':
  //       break
  //     case 'number':
  //       value = value.toString()
  //       break;
  //     default:
  //       value = 'JSON:' + JSON.stringify(value, '', 2)
  //       break
  //   }
  //   await dbArtifact.saveArtifact(this.#stepId, name, value)
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




  /*
   * Step return functions
   */


  /**
   *
   * @param {*} note
   * @param {*} stepOutput
   */
  async succeeded(note, stepOutput) {
    if (VERBOSE) console.log(`StepInstance.succeeded(note=${note}, ${typeof stepOutput})`, stepOutput)

    const myStepOutput = this._sanitizedOutput(stepOutput)
    if (VERBOSE) console.log(`succeeded: step out is `.magenta, myStepOutput)
    if (VERBOSE > 1) console.log(`this.#onComplete=`, this.#onComplete)

    // Sync any buffered logs
    this.trace(`instance.succeeded() - ${note}`, Transaction.LOG_SOURCE_SYSTEM)
    await this.syncLogs()

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
    // If the parent is in the node group of this node, then we assume that
    // it was invoked from within this node.
    // We keep the steps all running on the same node as their pipellines, so they all
    // use the same cached transaction. We only jump to another node when we are calling a
    // pipline that runs on another node.
    const myNodeGroup = schedulerForThisNode.getNodeGroup()
    const myNodeId = schedulerForThisNode.getNodeId()
    const parentNodeGroup = this.#onComplete.nodeGroup
    let queueName
    if (parentNodeGroup === myNodeGroup) {
      // Use this node's personal express queue
      queueName = Scheduler2.nodeRegularQueueName(myNodeGroup, myNodeId)
    } else {
      // Use this group queue for the different node group
      queueName = Scheduler2.groupQueueName(myNodeGroup)
    }
    await schedulerForThisNode.enqueue_StepCompleted(queueName, {
      txId: this.#txId,
      stepId: this.#stepId,
      completionToken: this.#onComplete.completionToken,
    })

  }

  /**
   *
   * @param {*} note
   * @param {*} stepOutput
   */
  async aborted(note, stepOutput) {
    if (VERBOSE) console.log(`StepInstance.aborted(note=${note}, ${typeof stepOutput})`, stepOutput)

    // Sync any buffered logs
    this.trace(`instance.aborted()`, Transaction.LOG_SOURCE_SYSTEM)
    await this.syncLogs()


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
    const queueName = Scheduler2.groupQueueName(this.#onComplete.nodeGroup)
    await schedulerForThisNode.enqueue_StepCompleted(queueName, {
      txId: this.#txId,
      stepId: this.#stepId,
      completionToken: this.#onComplete.completionToken,
    })
  }

  /**
   *
   * @param {*} note
   * @param {*} stepOutput
   */
  async failed(note, stepOutput) {
    if (VERBOSE) console.log(`StepInstance.failed(note=${note}, ${typeof stepOutput})`, stepOutput)

    // Sync any buffered logs
    this.trace(`instance.failed(${note})`, Transaction.LOG_SOURCE_SYSTEM)
    await this.syncLogs()

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
    const queueName = Scheduler2.groupQueueName(this.#onComplete.nodeGroup)
    await schedulerForThisNode.enqueue_StepCompleted(queueName, {
      txId: this.#txId,
      stepId: this.#stepId,
      completionToken: this.#onComplete.completionToken,
    })
  }

  /**
   *
   * @param {*} msg
   * @returns
   */
  async badDefinition(msg) {
    console.log(`StepInstance.badDefinition(${msg})`)

    // Sync any buffered logs
    this.trace(msg, Transaction.LOG_SOURCE_DEFINITION)
    await this.syncLogs()

    // Write this to the admin log.
    //ZZZZ

    // Write to the transaction / step
    this.error(msg)
    await this.log(Logbook.LEVEL_TRACE, `Step reported bad definition [${msg}]`)
    // await this.artifact('badStepDefinition', this.#stepDefinition)

    // Finish the step
    const status = STEP_INTERNAL_ERROR
    const data = {
      error: `Internal error: bad pipeline definition. Please notify system administrator.`,
      transactionId: this.#txId,
      stepId: this.#stepId
    }

    // Save the step status
    const tx = await TransactionCache.findTransaction(this.#txId, false)
    await tx.delta(this.#stepId, {
      status: STEP_INTERNAL_ERROR,
      note: `Internal error: bad pipeline definition. Please notify system administrator.`,
      stepOutput: data
    })

    // Tell the parent we've completed.
    const queueName = Scheduler2.groupQueueName(this.#onComplete.nodeGroup)
    await schedulerForThisNode.enqueue_StepCompleted(queueName, {
      txId: this.#txId,
      stepId: this.#stepId,
      completionToken: this.#onComplete.completionToken,
    })
  }

  /**
   *
   * @param {*} e
   * @returns
   */
  async exceptionInStep(message, e) {
    // console.log(Logbook.LEVEL_TRACE, `StepInstance.exceptionInStep()`)
    console.log(this.#indent + `StepInstance.exceptionInStep(${message})`, e)
    // console.log(new Error('YARP').stack)

    // Sync any buffered logs
    if (!message) {
      message = 'Exception in step'
    }
    this.trace(message, Transaction.LOG_SOURCE_EXCEPTION)
    if (e instanceof Error) {
      const str = `${message}\n${e.stack}`
      this.trace(str, Transaction.LOG_SOURCE_EXCEPTION)
    // } else {
    //   this.trace(message, Transaction.LOG_SOURCE_EXCEPTION)
    }
    await this.syncLogs()

    // Write this to the admin log.
    //ZZZZ

    // Write to the transaction / step
    await this.error(`Exception in step: ${e.stack}`)
    await this.log(Logbook.LEVEL_TRACE, `Exception in step.`, e)
    // this.artifact('exception', { stacktrace: e.stack })

    // Trim down the stacktract and save it
    let trace = e.stack
    let pos = trace.indexOf('    at processTicksAndRejections')
    if (pos >= 0) {
      trace = trace.substring(0, pos)
    }
    // await this.artifact('exception', trace)

    // Finish the step
    const status = STEP_INTERNAL_ERROR
    const data = {
      error: `Internal error: exception in step. Please notify system administrator.`,
      transactionId: this.#txId,
      stepId: this.#stepId
    }

    const tx = await TransactionCache.findTransaction(this.#txId, false)
    await tx.delta(this.#stepId, {
      status: STEP_INTERNAL_ERROR,
      note: `Internal error: exception in step. Please notify system administrator.`,
      stepOutput: data
    })

    // Tell the parent we've completed.
    const queueName = Scheduler2.groupQueueName(this.#onComplete.nodeGroup)
    await schedulerForThisNode.enqueue_StepCompleted(queueName, {
      txId: this.#txId,
      stepId: this.#stepId,
      completionToken: this.#onComplete.completionToken,
    })
  }

  /**
   * Provide information for status reports, explaining progress in the processing of the
   * transaction. This is commonly used in conjunction with sleeps and setting switches
   * to pause the transaction at various states while it waits for extrnal events.
   *
   * @param {Object} object JSON-able object that will be returned in the transaction status
   */
  async progressReport(object) {
    if (VERBOSE) console.log(`StepInstance.progressReport()`, object)

    // Sync any buffered logs
    this.trace(JSON.stringify(object, '', 2), Transaction.LOG_SOURCE_PROGRESS_REPORT)
    await this.syncLogs()

    // Save the progressReport
    const tx = await TransactionCache.findTransaction(this.#txId, false)
    await tx.delta(null, {
      progressReport: object
    })
  }

  /**
   * This function places a pipeline into sleep status, waiting until either
   * a fixed time has passed, or the value of a transaction switch changes.
   *
   * NOTE: It is essential that your step returns immediately after calling
   * this function!
   *
   * @param {string} nameOfSwitch Retry immediately if the value of this switch changes
   * @param {number} sleepDuration Number of seconds after which to retry (defaults to two minutes)
   */
  async retryLater(nameOfSwitch=null, sleepDuration=120) {
    if (VERBOSE) console.log(`StepInstance.retryLater(nameOfSwitch=${nameOfSwitch}, sleepDuration=${sleepDuration})`)

    // Sync any buffered logs
    this.trace(`instance.retryLater()`, Transaction.LOG_SOURCE_SYSTEM)
    await this.syncLogs()

    // Update the transaction status
    const tx = await TransactionCache.findTransaction(this.#txId, false)
    await tx.delta(null, {
      status: STEP_SLEEPING
      //ZZZZ Sleep completion time
    })
    await tx.delta(this.#stepId, {
      status: STEP_SLEEPING
    })

sleepDuration = 15

    if (sleepDuration < DEEP_SLEEP_DURATION) {
      console.log(`Step will nap for ${sleepDuration} seconds [${this.#stepId}]`)
      setTimeout(async() => {
        console.log(`Restarting step after a nap of ${sleepDuration} seconds [${this.#stepId}]`)
        const queueName = Scheduler2.groupQueueName(this.#nodeGroup)
        await schedulerForThisNode.enqueue_StepRestart(queueName, this.#txId, this.#stepId)
      }, sleepDuration * 1000)
    }
  }

  /**
   *
   * @param {*} name
   * @returns
   */
  async getSwitch(name) {
    if (VERBOSE) console.log(`StepInstance.getSwitch(${name})`)
    const tx = await TransactionCache.findTransaction(this.#txId, false)
    const value = await Transaction.getSwitch(tx.getOwner(), this.#txId, name)
    return value
  }


  // console(msg, p2) {
  //   if (!msg) {
  //     msg = ''
  //   }
  //   // Log this first
  //   this.debug(msg, null)

  //   // Now display on the console
  //   if (p2) {
  //     console.log(`${this.#indent} ${msg}`, p2)
  //   } else {
  //     console.log(`${this.#indent} ${msg}`)
  //   }
  // }

  dump(obj) {
    const type = obj.constructor.name
    switch (type) {
      case 'StepInstance':
        this.debug(`Dump: ${type}`)
        this.debug(`  pipeId: ${obj.pipeId}`)
        this.debug(`  data: `, obj.data)
        this.debug(`  ${obj.pipelineStack.length} pipelines deep`)
        break

      case 'String':
        this.debug(`Dump: ${obj}`)
        break

      default:
console.log(`ZZZZ. StepInstance.dump() called with unknown object type ${type}.`)
console.log(new Error().stack)
        console.log(this.#indent, obj)
      }
  }

  log(message, source=null) {
    this.dump(message, source)
  }

  debug(...args) {
    const { message, source } = this._checkLogParams(args)
    const level = Transaction.LOG_LEVEL_DEBUG
    this.#logBuffer.push({ level, source, message })
  }

  // trace(message, source=null) {
  trace(...args) {
    const { message, source } = this._checkLogParams(args)
    const level = Transaction.LOG_LEVEL_TRACE
    assert(typeof(source) !== 'undefined')
    assert(typeof(message) !== 'undefined')
    this.#logBuffer.push({ level, source, message })
  }

  warning(...args) {
    const { message, source } = this._checkLogParams(args)
    const level = Transaction.LOG_LEVEL_WARNING
    assert(typeof(source) !== 'undefined')
    assert(typeof(message) !== 'undefined')
    this.#logBuffer.push({ level, source, message })
  }

  error(...args) {
    const { message, source } = this._checkLogParams(args)
    const level = Transaction.LOG_LEVEL_ERROR
    assert(typeof(source) !== 'undefined')
    assert(typeof(message) !== 'undefined')
    this.#logBuffer.push({ level, source, message })
  }

  // addLog(level, source, message) {
  //   this.#logBuffer.push({ level, source, message })
  // }

  async syncLogs() {
    // console.log(`\n\n\n\n\n********************\n\n\nsyncLogs()`)
    if (this.#logBuffer.length > 0) {
      await Transaction.bulkLogging(this.#txId, this.#stepId, this.#logBuffer)
      this.#logBuffer = [ ]
    }
  }

  // Add details into toString() when debugging
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

  _checkLogParams(args) {
    // console.log(`_checkLogParams`, args)
    let message = ''
    let source = this.#rollingBack ? Transaction.LOG_SOURCE_ROLLBACK : Transaction.LOG_SOURCE_INVOKE
    if (args.length > 0) {
      const lastArg = args[args.length - 1]
      switch (lastArg) {
        case Transaction.LOG_SOURCE_DEFINITION:
        case Transaction.LOG_SOURCE_INVOKE:
        case Transaction.LOG_SOURCE_ROLLBACK:
        case Transaction.LOG_SOURCE_EXCEPTION:
        case Transaction.LOG_SOURCE_DEFINITION:
        case Transaction.LOG_SOURCE_SYSTEM:
        case Transaction.LOG_SOURCE_PROGRESS_REPORT:
        case Transaction.LOG_SOURCE_UNKNOWN:
          message = checkMessage(args, args.length - 1)
          source = lastArg
          break

        default:
          message = checkMessage(args, args.length)
          break
      }
      return { message, source}
    }
  }
}


function checkMessage(arr, len) {
  let sep = ''
  let s = ''
  for (let i = 0; i < len; i++) {
    const element = arr[i]
    if (typeof(element) === 'string') {
      s += sep + element
    } else {
      s += sep + JSON.stringify(element)
    }
    sep = ',\n'
  }
  return s
}
