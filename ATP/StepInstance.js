/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import StepTypes from './StepTypeRegister'
import Step, { STEP_ABORTED, STEP_FAILED, STEP_INTERNAL_ERROR, STEP_RUNNING, STEP_SLEEPING, STEP_SUCCESS } from './Step'
import dbPipelines from "../database/dbPipelines"
import XData, { dataFromXDataOrObject } from "./XData"
import { STEP_TYPE_PIPELINE } from './StepTypeRegister'
import { PERSIST_TRANSACTION_STATE } from "./Scheduler2/txState-level-1";
import indentPrefix from '../lib/indentPrefix'
import assert from 'assert'
import Transaction from './Scheduler2/Transaction'
import { schedulerForThisNode } from '..'
import dbLogbook from '../database/dbLogbook'
import { DEEP_SLEEP_SECONDS, isDevelopmentMode } from '../datp-constants'
import isEqual  from 'lodash.isequal'
import { GO_BACK_AND_RELEASE_WORKER } from './Scheduler2/Worker2'
import { requiresWebhookProgressReports, sendStatusByWebhook, WEBHOOK_EVENT_PROGRESS } from './Scheduler2/returnTxStatusCallback'

const VERBOSE = 0

export default class StepInstance {
  #worker

  #transactionState

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

  #waitingForCompletionFunction

  constructor() {
    // console.log(`StepInstance.materialize()`, options)
    this.#worker = null

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

    // This gets set when one of the step completion functions is called (succeeded, failed, aborted, etc)
    this.#waitingForCompletionFunction = true

    this.TRACE = 'trace'
    this.WARNING = 'warning'
    this.ERROR = 'error'
    this.DEBUG = 'debug'
  }


  async materialize(options, tx, worker) {
    // console.log(``)
    // console.log(`StepInstance.materialize options=`, options)
    // console.log(`  typeof(tx)=`, typeof(tx))

    assert(tx instanceof Transaction)

    this.#worker = worker
    this.#transactionState = tx
    // console.log(`tx.getTxId()=`, tx.getTxId())

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
    // this.trace(`Materialize stepInstance ${this.#stepId}`, dbLogbook.LOG_SOURCE_SYSTEM)


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
        // const arr = stepData.stepDefinition.split(':')
        // let pipelineName = arr[0]
        // let version = (arr.length > 0) ? arr[1] : null
        const pipelineName = stepData.stepDefinition
        const pipeline = await dbPipelines.getPipelineVersionInUse(pipelineName)
        // if (list.length < 1) {
        //   throw new Error(`Unknown pipeline (${stepData.stepDefinition})`)
        // }
        // const pipeline = list[list.length - 1]

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

  // This function gets the transaction state object. We want to keep this unpublished
  // ans hard to notice - we don't want developers mucking with the internals of DATP.
  _7agghtstrajj_37(txId) {
    if (this.#transactionState.getTxId() !== txId) {
      console.log(`Serious Error: Getting transaction that is not my transaction!!!!`)
      console.log(`this.#transactionState.txId =`, this.#transactionState.getTxId() )
      console.log(`txId=`, txId)
    }
    return this.#transactionState
  }

  getWorker() {
    return this.#worker
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

  getLangFromMetadata() {
    return this.#metadata.lang ? this.#metadata.lang : null
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
    } else {
      myStepOutput = dataFromXDataOrObject(stepOutput, `Invalid stepOutput parameter. Must be null, object or TXData`)
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
    this.#waitingForCompletionFunction = false

    const myStepOutput = this._sanitizedOutput(stepOutput)
    if (VERBOSE) console.log(`succeeded: step out is `.magenta, myStepOutput)
    if (VERBOSE > 1) console.log(`this.#onComplete=`, this.#onComplete)

    // Sync any buffered logs
    this.trace(`Step succeeded - ${note}`, dbLogbook.LOG_SOURCE_SYSTEM)
    await this.syncLogs()

    // Quick sanity check - make sure this step is actually running, and has not already exited.
    const tx = this.#transactionState
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
    }, 'stepInstance.succeeded()')


    // // Tell the parent we've completed.
    // // If the parent is in the node group of this node, then we assume that
    // // it was invoked from within this node.
    // // We keep the steps all running on the same node as their pipellines, so they all
    // // use the same cached transaction. We only jump to another node when we are calling a
    // // pipline that runs on another node.
    // const myNodeGroup = schedulerForThisNode.getNodeGroup()
    // const myNodeId = schedulerForThisNode.getNodeId()
    // let queueName
    // if (parentNodeGroup === myNodeGroup) {
    //   // Use this node's personal express queue
    //   queueName = Scheduler2.nodeRegularQueueName(myNodeGroup, myNodeId)
    // } else {
    //   // Use this group queue for the different node group
    //   queueName = Scheduler2.groupQueueName(myNodeGroup)
    // }
    // const rv = await schedulerForThisNode.enqueue_StepCompletedZZ(queueName, {
    const parentNodeGroup = this.#onComplete.nodeGroup
    const parentNodeId = this.#onComplete.nodeId ? this.#onComplete.nodeId : null
    const workerForShortcut = this.#worker
    const rv = await schedulerForThisNode.schedule_StepCompleted(tx, parentNodeGroup, parentNodeId, {
      txId: this.#txId,
      stepId: this.#stepId,
      completionToken: this.#onComplete.completionToken,
    }, workerForShortcut)
    assert(rv === GO_BACK_AND_RELEASE_WORKER)
    return GO_BACK_AND_RELEASE_WORKER
  }

  /**
   *
   * @param {*} note
   * @param {*} stepOutput
   */
  async aborted(note, stepOutput) {
    if (VERBOSE) console.log(`StepInstance.aborted(note=${note}, ${typeof stepOutput})`, stepOutput)
    this.#waitingForCompletionFunction = false

    // Sync any buffered logs
    this.trace(`Step aborted`, dbLogbook.LOG_SOURCE_SYSTEM)
    await this.syncLogs()

    const myStepOutput = this._sanitizedOutput(stepOutput)
    if (VERBOSE) console.log(`aborted: step out is `.magenta, myStepOutput)

    // Quick sanity check - make sure this step is actually running, and has not already exited.
    const tx = this.#transactionState
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
    }, 'stepInstance.aborted()')

    // Tell the parent we've completed.
    // const queueName = Scheduler2.groupQueueName(this.#onComplete.nodeGroup)
    const parentNodeGroup = this.#onComplete.nodeGroup
    const parentNodeId = this.#onComplete.nodeId ? this.#onComplete.nodeId : null
    const workerForShortcut = this.#worker
    const rv = await schedulerForThisNode.schedule_StepCompleted(tx, parentNodeGroup, parentNodeId, {
    // const rv = await schedulerForThisNode.enqueue_StepCompletedZZ(queueName, {
      txId: this.#txId,
      stepId: this.#stepId,
      completionToken: this.#onComplete.completionToken,
    }, workerForShortcut)
    assert(rv === GO_BACK_AND_RELEASE_WORKER)
    return GO_BACK_AND_RELEASE_WORKER
  }

  /**
   *
   * @param {*} note
   * @param {*} stepOutput
   */
  async failed(note, stepOutput) {
    if (VERBOSE) console.log(`Step failed (note=${note}, ${typeof stepOutput})`, stepOutput)
    this.#waitingForCompletionFunction = false

    // Sync any buffered logs
    this.trace(`instance.failed(${note})`, dbLogbook.LOG_SOURCE_SYSTEM)
    await this.syncLogs()

    let myStepOutput
    try {
      myStepOutput = this._sanitizedOutput(stepOutput)
    } catch (e) {
      console.log(`StepInstance.failed: Could not get the output from failed step.`)
      console.log(`stepOutput=`, stepOutput)
      myStepOutput = { }
    }
    if (VERBOSE) console.log(`failed: step out is `.magenta, myStepOutput)

    // Quick sanity check - make sure this step is actually running, and has not already exited.
    const tx = this.#transactionState
    // console.log(`tx=`, tx)
    const stepData = tx.stepData(this.#stepId)
    if (stepData.status !== STEP_RUNNING) {
      //ZZZ Write to the log
      const description = `Step status is not ${STEP_RUNNING}. Has this step already exited? [${this.#stepId}]`
      console.log(description)
      throw new Error(description)
    }

    // Persist the result and new status
    // await tx.delta(null, {
    //   progressReport: {}
    // }, 'stepInstance.failed()')
    await tx.delta(this.#stepId, {
      status: STEP_FAILED,
      note,
      stepOutput: myStepOutput
    }, 'stepInstance.failed()')

    // Tell the parent we've completed.
    // console.log(`replying to `, this.#onComplete)
    const parentNodeGroup = this.#onComplete.nodeGroup
    const parentNodeId = this.#onComplete.nodeId ? this.#onComplete.nodeId : null
    const workerForShortcut = this.#worker
    const rv = await schedulerForThisNode.schedule_StepCompleted(tx, parentNodeGroup, parentNodeId, {
    // const queueName = Scheduler2.groupQueueName(this.#onComplete.nodeGroup)
    // const rv = await schedulerForThisNode.enqueue_StepCompletedZZ(queueName, {
      txId: this.#txId,
      stepId: this.#stepId,
      completionToken: this.#onComplete.completionToken,
    }, workerForShortcut)
    assert(rv === GO_BACK_AND_RELEASE_WORKER)
    return GO_BACK_AND_RELEASE_WORKER
  }

  /**
   *
   * @param {*} msg
   * @returns
   */
  async badDefinition(msg) {
    console.log(`StepInstance.badDefinition(${msg})`)
    this.#waitingForCompletionFunction = false

    // Sync any buffered logs
    this.trace(msg, dbLogbook.LOG_SOURCE_DEFINITION)
    await this.syncLogs()

    // Write this to the admin log.
    //ZZZZ

    // Write to the transaction / step
    this.error(msg)
    await this.log(dbLogbook.LOG_LEVEL_ERROR, `Step reported bad definition [${msg}]`)
    // await this.artifact('badStepDefinition', this.#stepDefinition)

    // Finish the step
    const status = STEP_INTERNAL_ERROR
    const data = {
      error: `Internal error: bad pipeline definition. Please notify system administrator.`,
      transactionId: this.#txId,
      stepId: this.#stepId
    }

    // Save the step status
    const tx = this.#transactionState
    await tx.delta(this.#stepId, {
      status: STEP_INTERNAL_ERROR,
      note: `Internal error: bad pipeline definition. Please notify system administrator.`,
      stepOutput: data
    }, 'stepInstance.badDefinition()')

    // Tell the parent we've completed.
    const parentNodeGroup = this.#onComplete.nodeGroup
    const parentNodeId = this.#onComplete.nodeId ? this.#onComplete.nodeId : null
    const workerForShortcut = this.getWorker()
    const rv = await schedulerForThisNode.schedule_StepCompleted(tx, parentNodeGroup, parentNodeId, {
    // const queueName = Scheduler2.groupQueueName(this.#onComplete.nodeGroup)
    // const rv = await schedulerForThisNode.enqueue_StepCompletedZZ(queueName, {
      txId: this.#txId,
      stepId: this.#stepId,
      completionToken: this.#onComplete.completionToken,
    }, workerForShortcut)
    assert(rv === GO_BACK_AND_RELEASE_WORKER)
    return GO_BACK_AND_RELEASE_WORKER
  }

  /**
   *
   * @param {*} e
   * @returns
   */
  async exceptionInStep(message, e) {
    console.log(this.#indent + `StepInstance.exceptionInStep(${message})`, e)
    this.#waitingForCompletionFunction = false

    // Sync any buffered logs
    if (!message) {
      message = 'Exception in step'
    }
    this.error(message, dbLogbook.LOG_SOURCE_EXCEPTION)
    if (e instanceof Error) {
      const str = `${message}\n${e.stack}`
      this.error(str, dbLogbook.LOG_SOURCE_EXCEPTION)
    // } else {
    //   this.trace(message, dbLogbook.LOG_SOURCE_EXCEPTION)
    }
    await this.syncLogs()

    // Write this to the admin log.
    //ZZZZ

    // Write to the transaction / step
    await this.error(`Exception in step: ${e.stack}`)
    await this.log(dbLogbook.LOG_LEVEL_ERROR, `Exception in step.`, e)
    // this.artifact('exception', { stacktrace: e.stack })

    // Trim down the stacktract and save it
    let trace = e.stack
    let pos = trace.indexOf('    at processTicksAndRejections')
    if (pos >= 0) {
      trace = trace.substring(0, pos)
    }
    // await this.artifact('exception', trace)

    // Finish the step
    // const status = STEP_INTERNAL_ERROR
    const data = {
      error: `Internal error: exception in step. Please notify system administrator.`,
      transactionId: this.#txId,
      stepId: this.#stepId
    }

    // Update the status
    const tx = this.#transactionState
    await tx.delta(this.#stepId, {
      status: STEP_INTERNAL_ERROR,
      note: `Internal error: exception in step. Please notify system administrator.`,
      stepOutput: data
    }, 'stepInstance.exceptionInStep()')

    // Tell the parent we've completed.
    const parentNodeGroup = this.#onComplete.nodeGroup
    const parentNodeId = this.#onComplete.nodeId ? this.#onComplete.nodeId : null
    const workerForShortcut = this.getWorker()
    const rv = await schedulerForThisNode.schedule_StepCompleted(tx, parentNodeGroup, parentNodeId, {
    // const queueName = Scheduler2.groupQueueName(this.#onComplete.nodeGroup)
    // const rv = await schedulerForThisNode.enqueue_StepCompletedZZ(queueName, {
      txId: this.#txId,
      stepId: this.#stepId,
      completionToken: this.#onComplete.completionToken,
    }, workerForShortcut)
    assert(rv === GO_BACK_AND_RELEASE_WORKER)
    return GO_BACK_AND_RELEASE_WORKER
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
    this.trace(JSON.stringify(object, '', 2), dbLogbook.LOG_SOURCE_PROGRESS_REPORT)
    await this.syncLogs()

    // Save the progressReport
    const tx = this.#transactionState
    const previousProgressReport = tx.getProgressReport()
    if (VERBOSE) console.log(`previousProgressReport=`, previousProgressReport)

    // See if the progress report has changed
    if (isEqual(previousProgressReport, object)) {
      // Progress report has not changed
      // console.log(`Progress report has not changed`)
    } else {
      // Progress report has changed
      await tx.delta(null, {
        progressReport: object
      }, 'stepInstance.progressReport()')
  
      // If this transaction requires progress reports via webhooks, start that now.
      if (requiresWebhookProgressReports(this.#metadata)) {
        if (VERBOSE) console.log(`Sending progress report by webhook`)
        await sendStatusByWebhook(tx.getOwner(), tx.getTxId(), this.#metadata.webhook, WEBHOOK_EVENT_PROGRESS)
      }
    }
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
  async retryLater(nameOfSwitch=null, sleepDuration=120, forceDeepSleep=false) {
    sleepDuration = Math.round(sleepDuration)
    if (VERBOSE) console.log(`StepInstance.retryLater(nameOfSwitch=${nameOfSwitch}, sleepDuration=${sleepDuration})`)
    this.#waitingForCompletionFunction = false

    const wakeTime = new Date(Date.now() + (sleepDuration * 1000))

    // Sync any buffered logs
    this.trace(`Retry in ${sleepDuration} seconds at ${wakeTime.toLocaleTimeString('PST')}`, dbLogbook.LOG_SOURCE_SYSTEM)
    await this.syncLogs()

    // Update the transaction status
    const tx = this.#transactionState
    await tx.delta(null, {
      status: STEP_SLEEPING,
      wakeSwitch: nameOfSwitch,
      sleepDuration: sleepDuration,
      wakeStepId: this.#stepId
    }, 'stepInstance.retryLater()')
    await tx.delta(this.#stepId, {
      status: STEP_SLEEPING,
    }, 'stepInstance.retryLater()')

    // During development, the server is often restarted during a sleep.
    // At this point in the code the wake time has been written to the database,
    // but the transaction state is only in local memory. Upon restart the cron
    // process will see the wake request, but the transaction state is not
    // available. Normally we only persist the transaction state for a long
    // sleep, but in this paranoid mode we'll save it to REDIS immediately.
    if (isDevelopmentMode()) {
      await PERSIST_TRANSACTION_STATE(tx)
    }


    // sleepDuration = 15

    const nodeGroup = this.#nodeGroup
    const txId = this.#txId
    const stepId = this.#stepId
    if (sleepDuration < DEEP_SLEEP_SECONDS && !forceDeepSleep) {
      console.log(`Step will NAP for ${sleepDuration} seconds till ${wakeTime.toLocaleTimeString('PST')}`)
      console.log(`    tx: ${txId}`)
      console.log(`  step: ${stepId}`)
      setTimeout(async() => {
        console.log(`Restarting step after a nap of ${sleepDuration} seconds.`)
        console.log(`    tx: ${txId}`)
        console.log(`  step: ${stepId}`)
        await schedulerForThisNode.enqueue_StepRestart(tx, nodeGroup, txId, stepId)
      }, sleepDuration * 1000)
    } else {
      // Long term sleep - will be woken by our cron process.
      // We need to persist the transaction state, so the step can pick it up when it retries.
      console.log(`Step will SLEEP for ${sleepDuration} seconds till ${wakeTime.toLocaleTimeString('PST')}`)
      console.log(`    tx: ${txId}`)
      console.log(`  step: ${stepId}`)
    }
    return GO_BACK_AND_RELEASE_WORKER
  }

  /**
   *
   * @param {*} name
   * @returns
   */
  async getSwitch(name) {
    if (VERBOSE) console.log(`StepInstance.getSwitch(${name})`)
    const tx = this.#transactionState
    const value = await Transaction.getSwitch(tx.getOwner(), this.#txId, name)
    return value
  }

  async setSwitch(name, value) {
    if (VERBOSE) console.log(`StepInstance.getSwitch(${name})`)
    const tx = this.#transactionState
    await Transaction.setSwitch(tx.getOwner(), this.#txId, name, value, false)
  }

  async getRetryCounter() {
    if (VERBOSE) console.log(`StepInstance.getRetryCounter()`)
    const tx = this.#transactionState
    const counter = tx.getRetryCounter()
    return counter
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

  /**
   * Use as `trace(message [, source])`
   */
  trace(...args) {
    const { message, source } = this._checkLogParams(args)
    const level = dbLogbook.LOG_LEVEL_TRACE
    assert(typeof(source) !== 'undefined')
    assert(typeof(message) !== 'undefined')
    this.#logBuffer.push({ level, source, message })
  }

  /**
   * Use as `debug(message [, source])`
   */
   debug(...args) {
    const { message, source } = this._checkLogParams(args)
    const level = dbLogbook.LOG_LEVEL_DEBUG
    assert(typeof(source) !== 'undefined')
    assert(typeof(message) !== 'undefined')
    this.#logBuffer.push({ level, source, message })
  }

  /**
   * Use as `info(message [, source])`
   */
  info(...args) {
    const { message, source } = this._checkLogParams(args)
    const level = dbLogbook.LOG_LEVEL_INFO
    assert(typeof(source) !== 'undefined')
    assert(typeof(message) !== 'undefined')
    this.#logBuffer.push({ level, source, message })
  }

  /**
   * Use as `warning(message [, source])`
   */
  warning(...args) {
    const { message, source } = this._checkLogParams(args)
    const level = dbLogbook.LOG_LEVEL_WARNING
    assert(typeof(source) !== 'undefined')
    assert(typeof(message) !== 'undefined')
    this.#logBuffer.push({ level, source, message })
  }

  /**
   * Use as `error(message [, source])`
   */
  error(...args) {
    const { message, source } = this._checkLogParams(args)
    const level = dbLogbook.LOG_LEVEL_ERROR
    assert(typeof(source) !== 'undefined')
    assert(typeof(message) !== 'undefined')
    this.#logBuffer.push({ level, source, message })
  }

  /**
   * Use as `fatal(message [, source])`
   */
  fatal(...args) {
    const { message, source } = this._checkLogParams(args)
    const level = dbLogbook.LOG_LEVEL_FATAL
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
      await dbLogbook.bulkLogging(this.#txId, this.#stepId, this.#logBuffer)
      this.#logBuffer = [ ]
    }
  }

  /**
   * Normally, a step's invoke function must call one of the completion functions (succeeded, failed, aborted, etc) before
   * returning. Pipelines however do not hang around waiting for their child steps to complete, and do not call any of
   * these completion functions. Calling this function prevents an error occurring.
   */
  stepWillNotCallCompletionFunction() {
    this.#waitingForCompletionFunction = false
  }

  /**
   * This internal function is called by a worker to check that a step that has just completed used one of
   * the instance completion functions to complete properly (i.e instance.succeeded, instance.failed, etc)
   * @returns True if the step called one of the step completion functions.
   */
  _correctlyFinishedStep() {
    return (this.#waitingForCompletionFunction === false)
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
    let source = this.#rollingBack ? dbLogbook.LOG_SOURCE_ROLLBACK : dbLogbook.LOG_SOURCE_INVOKE
    if (args.length > 0) {
      const lastArg = args[args.length - 1]
      switch (lastArg) {
        case dbLogbook.LOG_SOURCE_DEFINITION:
        case dbLogbook.LOG_SOURCE_INVOKE:
        case dbLogbook.LOG_SOURCE_ROLLBACK:
        case dbLogbook.LOG_SOURCE_EXCEPTION:
        case dbLogbook.LOG_SOURCE_DEFINITION:
        case dbLogbook.LOG_SOURCE_SYSTEM:
        case dbLogbook.LOG_SOURCE_PROGRESS_REPORT:
        case dbLogbook.LOG_SOURCE_UNKNOWN:
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
