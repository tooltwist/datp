/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import StepTypes from './StepTypeRegister'
import Step, { STEP_ABORTED, STEP_FAILED, STEP_INTERNAL_ERROR, STEP_RUNNING, STEP_SLEEPING, STEP_SUCCESS } from './Step'
import XData, { dataFromXDataOrObject } from "./XData"
import indentPrefix from '../lib/indentPrefix'
import assert from 'assert'
import TransactionState from './Scheduler2/TransactionState'
import { schedulerForThisNode } from '..'
import dbLogbook from '../database/dbLogbook'
import { DEEP_SLEEP_SECONDS, isDevelopmentMode } from '../datp-constants'
import isEqual  from 'lodash.isequal'
import { GO_BACK_AND_RELEASE_WORKER } from './Scheduler2/Worker2'
import { DEFINITION_MATERIALIZE_STEP_EVENT, STEP_DEFINITION, validateStandardObject } from './Scheduler2/eventValidation'
import { FLOW_VERBOSE } from './Scheduler2/queuing/redis-lua'
import { requiresWebhookProgressReports, sendStatusByWebhook, WEBHOOK_EVENT_PROGRESS } from './Scheduler2/webhooks/tryTheWebhook'
import { flow2Msg } from './Scheduler2/flowMsg'
import Scheduler2 from './Scheduler2/Scheduler2'
import { luaEnqueue_startStep } from './Scheduler2/queuing/redis-startStep'
import { luaGetSwitch } from './Scheduler2/queuing/redis-retry'
require('colors')

const VERBOSE = 0
const VERBOSE_16aug22 = 0


export default class StepInstance {
  #worker

  #transactionState

  #txId
  // #nodeGroup  // Cluster of redundant servers, performing the same role
  #nodeId     // Specific server
  #stepId
  #stepDefinition
  #txdata // data passed to the step
  #metadata
  #fullSequence
  #vogPath

  #rollingBack
  #logBuffer

  #level
  #indent
  // #flowIndex
  #f2i

  // What to do after the step completes {nodeId, completionToken}
  // #onComplete

  #waitingForCompletionFunction

  constructor() {
    // console.log(`StepInstance.materialize()`, options)
    this.#worker = null

    // this.#nodeGroup = null
    this.#nodeId = null
    // Definition
    this.#stepId = null
    this.#stepDefinition = { stepType: null}
    // Transaction data
    this.#txdata = null
    // Private data for use within step
    // this.privateData = { }
    // Debug stuff
    this.#level = 0
    // this.#flowIndex = -1
    this.#f2i = -1
    this.#fullSequence = ''
    this.#vogPath = ''
    // Step completion handling
    // this.#onComplete = null

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


  async materialize(tx, event, worker) {
    // console.log(``)
    // console.log(`StepInstance.materialize event=`, event)
    // console.log(`  typeof(tx)=`, typeof(tx))
    // console.log(`event.f2i=`, event.f2i)


    assert(tx instanceof TransactionState)

    validateStandardObject('materialize(), event', event, DEFINITION_MATERIALIZE_STEP_EVENT)

    this.#worker = worker
    this.#transactionState = tx

    const stepId = tx.getF2stepId(event.f2i)
    const stepData = tx.stepData(stepId)
    validateStandardObject('materialize() step', stepData, STEP_DEFINITION)

    assert (typeof(stepData.fullSequence) === 'string')
    assert (typeof(stepData.vogPath) === 'string')
    assert (typeof(stepData.stepDefinition) !== 'undefined')
    assert (typeof(stepData.stepInput) === 'undefined')
    assert (typeof(stepData.level) === 'number')

    const metadata = tx.vog_getMetadata()

    this.#txId = tx.getTxId()
    // this.#nodeGroup = Scheduler2.getNodeGroup()
    this.#nodeId = schedulerForThisNode.getNodeId()
    this.#stepId = stepId //ZZZ Remove this
    this.#txdata = new XData(event.data)
    this.#metadata = metadata
    this.#level = stepData.level
    // this.#flowIndex = event.flowIndex
    this.#f2i = event.f2i
    this.#fullSequence = stepData.fullSequence
    this.#vogPath = stepData.vogPath

    // this.#onComplete = event.onComplete
    // this.#completionToken = event.completionToken

    // Log this step being materialized
    // this.#logbook = event.logbook
    //ZZZZZ
    // this.#logbook = new Logbook.cls({
    //   transactionId: this.#txId,
    //   description: `Pipeline logbook`
    // })
    // this.trace(`Materialize stepInstance ${this.#stepId}`, dbLogbook.LOG_SOURCE_SYSTEM)


    // Prepare an indent string to prepend to messages
    this.#indent = indentPrefix(this.#level)

    // const f2 = tx.vf2_getF2(this.#f2i)
    // f2.ts2 = Date.now()

    /*
     *  Load the definition of the step (which is probably a pipeline)
     */
    this.#stepDefinition = stepData.stepDefinition

    // Instantiate the step object
    const stepType = this.#stepDefinition.stepType
    this.stepObject = await StepTypes.factory(stepType, this.#stepDefinition)
    if (!(this.stepObject instanceof Step)) {
      throw Error(`Factory for ${stepType} did not return a step`)
    }
  }//- materialize

  // This function gets the transaction state object. We want to keep this unpublished
  // and hard to notice - we don't want developers mucking with the internals of DATP.
  _7agghtstrajj_37(txId) {
    if (this.#transactionState.getTxId() !== txId) {
      console.log(`Serious Error: Getting transaction that is not my transaction!!!!`.red)
      console.log(`this.#transactionState.txId=`, this.#transactionState.getTxId() )
      console.trace(`txId=`, txId)
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
    const nodeGroup = schedulerForThisNode.getNodeGroup()
    return nodeGroup
  }

  getNodeId() {
    return this.#nodeId
  }

  getStepId() {
    return this.#stepId
  }

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

  // vog_getFlowIndex() {
  //   return this.#flowIndex
  // }

  vog_getF2i() {
    return this.#f2i
  }

  getVogPath() {
    return this.#vogPath
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
    const tx = this.#transactionState
    if (FLOW_VERBOSE) flow2Msg(tx, `<<< stepInstance.succeeded()`)
    this.#waitingForCompletionFunction = false

    const myStepOutput = this._sanitizedOutput(stepOutput)
    if (VERBOSE) console.log(`succeeded: step out is `.magenta, myStepOutput)
    // if (VERBOSE > 1) console.log(`this.#onComplete=`, this.#onComplete)

    // Sync any buffered logs
    if (VERBOSE) this.trace(`Step succeeded - ${note}`, dbLogbook.LOG_SOURCE_SYSTEM)
    await this.syncLogs()

    // Quick sanity check - make sure this step is actually running, and has not already exited.
//ZZZZZ YARP use f2i!!!!
    const stepData = tx.stepData(this.#stepId)
    if (stepData.status !== STEP_RUNNING) {
      //ZZZ Write to the log
      const description = `Step status is not ${STEP_RUNNING}. Has this step already exited? [${this.#stepId}]`
      console.log(description)
      throw new Error(description)
    }

    // Not sleeping any more
    // tx.clearRetryValues()

    // Persist the result and new status
    // tx.vog_setStatusToSuceeded()
    console.log(`YAMYAP: succeeded calling delta`.brightRed)
    await tx.delta(this.#stepId, {
      status: STEP_SUCCESS,
    }, 'stepInstance.succeeded()')

    // Update flow2
    const f2 = tx.vf2_getF2(this.#f2i)
    f2.status = STEP_SUCCESS
    f2.note = note
    f2.output = stepOutput
    f2.ts3 = Date.now()

    const nextF2i = this.#f2i + 1
    const completionToken = null
    const workerForShortcut = this.#worker
    const rv = await schedulerForThisNode.enqueue_StepCompleted(tx, nextF2i, completionToken, workerForShortcut)
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
    const tx = this.#transactionState
    if (FLOW_VERBOSE) flow2Msg(tx, `<<< stepInstance.aborted()`)
    this.#waitingForCompletionFunction = false

    // Sync any buffered logs
    this.trace(`Step aborted`, dbLogbook.LOG_SOURCE_SYSTEM)
    await this.syncLogs()

    const myStepOutput = this._sanitizedOutput(stepOutput)
    if (VERBOSE) console.log(`aborted: step out is `.magenta, myStepOutput)

    // Quick sanity check - make sure this step is actually running, and has not already exited.
    const stepData = tx.stepData(this.#stepId)
    if (stepData.status !== STEP_RUNNING) {
      //ZZZ Write to the log
      const description = `Step status is not ${STEP_RUNNING}. Has this step already exited? [${this.#stepId}]`
      console.log(description)
      throw new Error(description)
    }

    // Not sleeping any more
    // tx.clearRetryValues()

    // Persist the result and new status
    await tx.delta(this.#stepId, {
      status: STEP_ABORTED,
    }, 'stepInstance.aborted()')

    // tx.vog_flowRecordStep_complete(this.#flowIndex, STEP_ABORTED, note, myStepOutput)

    // Update flow2
    const f2 = tx.vf2_getF2(this.#f2i)
    f2.status = STEP_ABORTED
    f2.note = note
    f2.output = stepOutput
    f2.ts3 = Date.now()

    const nextF2i = this.#f2i + 1
    const workerForShortcut = this.#worker
    const completionToken = null
    const rv = await schedulerForThisNode.enqueue_StepCompleted(tx, nextF2i, completionToken, workerForShortcut)
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
    const tx = this.#transactionState
    if (FLOW_VERBOSE) flow2Msg(tx, `<<< stepInstance.failed()`)
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
    const stepData = tx.stepData(this.#stepId)
    if (stepData.status !== STEP_RUNNING) {
      //ZZZ Write to the log
      const description = `Step status is not ${STEP_RUNNING}. Has this step already exited? [${this.#stepId}]`
      console.log(description)
      throw new Error(description)
    }

    // Not sleeping any more
    // tx.clearRetryValues()

    // Persist the result and new status
    // await tx.delta(null, {
    //   progressReport: {}
    // }, 'stepInstance.failed()')
    await tx.delta(this.#stepId, {
      status: STEP_FAILED,
    }, 'stepInstance.failed()')

    // Update flow2
    const f2 = tx.vf2_getF2(this.#f2i)
    f2.status = STEP_FAILED
    f2.note = note
    f2.output = stepOutput
    f2.ts3 = Date.now()

    // Tell the parent we've completed.
    const nextF2i = this.#f2i + 1
    const completionToken = null
    const workerForShortcut = this.#worker
    const rv = await schedulerForThisNode.enqueue_StepCompleted(tx, nextF2i, completionToken, workerForShortcut)
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
    const tx = this.#transactionState
    if (FLOW_VERBOSE) flow2Msg(`<<< stepInstance.badDefinition()`)
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
    const data = {
      error: `Internal error: bad pipeline definition. Please notify system administrator.`,
      transactionId: this.#txId,
      stepId: this.#stepId
    }

    // Not sleeping any more
    // tx.clearRetryValues()

    // Save the step status
    const note = `Internal error: bad pipeline definition. Please notify system administrator.`
    await tx.delta(this.#stepId, {
      status: STEP_INTERNAL_ERROR,
    }, 'stepInstance.badDefinition()')

    // Update flow2
    const f2 = tx.vf2_getF2(this.#f2i)
    f2.status = STEP_INTERNAL_ERROR
    f2.note = note
    f2.output = data
    f2.ts3 = Date.now()

    // Tell the parent we've completed.
    const nextF2i = this.#f2i + 1
    const completionToken = null
    const workerForShortcut = this.getWorker()
    const rv = await schedulerForThisNode.enqueue_StepCompleted(tx, nextF2i, completionToken, workerForShortcut)
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
    const tx = this.#transactionState
    if (FLOW_VERBOSE) flow2Msg(tx, `<<< stepInstance.exceptionInStep()`)
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

    // Trim down the stacktrace and save it
    let trace = e.stack
    let pos = trace.indexOf('    at processTicksAndRejections')
    if (pos >= 0) {
      trace = trace.substring(0, pos)
    }
    // await this.artifact('exception', trace)

    // Finish the step
    const data = {
      error: `Internal error: exception in step. Please notify system administrator.`,
      transactionId: this.#txId,
      stepId: this.#stepId
    }

    // Not sleeping any more
    // tx.clearRetryValues()

    // Update the status
    const note = `Internal error: exception in step. Please notify system administrator.`
    await tx.delta(this.#stepId, {
      status: STEP_INTERNAL_ERROR,
    }, 'stepInstance.exceptionInStep()')

    // Update flow2
    const f2 = tx.vf2_getF2(this.#f2i)
    f2.status = STEP_INTERNAL_ERROR
    f2.note = note
    f2.output = data
    f2.ts3 = Date.now()

    // Tell the parent we've completed.
    console.log(`----------------------`.bgMagenta)
    console.log(`In exceptionInStep`.bgMagenta)
    // console.log(`tx.pretty()=`, tx.pretty())
    console.log(`this.#f2i + 1=`, this.#f2i + 1)


    const nextF2i = this.#f2i + 1
    const completionToken = null
    const workerForShortcut = this.#worker
    const rv = await schedulerForThisNode.enqueue_StepCompleted(tx, nextF2i, completionToken, workerForShortcut)
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
      // await tx.delta(null, {
      //   progressReport: object
      // }, 'stepInstance.progressReport()')
      tx.vog_setProgressReport(object)
  
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
  async retryLater(nameOfSwitch=null, sleepDuration=0, forceDeepSleep=false) {
    sleepDuration = Math.round(sleepDuration)
    if (VERBOSE) console.log(`StepInstance.retryLater(nameOfSwitch=${nameOfSwitch}, sleepDuration=${sleepDuration} seconds)`)
    this.#waitingForCompletionFunction = false

    // const txState = this.#transactionState.asObject()
    // console.log(`txState=`, txState)
    // console.log(`this.#txId=`, this.#txId)
    // const parentNodeGroup = schedulerForThisNode.getNodeGroup()
    // console.log(`parentNodeGroup=`, parentNodeGroup)
    // const metadata = txState.transactionData.metadata
    // console.log(`metadata=`, metadata)
    // console.log(`this.#txdata=`, this.#txdata.getJson())
    // console.log(`this.#f2i=`, this.#f2i)

    const DEFAULT_SLEEP_FOR_SWITCH = 2 * 60 * 60 // 2 hours
    const DEFAULT_SLEEP_FOR_RETRY = 5 * 60 // 5 minutes


    // Are we waiting for a switch?
    if (nameOfSwitch) {
      nameOfSwitch = nameOfSwitch.trim()
      // this.#transactionState.vog_setWakeSwitch(nameOfSwitch)
      if (!sleepDuration) {
        sleepDuration = DEFAULT_SLEEP_FOR_SWITCH
      }
      if (VERBOSE) console.log(`WILL WAIT FOR SWITCH ${nameOfSwitch} or for ${sleepDuration} seconds`)
    } else {
      if (!sleepDuration) {
        sleepDuration = DEFAULT_SLEEP_FOR_RETRY
        if (VERBOSE) console.log(`WILL SLEEP FOR ${nameOfSwitch} seconds`)
      }
    }

    /*
     *  This event will ultimately be handled by Worker2.processEvent(), which gets
     *  fed in various ways:
     * 
     *    1. Via memory queues, loaded by Scheduler2.enqueue_StartStepOnThisNode().
     *    2. When Scheduler2.enqueue_StartStepOnThisNode() calls the function directly by
     *       reusing the current worker thread.
     *    3. Via REDIS queues, fed by luaEnqueue_startStep()
     *    4. From here

     *  1. Scheduler2.enqueue_StartStepOnThisNode() either via memory queues, or by reusing
     *  the current worker thread.
     * 
     */
    const event = {
      eventType: Scheduler2.STEP_START_EVENT,
      txId: this.#txId,
      parentNodeGroup: schedulerForThisNode.getNodeGroup(),
      stepDefinition: this.#stepDefinition,
      metadata: this.#transactionState.vog_getMetadata(),
      data: this.#txdata.getData(),
      f2i: this.#f2i,
    }
    // console.log(`event=`, event)


    const delayBeforeQueueing = sleepDuration * 1000

    // Sync any buffered logs
    const wakeTime = new Date(Date.now() + (sleepDuration * 1000))
    this.trace(`Retry in ${sleepDuration} seconds at ${wakeTime.toLocaleTimeString('PST')}`, dbLogbook.LOG_SOURCE_SYSTEM)
    await this.syncLogs()

    const result = await luaEnqueue_startStep('retry-step', this.#transactionState, event, delayBeforeQueueing, nameOfSwitch)
    // console.log(`StepInstance.retryLater(): luaEnqueue_startStep() result=`, result)
    return GO_BACK_AND_RELEASE_WORKER
  }//- retryLater

  /**
   *
   * @param {*} switchName
   * @returns
   */
  async getSwitch(switchName) {
    if (VERBOSE) console.log(`StepInstance.getSwitch(${switchName})`)
    // Remove any 'unacknowledged' marker from the switch (! prefix)
    // See README-switches.md for details.
    const acknowledgeValue = true
    const result = await luaGetSwitch(this.#txId, switchName, acknowledgeValue)
    return result
  }

  async setSwitch(name, value) {
    if (VERBOSE) console.log(`StepInstance.getSwitch(${name})`)
    return this.#transactionState.setSwitch(name, value)
  }

  async getRetryCounter() {
    if (VERBOSE) console.log(`StepInstance.getRetryCounter()`)
    const tx = this.#transactionState
    const counter = tx.getRetryCounter()
    return counter
  }

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
    this.#logBuffer.push({ level, source, message, ts: Date.now(), fullSequence: this.#fullSequence })
    // console.log(`yomp ${message}`)
  }

  /**
   * Use as `debug(message [, source])`
   */
   debug(...args) {
    const { message, source } = this._checkLogParams(args)
    const level = dbLogbook.LOG_LEVEL_DEBUG
    assert(typeof(source) !== 'undefined')
    assert(typeof(message) !== 'undefined')
    this.#logBuffer.push({ level, source, message, ts: Date.now(), fullSequence: this.#fullSequence })

// console.log(`yomp ${message}`)
  }

  /**
   * Use as `info(message [, source])`
   */
  info(...args) {
    const { message, source } = this._checkLogParams(args)
    const level = dbLogbook.LOG_LEVEL_INFO
    assert(typeof(source) !== 'undefined')
    assert(typeof(message) !== 'undefined')
    this.#logBuffer.push({ level, source, message, ts: Date.now(), fullSequence: this.#fullSequence })
    // console.log(`yomp ${message}`)
  }

  /**
   * Use as `warning(message [, source])`
   */
  warning(...args) {
    const { message, source } = this._checkLogParams(args)
    const level = dbLogbook.LOG_LEVEL_WARNING
    assert(typeof(source) !== 'undefined')
    assert(typeof(message) !== 'undefined')
    this.#logBuffer.push({ level, source, message, ts: Date.now(), fullSequence: this.#fullSequence })
    // console.log(`yomp ${message}`)
  }

  /**
   * Use as `error(message [, source])`
   */
  error(...args) {
    const { message, source } = this._checkLogParams(args)
    const level = dbLogbook.LOG_LEVEL_ERROR
    assert(typeof(source) !== 'undefined')
    assert(typeof(message) !== 'undefined')
    this.#logBuffer.push({ level, source, message, ts: Date.now(), fullSequence: this.#fullSequence })
    // console.log(`yomp ${message}`)
  }

  /**
   * Use as `fatal(message [, source])`
   */
  fatal(...args) {
    const { message, source } = this._checkLogParams(args)
    const level = dbLogbook.LOG_LEVEL_FATAL
    assert(typeof(source) !== 'undefined')
    assert(typeof(message) !== 'undefined')
    this.#logBuffer.push({ level, source, message, ts: Date.now(), fullSequence: this.#fullSequence })
    // console.log(`yomp ${message}`)
  }
  // addLog(level, source, message) {
  //   this.#logBuffer.push({ level, source, message })
  // }

  async syncLogs() {
    // console.log(`\n\n\n\n\n********************\n\n\nsyncLogs()`)
    // console.log(`---- yomp syncem`)
    // for (const event of this.#logBuffer) { console.log(`-- ${event.message}`) }
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
    let s = `${this.#stepDefinition.stepType}, ${this.#stepId}, ${this.#f2i}`
    // if (this.#parentStepId) {
    //   s += `, parent=${this.#parentStepId}`
    // } else {
    //   s += `, no parent`
    // }
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
