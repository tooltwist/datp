/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import StepInstance from "../StepInstance"
import XData from "../XData"
import CallbackRegister from "./CallbackRegister"
import Scheduler2 from "./Scheduler2"
import assert from 'assert'
import { STEP_ABORTED, STEP_FAILED, STEP_INTERNAL_ERROR, STEP_QUEUED, STEP_RUNNING, STEP_SLEEPING, STEP_SUCCESS, STEP_TIMEOUT } from "../Step"
import { schedulerForThisNode } from "../.."
import { CHECK_FOR_BLOCKING_WORKERS_TIMEOUT, INCLUDE_STATE_IN_NODE_HOPPING_EVENTS, SHORTCUT_STEP_START } from "../../datp-constants"
import TransactionState, { F2ATTR_CALLBACK, F2ATTR_NODEGROUP, F2ATTR_NODEID, F2ATTR_STEPID, F2_VERBOSE } from "./TransactionState"
import me from "../../lib/me"
import { DEFINITION_PROCESS_STEP_START_EVENT, FLOW_DEFINITION, STEP_DEFINITION, validateStandardObject } from "./eventValidation"
import { FLOW_VERBOSE } from "./queuing/redis-lua"
import { flow2Msg, flowMsg } from "./flowMsg"

const VERBOSE = 0
const VERBOSE_16aug22 = 0
const TX_FROM_EVENT = true

require('colors')

export const GO_BACK_AND_RELEASE_WORKER = 'goback'

export default class Worker2 {
  #workerId
  #state
  #correctlyFinishedStep
  #reuseCounter // With shortcuts, we continue to use the same worker.
  #recentTxId // Most recent transaction ID

  static STANDBY = 'standby'
  static WAITING = 'waiting'
  static INUSE = 'inuse'
  static SHUTDOWN = 'shutdown'

  constructor(workerId) {
    // console.log(`Worker2.constructor()`)
    this.#workerId = workerId
    this.#state = Worker2.WAITING
    // this.#correctlyFinishedStep = false
    this.#reuseCounter = 0
    this.#recentTxId = null
    if (VERBOSE) console.log(`[worker ${this.#workerId} => WAITING]`.bgRed.white)
  }

  getId() {
    return this.#workerId
  }

  getRecentTxId() {
    return this.#recentTxId
  }

  /**
   * Set the work thread to "IN USE" before using setImmediate()
   * to call processEvent below. This assures against the worker
   * getting allocated twice if setImmediate is slow to run processEvent.
   */
  setInUse() {
    this.#state = Worker2.INUSE
  }

  async processEvent(txState, event) {
    const typeStr = event.eventType ? ` (${event.eventType})` : ''
    if (FLOW_VERBOSE) console.log(`[worker ${this.#workerId} processing event${typeStr}]`.gray)
    // console.log(`event=`, JSON.stringify(event, '', 2))

    // Check that the event includes the transaction state
    assert (!event.txState)
    //   // This is a malformed event, that does not contain transaction state information.
    //   console.log(`Malformed event, with no state information, will be ignored.`)
    //   this.#state = Worker2.WAITING
    //   return
    // }
    // assert(event.txState)
    // console.log(`event.txState=`, event.txState)
    // console.log(`event=`, event)
    // const txState = event.txState

    // Process this event
    this.#state = Worker2.INUSE
    this.#recentTxId = event.txId
    // if (VERBOSE) console.log(`Worker ${this.#workerId} => INUSE`.bgRed.white)

    try {

      // Decide how to handle this event.
      const eventType = event.eventType
      if (F2_VERBOSE) console.log(`F2: EVENT ${event.eventType}   (f2i=${event.f2i})`.bgMagenta.white)
      if (F2_VERBOSE) txState.dumpFlow2(event.f2i)
      // delete event.eventType
      switch (eventType) {
        case Scheduler2.NULL_EVENT:
          // Ignore this. We send null events when we are shutting down, so these workers
          // stop blocking on the queue and get a chance to see they are being shut down.
          break

        case Scheduler2.PIPELINE_START_EVENT:
          delete event.txState
          const rv = await this.processEvent_StepStart(txState, event)
          assert(rv === GO_BACK_AND_RELEASE_WORKER)
          break

        case Scheduler2.STEP_START_EVENT:
          delete event.txState
          const rv2 = await this.processEvent_StepStart(txState, event)
          assert(rv2 === GO_BACK_AND_RELEASE_WORKER)
          break
      
        case Scheduler2.STEP_COMPLETED_EVENT:
          delete event.txState
          const rv3 = await this.processEvent_StepCompleted(txState, event)
          assert(rv3 === GO_BACK_AND_RELEASE_WORKER)
          break

        // case Scheduler2.TRANSACTION_CHANGE_EVENT:
        //   const rv3 = await this.processEvent_TransactionChanged(event)
        //   assert(rv3 === GO_BACK_AND_RELEASE_WORKER)
        //   break

        // case Scheduler2.TRANSACTION_COMPLETED_EVENT:
        //   const rv4 = await this.processEvent_TransactionCompleted(event)
        //   assert(rv4 === GO_BACK_AND_RELEASE_WORKER)
        //   break

        default:
          throw new Error(`Unknown event type ${eventType}`)
      }

      // Was the state changed to shutdown while we were off processing this event?
      if (this.#state === Worker2.SHUTDOWN) {
        this.enterStandbyState()
        return
      }
    } catch (e) {
      console.log(e.message)
      console.log(e.stack)
    }

    // Not shutting down. Go get another event!
    // if (VERBOSE) console.log(`[worker ${this.#workerId} => WAITING]`.bgRed.white)
    this.#state = Worker2.WAITING

    // Nothing happens after here - this function was called via setImmediate().
  }//- processEvent

  stop() {
    // This worker won't notice this yet, but will after it finishes
    // blocking on it's read of the event queue.
    if (VERBOSE) console.log(`[worker ${this.#workerId} => SHUTDOWN]`.bgRed.white)
    this.#state = Worker2.SHUTDOWN
  }

  enterStandbyState() {
    if (VERBOSE) console.log(`[worker ${this.#workerId} => STANDBY]`.bgRed.white)
    this.#state = Worker2.STANDBY
  }

  getState() {
    return this.#state
  }

  /**
   *
   * @param {XData} event
   */
  async processEvent_StepStart(txState, event) {
    if (FLOW_VERBOSE) flow2Msg(txState, `${me()}: Worker.processEvent_StepStart`)
    // if (VERBOSE) console.log(`Worker2.processEvent_StepStart()`)

    const f2 = txState.vf2_getF2(event.f2i)
    const stepId = f2[F2ATTR_STEPID]
    const stepData = txState.stepData(stepId)
    validateStandardObject('processEvent_StepStart event', event, DEFINITION_PROCESS_STEP_START_EVENT)
    // validateStandardObject('processEvent_StepStart flow', flow, FLOW_DEFINITION)
    validateStandardObject('processEvent_StepStart step', stepData, STEP_DEFINITION)

    if (FLOW_VERBOSE > 1) console.log(`>>> processEvent_StepStart() `.brightBlue + txState.vog_flowPath(flowIndex).gray)
    // console.log(`event=`, event)

    if (VERBOSE > 1) console.log(`${me()}: event=`, JSON.stringify(event, '', 2))
    // const zzz = JSON.parse(event.txState)
    // console.log(`txState=`, JSON.stringify(zzz, '', 2))

    // Update the timestamp
    const myF2 = txState.vf2_getF2(event.f2i)
    myF2.ts2 = Date.now()
    myF2[F2ATTR_NODEID] = schedulerForThisNode.getNodeId()
    myF2[F2ATTR_NODEGROUP] = schedulerForThisNode.getNodeGroup()

    
    
    this.#reuseCounter++
//    if (this.#reuseCounter > 1) console.log(`${this.#reuseCounter}: REUSING WORKER ${this.#workerId} (${this.#reuseCounter})`)

    // const txId = txState.getTxId()
    try {
      // assert(typeof(event.txId) === 'string')
      // assert(typeof(event.stepId) === 'string')


      // console.log(`MY NICE NEW TXSTATE=`, txState.asObject())
      // if (!txState) {
      //   // This should be flagged as a serious system error.ZZZZZ
      //   this.#reuseCounter--
      //   const msg = `SERIOUS ERROR: stepStart event for unknown transaction ${txId}. Step ID is ${stepId}. Where did this come from?`
      //   console.log(msg)
      //   throw new Error(msg)
      // }
      if (VERBOSE_16aug22) txState.xoxYarp('Worker starting step', stepId)
      // const txData = txState.transactionData()
      // console.log(`txData=`, txData)
      // if (!SHORTCUT_STEP_START) {
      //   assert(stepData.status === STEP_QUEUED)
      // }
      // assert(flow.vogPath)

      // console.log(`Starting step with txData ${txState.pretty()}`.cyan)
      const metadata = txState.vog_getMetadata()
      const trace = (typeof(metadata.traceLevel) === 'number') && metadata.traceLevel > 0
      if (trace || VERBOSE) console.log(`${this.#reuseCounter}: >>> processEvent_StepStart()`.brightGreen)
      if (trace || VERBOSE > 1) console.log(`event=`, event)
      if (trace || VERBOSE > 1) console.log(`stepDefinition is ${JSON.stringify(stepData.stepDefinition, '', 2)}`)

      /*
       * See if this is a test step.
       */
      if (stepData.stepDefinition === 'util.ping3') {

        // Boounce back via STEP_COMPLETION_EVENT, after creating fake transaction data.
        const description = 'processEvent_StepStart() - util.ping3 - returning via STEP_COMPLETED, without processing step'
        if (trace || VERBOSE) console.log(`description=`, description.bgBlue.white)

        // await txState.delta(stepId, {
        //   stepId,
        //   status: STEP_SUCCESS,
        //   // stepOutput: {
        //   //   happy: 'dayz',
        //   //   description
        //   // }
        // }, 'Worker2.processEvent_StepStart()')
        txState.vog_setStepStatus(stepId, STEP_SUCCESS)
        
        // const parentNodeGroup = stepData.onComplete.nodeGroup
        // const completionEvent = {
        //   txId: event.txId,
        //   stepId: event.stepId,
        //   // completionToken: stepData.onComplete.completionToken
        // }
        const nextF2i = event.f2i + 1
        const completionToken = null
        const workerForShortcut = this
        const rv = await schedulerForThisNode.enqueue_StepCompleted(txState, this.flowIndex, nextF2i, completionToken, workerForShortcut)
        assert(rv === GO_BACK_AND_RELEASE_WORKER)
        this.#reuseCounter--
        return GO_BACK_AND_RELEASE_WORKER
      }//- ping3

      /*
       *  Not a test step
       */
      // Create the StepInstance object, to provide context to the step when it runs.
      event.nodeGroup = schedulerForThisNode.getNodeGroup()
      event.nodeId = schedulerForThisNode.getNodeId()
      const instance = new StepInstance()
      if (VERBOSE > 1) console.log(`${this.#reuseCounter}: ------------------------------ ${event.nodeGroup} materialize ${txState.getTxId()}`)
      await instance.materialize(txState, event, this)
      if (trace || VERBOSE) console.log(`${this.#reuseCounter}: >>>>>>>>>> >>>>>>>>>> >>>>>>>>>> START [${instance.getStepType()}] ${instance.getStepId()}`)
      if (trace || VERBOSE > 1) console.log(`stepData=`, txState.stepData(stepId))

      // await txState.delta(stepId, {
      //   // stepId,
      //   status: STEP_RUNNING
      // }, 'Worker2.processEvent_StepStart()')

      txState.vog_setStepStatus(stepId, STEP_RUNNING)


      /*
       *  Start the step - we don't wait for it to complete
       */

      // console.log(`txState.asObject()=`, txState.asObject())
      txState.vog_flowRecordStep_invoked(stepId)

      const stepObject = instance.getStepObject()

      // const hackSource = 'system' // Not sure why, but using dbLogbook.LOG_SOURCE_SYSTEM causes a compile error
      // instance.trace(`Invoke step ${instance.getStepId()}`, hackSource)
      const stepDesc = (typeof(stepData.stepDefinition) === 'string') ? `Pipeline ${stepData.stepDefinition}` : `Step ${stepData.stepDefinition.stepType}`
      // instance.trace(`Invoked: ${stepDesc}`)
      if (trace || VERBOSE) instance.trace(stepDesc)
      if (trace || VERBOSE) instance.trace(`Run on ${schedulerForThisNode.getNodeGroup()} / ${schedulerForThisNode.getNodeId()}`)
      // instance.trace(`deltaCounter=${txState.getDeltaCounter()}`)
      await instance.syncLogs()

      // Start the step in the background, immediately
      // setTimeout(async () => {
        try {
          // Check for blocked step.
          let blockTimer = null
          if (CHECK_FOR_BLOCKING_WORKERS_TIMEOUT > 0) {
            blockTimer = setTimeout(() => {
              blockTimer = null
              console.log(`Step timeout after ${CHECK_FOR_BLOCKING_WORKERS_TIMEOUT} seconds. stepType=${instance.getStepType()}, txId=${instance.getTxId()},  stepId=${instance.getStepId()}`)
            }, CHECK_FOR_BLOCKING_WORKERS_TIMEOUT * 1000)
          }

          if (FLOW_VERBOSE) flow2Msg(txState, `[${this.#reuseCounter}: worker ${this.#workerId} STARTING STEP ${stepId} ]`)
          const rv = await stepObject.invoke(instance) // Provided by the step implementation
          // if (FLOW_VERBOSE) flow2Msg(txState, `[${this.#reuseCounter}: worker ${this.#workerId} RETURNED FROM ${stepId}] (and any nested steps)`)

          // Check that the step used the stepInstance functions to finalize the step.
          if (!instance._correctlyFinishedStep()) {
            console.log(`Application error: Step did not complete by calling instance.succeeded(), instance.failed() etc. stepType=${instance.getStepType()}, txId=${instance.getTxId()},  stepId=${instance.getStepId()}`)
          }
          // Return the worker state back to WAITING.
          if (this.#reuseCounter === 1) {
            // We don't do this if we have nested calls.
            this.#state = Worker2.WAITING
          }
          // Reset the step timeout
          if (blockTimer) {
            clearTimeout(blockTimer)
          }
          assert(rv === GO_BACK_AND_RELEASE_WORKER)
          this.#reuseCounter--
          return GO_BACK_AND_RELEASE_WORKER
        } catch (e) {
          if (VERBOSE) console.log(`[${this.#reuseCounter}: worker ${this.#workerId} EXCEPTION IN STEP ${stepId}]`.green, e)
          this.#reuseCounter--
          return await instance.exceptionInStep(null, e)
        }
      // }, 0)

    } catch (e) {
      this.#reuseCounter--
      console.log(`DATP internal error: processEvent_StepStart:`, e)
      throw e
    }
  }//- processEvent_StepStart

  /**
   * This event gets triggered by the stepInstance functions, when processing
   * of a step has completed (irrespective of success, failure or sleeping, etc).
   *
   * The purpose of this function is to call the completion handler
   * @param {object} event
   */
  async processEvent_StepCompleted(tx, event) {
    // console.log(`----------------------`.bgYellow)
    if (FLOW_VERBOSE > 1) console.log(`<<< Worker.processEvent_StepCompleted()`.cyan + ' ' + tx.vog_flowPath(event.flowIndex).gray)

    // console.log(`tx.pretty()=`, tx.pretty())
    // console.log(`tx=`, tx)
    // console.log(`event=`, event)


    try {
      const worker = this
      // const txId = event.txId
      // const stepId = event.stepId
      // const tx = await extractTransactionStateFromEvent(event)

      assert(event.eventType === Scheduler2.STEP_COMPLETED_EVENT)
      assert(typeof(event.txId) === 'string')
      assert(typeof(event.flowIndex) === 'number')
      assert(typeof(event.f2i) === 'number')

      const status = tx.vf2_getStatus(event.f2i)
      // console.log(`status =`.bgBrightRed, status)
      if (
        status === STEP_ABORTED
        || status === STEP_FAILED
        || status === STEP_INTERNAL_ERROR
        || status === STEP_SLEEPING
        || status === STEP_SUCCESS
        || status === STEP_RUNNING
        || status === STEP_TIMEOUT
      ) {
        // OK
      } else {
        console.log(`\n\nXXXXXXXXXXXXX           XXXXXXXXXXXXX           XXXXXXXXXXXXX           XXXXXXXXXXXXX           XXXXXXXXXXXXX`)
        console.log(`\n\nXXXXXXXXXXXXX           XXXXXXXXXXXXX           XXXXXXXXXXXXX           XXXXXXXXXXXXX           XXXXXXXXXXXXX`)
        console.log(`\n\nXXXXXXXXXXXXX           XXXXXXXXXXXXX           XXXXXXXXXXXXX           XXXXXXXXXXXXX           XXXXXXXXXXXXX`)
        console.log(`Invalid completion status: ${status}`)
        console.log(`schedulerForThisNode.getNodeGroup()=`, schedulerForThisNode.getNodeGroup())
        console.log(`schedulerForThisNode.getNodeId()=`, schedulerForThisNode.getNodeId())
        console.log(`event=`, event)
        console.log(`tx=`, JSON.stringify(tx.asObject(), '', 2))
        console.log(`broken1.`, new Error().stack)
        console.log(`\n\nXXXXXXXXXXXXX           XXXXXXXXXXXXX           XXXXXXXXXXXXX           XXXXXXXXXXXXX           XXXXXXXXXXXXX`)
        console.log(`\n\nXXXXXXXXXXXXX           XXXXXXXXXXXXX           XXXXXXXXXXXXX           XXXXXXXXXXXXX           XXXXXXXXXXXXX`)
        console.log(`\n\nXXXXXXXXXXXXX           XXXXXXXXXXXXX           XXXXXXXXXXXXX           XXXXXXXXXXXXX           XXXXXXXXXXXXX`)
        process.exit(1)//ZZZZZ
      }

      // Check the completionToken is correct
      // console.log(`Checking completionToken (${completionToken} vs ${stepData.onComplete.completionToken})`)
      // const completionToken = event.completionToken
      // if (completionToken !== stepData.onComplete.completionToken) {
      //   throw Error(`Invalid completionToken`)
      // }


      // Update the timestamp
      console.log(`event.f2i=`.red, event.f2i)
      const f2 = tx.vf2_getF2(event.f2i)
      f2.ts2 = Date.now()
      f2.ts3 = f2.ts2
      // console.log(`f2=`.bgYellow, f2)


      // Call the callback
      // console.log(`=> calling callback [${stepData.onComplete.callback}]`.dim)
      const nodeInfo = {
        nodeGroup: schedulerForThisNode.getNodeGroup(),
        nodeId: schedulerForThisNode.getNodeId()
      }
      // console.log(`VOG ZARP PEW 5`)
      // console.log(`processEvent_StepCompleted event.flowIndex=`.blue, event.flowIndex)
      // console.log(`VOG ZARP PEW 6`)

      const rv = await CallbackRegister.call(tx, f2[F2ATTR_CALLBACK], event.flowIndex, event.f2i, nodeInfo, worker)//MZMZMZ
      assert(rv === GO_BACK_AND_RELEASE_WORKER)
      return GO_BACK_AND_RELEASE_WORKER
    } catch (e) {
      this.#reuseCounter--
      console.log(`DATP internal error`, e)
      throw e
    }
  }//- processEvent_StepCompleted


  /**
   *
   * @param {XData} event
   * @returns
   */
   async processEvent_TransactionChanged(event) {
    if (VERBOSE > 1) console.log(`<<< processEvent_TransactionChanged()`.brightYellow, event)

    assert(typeof(event.flowIndex) === 'number')

    try {
      const worker = this
      const txId = event.txId
      const tx = await extractTransactionStateFromEvent(event)
      const txData = tx.transactionData()

      if (VERBOSE > 1) console.log(`processEvent_TransactionChanged txData=`, txData)
      const owner = tx.getOwner()
      const externalId = tx.getExternalId()
      const status = tx.getStatus()
      const sequenceOfUpdate = tx.getSequenceOfUpdate()

      const transactionInput = txData.transactionInput
      const progressReport = tx.getProgressReport()
      const transactionOutput = tx.getTransactionOutput()

      const extraInfo = {
        txId,
        owner,
        externalId,
        status,
        transactionInput,
        progressReport,
        transactionOutput,
        sequenceOfUpdate
      }
      const rv = await CallbackRegister.call(tx, txData.onChange.callback, event.flowIndex, event.f2i, extraInfo, worker)
      assert(rv == GO_BACK_AND_RELEASE_WORKER)
      return GO_BACK_AND_RELEASE_WORKER
    } catch (e) {
      console.log(`DATP internal error in processEvent_TransactionChanged():`, e)
      console.log(`event=`, event)
      throw e
    }
  }//- processEvent_TransactionChanged


  // /**
  //  *
  //  * @param {XData} event
  //  * @returns
  //  */
  //  async processEvent_TransactionCompleted(event) {
  //   if (VERBOSE > 1) console.log(`<<< processEvent_TransactionCompleted()`.brightYellow, event)

  //   assert(typeof(event.flowIndex) === 'number')
  //   try {
  //     const worker = this
  //     const txId = event.txId
  //     const tx = await extractTransactionStateFromEvent(event)
  //     const txData = tx.txData()

  //     if (VERBOSE > 1) console.log(`processEvent_TransactionCompleted txData=`, txData)
  //     const owner = tx.getOwner()
  //     const status = txData.status
  //     const note = txData.note
  //     const transactionOutput = txData.transactionOutput

  //     // Call the callback for 'transaction complete'.
  //     const extraInfo = {
  //       owner,
  //       txId,
  //       status,
  //       note,
  //       transactionOutput
  //     }
  //     const rv = await CallbackRegister.call(tx, txData.onComplete.callback, event.flowIndex, event.f2i, extraInfo, worker)
  //     assert(rv === GO_BACK_AND_RELEASE_WORKER)
  //     return GO_BACK_AND_RELEASE_WORKER
  //   } catch (e) {
  //     console.log(`DATP internal error in processEvent_TransactionCompleted():`, e)
  //     console.log(`event=`, event)
  //     throw e
  //   }
  // }//- processEvent_TransactionCompleted

}//- class



async function extractTransactionStateFromEvent(event) {
  // console.log(`extractTransactionStateFromEvent(${typeof(event)})`)
  // if (TX_FROM_EVENT) {

    // The transaction state is stored in the event
    if (typeof(event.txState) === 'undefined') {
      throw new Error(`SERIOUS INTERNAL ERROR: event.txState is undefined`)
    }
    if (typeof(event.txState) === null) {
      throw new Error(`SERIOUS INTERNAL ERROR: event.txState is null`)
    }
    if (typeof(event.txState) === 'string') {

      // String passed through the REDIS events
      const transactionState = new TransactionState(event.txState)
      delete event.txState
      return transactionState
    }
    if (event.txState instanceof TransactionState) {
      // Event object passed through local in-memory queue
      const transactionState = event.txState
      delete event.txState
      return transactionState
    }

    throw new Error(`SERIOUS INTERNAL ERROR: event.txState of unknown type (${typeof(event.txState)}):`, event.txState)
  // } else {

  //   // We need to get the transaction state from persistent storage
  //   console.log(`- getting tx from cache`)
  //   const tx = await TransactionCache.getTransactionState(txId)
  //   if (!tx) {
  //     // This should be flagged as a serious system error.ZZZZZ
  //     const msg = `SERIOUS ERROR: event for unknown transaction ${txId}. Step ID is ${stepId}.`
  //     console.error(msg)
  //     throw new Error(msg)
  //   }
  //   return tx
  // }
}//- extractTransactionStateFromEvent
