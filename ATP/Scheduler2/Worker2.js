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
import TransactionCache, { PERSIST_TRANSACTION_STATE } from "./txState-level-1"
import assert from 'assert'
import { STEP_ABORTED, STEP_FAILED, STEP_INTERNAL_ERROR, STEP_QUEUED, STEP_RUNNING, STEP_SLEEPING, STEP_SUCCESS, STEP_TIMEOUT } from "../Step"
import { schedulerForThisNode } from "../.."
import { CHECK_FOR_BLOCKING_WORKERS_TIMEOUT, INCLUDE_STATE_IN_NODE_HOPPING_EVENTS, SHORTCUT_STEP_START } from "../../datp-constants"
import Transaction from "./Transaction"
import { zalgo } from "colors"

const VERBOSE = 0
const TX_FROM_EVENT = true

require('colors')

export const GO_BACK_AND_RELEASE_WORKER = 'goback'

export default class Worker2 {
  #workerId
  #state
  #correctlyFinishedStep
  #reuseCounter // With shortcuts, we continue to use the same worker.

  static STANDBY = 'standby'
  static WAITING = 'waiting'
  static INUSE = 'inuse'
  static SHUTDOWN = 'shutdown'

  constructor(workerId) {
    // console.log(`Worker2.constructor()`)
    this.#workerId = workerId
    this.#state = Worker2.WAITING
    this.#correctlyFinishedStep = false
    this.#reuseCounter = 0
    if (VERBOSE) console.log(`[worker ${this.#workerId} => WAITING]`.bgRed.white)
  }

  getId() {
    return this.#workerId
  }

  /**
   * Set the work thread to "IN USE" before using setImmediate()
   * to call processEvent below. This assures against the worker
   * getting allocated twice if setImmediate is slow to run processEvent.
   */
  setInUse() {
    this.#state = Worker2.INUSE
  }

  async processEvent(event) {
    const typeStr = event.eventType ? ` (${event.eventType})` : ''
    if (VERBOSE) console.log(`\n[worker ${this.#workerId} processing event${typeStr}]`.bold)
    // console.log(`event=`, event)

  // console.log(`------ PROCESS EVENT ${event.eventType} ------`)
  // console.log(`event.txState=`, event.txState)

    // Check that the event includes the transaction state
    assert(event.txState)
    // console.log(`event.txState=`, event.txState)
    // console.log(`event=`, event)

    // Process this event
    this.#state = Worker2.INUSE
    // if (VERBOSE) console.log(`Worker ${this.#workerId} => INUSE`.bgRed.white)

    try {

      // Decide how to handle this event.
      const eventType = event.eventType
      // delete event.eventType
      switch (eventType) {
        case Scheduler2.NULL_EVENT:
          // Ignore this. We send null events when we are shutting down, so these workers
          // stop blocking on the queue and get a change to see they are being shut down.
          break

        case Scheduler2.STEP_START_EVENT:
          const rv = await this.processEvent_StepStart(event)
          assert(rv === GO_BACK_AND_RELEASE_WORKER)
          break

        case Scheduler2.STEP_COMPLETED_EVENT:
          const rv2 = await this.processEvent_StepCompleted(event)
          assert(rv2 === GO_BACK_AND_RELEASE_WORKER)
          break

        case Scheduler2.TRANSACTION_CHANGE_EVENT:
          const rv3 = await this.processEvent_TransactionChanged(event)
          assert(rv3 === GO_BACK_AND_RELEASE_WORKER)
          break

        case Scheduler2.TRANSACTION_COMPLETED_EVENT:
          const rv4 = await this.processEvent_TransactionCompleted(event)
          assert(rv4 === GO_BACK_AND_RELEASE_WORKER)
          break

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
  async processEvent_StepStart(event) {
    // if (VERBOSE) console.log(`Worker2.processEvent_StepStart()`)
    // if (VERBOSE > 1) console.log(`event=`, event)
    this.#reuseCounter++
//    if (this.#reuseCounter > 1) console.log(`${this.#reuseCounter}: REUSING WORKER ${this.#workerId} (${this.#reuseCounter})`)

    try {
      assert(typeof(event.txId) === 'string')
      assert(typeof(event.stepId) === 'string')

      const txId = event.txId
      const stepId = event.stepId
      const tx = await extractTransactionStateFromEvent(event)

      const txData = tx.txData()

      const stepData = tx.stepData(stepId)
      if (stepData === null) {
        console.log(`-----------------------------------------`)
        console.log(`processEvent_StepStart: missing step data`)
        console.log(`my nodeId=`, schedulerForThisNode.getNodeId())
        console.log(`event=`, event)
        console.log(`tx=`, tx)
        throw new Error(`ZZZZ: missing step`)
      }
      // if (!SHORTCUT_STEP_START) {
      //   assert(stepData.status === STEP_QUEUED)
      // }
      assert(stepData.fullSequence)

      const trace = (typeof(txData.metadata.traceLevel) === 'number') && txData.metadata.traceLevel > 0
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

        await tx.delta(stepId, {
          stepId,
          status: STEP_SUCCESS,
          stepOutput: {
            happy: 'dayz',
            description
          }
        }, 'Worker2.processEvent_StepStart()')

        await PERSIST_TRANSACTION_STATE(tx)
        
        const parentNodeGroup = stepData.onComplete.nodeGroup
        const parentNodeId = stepData.onComplete.nodeId ? stepData.onComplete.nodeId : null
        const workerForShortcut = this
        const rv = await schedulerForThisNode.schedule_StepCompleted(tx, parentNodeGroup, parentNodeId, {
          txId: event.txId,
          stepId: event.stepId,
          completionToken: stepData.onComplete.completionToken
        }, workerForShortcut)
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
      if (VERBOSE > 1) console.log(`${this.#reuseCounter}: ------------------------------ ${event.nodeGroup} materialize ${tx.getTxId()}`)
      await instance.materialize(event, tx, this)
      if (trace || VERBOSE) console.log(`${this.#reuseCounter}: >>>>>>>>>> >>>>>>>>>> >>>>>>>>>> START [${instance.getStepType()}] ${instance.getStepId()}`)
      if (trace || VERBOSE > 1) console.log(`stepData=`, tx.stepData(stepId))

      await tx.delta(stepId, {
        stepId,
        status: STEP_RUNNING
      }, 'Worker2.processEvent_StepStart()')


      await PERSIST_TRANSACTION_STATE(tx)

      /*
       *  Start the step - we don't wait for it to complete
       */
      const stepObject = instance.getStepObject()

      // const hackSource = 'system' // Not sure why, but using dbLogbook.LOG_SOURCE_SYSTEM causes a compile error
      // instance.trace(`Invoke step ${instance.getStepId()}`, hackSource)
      const stepDesc = (typeof(stepData.stepDefinition) === 'string') ? `Pipeline ${stepData.stepDefinition}` : `Step ${stepData.stepDefinition.stepType}`
      // instance.trace(`Invoked: ${stepDesc}`)
      instance.trace(stepDesc)
      instance.trace(`Run on ${schedulerForThisNode.getNodeGroup()} / ${schedulerForThisNode.getNodeId()}`)
      // instance.trace(`deltaCounter=${tx.getDeltaCounter()}`)
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

          if (VERBOSE) console.log(`[${this.#reuseCounter}: worker ${this.#workerId} STARTING STEP ${stepId} ]`.green)
          const rv = await stepObject.invoke(instance) // Provided by the step implementation
          if (VERBOSE) console.log(`[${this.#reuseCounter}: worker ${this.#workerId} RETURNED FROM ${stepId}] (and any nested steps)`.green)

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
  async processEvent_StepCompleted(event) {
    if (VERBOSE > 1) console.log(`<<< processEvent_StepCompleted()`.yellow, event)

    try {
      const worker = this
      const txId = event.txId
      const stepId = event.stepId
      const completionToken = event.completionToken
      const tx = await extractTransactionStateFromEvent(event)



      await PERSIST_TRANSACTION_STATE(tx)

      const stepData = tx.stepData(stepId)
      // console.log(`stepData for step ${stepId}`, stepData)
      if (
        stepData.status === STEP_ABORTED
        || stepData.status === STEP_FAILED
        || stepData.status === STEP_INTERNAL_ERROR
        || stepData.status === STEP_SLEEPING
        || stepData.status === STEP_SUCCESS
        || stepData.status === STEP_RUNNING
        || stepData.status === STEP_TIMEOUT
      ) {
        // OK
      } else {
        console.log(`\n\nXXXXXXXXXXXXX           XXXXXXXXXXXXX           XXXXXXXXXXXXX           XXXXXXXXXXXXX           XXXXXXXXXXXXX`)
        console.log(`\n\nXXXXXXXXXXXXX           XXXXXXXXXXXXX           XXXXXXXXXXXXX           XXXXXXXXXXXXX           XXXXXXXXXXXXX`)
        console.log(`\n\nXXXXXXXXXXXXX           XXXXXXXXXXXXX           XXXXXXXXXXXXX           XXXXXXXXXXXXX           XXXXXXXXXXXXX`)
        console.log(`schedulerForThisNode.getNodeGroup()=`, schedulerForThisNode.getNodeGroup())
        console.log(`schedulerForThisNode.getNodeId()=`, schedulerForThisNode.getNodeId())
        console.log(`event=`, event)
        console.log(`tx=`, JSON.stringify(tx.asObject(), '', 2))
        console.log(`broken1.`, new Error().stack)

        console.log(`tx=`, JSON.stringify(tx.asObject(), '', 2))

        process.exit(1)//ZZZZZ
      }


      assert(
        stepData.status === STEP_ABORTED
        || stepData.status === STEP_FAILED
        || stepData.status === STEP_INTERNAL_ERROR
        || stepData.status === STEP_SLEEPING
        || stepData.status === STEP_SUCCESS
        || stepData.status === STEP_RUNNING
        || stepData.status === STEP_TIMEOUT
      )

      // Check the completionToken is correct
      // console.log(`Checking completionToken (${completionToken} vs ${stepData.onComplete.completionToken})`)
      if (completionToken !== stepData.onComplete.completionToken) {
        throw Error(`Invalid completionToken`)
      }

      // Call the callback
      // console.log(`=> calling callback [${stepData.onComplete.callback}]`.dim)
      const nodeInfo = {
        nodeGroup: schedulerForThisNode.getNodeGroup(),
        nodeId: schedulerForThisNode.getNodeId()
      }
      const rv = await CallbackRegister.call(tx, stepData.onComplete.callback, stepData.onComplete.context, nodeInfo, worker)
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

    try {
      const worker = this
      const txId = event.txId
      const tx = await extractTransactionStateFromEvent(event)
      const txData = tx.txData()

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
      const rv = await CallbackRegister.call(tx,txData.onChange.callback, txData.onChange.context, extraInfo, worker)
      assert(rv == GO_BACK_AND_RELEASE_WORKER)
      return GO_BACK_AND_RELEASE_WORKER
    } catch (e) {
      console.log(`DATP internal error in processEvent_TransactionChanged():`, e)
      console.log(`event=`, event)
      throw e
    }
  }//- processEvent_TransactionChanged


  /**
   *
   * @param {XData} event
   * @returns
   */
   async processEvent_TransactionCompleted(event) {
    if (VERBOSE > 1) console.log(`<<< processEvent_TransactionCompleted()`.brightYellow, event)

    try {
      const worker = this
      const txId = event.txId
      const tx = await extractTransactionStateFromEvent(event)
      const txData = tx.txData()

      if (VERBOSE > 1) console.log(`processEvent_TransactionCompleted txData=`, txData)
      const owner = tx.getOwner()
      const status = txData.status
      const note = txData.note
      const transactionOutput = txData.transactionOutput

      // Once the transaction is complete (i.e. here) the Transation State is no longer required,
      // because there is no more processing. Any polling (long or short) or webhook reply can
      // work entirely using the transaction status. Also, the progress report is no longer needed.
      await PERSIST_TRANSACTION_STATE(tx)

      // Call the callback for 'transaction complete'.
      const extraInfo = {
        owner,
        txId,
        status,
        note,
        transactionOutput
      }
      const rv = await CallbackRegister.call(tx, txData.onComplete.callback, txData.onComplete.context, extraInfo, worker)
      assert(rv === GO_BACK_AND_RELEASE_WORKER)
      return GO_BACK_AND_RELEASE_WORKER
    } catch (e) {
      console.log(`DATP internal error in processEvent_TransactionCompleted():`, e)
      console.log(`event=`, event)
      throw e
    }
  }//- processEvent_TransactionCompleted

}//- class



async function extractTransactionStateFromEvent(event) {
  // console.log(`extractTransactionStateFromEvent(${typeof(event)})`)
  if (TX_FROM_EVENT) {

    // The transaction state is stored in the event
    if (typeof(event.txState) === 'undefined') {
      throw new Error(`SERIOUS INTERNAL ERROR: event.txState is undefined`)
    }
    if (typeof(event.txState) === null) {
      throw new Error(`SERIOUS INTERNAL ERROR: event.txState is null`)
    }
    if (typeof(event.txState) === 'string') {

      // String passed through the REDIS events
      const transactionState = Transaction.transactionStateFromJSON(event.txState)
      delete event.txState
      return transactionState
    }
    if (event.txState instanceof Transaction) {
      // Event object passed through local in-memory queue
      const transactionState = event.txState
      delete event.txState
      return transactionState
    }

    throw new Error(`SERIOUS INTERNAL ERROR: event.txState of unknown type (${typeof(event.txState)}):`, event.txState)
  } else {

    // We need to get the transaction state from persistent storage
    console.log(`- getting tx from cache`)
    const tx = await TransactionCache.getTransactionState(txId)
    if (!tx) {
      // This should be flagged as a serious system error.ZZZZZ
      const msg = `SERIOUS ERROR: event for unknown transaction ${txId}. Step ID is ${stepId}.`
      console.error(msg)
      throw new Error(msg)
    }
    return tx
  }
}//- extractTransactionStateFromEvent
