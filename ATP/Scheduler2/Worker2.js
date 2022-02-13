/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import StepInstance from "../StepInstance"
import XData from "../XData"
import CallbackRegister from "./CallbackRegister"
import { getQueueConnection } from "./queuing/Queue2"
import Scheduler2 from "./Scheduler2"
import TransactionCache from "./TransactionCache"
import assert from 'assert'
import { STEP_ABORTED, STEP_FAILED, STEP_INTERNAL_ERROR, STEP_QUEUED, STEP_RUNNING, STEP_SLEEPING, STEP_SUCCESS, STEP_TIMEOUT } from "../Step"
import { schedulerForThisNode } from "../.."

const VERBOSE = 0
require('colors')


export default class Worker2 {
  #debugLevel

  #nodeGroup
  #nodeId
  #workerId
  // #fullQueueName
  // #myQueue
  #state

  static STANDBY = 'standby'
  static WAITING = 'waiting'
  static INUSE = 'inuse'
  static SHUTDOWN = 'shutdown'

  // constructor(workerId, nodeGroup, queueName=null) {
  constructor(workerId, nodeGroup, nodeId) {
    // console.log(`Worker2.constructor()`)
    this.#debugLevel = VERBOSE

    this.#nodeGroup = nodeGroup
    this.#nodeId = nodeId
    this.#workerId = workerId
    // this.#fullQueueName = queueName
    // this.#myQueue = null

    // this.#queue = new QueueManager()
    this.#state = Worker2.WAITING
    if (this.#debugLevel > 0) { console.log(`[worker ${this.#workerId} => WAITING]`.bgRed.white) }
  }

  getId() {
    return this.#workerId
  }

  async processEvent(event) {
    const typeStr = event.eventType ? ` (${event.eventType})` : ''
    if (this.#debugLevel > 0) { console.log(`\n[worker ${this.#workerId} processing event${typeStr  }]`.bold) }
    // console.log(`event=`, event)

    // Process this event
    this.#state = Worker2.INUSE
    if (VERBOSE) console.log(`Worker ${this.#workerId} => INUSE`.bgRed.white)

    // If this event came from a different node, then assume the transaction
    // was modified over there, and so the transaction in our cache is dirty.
    if (event.fromNodeId !== schedulerForThisNode.getNodeId()) {
      if (event.txId) {
        if (VERBOSE) console.log(`Remove TX ${event.txId} from the cache (event from different node)`)
        await TransactionCache.removeFromCache(event.txId)
      }
    }


    // Decide how to handle this event.
    const eventType = event.eventType
    delete event.eventType
    switch (eventType) {
      case Scheduler2.NULL_EVENT:
        // Ignore this. We send null events when we are shutting down, so these workers
        // stop blocking on the queue and get a change to see they are being shut down.
        break

      case Scheduler2.STEP_START_EVENT:
        await this.processEvent_StepStart(event)
        break

      case Scheduler2.STEP_COMPLETED_EVENT:
        await this.processEvent_StepCompleted(event)
        break

      case Scheduler2.TRANSACTION_CHANGE_EVENT:
        await this.processEvent_TransactionChanged(event)
        break

      case Scheduler2.TRANSACTION_COMPLETED_EVENT:
        await this.processEvent_TransactionCompleted(event)
        break

      default:
        throw new Error(`Unknown event type ${eventType}`)
    }

    // Was the state changed to shutdown while we were off processing this event?
    if (this.#state === Worker2.SHUTDOWN) {
      this.enterStandbyState()
      return
    }

    // Not shutting down. Go get another event!
    if (this.#debugLevel > 0) { console.log(`[worker ${this.#workerId} => WAITING]`.bgRed.white) }
    this.#state = Worker2.WAITING
  }//- processEvent

  stop() {
    // This worker won't notice this yet, but will after it finishes
    // blocking on it's read of the event queue.
    if (this.#debugLevel > 0) { console.log(`[worker ${this.#workerId} => SHUTDOWN]`.bgRed.white) }
    this.#state = Worker2.SHUTDOWN
  }

  enterStandbyState() {
    if (this.#debugLevel > 0) { console.log(`[worker ${this.#workerId} => STANDBY]`.bgRed.white) }
    this.#state = Worker2.STANDBY
  }

  /**
   * We are finished with this worker.
   * Disconnect from the queue
   */
  // async destroy() {
  //   if (this.#myQueue) {
  //     this.#myQueue.close()
  //   }
  // }

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

    try {
      assert(typeof(event.txId) === 'string')
      assert(typeof(event.stepId) === 'string')

      const txId = event.txId
      const stepId = event.stepId

      // Sanity check - check the status, before doing anything.
      const tx = await TransactionCache.findTransaction(txId, true)
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
      assert(stepData.status === STEP_QUEUED)
      assert(stepData.fullSequence)

      const trace = (typeof(txData.metadata.traceLevel) === 'number') && txData.metadata.traceLevel > 0
      if (trace || this.#debugLevel > 0) console.log(`>>> processEvent_StepStart()`.brightGreen)
      if (trace || this.#debugLevel > 1) console.log(`event=`, event)

      /*
       * See if this is a test step.
       */
      if (trace || this.#debugLevel > 1) console.log(`stepDefinition is ${JSON.stringify(stepData.stepDefinition, '', 2)}`)
      if (stepData.stepDefinition === 'util.ping3') {

        // Boounce back via STEP_COMPLETION_EVENT, after creating fake transaction data.
        const description = 'processEvent_StepStart() - util.ping3 - returning via STEP_COMPLETED, without processing step'
        if (trace || this.#debugLevel > 0) console.log(`description=`, description.bgBlue.white)

        await tx.delta(stepId, {
          stepId,
          status: STEP_SUCCESS,
          stepOutput: {
            happy: 'dayz',
            description
          }
        })
        const queueName = Scheduler2.groupQueueName(stepData.onComplete.nodeGroup)
        await schedulerForThisNode.enqueue_StepCompleted(queueName, {
          txId: event.txId,
          stepId: event.stepId,
          completionToken: stepData.onComplete.completionToken
        })
        return
      }//- ping3

      /*
       *  Not a test step
       */
      // Create the StepInstance object, to provide context to the step when it runs.
      event.nodeId = this.#nodeGroup
      event.nodeGroup = this.#nodeGroup
      const instance = new StepInstance()
      if (this.#debugLevel > 1) console.log(`------------------------------ ${this.#nodeGroup} materialize ${tx.getTxId()}`)
      await instance.materialize(event, tx)
      if (trace || this.#debugLevel > 0) console.log(`>>>>>>>>>> >>>>>>>>>> >>>>>>>>>> START [${instance.getStepType()}] ${instance.getStepId()}`)
      if (trace || this.#debugLevel > 1) console.log(`stepData=`, tx.stepData(stepId))

      await tx.delta(stepId, {
        stepId,
        status: STEP_RUNNING
      })

      /*
       *  Start the step - we don't wait for it to complete
       */
      const stepObject = instance.getStepObject()

      const hackSource = 'system' // Not sure why, but using Transaction.LOG_SOURCE_SYSTEM causes a compile error
      instance.trace(`Invoke step ${instance.getStepId()}`, hackSource)
      await instance.syncLogs()

      // Start the step in the background, immediately
      // setTimeout(async () => {
        try {
          if (this.#debugLevel > 0) { console.log(`[worker ${this.#workerId} STARTING STEP ${stepId} ]`.green) }
          await stepObject.invoke(instance) // Provided by the step implementation
          if (this.#debugLevel > 0) { console.log(`[worker ${this.#workerId} COMPLETED STEP ${stepId}]`.green) }
        } catch (e) {
          if (this.#debugLevel > 0) { console.log(`[worker ${this.#workerId} EXCEPTION IN STEP ${stepId}]`.green, e) }
          return await instance.exceptionInStep(null, e)
        }
      // }, 0)


    } catch (e) {
      console.log(`DATP internal error: processEvent_StepStart:`, e)
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
    if (this.#debugLevel > 1) {
      console.log(`<<< processEvent_StepCompleted()`.yellow, event)
    }

    try {
      const txId = event.txId
      const stepId = event.stepId
      const completionToken = event.completionToken

      // See what we saved before calling the step
      const tx = await TransactionCache.findTransaction(txId, true)
      const stepData = tx.stepData(stepId)
      // console.log(`stepData for step ${stepId}`, stepData)
if (!stepData) {
  console.log(`--------------`)
  console.log(`YARP 6625: Could not get stepData of tx ${txId}, step ${stepId}`)
  console.log(`tx=`, tx)
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
        nodeGroup: this.#nodeGroup,
        nodeId: this.#nodeGroup
      }
      await CallbackRegister.call(stepData.onComplete.callback, stepData.onComplete.context, nodeInfo)
    } catch (e) {
      console.log(`DATP internal error`, e)
    }
  }//- processEvent_StepCompleted


  /**
   *
   * @param {XData} event
   * @returns
   */
   async processEvent_TransactionChanged(event) {
    if (this.#debugLevel > 1) {
      console.log(`<<< processEvent_TransactionChanged()`.brightYellow, event)
    }

    try {
      const txId = event.txId
      const tx = await TransactionCache.findTransaction(txId, true)
      const txData = tx.txData()

      if (this.#debugLevel > 1) {
        console.log(`processEvent_TransactionChanged txData=`, txData)
      }
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
      await CallbackRegister.call(txData.onChange.callback, txData.onChange.context, extraInfo)
      return
    } catch (e) {
      console.log(`DATP internal error in processEvent_TransactionChanged():`, e)
      console.log(`event=`, event)
    }
  }//- processEvent_TransactionCompleted


  /**
   *
   * @param {XData} event
   * @returns
   */
   async processEvent_TransactionCompleted(event) {
    if (this.#debugLevel > 1) {
      console.log(`<<< processEvent_TransactionCompleted()`.brightYellow, event)
    }

    try {
      const txId = event.txId
      const tx = await TransactionCache.findTransaction(txId, true)
      const txData = tx.txData()
      // console.log(`txData=`, txData)


      if (this.#debugLevel > 1) {
        console.log(`processEvent_TransactionCompleted txData=`, txData)
      }
      const owner = tx.getOwner()
      const status = txData.status
      const note = txData.note
      const transactionOutput = txData.transactionOutput

      const extraInfo = {
        owner,
        txId,
        status,
        note,
        transactionOutput
      }
// console.log(`\n\n YARP transaction complete - calling ${txData.onComplete.callback}`)
      await CallbackRegister.call(txData.onComplete.callback, txData.onComplete.context, extraInfo)
      return
    } catch (e) {
      console.log(`DATP internal error in processEvent_TransactionCompleted():`, e)
      console.log(`event=`, event)
    }
  }//- processEvent_TransactionCompleted

}