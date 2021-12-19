import StepInstance from "../StepInstance"
import XData from "../XData"
import CallbackRegister from "./CallbackRegister"
import { getQueueConnection } from "./queuing/Queue2"
import Scheduler2, { DEFAULT_QUEUE } from "./Scheduler2"
import TransactionCache from "./TransactionCache"
import assert from 'assert'
import { STEP_ABORTED, STEP_FAILED, STEP_INTERNAL_ERROR, STEP_QUEUED, STEP_RUNNING, STEP_SLEEPING, STEP_SUCCESS, STEP_TIMEOUT } from "../Step"

const VERBOSE = 0
require('colors')


export default class Worker2 {
  #debugLevel

  #nodeGroup
  #workerId
  #fullQueueName
  #myQueue
  #state

  static STANDBY = 'standby'
  static WAITING = 'waiting'
  static INUSE = 'inuse'
  static SHUTDOWN = 'shutdown'

  constructor(workerId, nodeGroup, queueName=null) {
    // console.log(`Worker2.constructor()`)
    this.#debugLevel = VERBOSE

    this.#nodeGroup = nodeGroup
    this.#workerId = workerId
    this.#fullQueueName = queueName
    this.#myQueue = null

    // this.#queue = new QueueManager()
    this.#state = Worker2.STANDBY
  }

  getId() {
    return this.#workerId
  }

  async start() {
    if (this.#debugLevel > 0) { console.log(`[worker ${this.#workerId} started]`.dim) }
    if (!this.#myQueue) {
      this.#myQueue = await getQueueConnection()
    }

    switch (this.#state) {
      case Worker2.WAITING:
      case Worker2.INUSE:
        // Either waiting for a task, or running a task
        return

      case Worker2.SHUTDOWN:
        // Don't shut down
        break

      case Worker2.STANDBY:
        // Okay, let's go!
        // Run the event loop in the background
        this.#state = Worker2.WAITING
        setTimeout(() => {
          this.eventLoop()
        }, 0)
        break
      }
  }

  async eventLoop() {
    if (VERBOSE) console.log(`Start event loop in worker ${this.#workerId}`)
    this.#state = Worker2.WAITING
    while (this.#state === Worker2.WAITING) {

      // Get an event from the queue
      if (this.#debugLevel > 0) { console.log(`[worker ${this.#workerId} waiting for event on queue ${this.#fullQueueName}]`.yellow.bold) }
      // console.log(`this.#myQueue=`, this.#myQueue)
      // console.log(`this.#myQueue.dequeue=`, this.#myQueue.dequeue)
      const event = await this.#myQueue.dequeue(this.#fullQueueName)
      const typeStr = event.eventType ? ` (${event.eventType})` : ''
      if (this.#debugLevel > 0) { console.log(`\n[worker ${this.#workerId} processing event${typeStr  }]`.bold) }
      // console.log(`event=`, event)

      // While we were waiting for the queue to give us an event, was this worker marked for shutdown?
      let markedForShutdown = false
      if (this.#state === Worker2.SHUTDOWN) {
        // Don't look for more events after processing this one
        markedForShutdown = true
      }

      // Process this event
      this.#state = Worker2.INUSE
      // await this.processEvent(event)



      // If this event came from a different node, then assume the transaction
      // was modified over there, and so the transaction in our cache is dirty.
      //ZZZZZ


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

        // case
        default:
          throw new Error(`Unknown event type ${eventType}`)
      }


      // Should we shutdown this worker?
      if (
        // Was it marked for shutdown before processing, and didn't state wasn't change during processing?
        (markedForShutdown && this.#state === Worker2.INUSE)
        ||
        // Was the state changed to shutdown while we were off processing this event?
        this.#state === Worker2.SHUTDOWN
      ) {
        // Finish up
        if (this.#debugLevel > 0) { console.log(`[worker ${this.#workerId} going to standby mode]`.dim) }
        this.#state = Worker2.STANDBY
        return
      }

      // Not shutting down. Go get another event!
      if (this.#debugLevel > 0) { console.log(`[worker ${this.#workerId} ready for more]`.dim) }
      this.#state = Worker2.WAITING
    }
  }

  // async processEvent(event) {
  //   // console.log(`processEvent()`, event)
  //   // const type = event.getType()
  //   const eventType = event.eventType
  //   delete event.eventType


  //   // If this event came from a different node, then assume the transaction
  //   // was modified over there, and so the transaction in our cache is dirty.
  //   //ZZZZZ


  //   // Decide how to handle this event.
  //   switch (eventType) {
  //     case Scheduler2.NULL_EVENT:
  //       // Ignore this
  //       break

  //     // case Scheduler2.TRANSACTION_START_EVENT:
  //     //   await this.TRANSACTION_START(event.data)
  //     //   break

  //     case Scheduler2.STEP_START_EVENT:
  //       await this.processEvent_StepStart(event)
  //       break

  //     case Scheduler2.STEP_COMPLETED_EVENT:
  //       await this.processEvent_StepCompleted(event)
  //       break

  //     case Scheduler2.TRANSACTION_COMPLETED_EVENT:
  //       await this.processEvent_TransactionCompleted(event)
  //       break

  //     // case
  //     default:
  //       throw new Error(`Unknown event type ${eventType}`)
  //   }
  // }

  async stop() {
    // This worker won't notice this yet, but will after it finishes
    // blocking on it's read of the event queue.
    this.#state = Worker2.SHUTDOWN
  }

  /**
   * We are finished with this worker.
   * Disconnect from the queue
   */
  async destroy() {
    if (this.#myQueue) {
      this.#myQueue.close()
    }
  }

  getState() {
    return this.#state
  }

  /**
   *
   * @param {XData} event
   */
  async processEvent_StepStart(event) {

    try {
      assert(typeof(event.txId) === 'string')
      assert(typeof(event.stepId) === 'string')
      assert(typeof(event.onComplete) === 'object')
      assert(typeof(event.onComplete.completionToken) === 'string')

      const txId = event.txId
      const stepId = event.stepId

      // Sanity check - check the status, before doing anything.
      const tx = await TransactionCache.findTransaction(txId, true)
      const txData = tx.txData()
      const stepData = tx.stepData(stepId)
      assert(stepData.status === STEP_QUEUED)

      const trace = (typeof(txData.metadata.traceLevel) === 'number') && txData.metadata.traceLevel > 0
      if (trace || this.#debugLevel > 0) {
        console.log(`>>> processEvent_StepStart()`.brightGreen, event)
      }

      // See if this is a test step.
      if (trace || this.#debugLevel > 0) console.log(`step type is ${stepData.stepDefinition}`)
      if (stepData.stepDefinition === 'util.ping3') {

        // Boounce back via STEP_COMPLETION_EVENT, after creating fake transaction data.
        const description = 'processEvent_StepStart() - util.ping3 - returning via STEP_COMPLETED, without processing step'
        if (trace || this.#debugLevel > 0) console.log(description.bgBlue.white)
        await tx.delta(stepId, {
          status: STEP_SUCCESS,
          stepOutput: {
            happy: 'dayz',
            description
          }
        })
        const queueName = Scheduler2.standardQueueName(event.onComplete.nodeGroup, DEFAULT_QUEUE)
        await Scheduler2.enqueue_StepCompleted(queueName, {
          txId: event.txId,
          stepId: event.stepId,
          completionToken: event.onComplete.completionToken
        })
        return
      }



      // Create the StepInstance object, to provide context to the step when it runs.
      event.nodeId = this.#nodeGroup
      event.nodeGroup = this.#nodeGroup
      const instance = new StepInstance()
      if (VERBOSE) console.log(`------------------------------ ${this.#nodeGroup} materialize ${tx.getTxId()}`)
      await instance.materialize(event, tx)
      if (trace || this.#debugLevel > 0) console.log(`>>>>>>>>>> >>>>>>>>>> >>>>>>>>>> START [${instance.getStepType()}] ${instance.getStepId()}`, tx.stepData(stepId))

      await tx.delta(stepId, {
        status: STEP_RUNNING
      })

      /*
       *  Start the step - we don't wait for it to complete
       */
      const stepObject = instance.getStepObject()
      await stepObject.invoke_internal(instance)

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
      // const obj = tx.asObject()
      // console.log(`transaction ${txId} is`, obj)

      // const step = obj.steps[stepId]
      // console.log(`step=`, step)
      const stepData = tx.stepData(stepId)
      // console.log(`stepData for step ${stepId}`, stepData)
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

      if (this.#debugLevel > 1) {
        console.log(`processEvent_TransactionCompleted txData=`, txData)
      }
      const status = txData.status
      const note = txData.note
      const transactionOutput = txData.transactionOutput

      const extraInfo = {
        status,
        note,
        transactionOutput
      }
      await CallbackRegister.call(txData.onComplete.callback, txData.onComplete.context, extraInfo)
      return
    } catch (e) {
      console.log(`DATP internal error in processEvent_TransactionCompleted():`, e)
      console.log(`event=`, event)
    }
  }//- processEvent_TransactionCompleted

}