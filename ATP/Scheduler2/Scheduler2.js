/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import dbTransactionType from '../../database/dbTransactionType'
import GenerateHash from '../GenerateHash'
import TransactionIndexEntry from '../TransactionIndexEntry'
import XData from '../XData'
import CallbackRegister from './CallbackRegister'
import { getQueueConnection } from './queuing/Queue2'
import { ROOT_STEP_COMPLETE_CALLBACK } from './rootStepCompleteCallback'
import TransactionCache from './TransactionCache'
import Worker2 from './Worker2'
import assert from 'assert'
import { STEP_QUEUED, STEP_SUCCESS } from '../Step'
import { schedulerForThisNode } from '../..'
import StatsCollector from '../../lib/statsCollector'
import pause from '../../lib/pause'
import { DuplicateExternalIdError } from './TransactionPersistance'

// Debug related
const VERBOSE = 0
require('colors')


const DEFAULT_NUM_WORKERS = 100
const DELAY_WHEN_ALL_WORKERS_ARE_BUSY = 20
const DELAY_WHEN_QUEUES_ARE_EMPTY = 50
const DELAY_BEFORE_RESTARTING_EVENTLOOP = 5 // Give the workers time to set state=RUNNING, before checking again.

// How long REDIS should keep the record while checking for an externalId. This
// just need to be long enough that we csn be certain the database will have been
// written to all/any distributed copies of the database.
const DUP_EXTERNAL_ID_DELAY = 60
const DUP_EXTERNAL_ID_TESTING_HACK = true


export default class Scheduler2 {
  #debugLevel

  /*
   *  External code only interacts with the scheduler via events.
   */
  #nodeGroup // Group of this node
  #nodeId

  // Queueing
  #queueObject
  #groupQueue // Name of the queue
  #nodeRegularQueue
  #nodeExpressQueue

  // Worker threads
  #requiredWorkers
  #workers

  // Timeout handler for each clock tick
  // #timeout
  // #tickCounter
  #state

  // Statistics collection
  #transactionsInPastMinute
  #transactionsInPastHour
  #transactionsOutPastMinute
  #transactionsOutPastHour
  #stepsPastMinute
  #stepsPastHour
  #enqueuePastMinute
  #enqueuePastHour
  #dequeuePastMinute
  #dequeuePastHour

  // Scheduler states
  static STOPPED = 'stopped'
  static RUNNING = 'running'
  static SHUTTING_DOWN = 'shutting-down'

  // Event types
  static NULL_EVENT = 'no-op'
  // static TRANSACTION_START_EVENT = 'tx-start'
  // static TRANSACTION_PROGRESS_EVENT = 'tx-progress' //ZZZZZ Is this used ???
  static TRANSACTION_COMPLETED_EVENT = 'tx-completed'
  static TRANSACTION_CHANGE_EVENT = 'tx-changed'
  static STEP_START_EVENT = 'step-start'
  static STEP_COMPLETED_EVENT = 'step-end'
  static LONG_POLL = 'long-poll'

  // Queue prefixes
  static GROUP_QUEUE_PREFIX = 'group'
  static REGULAR_QUEUE_PREFIX = 'node'
  static EXPRESS_QUEUE_PREFIX = 'express'

  /**
   * Remember the response object for if the user wants long polling.
   */
  // #responsesForSynchronousReturn //ZZZZ Separate this out

  constructor(groupName, queueName=null, options= { }) {
    if (VERBOSE) console.log(`Scheduler2.constructor(${groupName}, ${queueName})`)

    this.#debugLevel = 0

    this.#nodeGroup = groupName
    this.#nodeId = GenerateHash('nodeId')
    this.#groupQueue = Scheduler2.groupQueueName(groupName)
    this.#nodeRegularQueue = Scheduler2.nodeRegularQueueName(groupName, this.#nodeId)
    this.#nodeExpressQueue = Scheduler2.nodeExpressQueueName(groupName, this.#nodeId)
    this.#queueObject = null

    // Allocate some StepWorkers
    this.#requiredWorkers = (options.numWorkers ? options.numWorkers : DEFAULT_NUM_WORKERS)
    this.#workers = [ ]

    // this.#timeout = null
    // this.#tickCounter = 1
    this.#state = Scheduler2.STOPPED

    // console.log(`Have scheduler for node ${this.#nodeId}`)
    // console.log(`  #groupQueue=`, this.#groupQueue)
    // console.log(`  #nodeRegularQueue=`, this.#nodeRegularQueue)
    // console.log(`  #nodeExpressQueue=`, this.#nodeExpressQueue)

    // Statistocs collection
    this.#transactionsInPastMinute = new StatsCollector(1000, 60)
    this.#transactionsInPastHour = new StatsCollector(60 * 1000, 60)
    this.#transactionsOutPastMinute = new StatsCollector(1000, 60)
    this.#transactionsOutPastHour = new StatsCollector(60 * 1000, 60)
    this.#stepsPastMinute = new StatsCollector(1000, 60)
    this.#stepsPastHour = new StatsCollector(60 * 1000, 60)
    this.#enqueuePastMinute = new StatsCollector(1000, 60)
    this.#enqueuePastHour = new StatsCollector(60 * 1000, 60)
    this.#dequeuePastMinute = new StatsCollector(1000, 60)
    this.#dequeuePastHour = new StatsCollector(60 * 1000, 60)
  }

  getNodeGroup() {
    return this.#nodeGroup
  }

  getNodeId() {
    return this.#nodeId
  }

  async _checkConnectedToQueue() {
    // console.log(`_checkConnectedToQueue`)
    if (!this.#queueObject) {
      if (VERBOSE) console.log(`Getting a new connection`)
      // Get a connection to my queue
      this.#queueObject = await getQueueConnection()
    }
    // if (VERBOSE) console.log(`this.#queueObject=`, this.#queueObject)
  }

  async start() {
    if (VERBOSE) console.log(`Scheduler2.start(${this.#groupQueue})`)
    await this._checkConnectedToQueue()

    if (this.#state === Scheduler2.RUNNING) {
      if (VERBOSE) console.log(`  - already running`)
      return
    }
    this.#state = Scheduler2.RUNNING

    // Allocate and start workers
    if (VERBOSE) console.log(`Creating ${this.#requiredWorkers} workers`)
    while (this.#workers.length < this.#requiredWorkers) {
      const id = this.#workers.length
      // const worker = new Worker2(id, this.#nodeGroup, this.#groupQueue)
      const worker = new Worker2(id, this.#nodeGroup, this.#nodeId)
      this.#workers.push(worker)
      // await worker.start()
    }
    // console.log(`  - created ${this.#workers.length} workers`)


    // Loop around getting events and passing them to the workers
    // We count how many workers are waiting for an event, then
    // hand out that many events before counting again.
    const eventLoop = async () => {
      // if (VERBOSE)
      // console.log(`######## Start event loop.`)

      // Create a list of all the workers waiting for an event
      const workersWaiting = [ ]
      for (const worker of this.#workers) {
        const state = await worker.getState()
        if (state === Worker2.WAITING) {
          workersWaiting.push(worker)
        }
      }
      // if (VERBOSE)

      // If no workers are available, wait a bit and try again
      if (workersWaiting.length === 0) {
        // if (VERBOSE)
        // console.log(`All workers are busy - Event loop sleeping a bit`)
        setTimeout(eventLoop, DELAY_WHEN_ALL_WORKERS_ARE_BUSY)
        return
      }

      // Get events for these workers
      // We read from three queues, in the following priority:
      //  1. Express queue for node (replies)
      //  2. Normal queue for node (steps)
      //  3. Queue for group (incoming transactions)
      const numEvents = workersWaiting.length
      const block = false
      const queues = [
        this.#nodeExpressQueue,
        this.#nodeRegularQueue,
        this.#groupQueue
      ]
      const events = await this.#queueObject.dequeue(queues, numEvents, block)
      // console.log(`events=`, events)
      if (events.length === 0) {
        // if (VERBOSE)
        // console.log(`Event queue is empty - Event loop sleeping a bit`)
        setTimeout(eventLoop, DELAY_WHEN_QUEUES_ARE_EMPTY)
        return
      }

      // Update our statistics
      this.#dequeuePastMinute.add(events.length)
      this.#dequeuePastHour.add(events.length)

      // const tick = (events.length === workersWaiting.length) ? ' <==' : ''
      // console.log(`${events.length} events <==> ${workersWaiting.length} available workers${tick}`)

      // Pair up the workers and the events
      for (let i = 0; i < workersWaiting.length && i < events.length; i++) {

        // Process the event
        // Do NOT wait for it to complete. It will set it's state back to 'waiting' when it's ready.
        const worker = workersWaiting[i]
        const event = events[i]
        // console.log(`Scheduler2.start: have ${event.eventType} event`)
        worker.processEvent(event)
      }

      // Let's start all over again, because some workers may be available by now.
      // We use the timeout, so the stack can reset itself.
      // A tiny delay is required, to ensure processEvent has time to set the
      // state away from WAITING.
      setTimeout(eventLoop, DELAY_BEFORE_RESTARTING_EVENTLOOP)


      // // Find an event for each of these workers that are waiting
      // while (workersWaiting.length > 0) {

      //   // Get the next worker that WAS waiting for an event.
      //   // It's possible the worker has been marked for shutdown while we've been
      //   // waiting for events here. If so, put it in standby state now.
      //   const worker = workersWaiting.pop()

      //   // If this worker is now needing to go to
      //   const state = worker.getState()
      //   if (state === Worker2.SHUTDOWN) {
      //     worker.enterStandbyState()
      //     continue
      //   }
      //   //  else  if (state !== Worker2.WAITING) {
      //   //   // Maybe it's shut down
      //   //   continue
      //   // }
      //   // if (VERBOSE) console.log(`Getting an event for worker`)

      //   // Get an event for this worker
      //   const numEvents = 1
      //   const event = events[0]
      //   console.log(`- event -> worker`)
      //   if (VERBOSE) {
      //     console.log(`\n<<< dequeued EVENT`.green)
      //     // console.log(`event=`, event)
      //   }

      //   // Process the event
      //   // Do NOT wait for it to complete. It will set it's state back to 'waiting' when it's ready.
      //   worker.processEvent(event)
      // }

      // // Let's start all over again, because some workers may be available by now.
      // // We use the timeout, so the stack can reset itself.
      // setTimeout(eventLoop, 0)
    }

    // Let's start the event loop in the background
    setTimeout(eventLoop, 0)
  }

  /**
   * Display extra debug information if turned on.
   * @param {number} level
   */
  async setDebugLevel(level = 1) {
    this.#debugLevel = level
  }


  /**
   *
   * @param {XData | object} input
   * @returns
   */
  async startTransaction(input) {
    assert (typeof(input.metadata) === 'object')
    assert (typeof(input.metadata.owner) === 'string')
    assert (typeof(input.metadata.nodeGroup) === 'string')
    assert (typeof(input.metadata.externalId) === 'string' || input.metadata.externalId === null)
    assert (typeof(input.metadata.transactionType) === 'string')
    assert (typeof(input.metadata.onComplete) === 'object')
    assert (typeof(input.metadata.onComplete.callback) === 'string')
    assert (typeof(input.metadata.onComplete.context) === 'object')
    if (input.metadata.onChange) {
      assert (typeof(input.metadata.onChange) === 'object')
      assert (typeof(input.metadata.onChange.callback) === 'string')
      assert (typeof(input.metadata.onChange.context) === 'object')
    }
    assert (typeof(input.data) === 'object')

    try {
      const metadata = input.metadata
      const trace = (typeof(metadata.traceLevel) === 'number') && (metadata.traceLevel > 0)
      if (VERBOSE||trace) console.log(`*** Scheduler2.startTransaction()`.bgYellow.black)
      if (VERBOSE > 1 || trace) console.log(input)
      if (VERBOSE||trace) console.log(`*** Scheduler2.startTransaction() - transaction type is ${input.metadata.transactionType}`.bgYellow.black)

      // Sanitize the input data by convering it to JSON and back
      const initialData = JSON.parse(JSON.stringify(input.data))

      /*
      *  A 'ping1' test transaction returns by immediately calling the callback function.
      */
      if (metadata.transactionType === 'ping1') {
        const description = 'ping1 - Scheduler2.startTransaction() immediately invoked the callback, without processing'
        if (VERBOSE||trace) {console.log(description.bgBlue.white)}
        const fakeTransactionOutput = {
          status: STEP_SUCCESS,
          transactionOutput: { foo: 'bar', description }
        }
        await CallbackRegister.call(metadata.onComplete.callback, metadata.onComplete.context, fakeTransactionOutput)
        return
      }

      /*
       *  A 'ping2' test transaction returns via the TRANSACTION_COMPLETE_EVENT
       */
      if (metadata.transactionType === 'ping2') {
        // Bounce back via a normal TRANSACTION_COMPLETE_EVENT
        const description = 'ping2 - Scheduler2.startTransaction() returning without processing step'
        if (VERBOSE||trace) console.log(description.bgBlue.white)

        // Create a new transaction
        const tx = await TransactionCache.newTransaction(metadata.owner, metadata.externalId, metadata.transactionType)
        const txId = tx.getTxId()
        await tx.delta(null, {
          onComplete: {
            nodeGroup: schedulerForThisNode.getNodeGroup(),
            nodeId: schedulerForThisNode.getNodeId(),
            callback: metadata.onComplete.callback,
            context: metadata.onComplete.context
          },
          status: STEP_SUCCESS,
          transactionOutput: { whoopee: 'doo', description }
        })
        // console.log(`tx=`, (await tx).toString())
        const queueName = Scheduler2.groupQueueName(input.metadata.nodeGroup)
        await schedulerForThisNode.enqueue_TransactionCompleted(queueName, {
          txId,
        })
        return
      }

      // Create a version of the metadata that can be passed to steps
      const metadataCopy = JSON.parse(JSON.stringify(metadata))
      // console.log(`metadataCopy 1=`, metadataCopy)
      delete metadataCopy['transactionType']
      delete metadataCopy['owner']
      delete metadataCopy['externalId']
      delete metadataCopy['nodeGroup']
      delete metadataCopy['onComplete']
      delete metadataCopy['onChange']
      // console.log(`metadataCopy 2=`, metadataCopy)

      // Which pipeline should we use?
      if (VERBOSE||trace) console.log(`Scheduler2.startTransaction() - looking for pipeline for transactionType '${metadata.transactionType}'.`)
      const pipelineDetails = await dbTransactionType.getPipeline(metadata.transactionType)
      // console.log(`initiateTransaction() - pipelineDetails:`, pipelineDetails)
      if (VERBOSE||trace) console.log(`Scheduler2.startTransaction() - pipelineDetails:`, pipelineDetails)
      if (!pipelineDetails) {
        throw new Error(`Unknown transaction type ${metadata.transactionType}`)
      }

      const pipelineName = pipelineDetails.pipelineName
      if (VERBOSE||trace) console.log(`Scheduler2.startTransaction() - pipelineName: ${pipelineName}`)
      // console.log(`pipelineName=`, pipelineName)
      //ZZZZ How about the version?  Should we use name:version ?


      // If there is an externalId, there is concern that uniqueness cannot be guaranteed by
      // the database under some circumstances (distributed database nodes, and extremely
      // close repeated calls). To add an extra layer of protection we'll use a REDIS increment,
      // with an expiry of thirty seconds. The first INCR call will return 1, the second will
      // return 2, etc. This will continue until the key is removed after 30 seconds. By that
      // time the externalId should certainly be stored in the darabase.
      if (metadata.externalId) {
        const key = `externalId-${metadata.externalId}`
        if (await this.#queueObject.repeatEventDetection(key, DUP_EXTERNAL_ID_DELAY)) {
          // A transaction already exists with this externalId
          console.log(`Scheduler2.startTransaction: detected duplicate externalId via REDIS`)
          throw new DuplicateExternalIdError()
        }
        // if (DUP_EXTERNAL_ID_TESTING_HACK) {
        //   // The intention here is to give us time to test the REDIS check
        //   // before the database error detects the duplicate.
        //   console.log(``)
        //   console.log(`WARNING!!!!!!`)
        //   console.log(`DUP_EXTERNAL_ID_TESTING_HACK is set.`)
        //   console.log(`Inserting the transaction record is delayed 10 seconds, so you have time to test the backup duplicate externalId check...`)
        //   await pause(10 * 1000)
        //   console.log(`Database insert proceeding now..`)
        // }
      }


      // Persist the transaction details
      let tx = await TransactionCache.newTransaction(metadata.owner, metadata.externalId, metadata.transactionType)

      const def = {
        transactionType: metadata.transactionType,
        nodeGroup: metadata.nodeGroup,
        nodeId: metadata.nodeGroup, //ZZZZ Set to current node
        pipelineName,
        status: TransactionIndexEntry.RUNNING,//ZZZZ
        metadata: metadataCopy,
        transactionInput: initialData,
        onComplete: {
          nodeGroup: schedulerForThisNode.getNodeGroup(),
          nodeId: schedulerForThisNode.getNodeId(),
          callback: metadata.onComplete.callback,
          context: metadata.onComplete.context,
        }
      }
      if (input.metadata.onChange) {
        def.onChange = {
          callback: input.metadata.onChange.callback,
          context: input.metadata.onChange.context
        }
      }
      await tx.delta(null, def)
      // console.log(`txData=`, tx.txData())


      // console.log(`tx=`, tx.toString())

      // tx.persist() //YARP2

      // Remember the transaction
      // const transactionIndexEntry = new TransactionIndexEntry(txId, transactionType, status, initiatedBy, initialTxData)
      // const txId = await transactionIndexEntry.getTxId()

      // Persist the transaction
      // const inquiryToken = await dbTransactionInstance.persist(transactionIndexEntry, pipelineName)
      // console.log(`inquiryToken=`, inquiryToken)//YARP2
      // this.#transactionIndex[txId] = transactionIndexEntry


      // Create a logbook for this transaction/pipeline
      // const logbook = new Logbook.cls({
      //   txId: txId,
      //   description: `Pipeline logbook`
      // })

      // Generate a new ID for this step
      const txId = tx.getTxId()
      const stepId = GenerateHash('s')

      // Get the name of the queue to the node where this pipeline will run
      const nodeGroupWherePipelineRuns = metadata.nodeGroup // Should come from the pipeline definition
      // const queueToPipelineNode = Scheduler2.groupQueueName(nodeGroupWherePipelineRuns)

      // If this pipeline runs in a different node group, we'll start it via the group
      // queue for that nodeGroup. If the pipeline runs in the current node group, we'll
      // run it in this current node, so it'll have access to the cached transaction.
      const myNodeGroup = schedulerForThisNode.getNodeGroup()
      const myNodeId = schedulerForThisNode.getNodeId()
      let queueToPipelineNode
      if (nodeGroupWherePipelineRuns === myNodeGroup) {
        // Run the new pipeline in this node - put the event in this node's pipeline.
        queueToPipelineNode = Scheduler2.nodeRegularQueueName(myNodeGroup, myNodeId)
      } else {
        // The new pipeline will run in a different nodeGroup. Put the event in the group queue.
        queueToPipelineNode = Scheduler2.groupQueueName(nodeGroupWherePipelineRuns)
      }


      // console.log(`metadataCopy=`, metadataCopy)
      // console.log(`data=`, data)
      if (VERBOSE||trace) console.log(`Scheduler2.startTransaction() - adding to queue ${queueToPipelineNode}`)
      // console.log(`txId=`, txId)
      const fullSequence = txId.substring(3, 9)
      // console.log(`fullSequence=`, fullSequence)
      await schedulerForThisNode.enqueue_StepStart(queueToPipelineNode, {
        txId,
        stepId,
        parentNodeGroup: myNodeGroup,
        // parentNodeId: myNodeId,
        parentStepId: '',
        fullSequence,
        stepDefinition: pipelineName,
        metadata: metadataCopy,
        data: initialData,
        level: 0,

        onComplete: {
          nodeGroup: myNodeGroup,
          nodeId: myNodeId,
          callback: ROOT_STEP_COMPLETE_CALLBACK,
          context: { txId, stepId },
        }
      })


      // Update our statistics
      this.#transactionsInPastMinute.add(1)
      this.#transactionsInPastHour.add(1)
      this.#enqueuePastMinute.add(1)
      this.#enqueuePastHour.add(1)

      return tx
    } catch (e) {
      // if (!(e instanceof DuplicateExternalIdError)) {
      // console.log(`DATP internal error in startTransaction()`, e)
      // }
      throw e
    }
  }//- TRANSACTION_START

  /**
   * How replying works
   * ------------------
   * In this function we persist these fields to the step:
   *    callback          // A callback name, registered with CallbackRegister.js
   *    callbackContext   // Everything the callback needs to work
   *    completionToken   // A random generated hash
   *
   * To the child we send:
   *    callbackNodeGroup  // Where to send the completion event
   *    txId
   *    stepId
   *    completionToken
   *    -- other info --
   *
   * Using these details the child step can send a completion event back to the
   * node group that called it, containing it's stepId and the completionToken.
   * The worker that handles the event looks up the persisted step details and
   * then (1) checks the completionToken matches (to prevent hack events) and
   * then (2) calls the callback function with the callbackContext and the
   * child details.
   *
   * The callback function should then have all the information it needs to do
   * whatever it needs to do. In the case of a pipeline it may run another step
   * or return. If this is the root pipeline, the rootStepCompletionHandler will
   * complete the transaction.
   *
   * @param {string} eventType
   * @param {object} data
   */
  async enqueue_StepStart(queueName, data) {
    if (VERBOSE) {
      console.log(`\n<<< enqueueStepStart EVENT(${queueName})`.green)
      // console.log(new Error(`TRACE ONLY 1`).stack.magenta)
    }
    if (VERBOSE > 1) console.log(`data=`, data)
    if (queueName.startsWith(Scheduler2.GROUP_QUEUE_PREFIX)) {
      console.log(`////////////////////////////////////////////`.magenta)
      console.log(`enqueue_StepStart with group queue ${queueName}`.magenta)
      console.log(new Error('for debug').stack.magenta)
      console.log(`////////////////////////////////////////////`.magenta)
    }

    assert(typeof(queueName) === 'string')
    let obj
    if (data instanceof XData) {
      obj = data.getData()
    } else if (typeof(data) === 'object') {
      obj = data
    } else {
      throw new Error('enqueueStepStart() - data parameter must be XData or object')
    }

    // Verify the event data
    assert (typeof(obj.txId) === 'string')
    assert (typeof(obj.stepId) === 'string')

    assert (typeof(obj.fullSequence) === 'string')
    assert (typeof(obj.stepDefinition) !== 'undefined')
    assert (typeof(obj.data) === 'object')
    assert ( !(obj.data instanceof XData))
    assert (typeof(obj.metadata) === 'object')
    assert (typeof(obj.level) === 'number')

    // How to reply when complete
    assert (typeof(obj.onComplete) === 'object')
    assert (typeof(obj.onComplete.nodeGroup) === 'string')
    assert (typeof(obj.onComplete.nodeId) === 'string')
    assert (typeof(obj.onComplete.callback) === 'string')
    assert (typeof(obj.onComplete.context) === 'object')

    // Add a completionToken to the event, so we can check that the return EVENT is legitimate
    obj.onComplete.completionToken = GenerateHash('ctok')

    // Remember the callback details, and do not pass to the step
    const tx = await TransactionCache.findTransaction(obj.txId, false)
    // console.log(`tx=`, tx)
    await tx.delta(null, {
      nextStepId: obj.stepId, //ZZZZZ Choose a better field name
    })
    await tx.delta(obj.stepId, {
      // Used on return from the step.
      onComplete: {
        nodeGroup: obj.onComplete.nodeGroup,
        nodeId: obj.onComplete.nodeId,
        callback: obj.onComplete.callback,
        context: obj.onComplete.context,
        completionToken: obj.onComplete.completionToken,
      },

      // Other information about the step
      parentStepId: obj.parentStepId, // Is this needed?
      fullSequence: obj.fullSequence,
      stepDefinition: obj.stepDefinition,
      stepInput: obj.data,
      level: obj.level,
      status: STEP_QUEUED
    })
    // delete obj.onComplete.callback
    // delete obj.onComplete.context
    // delete obj.metadata
    // delete obj.data

    // Add to an event queue
    // const queue = await getQueueConnection()
    // obj.eventType = Scheduler2.STEP_START_EVENT
    // console.log(`Adding ${obj.eventType} event to queue ${queueName}`.brightGreen)
    const event = {
      // Just what we need
      eventType: Scheduler2.STEP_START_EVENT,
      txId: obj.txId,
      stepId: obj.stepId,
      // onComplete: obj.onComplete,
      // completionToken: obj.onComplete.completionToken
      fromNodeId: this.#nodeId
    }
    // console.log(`QUEing event to start step`)
    await this._checkConnectedToQueue()
    await this.#queueObject.enqueue(queueName, event)
    // queue.close()

    // Update our statistics
    this.#stepsPastMinute.add(1)
    this.#stepsPastHour.add(1)
    this.#enqueuePastMinute.add(1)
    this.#enqueuePastHour.add(1)

  }//- enqueueStepStart

  async enqueue_StepRestart(queueName, txId, stepId) {
    if (VERBOSE) {
      console.log(`\n<<< enqueue_StepRestart EVENT(${queueName})`.green, data)
    }
    assert(typeof(queueName) === 'string')
    assert(typeof(txId) === 'string')
    assert(typeof(stepId) === 'string')

    // Change the step status
    const tx = await TransactionCache.findTransaction(txId, false)
    await tx.delta(stepId, {
      status: STEP_QUEUED
    })

    // Add to the event queue
    // const queue = await getQueueConnection()
    const event = {
      eventType: Scheduler2.STEP_START_EVENT,
      txId,
      stepId,
      fromNodeId: this.#nodeId
    }
    await this._checkConnectedToQueue()
    await this.#queueObject.enqueue(queueName, event)

    // Update our statistics
    this.#stepsPastHour.add(1)
    this.#stepsPastMinute.add(1)
    this.#enqueuePastMinute.add(1)
    this.#enqueuePastHour.add(1)
  }//- enqueue_StepRestart


  /**
   *
   * @param {string} queueName
   * @param {object} event
   */
   async enqueue_StepCompleted(queueName, event) {
    if (VERBOSE) {
      console.log(`\n<<< enqueue_StepCompleted(${queueName})`.green)
      if (VERBOSE > 1) console.log(`event=`, event)
    }
    if (queueName.startsWith(Scheduler2.GROUP_QUEUE_PREFIX)) {
      console.log(`////////////////////////////////////////////`.magenta)
      console.log(`enqueue_StepCompleted with group queue ${queueName}`.magenta)
      console.log(new Error('for debug').stack.magenta)
      console.log(`////////////////////////////////////////////`.magenta)
    }

    // Validate the event data
    assert(typeof(queueName) == 'string')
    assert(typeof(event) == 'object')
    assert (typeof(event.txId) === 'string')
    // assert (typeof(event.parentStepId) === 'string')
    assert (typeof(event.stepId) === 'string')
    assert (typeof(event.completionToken) === 'string')

    // DO NOT try to reply stuff
    assert (typeof(event.status) === 'undefined')
    assert (typeof(event.stepOutput) === 'undefined')

    // Add the event to the queue
    event.eventType = Scheduler2.STEP_COMPLETED_EVENT
    // console.log(`Adding ${event.eventType} event to queue ${queueName}`.brightGreen)
    // const queue = await getQueueConnection()
    await this._checkConnectedToQueue()
    event.fromNodeId = this.#nodeId
    await this.#queueObject.enqueue(queueName, event)

    // Update our statistics
    this.#enqueuePastMinute.add(1)
    this.#enqueuePastHour.add(1)
  }//- enqueue_StepCompleted


  /**
   *
   * @param {string} queueName
   * @param {object} event
   */
  async enqueue_TransactionCompleted(queueName, event) {
    if (VERBOSE) {
      console.log(`\n<<< enqueue_TransactionCompleted(${queueName})`.green, event)
    }
    if (queueName.startsWith(Scheduler2.GROUP_QUEUE_PREFIX)) {
      console.log(`////////////////////////////////////////////`.magenta)
      console.log(`enqueue_TransactionCompleted with group queue ${queueName}`.magenta)
      console.log(new Error('for debug').stack.magenta)
      console.log(`////////////////////////////////////////////`.magenta)
    }

    if (!queueName) {
      throw new Error(`enqueue_TransactionCompleted() requires queueName parameter`)
    }
    // Validate the event data
    assert(typeof(queueName) == 'string')
    assert(typeof(event) == 'object')
    assert (typeof(event.txId) === 'string')
    assert (typeof(event.transactionOutput) === 'undefined')

    // Add to the event queue
    event.eventType = Scheduler2.TRANSACTION_COMPLETED_EVENT
    // console.log(`Adding ${event.eventType} event to queue ${queueName}`.brightGreen)
    // const queue = await getQueueConnection()
    await this._checkConnectedToQueue()
    event.fromNodeId = this.#nodeId
    await this.#queueObject.enqueue(queueName, event)

    // Update our statistics
    this.#transactionsOutPastMinute.add(1)
    this.#transactionsOutPastHour.add(1)
    this.#enqueuePastMinute.add(1)
    this.#enqueuePastHour.add(1)
  }//- enqueue_TransactionCompleted


  /**
   *
   * @param {string} queueName
   * @param {object} event
   */
   async enqueue_TransactionChange(queueName, event) {
    if (VERBOSE) {
      console.log(`\n<<< enqueue_TransactionChange(${queueName})`.green, event)
    }
    if (queueName.startsWith(Scheduler2.GROUP_QUEUE_PREFIX)) {
      console.log(`////////////////////////////////////////////`.magenta)
      console.log(`enqueue_TransactionChange with group queue ${queueName}`.magenta)
      console.log(new Error('for debug').stack.magenta)
      console.log(`////////////////////////////////////////////`.magenta)
    }

    if (!queueName) {
      throw new Error(`enqueue_TransactionChange() requires queueName parameter`)
    }
    // Validate the event data
    assert(typeof(queueName) == 'string')
    assert(typeof(event) == 'object')
    assert (typeof(event.txId) === 'string')
    assert (typeof(event.transactionOutput) === 'undefined')

    // Add to the event queue
    event.eventType = Scheduler2.TRANSACTION_CHANGE_EVENT
    // console.log(`Adding ${event.eventType} event to queue ${queueName}`.brightGreen)
    // const queue = await getQueueConnection()
    await this._checkConnectedToQueue()
    event.fromNodeId = this.#nodeId
    await this.#queueObject.enqueue(queueName, event)

    // Update our statistics
    this.#enqueuePastMinute.add(1)
    this.#enqueuePastHour.add(1)
  }//- enqueue_TransactionChange


  /**
   *
   * @param {string} eventType
   * @param {object} data
   */
  async enqueue_NoOperation(queueName) {
    if (VERBOSE) {
      console.log(`\n<<< enqueue_NoOperation(${queueName})`.green)
    }

    assert(typeof(queueName) === 'string')

    // Add to the event queue
    const event = {
      eventType: Scheduler2.NULL_EVENT,
      fromNodeId: this.#nodeId
    }
    // console.log(`Adding ${event.eventType} event to queue ${queueName}`.brightGreen)
    // const queue = await getQueueConnection()
    await this._checkConnectedToQueue()
    await this.#queueObject.enqueue(queueName, event)

    // Update our statistics
    this.#enqueuePastMinute.add(1)
    this.#enqueuePastHour.add(1)
  }//- enqueue_NoOperation








  async queueLength() {
    await this._checkConnectedToQueue()
    return await this.#queueObject.queueLength(this.#groupQueue)
  }

  async queueLengths() {
    await this._checkConnectedToQueue()
    return await this.#queueObject.queueLengths()
  }

  async drainQueue() {
    await this._checkConnectedToQueue()
    return await this.#queueObject.drainQueue(this.#groupQueue)
  }

  /**
   * @param {String} nodeGroup
   * @returns Queue name
   */
   static groupQueueName(nodeGroup) {
    assert(nodeGroup)
    return `${Scheduler2.GROUP_QUEUE_PREFIX}:${nodeGroup}`
  }

  /**
   * Returns the queue name for regular node-specific events.
   * @param {String} nodeGroup
   * @param {String} nodeId
   * @returns Queue name
   */
   static nodeRegularQueueName(nodeGroup, nodeId) {
    assert(nodeGroup)
    assert(nodeId)
    return `${Scheduler2.REGULAR_QUEUE_PREFIX}:${nodeGroup}:${nodeId}`
  }

  /**
   * Returns the queue name for faster node-specific events that
   * should take priority, such as replies. This allows completed
   * steps and transactions to be cleared out quickly. Sort of like
   * letting people out of an elevator before you try to get in.
   * @param {String} nodeGroup
   * @param {String} nodeId
   * @returns Queue name
   */
   static nodeExpressQueueName(nodeGroup, nodeId) {
    assert(nodeGroup)
    assert(nodeId)
    return `${Scheduler2.EXPRESS_QUEUE_PREFIX}:${nodeGroup}:${nodeId}`
  }

  /**
   * Stop the scheduler from processing ticks.
   */
  async stop() {
    if (VERBOSE||this.#debugLevel > 0) {
      console.log(`Scheduler2.shutdown()`)
    }
    this.#state = Scheduler2.STOPPED

    // Set all the workers to "shutting down" mode.
    // When they finish their next event they'll stop looking at the queue.
    for (const worker of this.#workers) {
      await worker.stop()
    }

    //ZZZ Does this still apply?

    // There is a problem, in that the workers won't have noticed they have been
    // stopped yet, because they will be blocking on a read of the event queue. They
    // will need to read (and process) an event before they can go to standby mode
    // again, and this won't happen if there are no events in the queue.
    // To help them clear out we'll send a bunch of null events down the queue.
    for (const worker of this.#workers) {
      // console.log(`Sending null event`)
      await schedulerForThisNode.enqueue_NoOperation(this.#groupQueue)
    }
  }

  async destroy() {
    if (VERBOSE||this.#debugLevel > 0) {
      console.log(`Scheduler2.destroy(${this.#groupQueue})`)
    }

    // Ask all the workers to disconnect from queues
    // for (const worker of this.#workers) {
    //   await worker.destroy()
    // }
    this.#workers = [ ] // Free up the workers

    if (this.#queueObject) {
      this.#queueObject.close()
    }

    // Make this scheduler unusable
    this.#state = Scheduler2.DESTROYED
  }

  async getStatus() {
    // if (VERBOSE) console.log(`Scheduler2.getStatus()`)
    await this._checkConnectedToQueue()

    // this.#transactionsInPastMinute.display('tx/sec')
    // this.#stepsPastMinute.display('steps/sec')

    const waiting = await this.#queueObject.queueLength(this.#groupQueue)
    const obj = {
      events: {
        waiting,
        processing: 0
      },
      workers: {
        total: this.#workers.length,
        running: 0,
        waiting: 0,
        shuttingDown: 0,
        standby: 0
      },
      stats: {
        transactionsInPastMinute: this.#transactionsInPastMinute.getStats().values,
        transactionsInPastHour: this.#transactionsInPastHour.getStats().values,
        transactionsOutPastMinute: this.#transactionsOutPastMinute.getStats().values,
        transactionsOutPastHour: this.#transactionsOutPastHour.getStats().values,
        stepsPastMinute: this.#stepsPastMinute.getStats().values,
        stepsPastHour: this.#stepsPastHour.getStats().values,
        enqueuePastMinute: this.#enqueuePastMinute.getStats().values,
        enqueuePastHour: this.#enqueuePastHour.getStats().values,
        dequeuePastMinute: this.#dequeuePastMinute.getStats().values,
        dequeuePastHour: this.#dequeuePastHour.getStats().values,
      }
    }

    // Check the status of the workers
    for (const worker of this.#workers) {
      switch (await worker.getState()) {
        case Worker2.INUSE:
          obj.workers.running++
          obj.events.processing++
          break

        case Worker2.WAITING:
          obj.workers.waiting++
          break

        case Worker2.SHUTDOWN:
          obj.workers.shuttingDown++
          break

        case Worker2.STANDBY:
          obj.workers.standby++
          break
      }
    }
    // if (VERBOSE) console.log(`getStatus returning`, obj)
    return obj
  }

  async dump() {
    await this._checkConnectedToQueue()
    // console.log(``)
    // console.log(``)
    console.log(``)
    console.log(`DUMP (${this.#groupQueue}):`)
    // console.log(`SCHEDULER`)
    // console.log(`---------`)
    console.log(`  - ${await this.#queueObject.queueLength(this.#groupQueue)} waiting events.`)

    // console.log(`${this.#waitingToRun.size()} steps waiting to run:`)
    // for (const job of this.#waitingToRun.items()) {
    //   console.log(`    - ${job.getInput()}`)
    // }
    console.log(`  - ${this.#workers.length} workers:`)
    for (let i = 0; i < this.#workers.length; i++) {
      let worker = this.#workers[i]
      console.log(`    worker #${worker.getId()}: ${worker.getState()}`)
    }
    // console.log(`Transaction cache:`)
    // TransactionCache.dump()
  }
}//- Scheduler2
