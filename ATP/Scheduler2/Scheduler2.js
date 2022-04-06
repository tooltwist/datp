/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import GenerateHash from '../GenerateHash'
import TransactionIndexEntry from '../TransactionIndexEntry'
import XData, { dataFromXDataOrObject } from '../XData'
import CallbackRegister from './CallbackRegister'
import { getQueueConnection } from './queuing/Queue2'
import { ROOT_STEP_COMPLETE_CALLBACK } from './rootStepCompleteCallback'
import TransactionCache from './TransactionCache'
import Worker2, { GO_BACK_AND_RELEASE_WORKER } from './Worker2'
import assert from 'assert'
import { STEP_QUEUED, STEP_RUNNING, STEP_SUCCESS } from '../Step'
import { schedulerForThisNode } from '../..'
import StatsCollector from '../../lib/statsCollector'
import { DuplicateExternalIdError } from './TransactionPersistance'
import StepTypeRegister from '../StepTypeRegister'
import { getNodeGroup } from '../../database/dbNodeGroup'
import { appVersion, datpVersion, buildTime } from '../../build-version'
import { validateEvent_StepCompleted, validateEvent_StepStart, validateEvent_TransactionChange, validateEvent_TransactionCompleted } from './eventValidation'
import { SHORTCUT_STEP_START, SHORTCUT_STEP_COMPLETE, SHORTCUT_TX_COMPLETION } from '../../datp-constants'
import { DUP_EXTERNAL_ID_DELAY, INCLUDE_STATE_IN_NODE_HOPPING_EVENTS } from '../../datp-constants'
import { getPipelineVersionInUse } from '../../database/dbPipelines'
import juice from '@tooltwist/juice-client'
// import { A_VERY_LONG_TIME } from './queuing/RedisQueue-ioredis'
import Transaction from './Transaction'
import dbLogbook from '../../database/dbLogbook'
import { getSystemErrorMap } from 'util'

// Debug related
const VERBOSE = 0
require('colors')

let txStartCounter = 0

export default class Scheduler2 {
  #debugLevel

  /*
   *  External code only interacts with the scheduler via events.
   */
  #nodeGroup // Group of this node
  #nodeId
  #name
  #description

  // Queueing
  #queueObject
  #groupQueue // Name of the queue
  #nodeRegularQueue
  #nodeExpressQueue

  // Worker threads
  #requiredWorkers
  #workers

  // Event loop parameters
  #eventloopPauseBusy
  #eventloopPauseIdle
  #eventloopPause
  #delayToEnterSlothMode
  #timeSinceLastEvent

  // Options for debug levels
  #debugScheduler
  #debugWorkers
  #debugSteps
  #debugPipelines
  #debugRouters
  #debugLongpolling
  #debugWebhooks
  #debugTransactions
  #debugTransactionCache
  #debugRedis
  #debugDb


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
  static TRANSACTION_COMPLETED_EVENT = 'tx-completed'
  static TRANSACTION_CHANGE_EVENT = 'tx-changed'
  static STEP_START_EVENT = 'step-start'
  static STEP_COMPLETED_EVENT = 'step-end'
  static LONG_POLL = 'long-poll'

  // Queue prefixes
  static GROUP_QUEUE_PREFIX = 'group'
  static REGULAR_QUEUE_PREFIX = 'node'
  static EXPRESS_QUEUE_PREFIX = 'express'

  constructor(groupName, queueName=null, options= { }) {
    if (VERBOSE) console.log(`Scheduler2.constructor(${groupName}, ${queueName})`)

    this.#debugLevel = 0

    this.#nodeGroup = groupName
    this.#nodeId = GenerateHash('nodeId')
    this.#name = (options.name) ? options.name : this.#nodeGroup
    this.#description = (options.description) ? options.description : this.#nodeGroup


    // Queues
    this.#groupQueue = Scheduler2.groupQueueName(groupName)
    this.#nodeRegularQueue = Scheduler2.nodeRegularQueueName(groupName, this.#nodeId)
    this.#nodeExpressQueue = Scheduler2.nodeExpressQueueName(groupName, this.#nodeId)
    this.#queueObject = null

    // Allocate some StepWorkers
    this.#workers = [ ]

    // this.#timeout = null
    // this.#tickCounter = 1
    this.#state = Scheduler2.STOPPED

    // console.log(`Have scheduler for node ${this.#nodeId}`)
    // console.log(`  #groupQueue=`, this.#groupQueue)
    // console.log(`  #nodeRegularQueue=`, this.#nodeRegularQueue)
    // console.log(`  #nodeExpressQueue=`, this.#nodeExpressQueue)

    // Statistics collection
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

    // Initialize with no debug
    this.#debugScheduler = 0
    this.#debugWorkers = 0
    this.#debugSteps = 0
    this.#debugPipelines = 0
    this.#debugRouters = 0
    this.#debugLongpolling = 0
    this.#debugWebhooks = 0
    this.#debugTransactions = 0
    this.#debugTransactionCache = 0
    this.#debugRedis = 0
    this.#debugDb = 0
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
      // Get a connection to my queue
      if (VERBOSE) console.log(`Getting a new connection`)
      this.#queueObject = await getQueueConnection()
    }
  }

  async start() {
    if (VERBOSE) console.log(`Scheduler2.start(${this.#groupQueue})`)
    await this._checkConnectedToQueue()

    if (this.#state === Scheduler2.RUNNING) {
      if (VERBOSE) console.log(`  - already running`)
      return
    }
    this.#state = Scheduler2.RUNNING

    // Get node group parameters from the database
    await this.loadNodeGroupParameters()

    // Allocate and start workers
    if (VERBOSE) console.log(`Creating ${this.#requiredWorkers} workers`)
    while (this.#workers.length < this.#requiredWorkers) {
      const id = this.#workers.length
      const worker = new Worker2(id)
      this.#workers.push(worker)
    }
    // console.log(`  - created ${this.#workers.length} workers`)


    // Loop around getting events and passing them to the workers
    // We count how many workers are waiting for an event, then
    // hand out that many events before counting again.
    const eventLoop = async () => {
      // if (VERBOSE) console.log(`######## Start event loop.`)

      // Create a list of all the workers waiting for an event
      const workersWaiting = [ ]
      for (const worker of this.#workers) {
        const state = await worker.getState()
        if (state === Worker2.WAITING) {
          workersWaiting.push(worker)
        }
      }

      // If no workers are available, wait a bit and try again
      if (workersWaiting.length === 0) {
        // if (VERBOSE)
        // console.log(`All workers are busy - Event loop sleeping a bit`)
        setTimeout(eventLoop, this.#eventloopPauseBusy)
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

      // const l0 = await this.#queueObject.queueLength(queues[0])
      // const l1 = await this.#queueObject.queueLength(queues[1])
      // const l2 = await this.#queueObject.queueLength(queues[2])
      // if (l0 || l1 || l2) {
      //   console.log(`exp/node/grp= ${l0} ${l1} ${l2}`)
      // }

      const events = await this.#queueObject.dequeue(queues, numEvents, block)
      // console.log(`events=`, events)
      if (events.length === 0) {
        // if (VERBOSE) console.log(`Event queue is empty - Event loop sleeping a bit (${this.#eventloopPauseIdle})`)
        setTimeout(eventLoop, this.#eventloopPauseIdle)
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
        worker.setInUse()
        setImmediate(() => {
          // Do not wait. The worker will set itself to INUSE and
          // then back to WAITING again once it is finished.
          worker.processEvent(event)
        })
      }

      // Let's start all over again, because some workers may be available by now.
      // We use the timeout, so the stack can reset itself.
      // A tiny delay is required, to ensure processEvent has time to set the
      // state away from WAITING.
      setTimeout(eventLoop, this.#eventloopPause)

    }

    // Let's start the event loop in the background
    setTimeout(eventLoop, 0)
  }

  /**
   * This function gets called periodically, to allow this node
   * to keep itself registered within REDIS.
   */
  async keepAlive () {
    // console.log(`keepAlive()`)

    // if (VERBOSE) console.log(`Scheduler2.getStatus()`)
    await this._checkConnectedToQueue()

    // this.#transactionsInPastMinute.display('tx/sec')
    // this.#stepsPastMinute.display('steps/sec')

    const waiting = await this.#queueObject.queueLength(this.#groupQueue)

    const info = {
      appVersion,
      datpVersion,
      buildTime,
      stepTypes: await StepTypeRegister.myStepTypes(),
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
      throughput: { }
      // stats: {
      //   transactionsInPastMinute: this.#transactionsInPastMinute.getStats().values,
      //   transactionsInPastHour: this.#transactionsInPastHour.getStats().values,
      //   transactionsOutPastMinute: this.#transactionsOutPastMinute.getStats().values,
      //   transactionsOutPastHour: this.#transactionsOutPastHour.getStats().values,
      //   stepsPastMinute: this.#stepsPastMinute.getStats().values,
      //   stepsPastHour: this.#stepsPastHour.getStats().values,
      //   enqueuePastMinute: this.#enqueuePastMinute.getStats().values,
      //   enqueuePastHour: this.#enqueuePastHour.getStats().values,
      //   dequeuePastMinute: this.#dequeuePastMinute.getStats().values,
      //   dequeuePastHour: this.#dequeuePastHour.getStats().values,
      // }
      
    }

    // transactions in per second
    {
      const arr = this.#transactionsInPastMinute.getStats().values
      const l = arr.length
      const total = arr[l-1] + arr[l-2] + arr[l-3] + arr[l-4] + arr[l-5]
      info.throughput.txInSec = Math.round(total / 5)
    }

    // transactions out per second
    {
      const arr = this.#transactionsOutPastMinute.getStats().values
      const l = arr.length
      const total = arr[l-1] + arr[l-2] + arr[l-3] + arr[l-4] + arr[l-5]
      info.throughput.txOutSec = Math.round(total / 5)
    }

    // steps per second
    {
      const arr = this.#stepsPastMinute.getStats().values
      const l = arr.length
      const total = arr[l-1] + arr[l-2] + arr[l-3] + arr[l-4] + arr[l-5]
      info.throughput.stepsSec = Math.round(total / 5)
    }

    // Check the status of the workers
    for (const worker of this.#workers) {
      switch (await worker.getState()) {
        case Worker2.INUSE:
          info.workers.running++
          info.events.processing++
          break

        case Worker2.WAITING:
          info.workers.waiting++
          break

        case Worker2.SHUTDOWN:
          info.workers.shuttingDown++
          break

        case Worker2.STANDBY:
          info.workers.standby++
          break
      }
    }

    await this.#queueObject.registerNode(this.#nodeGroup, this.#nodeId, info)
  }

  async loadNodeGroupParameters() {
    const group = await getNodeGroup(this.#nodeGroup)
    // console.log(`this.#nodeGroup=`, this.#nodeGroup)
    // console.log(`group=`, group)

    if (!group) {
      // Node group is not in the database
      console.log(`Fatal error: node group '${this.#nodeGroup}' is not defined in the database.`)
      console.log(`This error is too dangerous to contine. Shutting down now.`)
      process.exit(1)
    }
    this.#requiredWorkers = group.numWorkers
    this.#eventloopPause = group.eventloopPause
    this.#eventloopPauseBusy = group.eventloopPauseBusy
    this.#eventloopPauseIdle = group.eventloopPauseIdle
    this.#delayToEnterSlothMode = group.delayToEnterSlothMode
    console.log(`  requiredWorkers=`, this.#requiredWorkers)
    console.log(`  eventloopPause=`, this.#eventloopPause)
    console.log(`  eventloopPauseBusy=`, this.#eventloopPauseBusy)
    console.log(`  eventloopPauseIdle=`, this.#eventloopPauseIdle)
    console.log(`  delayToEnterSlothMode=`, this.#delayToEnterSlothMode)

    this.#delayToEnterSlothMode = group.delayToEnterSlothMode

    this.#debugScheduler = group.debugScheduler
    this.#debugWorkers = group.debugWorkers
    this.#debugSteps = group.debugSteps
    this.#debugPipelines = group.debugPipelines
    this.#debugRouters = group.debugRouters
    this.#debugLongpolling = group.debugLongpolling
    this.#debugWebhooks = group.debugWebhooks
    this.#debugTransactions = group.debugTransactions
    this.#debugTransactionCache = group.debugTransactionCache
    this.#debugRedis = group.debugRedis
    this.#debugDb = group.debugDb
  }

  async getDebugScheduler() { return this.#debugScheduler }
  async getDebugWorkers() { return this.#debugWorkers }
  async getDebugSteps() { return this.#debugSteps }
  async getDebugPipelines() { return this.#debugPipelines }
  async getDebugRouters() { return this.#debugRouters }
  async getDebugLongpolling() { return this.#debugLongpolling }
  async getDebugWebhooks() { return this.#debugWebhooks }
  async getDebugTransactions() { return this.#debugTransactions }
  async getDebugTransactionCache() { return this.#debugTransactionCache }
  async getDebugRedis() { return this.#debugRedis }
  async getDebugDb() { return this.#debugDb }

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
        const worker = null
        await CallbackRegister.call(metadata.onComplete.callback, metadata.onComplete.context, fakeTransactionOutput, worker)
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
        }, 'startTransaction()')
        // console.log(`tx=`, (await tx).toString())
        const txInitNodeGroup = txData.nodeGroup
        const txInitNodeId = txData.nodeId
        // const queueName = Scheduler2.groupQueueName(input.metadata.nodeGroup)
        const workerForShortcut = null
        await schedulerForThisNode.schedule_TransactionCompleted(txInitNodeGroup, txInitNodeId, workerForShortcut, {
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
      const pipelineName = metadata.transactionType
      const pipelineDetails = await getPipelineVersionInUse(pipelineName)
      if (VERBOSE||trace) console.log(`Scheduler2.startTransaction() - pipelineDetails:`, pipelineDetails)
      if (!pipelineDetails) {
        throw new Error(`Unknown transaction type ${metadata.transactionType}`)
      }
      const pipelineVersion = pipelineDetails.version
      const pipelineNodeGroup = pipelineDetails.nodeGroup
      if (VERBOSE||trace) console.log(`Scheduler2.startTransaction() - Start pipeline ${pipelineName}:${pipelineVersion} in node group ${pipelineNodeGroup}.`)

      // If there is an externalId, there is concern that uniqueness cannot be guaranteed by
      // the database under some circumstances (distributed database nodes, and extremely
      // close repeated calls). To add an extra layer of protection we'll use a REDIS increment,
      // with an expiry of thirty seconds. The first INCR call will return 1, the second will
      // return 2, etc. This will continue until the key is removed after 30 seconds. By that
      // time the externalId should certainly be stored in the darabase.
      if (metadata.externalId) {
        await this._checkConnectedToQueue()
        const key = `externalId-${metadata.externalId}-${metadata.owner}`
        if (await this.#queueObject.repeatEventDetection(key, DUP_EXTERNAL_ID_DELAY)) {
          // A transaction already exists with this externalId
          console.log(`Scheduler2.startTransaction: detected duplicate externalId via REDIS`)
          throw new DuplicateExternalIdError()
        }
      }
      const myNodeGroup = schedulerForThisNode.getNodeGroup()
      const myNodeId = schedulerForThisNode.getNodeId()


      // Persist the transaction details
      let tx = await TransactionCache.newTransaction(metadata.owner, metadata.externalId, metadata.transactionType)

      const def = {
        transactionType: metadata.transactionType,
        // Where the transaction initiated. We must come back here for longpoll.
        nodeGroup: myNodeGroup,
        nodeId: myNodeId,
        // The root pipeline / transactionType.
        pipelineName: `${pipelineName}:${pipelineVersion}`,
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
      await tx.delta(null, def, 'Scheduler2.startTransaction()')

      // Generate a new ID for this step
      const txId = tx.getTxId()
      const stepId = GenerateHash('s')

      // Get the name of the queue to the node where this pipeline will run
      // const nodeGroupWherePipelineRuns = metadata.nodeGroup // Should come from the pipeline definition
      // const queueToPipelineNode = Scheduler2.groupQueueName(nodeGroupWherePipelineRuns)

      // If this pipeline runs in a different node group, we'll start it via the group
      // queue for that nodeGroup. If the pipeline runs in the current node group, we'll
      // run it in this current node, so it'll have access to the cached transaction.




      // let queueToPipelineNode
      // if (nodeGroupWherePipelineRuns === myNodeGroup) {
      //   // Run the new pipeline in this node - put the event in this node's pipeline.
      //   queueToPipelineNode = Scheduler2.nodeRegularQueueName(myNodeGroup, myNodeId)
      // } else {
      //   // The new pipeline will run in a different nodeGroup. Put the event in the group queue.
      //   await this.saveTransactionStateToREDIS(tx, A_VERY_LONG_TIME)
      //   queueToPipelineNode = Scheduler2.groupQueueName(nodeGroupWherePipelineRuns)
      // }

      // console.log(`metadataCopy=`, metadataCopy)
      // console.log(`data=`, data)
      // if (VERBOSE||trace)

// console.log(`Scheduler2.startTransaction() - adding to queue for group '${nodeGroupWherePipelineRuns}'.`)
// console.log(`txStartCounter=`, txStartCounter++)

      // Update our statistics
      this.#transactionsInPastMinute.add(1)
      this.#transactionsInPastHour.add(1)
      this.#enqueuePastMinute.add(1)
      this.#enqueuePastHour.add(1)

      // console.log(`txId=`, txId)
      const fullSequence = txId.substring(3, 9)
      const workerForShortcut = null
      await this.schedule_StepStart(pipelineNodeGroup, null, workerForShortcut, {
        txId,
        stepId,
        parentNodeGroup: myNodeGroup,
        parentNodeId: myNodeId,
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

      return tx
    } catch (e) {
      // if (!(e instanceof DuplicateExternalIdError)) {
      // console.log(`DATP internal error in startTransaction()`, e)
      // }
      throw e
    }
  }//- TRANSACTION_START

  /**
   * How step replying works
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
  async schedule_StepStart(nodeGroupWhereStepRuns, nodeIdWhereStepRuns, workerForShortcut, data) {
    if (VERBOSE) console.log(`\n<<< schedule_StepStart(${nodeGroupWhereStepRuns}, ${nodeIdWhereStepRuns})`.green)
    //   if (VERBOSE > 1) console.log(`data=`, data)
    assert(typeof(nodeGroupWhereStepRuns) === 'string')
    assert(typeof(nodeIdWhereStepRuns) === 'string' || nodeIdWhereStepRuns === null)
    assert(typeof(workerForShortcut) !== 'undefined')
    assert(typeof(data) === 'object' && data != null)

    const myNodeGroup = schedulerForThisNode.getNodeGroup()
    const myNodeId = schedulerForThisNode.getNodeId()
    let bypassQueue = false
    let hoppingToDifferentNode = false
    let queueName

    // Can we shortcut the reply, bypassing the queue?
    if (nodeGroupWhereStepRuns === myNodeGroup && nodeIdWhereStepRuns === myNodeId) {

      /*
       *  The step will runs on this node.
       */
      if (SHORTCUT_STEP_START && workerForShortcut !== null) {

        // We won't use a queue - we'll continue to use the current worker.
        bypassQueue = true
      } else {

        // Place the event in this node's dedicated express queue.
        queueName = Scheduler2.nodeRegularQueueName(myNodeGroup, myNodeId)
      }
    } else {

      /*
       *  The step runs in a different group or a different node in this group.
       */
      if (nodeGroupWhereStepRuns === myNodeGroup && nodeIdWhereStepRuns !== null) {

        // Put the event in a specific node's normal queue.
        hoppingToDifferentNode = true
        queueName = Scheduler2.nodeRegularQueueName(nodeGroupWhereStepRuns, nodeIdWhereStepRuns)
      } else {

        // Put the event in the node group's queue
        hoppingToDifferentNode = true
        queueName = Scheduler2.groupQueueName(nodeGroupWhereStepRuns)
      }
    }



    // if (nodeGroupWhereStepRuns !== myNodeGroup || placeOnGroupQueue) {

    //   // The new pipeline will run in a different nodeGroup. Save the transaction
    //   // state to REDIS and put the event in the group's queue.
    //   workerForShortcut = null
    //   hoppingToDifferentNode = true
    //   queueName = Scheduler2.groupQueueName(nodeGroupWhereStepRuns)

    // } else {

    //   // This step needs to run in the current node group, so we'll run it in this node.
    //   queueName = Scheduler2.nodeRegularQueueName(myNodeGroup, myNodeId)
    // }

    // console.log(`queueName=`, queueName)
    // return await this.enqueue_StepStart(queueName, data, workerForShortcut)
  // }

  // async enqueue_StepStart(queueName, data, workerForShortcut=null) {
  //   if (VERBOSE) console.log(`\n<<< enqueue_StepStart EVENT(${queueName})`.green)
  //   if (VERBOSE > 1) console.log(`data=`, data)
    // if (queueName.startsWith(Scheduler2.GROUP_QUEUE_PREFIX)) {
    //   console.log(`////////////////////////////////////////////`.magenta)
    //   console.log(`enqueue_StepStart with group queue ${queueName}`.magenta)
    //   console.log(new Error('for debug').stack.magenta)
    //   console.log(`////////////////////////////////////////////`.magenta)
    // }

    const obj = dataFromXDataOrObject(data, 'schedule_StepStart() - data parameter must be XData or object')
    validateEvent_StepStart(obj)

    // Add a completionToken to the event, so we can check that the return EVENT is legitimate
    obj.onComplete.completionToken = GenerateHash('ctok')

    // Remember the callback details, and do not pass to the step
    const tx = await TransactionCache.getTransactionState(obj.txId)
    // console.log(`tx=`, tx)
    await tx.delta(null, {
      nextStepId: obj.stepId, //ZZZZZ Choose a better field name
    }, 'Scheduler2.schedule_StepStart()')
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
      status: bypassQueue ? STEP_RUNNING : STEP_QUEUED
    }, 'Scheduler2.schedule_StepStart()')
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
      // fromNodeId: this.#nodeId
    }

    // Update our statistics
    this.#stepsPastMinute.add(1)
    this.#stepsPastHour.add(1)
    this.#enqueuePastMinute.add(1)
    this.#enqueuePastHour.add(1)
    
    // console.log(`QUEing event to start step`)
    if (bypassQueue) {

      // Start the step without queueing, in the current worker thread.
      // console.log(`Shortcutting StepStart !!!!!`)
      const rv = await workerForShortcut.processEvent_StepStart(event)
      assert(rv === GO_BACK_AND_RELEASE_WORKER)
    } else {

      // Queue this for execution
      if (hoppingToDifferentNode) {
        // await TransactionCache.moveToGlobalCache(obj.txId)

        // // Check the transaction state is no longer in memory
        // const yarpTx = await TransactionCache.getTransactionState(obj.txId, false, false)
        // // if (yarpTx) {
        // //   console.trace(`\n\n\n\n\n STILL IN MEMORY CACHE AFTER BEING MOVED!!!!!!`)
        // //   console.log(`After jumping server, ${obj.txId}:`, yarpTx ? yarpTx.asObject() : 'NULL')
        // //   await TransactionCache.removeFromCache(obj.txId)
        // //   const yarpTx2 = await TransactionCache.getTransactionState(obj.txId, false, false)
        // //   console.log(`After remove, ${obj.txId}:`, yarpTx2 ? yarpTx2.asObject() : 'NULL')
        // //   system.exit(1)
        // // }
        // TransactionCache.removeFromCache(obj.txId)
        // TransactionCache.assertNotInCache(obj.txId)

        // Need to include the transaction state in the event
        if (INCLUDE_STATE_IN_NODE_HOPPING_EVENTS) {
          event.state = tx.asJSON()
        }
      }
      await this._checkConnectedToQueue()
      await this.#queueObject.enqueue(queueName, event)
    }
    return GO_BACK_AND_RELEASE_WORKER
  }//- enqueue_StepStart

  /**
   * 
   * @param {string} nodeGroup 
   * @param {string} txId 
   * @param {string} stepId 
   */
  async enqueue_StepRestart(nodeGroup, txId, stepId) {
    if (VERBOSE) {
      console.log(`\n<<< enqueue_StepRestart EVENT(${nodeGroup})`.green)
    }
    assert(typeof(nodeGroup) === 'string')
    assert(typeof(txId) === 'string')
    assert(typeof(stepId) === 'string')

    const queueName = Scheduler2.groupQueueName(this.#nodeGroup)

    // Change the step status
    const tx = await TransactionCache.getTransactionState(txId)
    if (!tx) {
      throw new Error(`Unknown transaction ${txId}`)
    }
    await tx.delta(null, {
      status: STEP_RUNNING
    }, 'Scheduler2.enqueue_StepRestart()')
    await tx.delta(stepId, {
      status: STEP_QUEUED
    }, 'Scheduler2.enqueue_StepRestart()')

    // Add to the event queue
    // const queue = await getQueueConnection()
    const event = {
      eventType: Scheduler2.STEP_START_EVENT,
      txId,
      stepId,
      // fromNodeId: this.#nodeId
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
   *     // Tell the parent we've completed.
    // If the parent is in the node group of this node, then we assume that
    // it was invoked from within this node.
    // We keep the steps all running on the same node as their pipellines, so they all
    // use the same cached transaction. We only jump to another node when we are calling a
    // pipline that runs on another node.

   *
   * @param {string} queueName
   * @param {object} event
   */
   async schedule_StepCompleted(nodeGroupOfParent, nodeIdOfParent, tx, event, workerForShortcut=null) {
    if (VERBOSE) console.log(`\n<<< schedule_StepStart(${nodeGroupOfParent})`.green)

    // Validate the event data
    assert(typeof(nodeGroupOfParent) == 'string')
    assert(typeof(nodeIdOfParent) == 'string')
    validateEvent_StepCompleted(event)


    const myNodeGroup = schedulerForThisNode.getNodeGroup()
    const myNodeId = schedulerForThisNode.getNodeId()
    let bypassQueue = false
    let hoppingToDifferentNode = false
    let queueName

    // Can we shortcut the reply, bypassing the queue?
    if (nodeGroupOfParent === myNodeGroup && nodeIdOfParent === myNodeId) {

      /*
       *  The step completion callback runs on this node.
       */
      if (SHORTCUT_STEP_COMPLETE && workerForShortcut !== null) {

        // We won't use a queue - we'll continue to use the current worker.
        bypassQueue = true
        // dbLogbook.bulkLogging(event.txId, pipelineStepId, [{ message: `step complete shortcut`,  level: dbLogbook.LOG_LEVEL_TRACE, source: dbLogbook.LOG_SOURCE_SYSTEM }])
      } else {

        // Reply goes in this node's dedicated express queue.
        queueName = Scheduler2.nodeExpressQueueName(myNodeGroup, myNodeId)
        // dbLogbook.bulkLogging(event.txId, pipelineStepId, [{ message: `step complete -> express ${queueName}`,  level: dbLogbook.LOG_LEVEL_TRACE, source: dbLogbook.LOG_SOURCE_SYSTEM }])
      }
    } else {

      /*
       *  The completion callback runs in a different group or a different node in this group.
       */
      if (nodeGroupOfParent === myNodeGroup && nodeIdOfParent !== null) {

        // Send the reply to the specific node's express queue.
        hoppingToDifferentNode = true
        queueName = Scheduler2.nodeExpressQueueName(nodeGroupOfParent, nodeIdOfParent)
        // dbLogbook.bulkLogging(event.txId, pipelineStepId, [{ message: `step complete -> express ${queueName}`,  level: dbLogbook.LOG_LEVEL_TRACE, source: dbLogbook.LOG_SOURCE_SYSTEM }])
      } else {

        // Put the event in the node group's queue
        hoppingToDifferentNode = true
        queueName = Scheduler2.groupQueueName(nodeGroupOfParent)
        // dbLogbook.bulkLogging(event.txId, pipelineStepId, [{ message: `step complete -> group ${queueName}`,  level: dbLogbook.LOG_LEVEL_TRACE, source: dbLogbook.LOG_SOURCE_SYSTEM }])
      }
    }



    // if (nodeGroupOfParent === myNodeGroup) {
    //   // Run the new pipeline in this node - put the event in this node's pipeline.
    //   // This might be run in this current worker thread
    //   queueName = Scheduler2.nodeRegularQueueName(myNodeGroup, myNodeId)
    // } else {

    //   // The completion handler runs in a different nodeGroup.
    //   // Save the trasaction state and put the event in the group queue.
    //   workerForShortcut = null
    //   await TransactionCache.moveToGlobalCache(tx.txId)
    //   queueName = Scheduler2.groupQueueName(nodeGroupOfParent)
    // }

  //   return await this.enqueue_StepCompleted(queueName, event, workerForShortcut)
  // }

  //  async enqueue_StepCompleted(queueName, event, workerForShortcut=null) {
    if (VERBOSE) {
      console.log(`\n Queue=(${queueName}, bypassQueue=${bypassQueue}, hoppingToDifferentNode=${hoppingToDifferentNode})`.green)
      if (VERBOSE > 1) console.log(`event=`, event)
    }
    // if (queueName.startsWith(Scheduler2.GROUP_QUEUE_PREFIX)) {
    //   // console.log(`////////////////////////////////////////////`.magenta)
    //   console.log(`enqueue_StepCompleted with group queue ${queueName}`.magenta)
    //   // console.log(new Error('for debug').stack.magenta)
    //   // console.log(`////////////////////////////////////////////`.magenta)
    // }

    // Add the event to the queue
    event.eventType = Scheduler2.STEP_COMPLETED_EVENT
    event.fromNodeId = this.#nodeId
    // console.log(`Adding ${event.eventType} event to queue ${queueName}`.brightGreen)
    // const queue = await getQueueConnection()

    if (bypassQueue) {

      // Complete the step without queueing, in the current worker thread
      // console.log(`Shortcutting StepCompleted !!!!!`)
      const rv = await workerForShortcut.processEvent_StepCompleted(event)
      assert(rv === GO_BACK_AND_RELEASE_WORKER)
    } else {

      // Queue this for execution
      if (hoppingToDifferentNode) {
        // dbLogbook.bulkLogging(event.txId, pipelineStepId, [{ message: `step complete -> TX TO REDIS`,  level: dbLogbook.LOG_LEVEL_TRACE, source: dbLogbook.LOG_SOURCE_SYSTEM }])
        // TransactionCache.moveToGlobalCache(event.txId)

        // Need to include the transaction state in the event
        if (INCLUDE_STATE_IN_NODE_HOPPING_EVENTS) {
          event.state = tx.asJSON()
        }

      } else {
        // dbLogbook.bulkLogging(event.txId, pipelineStepId, [{ message: `step complete -> NOT SAVOING STATE`,  level: dbLogbook.LOG_LEVEL_TRACE, source: dbLogbook.LOG_SOURCE_SYSTEM }])
      }
      await this._checkConnectedToQueue()
      await this.#queueObject.enqueue(queueName, event)
    }

    // Update our statistics
    this.#enqueuePastMinute.add(1)
    this.#enqueuePastHour.add(1)

    return GO_BACK_AND_RELEASE_WORKER
  }//- enqueue_StepCompleted


  /**
   *
   * @param {string} nodeGroup mandatory
   * @param {string} nodeId mandatory
   * @param {object} event
   */
  async schedule_TransactionCompleted(txInitNodeGroup, txInitNodeId, workerForShortcut, event) {
    if (VERBOSE) {
      console.log(`\n<<< schedule_TransactionCompleted(${txInitNodeGroup}, ${txInitNodeId})`.green, event)
    }

    // Validate the event data
    assert(typeof(txInitNodeGroup) === 'string')
    assert(typeof(txInitNodeId) === 'string')
    // assert(typeof(event) === 'object' && event != null)
    validateEvent_TransactionCompleted(event)

    const myNodeGroup = schedulerForThisNode.getNodeGroup()
    const myNodeId = schedulerForThisNode.getNodeId()
    let bypassQueue = false
    let hoppingToDifferentNode = false
    let queueName

    // Can we shortcut the reply, bypassing the queue?
    if (txInitNodeGroup === myNodeGroup && txInitNodeId === myNodeId) {

      /*
        *  The step will runs on this node.
        */
      if (SHORTCUT_TX_COMPLETION && workerForShortcut !== null) {

        // We won't use a queue - we'll continue to use the current worker.
        bypassQueue = true
        // console.log(`TX COMPLETE - BYPASSING QUEUEING`)
      } else {

        // Place the event in this node's dedicated express queue.
        queueName = Scheduler2.nodeExpressQueueName(myNodeGroup, myNodeId)
      }
    } else {

      // Put the event in a specific node's express queue.
      // console.log(`TX COMPLETE - SAVING STATE IN REDIS`)
      hoppingToDifferentNode = true
      // console.log(`schedule_TransactionCompleted: ${event.txId} - queue event to different node`)
      queueName = Scheduler2.nodeExpressQueueName(txInitNodeGroup, txInitNodeId)
    }
    


    // if (queueName.startsWith(Scheduler2.GROUP_QUEUE_PREFIX)) {
    //   // console.log(`////////////////////////////////////////////`.magenta)
    //   console.log(`schedule_TransactionCompleted with group queue ${queueName}`.magenta)
    //   // console.log(new Error('for debug').stack.magenta)
    //   // console.log(`////////////////////////////////////////////`.magenta)
    // }


    // Add to the event queue
    event.eventType = Scheduler2.TRANSACTION_COMPLETED_EVENT
    event.fromNodeId = this.#nodeId
    // console.log(`Adding ${event.eventType} event to queue ${queueName}`.brightGreen)
    // const queue = await getQueueConnection()


    if (bypassQueue) {

      // Complete the step without queueing, in the current worker thread
      // console.log(`Shortcutting TxCompleted !!!!!`)
      const rv = await workerForShortcut.processEvent_TransactionCompleted(event)
      assert(rv === GO_BACK_AND_RELEASE_WORKER)
    } else {

      // Queue the transaction-complete event
      if (hoppingToDifferentNode) {
        // TransactionCache.moveToGlobalCache(event.txId)

        // Need to include the transaction state in the event
        if (INCLUDE_STATE_IN_NODE_HOPPING_EVENTS) {
          event.state = tx.asJSON()
        }
      }
      await this._checkConnectedToQueue()
      await this.#queueObject.enqueue(queueName, event)
    }

    // Update our statistics
    this.#transactionsOutPastMinute.add(1)
    this.#transactionsOutPastHour.add(1)
    this.#enqueuePastMinute.add(1)
    this.#enqueuePastHour.add(1)

    return GO_BACK_AND_RELEASE_WORKER
  }//- schedule_TransactionCompleted


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
    validateEvent_TransactionChange(event)

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

    return GO_BACK_AND_RELEASE_WORKER
  }//- enqueue_TransactionChange


  // /**
  //  *
  //  * @param {string} eventType
  //  * @param {object} data
  //  */
  // async enqueue_NoOperation(queueName) {
  //   if (VERBOSE) {
  //     console.log(`\n<<< enqueue_NoOperation(${queueName})`.green)
  //   }

  //   assert(typeof(queueName) === 'string')

  //   // Add to the event queue
  //   const event = {
  //     eventType: Scheduler2.NULL_EVENT,
  //     fromNodeId: this.#nodeId
  //   }
  //   // console.log(`Adding ${event.eventType} event to queue ${queueName}`.brightGreen)
  //   // const queue = await getQueueConnection()
  //   await this._checkConnectedToQueue()
  //   await this.#queueObject.enqueue(queueName, event)

  //   // Update our statistics
  //   this.#enqueuePastMinute.add(1)
  //   this.#enqueuePastHour.add(1)
  // }//- enqueue_NoOperation








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
    await this.#queueObject.drainQueue(this.#groupQueue)
    await this.#queueObject.drainQueue(this.#nodeRegularQueue)
    await this.#queueObject.drainQueue(this.#nodeExpressQueue)
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
   * If a node dies, there will be noone to process the events in it's queues.
   * This function moves the events from it's regular and express queues over
   * to the group queue so they can be processed by another node.
   *
   * @param {*} nodeGroup
   * @param {*} nodeId
   */
  async handleOrphanQueues(nodeGroup, nodeId) {
    console.log(`handleOrphanQueues(${nodeGroup}, ${nodeId})`)
    this._checkConnectedToQueue()
    const regularQueue = Scheduler2.nodeRegularQueueName(nodeGroup, nodeId)
    const expressQueue = Scheduler2.nodeExpressQueueName(nodeGroup, nodeId)
    const groupQueue = Scheduler2.groupQueueName(nodeGroup)
    let num = await this.#queueObject.moveElementsToAnotherQueue(regularQueue, groupQueue)
    num += await this.#queueObject.moveElementsToAnotherQueue(expressQueue, groupQueue)
    return num
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
    // for (const worker of this.#workers) {
    //   // console.log(`Sending null event`)
    //   await schedulerForThisNode.enqueue_NoOperation(this.#groupQueue)
    // }
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

    await this._checkConnectedToQueue()
    if (this.#queueObject) {
      this.#queueObject.close()
    }

    // Make this scheduler unusable
    this.#state = Scheduler2.DESTROYED
  }

  async getDetailsOfActiveNodes(withStepTypes=false) {
    // if (VERBOSE) console.log(`Scheduler2.getDetailsOfActiveNodes(withStepTypes=${withStepTypes})`)
    await this._checkConnectedToQueue()
    return await this.#queueObject.getDetailsOfActiveNodes(withStepTypes)
  }

  async getNodeDetails(nodeGroup, nodeId) {
    // if (VERBOSE) console.log(`Scheduler2.getNodeDetails(${nodeGroup}, ${nodeId})`)
    await this._checkConnectedToQueue()
    await this.#queueObject.getNodeDetails(nodeGroup, nodeId)
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

  /**
   * Store a value for _duration_ seconds. During this period the
   * value can be accessed using _getTemporaryValue_. This is commonly used
   * with the following design pattern to cache slow-access information.
   * ```javascript
   * const value = await getTemporaryValue(key)
   * if (!value) {
   *    value = await get_value_from_slow_location()
   *    await setTemporaryValue(key, value, EXOPIRY_TIME_IN_SECONDS)
   * }
   * ```
   * @param {string} key
   * @param {string}} value
   * @param {number} duration Expiry time in seconds
   */
   async setTemporaryValue(key, value, duration) {
     this._checkConnectedToQueue()
     await this.#queueObject.setTemporaryValue(key, value, duration)
  }

  /**
   * Access a value saved using _setTemporaryValue_. If the expiry duration for
   * the temporary value has passed, null will be returned.
   *
   * @param {string} key
   * @returns The value saved using _setTemporaryValue_.
   */
  async getTemporaryValue(key) {
    this._checkConnectedToQueue()
    return await this.#queueObject.getTemporaryValue(key)
  }

  /**
   * @param {Transaction} transaction 
   * @param {number} persistAfter Time before it is written to long term storage (seconds)
   */
  async saveTransactionStateToREDIS(transaction, persistAfter) {
    // console.log(`Scheduler2.saveTransactionStateToREDIS(transaction, persistAfter=${persistAfter})`)
    // console.log(`typeof(transaction)=`, typeof(transaction))
    await this._checkConnectedToQueue()
    await this.#queueObject.saveTransactionState(transaction, persistAfter)
  }

  async getTransactionStateFromREDIS(txId) {
    // console.log(`Scheduler2.getTransactionStateFromREDIS(${txId})`)
    await this._checkConnectedToQueue()
    const tx = await this.#queueObject.getTransactionState(txId)
    return tx
  }

  async cacheStats() {
    const q = await this.#queueObject.queueStats()

    const statePersistanceInterval = await juice.integer('datp.statePersistanceInterval', 0)

    const stats = {
      ...q,
      // transactionStates: q.transactionStates,
      // transactionStatesPending: q.transactionStatesPending,
      // transactionStatesPending: q.transactionStatesPending,
      statePersistanceInterval,
    }
    return stats
  }

  /**
   * Called periodically to shift transaction states from REDIS to the database.
   */
  async persistTransactionStatesToLongTermStorage() {
    // console.log(`persistTransactionStatesToLongTermStorage()`)
    await this._checkConnectedToQueue()
    await this.#queueObject.persistTransactionStatesToLongTermStorage()
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
