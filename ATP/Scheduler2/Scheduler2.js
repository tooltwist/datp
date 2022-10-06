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
import { TX_COMPLETE_CALLBACK } from './txCompleteCallback'
import TransactionCache, { PERSIST_TRANSACTION_STATE } from './txState-level-1'
import Worker2, { GO_BACK_AND_RELEASE_WORKER } from './Worker2'
import assert from 'assert'
import { STEP_QUEUED, STEP_RUNNING, STEP_SUCCESS } from '../Step'
import { schedulerForThisNode } from '../..'
import StatsCollector from '../../lib/statsCollector'
import { DuplicateExternalIdError } from './TransactionPersistance'
import StepTypeRegister from '../StepTypeRegister'
import { getNodeGroup } from '../../database/dbNodeGroup'
import { appVersion, datpVersion, buildTime } from '../../build-version'
import { validateEvent_TransactionChange, validateEvent_TransactionCompleted } from './eventValidation'
import { SHORTCUT_STEP_START, SHORTCUT_STEP_COMPLETE, SHORTCUT_TX_COMPLETION, WORKER_CHECK_INTERVAL } from '../../datp-constants'
import { DUP_EXTERNAL_ID_DELAY, INCLUDE_STATE_IN_NODE_HOPPING_EVENTS } from '../../datp-constants'
import { getPipelineVersionInUse } from '../../database/dbPipelines'
import juice from '@tooltwist/juice-client'
import Transaction from './Transaction'
// import dbLogbook from '../../database/dbLogbook'
import LongPoll from './LongPoll'
import { MemoryEventQueue } from './MemoryEventQueue'
import { RedisQueue } from './queuing/RedisQueue-ioredis'
import me from '../../lib/me'
import pause from '../../lib/pause'

// Debug related
const VERBOSE = 0
const VERBOSE_16aug22 = 0
const Q_VERBOSE = 0
require('colors')

const YARPLUA_FOR_STEP_START = true


let txStartCounter = 0

export default class Scheduler2 {
  #debugLevel

  /*
   *  External code only interacts with the scheduler via events.
   */
  #nodeGroup // Group of this node
  #nodeId
  #description

  // Queue names
  #groupQueue
  #groupExpressQueue
  #nodeRegularQueue
  #nodeExpressQueue

  // In-memory queues
  #regularMemoryQueue
  #expressMemoryQueue

  // Worker threads
  #requiredWorkers // int
  #workers // Worker2[]

  // Event loop parameters
  #eventloopPause // Default time till we re-check the queues
  #eventloopPauseBusy // time if all workers are busy
  #eventloopPauseIdle // time if no workers are busy
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
  static SHUTDOWN_PHASE_1 = 'shutdown1' // Stop processing REDIS events
  static PHASE_1_DELAY = 20 * 1000
  static SHUTDOWN_PHASE_2 = 'shutdown2' // Move memory events to REDIS
  static PHASE_2_DELAY = 0 * 1000
  static SHUTDOWN_PHASE_3 = 'shutdown3' // Give running events time to complete
  static PHASE_3_DELAY = 20 * 1000
  static SHUTDOWN_PHASE_4 = 'shutdown4' // Report running workers
  static PHASE_4_DELAY = 0 * 1000
  static SHUTDOWN_PHASE_5 = 'shutdown5' // Shutdown

  // Event types
  static NULL_EVENT = 'no-op'
  static TRANSACTION_COMPLETED_EVENT = 'tx-completed'
  static TRANSACTION_CHANGE_EVENT = 'tx-changed'
  static STEP_START_EVENT = 'step-start'
  static STEP_COMPLETED_EVENT = 'step-end'
  static LONG_POLL = 'long-poll'

  // Queue prefixes
  static GROUP_QUEUE_PREFIX = 'group'
  static GROUP_EXPRESS_QUEUE_PREFIX = 'groupOut'
  static REGULAR_QUEUE_PREFIX = 'node'
  static EXPRESS_QUEUE_PREFIX = 'express'

  constructor(groupName, queueName=null, options= { }) {
    if (VERBOSE) console.log(`Scheduler2.constructor(${groupName}, ${queueName})`)

    this.#debugLevel = 0

    this.#nodeGroup = groupName
    this.#nodeId = GenerateHash('nodeId')
    this.#description = (options.description) ? options.description : this.#nodeGroup


    // Queues
    this.#groupQueue = Scheduler2.groupQueueName(groupName)
    this.#groupExpressQueue = Scheduler2.groupExpressQueueName(groupName)
    this.#nodeRegularQueue = Scheduler2.nodeRegularQueueName(groupName, this.#nodeId)
    this.#nodeExpressQueue = Scheduler2.nodeExpressQueueName(groupName, this.#nodeId)

    this.#regularMemoryQueue = new MemoryEventQueue()
    this.#expressMemoryQueue = new MemoryEventQueue()

    // Allocate some StepWorkers
    this.#workers = [ ]
    this.#requiredWorkers = 0


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

  /**
   *  This is the main entry point of the scheduler.
   */
  async start() {
    if (VERBOSE) console.log(`Scheduler2.start(${this.#groupQueue})`)

    if (this.#state === Scheduler2.RUNNING) {
      if (VERBOSE) console.log(`  - already running`)
      return
    }
    this.#state = Scheduler2.RUNNING

    // Start watching for 
    const redisLua = await RedisQueue.getRedisLua()
    await redisLua.listenToNotifications(async (type, txId, status) => {
      // console.log(`\n*\n*    VOG VOG VOG Transaction ${txId} has ${type}. Status=${status}\n*\n`)
      await LongPoll.tryToReply(txId)
    })

    // Handle Operating Systems signals so we can prepare for system shutdown.
    const me = this
    process.on('SIGTERM', () => me.shutdownPhase1('SIGTERM'))
    process.on('SIGINT', () => me.shutdownPhase1('SIGINT'))

    // Loop around getting events and passing them to the workers
    // We count how many workers are waiting for an event, then
    // hand out that many events before counting again.
    let lastCheck = -1 // Last time we check the required number of workers.
    const eventLoop = async () => {
      // if (VERBOSE) console.log(`######## Start event loop.`)

      // If we are shutting down, do not process any more events
      if (this.#state === Scheduler2.SHUTDOWN_PHASE_5) {
        // End of event loop
        console.log(`Event loop stopped.`)
        return
      }

      // If we haven't checked for a while (or ever) then make
      // sure we have the number of required workers available.
      const now = Date.now()
      if (lastCheck < 0 || now > lastCheck + WORKER_CHECK_INTERVAL) {
        if (VERBOSE) console.log(`Checking number of worker threads.`)

        // Get node group parameters from the database
        await this.loadNodeGroupParameters()

        // Create new workers if required
        let numToAllocate = this.#requiredWorkers - this.#workers.length
        if (VERBOSE && numToAllocate > 0) console.log(`Creating ${numToAllocate} new workers`)
        while (numToAllocate-- > 0) {
          const id = this.#workers.length
          const worker = new Worker2(id)
          this.#workers.push(worker)
        }
        lastCheck = now
      }
  
      // Create a list of all the workers waiting for an event
      const workersWaiting = [ ]
      for (const worker of this.#workers) {
        const state = await worker.getState()
        if (state === Worker2.WAITING) {
          workersWaiting.push(worker)
        }
      }

      // If no workers are available, wait a bit and try again
      const numBusy = this.#workers.length - workersWaiting.length
      let numberOfAvailableWorkers = this.#requiredWorkers - numBusy
      if (numberOfAvailableWorkers === 0) {
        if (VERBOSE) console.log(`No workers available - event loop sleeping for a while...`)
        setTimeout(eventLoop, this.#eventloopPauseBusy)
        return
      }
      // if (VERBOSE) console.log(`Need ${numberOfAvailableWorkers} events (${this.#workers.length} workers, require ${this.#requiredWorkers}, ${numBusy} busy)`)

      // Get events for these workers
      // We read from five queues, in the following priority:
      //  1. Local memory queue - regular events
      //  2. Local memory queue - express events
      //  3. Express REDIS queue for node (replies)
      //  4. Normal REDIS queue for node (steps)
      //  5. REDIS Queue for group (incoming transactions)

      let eventsStarted = 0
      let countWorker = 0
      if (this.processingEventsFromMemoryQueues()) {

        // Read from the express local memory queue first
        while (workersWaiting.length > 0 && this.#expressMemoryQueue.len() > 0) {
          // for ( ; countWorker < numberOfAvailableWorkers && this.#expressMemoryQueue.len() > 0; countWorker++) {
          if (Q_VERBOSE) console.log(` - got event from express memory queue`)
          // Process the event
          // Do NOT wait for it to complete. It will set it's state back to 'waiting' when it's ready.
          const event = this.#expressMemoryQueue.next()
          const worker = workersWaiting.pop()
          // console.log(`Scheduler2.start: have ${event.eventType} event`)
          worker.setInUse()
          setImmediate(() => {
            // Do not wait. The worker will set itself to INUSE and
            // then back to WAITING again once it is finished.
            worker.processEvent(event)
          })
          eventsStarted++
        }

        // Now try the regular memory queue
        while (workersWaiting.length > 0 && this.#regularMemoryQueue.len() > 0) {
          // for ( ; countWorker < numberOfAvailableWorkers && this.#regularMemoryQueue.len() > 0; countWorker++) {
          if (Q_VERBOSE) console.log(` - got event from regular memory queue`)
          // Process the event
          // Do NOT wait for it to complete. It will set it's state back to 'waiting' when it's ready.
          const event = this.#regularMemoryQueue.next()
          const worker = workersWaiting.pop()
          // console.log(`Scheduler2.start: have ${event.eventType} event`)
          worker.setInUse()
          setImmediate(() => {
            // Do not wait. The worker will set itself to INUSE and
            // then back to WAITING again once it is finished.
            worker.processEvent(event)
          })
          eventsStarted++
        }
      }

      if (this.processingEventsFromREDIS()) {

        // NEW LUA-based queueing
        if (workersWaiting.length > 0) {
          // let requiredEvents = numberOfAvailableWorkers - countWorker
        // if (requiredEvents > 0) {
          const required = workersWaiting.length
          const eventsFromLua = await redisLua.getEvents(this.#nodeGroup, required)
          if (eventsFromLua.length > 0) {
          //   console.log(`eventsFromLua=`, JSON.stringify(eventsFromLua, '', 2))
            if (Q_VERBOSE) console.log(` - got ${eventsFromLua.length} events from LUA queues`)
            for (const event of eventsFromLua) {
              // console.log(`EVENT FROM LUA =`, event)
              // console.log(`event.txState=`, event.txState.stringify())

              const worker = workersWaiting.pop()
              // console.log(`Scheduler2.start: have ${event.eventType} event`)
              worker.setInUse()
              setImmediate(() => {
                // Do not wait. The worker will set itself to INUSE and
                // then back to WAITING again once it is finished.
                worker.processEvent(event)
              })
              eventsStarted++
    
            }
            // await pause(5000)//ZZZZ
          }
        }




        // Now we read from the REDIS queues. These can be read from multiple queues in one call.
        // const requiredEvents = numberOfAvailableWorkers - countWorker
        // if (requiredEvents > 0) {
        if (workersWaiting.length > 0) {
          /*
          *  We've emptied the memory queues. Now look at external queues.
          */
          const queues = [
            this.#nodeExpressQueue,
            this.#nodeRegularQueue,
            this.#groupExpressQueue,
            this.#groupQueue
          ]
          const block = false
          // console.log(`${requiredEvents} = ${numberOfAvailableWorkers} - ${countWorker}`)
          const requiredEvents = workersWaiting.length
          const events = await RedisQueue.dequeue(queues, requiredEvents, block)
          // console.log(`events=`, events)
          eventsStarted += events.length

          // Pair up the workers and the events
          for (let i = 0; i < events.length; i++) {
          // for (let i = 0; countWorker < numberOfAvailableWorkers && i < events.length; i++, countWorker++) {

            // Process the event
            // Do NOT wait for it to complete. It will set it's state back to 'waiting' when it's ready.
            if (Q_VERBOSE) console.log(` - got event from REDIS queue`)

            const event = events[i]
            const worker = workersWaiting.pop()
            // console.log(`Scheduler2.start: have ${event.eventType} event`)
            worker.setInUse()
            setImmediate(() => {
              // Do not wait. The worker will set itself to INUSE and
              // then back to WAITING again once it is finished.
              worker.processEvent(event)
            })
            eventsStarted++
          }//- next event
        }
      }

      // Update our statistics
      this.#dequeuePastMinute.add(eventsStarted)
      this.#dequeuePastHour.add(eventsStarted)

      // Restart our event loop.
      // We use the timeout, so the stack can reset itself.
      if (eventsStarted === 0) {

        // Nothing was started.
        // We'll delay a bit longer than normal, so we don't go into a CPU intensive loop.
        setTimeout(eventLoop, this.#eventloopPauseIdle)
      } else {

        // Let's start all over again, because some workers may be available by now.
        // A tiny delay is required, to ensure processEvent has time to set the state away from WAITING.
        setTimeout(eventLoop, this.#eventloopPause)
      }

    }//- eventLoop()

    // Let's start the event loop in the background
    setTimeout(eventLoop, 0)
  }//- start

  /**
   * Phase 1 of shutdown - stop processing events from REDIS queues
   * 
   * @param {*} signal 
   */
  async shutdownPhase1(signal) {
    // We sometimes get multiple signals. Only respond to the first.
    console.log(`Received ${signal} signal.`)
    if (this.shuttingDown()) {
      return
    }
    console.log(``)
    console.log(`### SHUTDOWN PHASE 1 - Stopped processing events from REDIS queues`)
    this.#state = Scheduler2.SHUTDOWN_PHASE_1

    const me = this
    const eventsInMemoryQueues = this.#expressMemoryQueue.len() + this.#regularMemoryQueue.len()
    const delay = (eventsInMemoryQueues > 0) ? Scheduler2.PHASE_1_DELAY : 0
    setTimeout(async () => { await me.shutdownPhase2() }, delay)
  }//- shutdownPhase1

  /**
   * Phase 2 of shutdown - move memory queue events to REDIS
   */
  async shutdownPhase2() {
    console.log(``)
    console.log(`### SHUTDOWN PHASE 2 - Moving memory queue events to REDIS`)
    this.#state = Scheduler2.SHUTDOWN_PHASE_2

    // In the memory queue the transaction state is an object, so we'll
    // need to convert it to JSON before the event put in REDIS.
    const nodeGroup = schedulerForThisNode.getNodeGroup()
    const queueName = Scheduler2.groupExpressQueueName(nodeGroup)
    console.log(`  ${this.#expressMemoryQueue.len()} events in express memory queue.`)
    while (this.#expressMemoryQueue.len() > 0) {
      const event = this.#expressMemoryQueue.next()
      // console.log(`  express event=`, event)
      // console.log(`          1. event.txState=`, event.txState)
      event.txState = await event.txState.stringify()
      // console.log(`          2. event.txState=`, event.txState)
      console.log(`  - moving ${event.eventType} event for ${event.txId}`)
      await RedisQueue.enqueue(queueName, event)
    }
    console.log(`  ${this.#regularMemoryQueue.len()} events in regular memory queue.`)
    while (this.#regularMemoryQueue.len() > 0) {
      const event = this.#regularMemoryQueue.next()
      // console.log(`  express event=`, event)
      // console.log(`          1. event.txState=`, event.txState)
      event.txState = await event.txState.stringify()
      // console.log(`          2. event.txState=`, event.txState)
      console.log(`  - moving ${event.eventType} event for ${event.txId}`)
      await RedisQueue.enqueue(queueName, event)
    }    

    const me = this
    setTimeout(() => { me.shutdownPhase3() }, Scheduler2.PHASE_2_DELAY)
  }//- shutdown2

  /**
   * Phase 3 of shutdown - give running events time to complete
   */
  async shutdownPhase3() {
    console.log(``)
    console.log(`### SHUTDOWN PHASE 3 - Give running workers time to complete`)
    this.#state = Scheduler2.SHUTDOWN_PHASE_3
    // let i = 0
    // for (const worker of me.#workers) {
    //   const state = await worker.getState()
    //   console.log(`worker ${i++} - ${state}`)
    // }  

    // Wait a while, then report still-running transactions.
    const numStillRunning = await this.numberOfRunningWorkers()
    if (numStillRunning > 0) {
      const me = this
      console.log(`  - ${numStillRunning} workers are still running`)
      setTimeout(() => {me.shutdownPhase4()}, Scheduler2.PHASE_3_DELAY)
    } else {
      const me = this
      console.log(`  - No workers are running`)
      setTimeout(() => {me.shutdownPhase4()}, 0)
    }
  }//- shutdownPhase3

  /**
   * Phase 4 of shutdown - report still running steps
   */
  async shutdownPhase4() {
    console.log(``)
    console.log(`### SHUTDOWN PHASE 4 - Send a notification for still running steps`)
    console.log(`(Not implemented yet)`)


    let stillRunning = 0
    let i = 0
    for (const worker of this.#workers) {
      const state = await worker.getState()
      if (state !== Worker2.WAITING) {
        if (stillRunning++ === 0) console.log(`Workers still running:`)
        console.log(`worker ${i++} - ${state} - ${worker.getRecentTxId()}`)
      }
    }

    // On to the next phase
    const me = this
    setTimeout(() => { me.shutdownPhase5() }, Scheduler2.PHASE_4_DELAY)
  }//- shutdownPhase4

  /**
   * Phase 5 of shutdown - exit
   */
   async shutdownPhase5() {

    // Shut down this process now.
    const stillRunning = await this.numberOfRunningWorkers()
    if (stillRunning > 0) {
      console.log(``)
      console.log(`### SHUTDOWN PHASE 5 - Exit with ${stillRunning} workers still running.`)
      process.exit(1)
    } else {
      console.log(``)
      console.log(`### SHUTDOWN PHASE 5 - Exit with no incomplete workers.`)
      process.exit(0)
    }
  }//- shutdownPhase5

  shuttingDown() {
    return (
      this.#state === Scheduler2.SHUTDOWN_PHASE_1
      || this.#state === Scheduler2.SHUTDOWN_PHASE_2
      || this.#state === Scheduler2.SHUTDOWN_PHASE_3
      || this.#state === Scheduler2.SHUTDOWN_PHASE_4
      || this.#state === Scheduler2.SHUTDOWN_PHASE_5
    )
  }

  processingEventsFromREDIS() {
    return !this.shuttingDown()
  }

  processingEventsFromMemoryQueues() {
    // We keep processing from memory until shutdown phase 2
    return (
      !this.shuttingDown()
      ||
      this.#state !== Scheduler2.SHUTDOWN_PHASE_1
    )
  }

  async numberOfRunningWorkers() {
    let stillRunning = 0
    for (const worker of this.#workers) {
      const state = await worker.getState()
      if (state !== Worker2.WAITING) {
        stillRunning++
      }
    }
    return stillRunning
  }

  /**
   * This function gets called periodically, to allow this node
   * to keep itself registered within REDIS.
   */
  async keepAlive () {
    // console.log(`keepAlive()`)
    const info = await this.getCurrentNodeDetails({ withStats: true, withStepTypes: true })
    await RedisQueue.registerNodeInREDIS(this.#nodeGroup, this.#nodeId, info)
  }// - keepAlive

  async loadNodeGroupParameters() {
    if (VERBOSE) console.log(`loadNodeGroupParameters()`)

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
    // console.log(`  requiredWorkers=`, this.#requiredWorkers)
    // console.log(`  eventloopPause=`, this.#eventloopPause)
    // console.log(`  eventloopPauseBusy=`, this.#eventloopPauseBusy)
    // console.log(`  eventloopPauseIdle=`, this.#eventloopPauseIdle)
    // console.log(`  delayToEnterSlothMode=`, this.#delayToEnterSlothMode)

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
  }//- loadNodeGroupParameters

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

    console.log(`  (started ${Date.now() % 10000})`)

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
        const tx = await TransactionCache.newTransaction(metadata.owner, metadata.externalId, metadata.transactionType)
        const worker = null
        await CallbackRegister.call(tx, metadata.onComplete.callback, 0, fakeTransactionOutput, worker)
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
            // nodeId: schedulerForThisNode.getNodeId(),
            callback: metadata.onComplete.callback,
            context: metadata.onComplete.context
          },
          status: STEP_SUCCESS,
          transactionOutput: { whoopee: 'doo', description }
        }, 'startTransaction()')

        await PERSIST_TRANSACTION_STATE(tx)

        // console.log(`tx=`, (await tx).toString())
        const txInitNodeGroup = txData.nodeGroup
        const txInitNodeId = txData.nodeId
        // const queueName = Scheduler2.groupQueueName(input.metadata.nodeGroup)
        const workerForShortcut = null
        await schedulerForThisNode.schedule_TransactionCompleted(tx, txInitNodeGroup, txInitNodeId, workerForShortcut, {
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
      // const pipelineDetails = await getPipelineVersionInUse(pipelineName)
      // if (VERBOSE||trace) console.log(`Scheduler2.startTransaction() - pipelineDetails:`, pipelineDetails)
      // if (!pipelineDetails) {
      //   throw new Error(`Unknown transaction type ${metadata.transactionType}`)
      // }
      // const pipelineVersion = pipelineDetails.version
      // const pipelineNodeGroup = pipelineDetails.nodeGroup
      if (VERBOSE||trace) console.log(`Scheduler2.startTransaction() - Start pipeline ${pipelineName}:${pipelineVersion} in node group ${pipelineNodeGroup}.`)

      // If there is an externalId, there is concern that uniqueness cannot be guaranteed by
      // the database under some circumstances (distributed database nodes, and extremely
      // close repeated calls). To add an extra layer of protection we'll use a REDIS increment,
      // with an expiry of thirty seconds. The first INCR call will return 1, the second will
      // return 2, etc. This will continue until the key is removed after 30 seconds. By that
      // time the externalId should certainly be stored in the darabase.
      if (metadata.externalId) {
        const key = `externalId-${metadata.externalId}-${metadata.owner}`
        if (await RedisQueue.repeatEventDetection(key, DUP_EXTERNAL_ID_DELAY)) {
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
        // pipelineName: `${pipelineName}:${pipelineVersion}`,
        pipelineName,//ZZZZ Is this needed?
        status: TransactionIndexEntry.RUNNING,//ZZZZ
        metadata: metadataCopy,
        transactionInput: initialData,
        onComplete: {
          nodeGroup: schedulerForThisNode.getNodeGroup(),
          // nodeId: schedulerForThisNode.getNodeId(),
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

      await PERSIST_TRANSACTION_STATE(tx)


      // Generate a new ID for this step
      const txId = tx.getTxId()
      // const stepId = GenerateHash('s')

      const stepId = await tx.addInitialStep(pipelineName)
      await tx.delta(stepId, { vogAddedBy: 'Scheduler2.startTransaction()' }, 'pipelineStep.invoke()')/// Temporary - remove this

      // Update our statistics
      this.#transactionsInPastMinute.add(1)
      this.#transactionsInPastHour.add(1)
      // this.#enqueuePastMinute.add(1)
      // this.#enqueuePastHour.add(1)



console.log(`LKJHUHUHhhhhkjlha   tx.stepData(stepId)=`, tx.stepData(stepId))

      // console.log(`txId=`, txId)
      const fullSequence = txId.substring(3, 9)
      const vogPath = `${txId.substring(3, 9)}=${pipelineName}`
      const workerForShortcut = null
      const vog_event = {
        eventType: Scheduler2.STEP_START_EVENT,
        // Need either a pipeline or a nodeGroup
        yarpLuaPipeline: pipelineName,
        txId,
        stepId,
        parentNodeGroup: myNodeGroup,
        // parentNodeId: myNodeId,
        parentStepId: '',
        fullSequence,
        vogPath,
        stepDefinition: pipelineName,
        metadata: metadataCopy,
        data: initialData,
        level: 0,
      }

      const onComplete = {
        nodeGroup: myNodeGroup,
        // nodeId: myNodeId,
        callback: TX_COMPLETE_CALLBACK,
        context: { txId, stepId },
      }

      // Start a step for this pipeline
      const pipelineNodeGroup = 'zzzz'
      //VOGGY
      console.log(`--------------------------`)
      console.log(`VOGGY A - startTransaction`)
      console.log(`--------------------------`)


      //ZZZZZ Stuff to delete
      await tx.delta(stepId, {
        stepDefinition: pipelineName,
      })

      // const onComplete = {
      //   nodeGroup: myNodeGroup,
      //   // nodeId: myNodeId,
      //   callback: ROOT_STEP_COMPLETE_CALLBACK_ZZZ,
      //   context: { txId, stepId },
      // }
      const flowIndex = null
      await this.schedule_StepStart(tx,
        // pipelineNodeGroup, null,
        workerForShortcut, vog_event, stepId, onComplete, flowIndex)

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
   * @param {object} options
   */
  async schedule_StepStart(tx,
    // nodeGroupWhereStepRuns, nodeIdWhereStepRunsZZZZ,
    workerForShortcut, vog_event, stepId, onComplete, parentFlowIndex) {
    if (VERBOSE) console.log(`\n<<< schedule_StepStart(parentFlowIndex=${parentFlowIndex})`.green)
    // if (VERBOSE) console.log(`\n<<< schedule_StepStart(${nodeGroupWhereStepRuns})`.green)
    if (VERBOSE > 1) console.log(`schedule_StepStart event=`, vog_event)

    // Are we running a pipeline? If so, the message will be queued via REDIS,
    // otherwise we'll run it in this same node via the memory queues.
    const S = tx.stepData(stepId)
    // console.log(`S=`, S)
    const vogRunningAPipeline = !!S.vogPipeline
    console.log(`vogRunningAPipeline=`, vogRunningAPipeline)


    // const nodeGroupWhereStepRuns = tx.JnodeGroupWhereStepRuns(stepId)
    // assert(typeof(nodeGroupWhereStepRuns) === 'string')
    // assert(typeof(nodeIdWhereStepRuns) === 'string' || nodeIdWhereStepRuns === null)
    assert(typeof(workerForShortcut) !== 'undefined')
    assert(typeof(vog_event) === 'object' && vog_event != null)

    // const myNodeGroup = schedulerForThisNode.getNodeGroup()
    // const myNodeId = schedulerForThisNode.getNodeId()
    // const yarpLuaPipeline = options.yarpLuaPipeline ? options.yarpLuaPipeline : null


    // options = dataFromXDataOrObject(options, 'schedule_StepStart() - options parameter must be XData or object')
    // validateEvent_StepStart(vog_event)
    assert(vog_event.eventType === Scheduler2.STEP_START_EVENT)
    assert (typeof(vog_event.txId) === 'string')
    assert (typeof(vog_event.stepId) === 'string')
  
    assert (typeof(vog_event.fullSequence) === 'string')
    assert (typeof(vog_event.vogPath) === 'string')
    assert (typeof(vog_event.stepDefinition) !== 'undefined')
    assert (typeof(vog_event.data) === 'object')
    assert ( !(vog_event.data instanceof XData))
    assert (typeof(vog_event.metadata) === 'object')
    assert (typeof(vog_event.level) === 'number')
    assert(S.fullSequence)
    assert(S.vogPath)
    assert(S.stepDefinition) //RRRR
  
    // How to reply when complete
    assert (typeof(onComplete.nodeGroup) === 'string')
    assert (typeof(onComplete.callback) === 'string')
    assert (typeof(onComplete.context) === 'object')
  
    // Add a completionToken to the event, so we can check that the return EVENT is legitimate
    // const completionToken = GenerateHash('ctok')
    // onComplete.completionToken = completionToken

    // Remember the callback details, and do not pass to the step
    // const tx = await TransactionCache.getTransactionStateZ(options.txId)
    // console.log(`tx=`, tx)
    await tx.delta(null, {
      nextStepId: stepId, //ZZZZZ Choose a better field name
    }, 'Scheduler2.schedule_StepStart()')


    // // Add to an event queue
    // // const queue = await getQueueConnection()
    // // options.eventType = Scheduler2.STEP_START_EVENT
    // // console.log(`Adding ${options.eventType} event to queue ${queueName}`.brightGreen)
    // const event = {
    //   // Just what we need
    //   eventType: Scheduler2.STEP_START_EVENT,
    //   txId: options.txId,
    //   stepId,
    //   onComplete,
    //   // onComplete: options.onComplete,
    //   // completionToken: options.onComplete.completionToken
    //   // fromNodeId: this.#nodeId
    // }
    const childFlowIndex = tx.vog_flowRecordStepScheduled(stepId, vog_event.data, vog_event.vogPath, parentFlowIndex, onComplete)
    vog_event.flowIndex = childFlowIndex
    // onComplete.flowIndex = flowIndex
    // console.log(`tx.vog_getFlow()=`, tx.vog_getFlow())
    console.log(`stepStart vog_event=`, vog_event)

    await tx.delta(stepId, {
      vogPath: vog_event.vogPath,
      // Other information about the step
      // nodeGroup: nodeGroupWhereStepRuns,
      parentStepId: vog_event.parentStepId, // Is this needed?
      fullSequence: vog_event.fullSequence,
      stepDefinition: vog_event.stepDefinition,
      stepInput: vog_event.data,
      level: vog_event.level,
      status: STEP_QUEUED
    }, 'Scheduler2.schedule_StepStart()')
    // tx.vog_setStepCompletionHandler(stepId, flowIndex, onComplete.nodeGroup, onComplete.callback, onComplete.context, completionToken)

    await PERSIST_TRANSACTION_STATE(tx)

    // {
    //   console.log(`schedule_StepStart event=`, event)
    //   console.log(`schedule_StepStart txState=`, tx.stringify())
    //   // console.log(`schedule_StepStart txState=`, JSON.stringify(tx.asObject(), '', 2))
    // }

    // Update our statistics
    this.#stepsPastMinute.add(1)
    this.#stepsPastHour.add(1)
    this.#enqueuePastMinute.add(1)
    this.#enqueuePastHour.add(1)

    // console.log(`tx.asObject()=`, tx.asObject())
    
    if (!vogRunningAPipeline) {
    // if (nodeGroupWhereStepRuns === myNodeGroup && nodeIdWhereStepRuns === myNodeId) {

      /*
       *  The step will runs on this node.
       */
      if (SHORTCUT_STEP_START && workerForShortcut !== null) {

        // We won't use a queue - we'll continue to use the current worker.
        if (Q_VERBOSE) console.log(`YARP stepStart A - bypass queueing`)
        vog_event.txState = tx
        // console.log(`schedule_StepStart: vog_event=`, vog_event)
        const rv = await workerForShortcut.processEvent_StepStart(vog_event)
        assert(rv === GO_BACK_AND_RELEASE_WORKER)
      } else {

        // Place the event in this node's dedicated express memory queue.
        if (Q_VERBOSE) console.log(`YARP stepStart B - adding to local regular memory queue`)
        vog_event.txState = tx
        // console.log(`schedule_StepStart: vog_event=`, vog_event)
        this.#regularMemoryQueue.add(vog_event)
      }
    } else {

      // /*
      //  *  The step runs in a different group or a different node in this group.
      //  */
      // if (nodeGroupWhereStepRuns === myNodeGroup && nodeIdWhereStepRuns !== null) {

      //   // Put the event in a specific node's normal queue.
      //   const queueName = Scheduler2.nodeRegularQueueName(nodeGroupWhereStepRuns, nodeIdWhereStepRuns)
      //   if (Q_VERBOSE) console.log(`YARP stepStart C - adding to queue ${queueName}`)
      //   event.txState = await tx.stringify()
      //   // console.log(`schedule_StepStart: event=`, event)
      //   await RedisQueue.enqueue(queueName, event)
      // } else {

        // Put the event in the node group's queue
        // const queueName = Scheduler2.groupQueueName(nodeGroupWhereStepRuns)
        // if (Q_VERBOSE) console.log(`YARP stepStart D - adding to queue ${queueName}`)

        // if (YARPLUA_FOR_STEP_START) {
          const redisLua = await RedisQueue.getRedisLua()
          const pipelineName = vogRunningAPipeline ? S.vogPipeline : null
          // console.log(`\n\nPIPELINE IS ${yarpLuaPipeline}`)
          vog_event.txState = tx
          // console.log(`schedule_StepStart: vog_event=`, vog_event)
          redisLua.enqueue('in', pipelineName, vog_event)
        // } else {
        //   vog_event.txState = await tx.stringify()
        //   // console.log(`schedule_StepStart: vog_event=`, vog_event)
        //   await RedisQueue.enqueue(queueName, vog_event)
        // }

      // }
    }

    return GO_BACK_AND_RELEASE_WORKER
  }//- enqueue_StepStart

  /**
   * 
   * @param {string} nodeGroup 
   * @param {string} txId 
   * @param {string} stepId 
   */
  async enqueue_StepRestart(tx, nodeGroup, txId, stepId) {
    if (VERBOSE_16aug22) console.log(`${me()}: ********** RESTART STEP, ADD TO QUEUE`)
    if (VERBOSE) console.log(`\n<<< enqueue_StepRestart EVENT(${nodeGroup})`.green)
    assert(typeof(nodeGroup) === 'string')
    assert(typeof(txId) === 'string')
    assert(typeof(stepId) === 'string')

    const queueName = Scheduler2.groupQueueName(this.#nodeGroup)

    // Change the step status
    // const tx = await TransactionCache.getTransactionStateZ(txId)
    if (!tx) {
      throw new Error(`Unknown transaction ${txId}`)
    }
    if (VERBOSE_16aug22) tx.xoxYarp('Loaded by restart', stepId)
    // A quick sanity check...
    const stepData = tx.stepData(stepId)
    if (!stepData) {
      console.log(`\n\n\n\n\n\n`)
      console.log(`***********************`)
      console.log(`SERIOUS INTERNAL ERROR!!!!!`)
      console.log(`enqueue_StepRestart() is trying to set the status of a step, but`)
      console.log(`the transaction state does not know about that step (${stepId})`)
      console.log(`***********************`)
      console.log(`\n\n\n\n\n\n`)
    }

    // Save our changes to the transaction state
    await tx.delta(null, {
      status: STEP_RUNNING
    }, 'Scheduler2.enqueue_StepRestart()')
    await tx.delta(stepId, {
      status: STEP_QUEUED
    }, 'Scheduler2.enqueue_StepRestart()')
    await PERSIST_TRANSACTION_STATE(tx)

    // Add to the event queue
    // const queue = await getQueueConnection()
    const event = {
      eventType: Scheduler2.STEP_START_EVENT,
      txId,
      stepId,
    }
    event.txState = tx.stringify()
    await RedisQueue.enqueue(queueName, event)

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
   async schedule_StepCompleted(tx, nodeGroupOfParentZZZZZ, event, workerForShortcut=null) {
    // if (VERBOSE)
    console.log(`\n<<< schedule_StepCompleted()`.green, event)
    // console.log(`event=`, event)

    // Validate the event data
    // assert(typeof(nodeGroupOfParent) == 'string')
    // validateEvent_StepCompleted(event)
    assert(typeof(event) == 'object')
    assert(event.eventType === Scheduler2.STEP_COMPLETED_EVENT)
    assert(typeof(event.txId) === 'string')
    assert(typeof(event.flowIndex) === 'number')
    // assert (typeof(event.parentStepId) === 'string')
    // assert (typeof(event.stepId) === 'string')
    // assert (typeof(event.completionToken) === 'string')
  
    // DO NOT try to reply stuff
    assert (typeof(event.status) === 'undefined')
    assert (typeof(event.stepOutput) === 'undefined')

    // Get the node group of the parent, so we know where to run the
    // completion handler callback.
    // tx.vog_getParentFlowIndex(event.flowIndex)
    const flow = tx.vog_getFlowRecord(event.flowIndex)
    console.log(`flow=`, flow)
    const callbackNodeGroup = flow.onComplete.nodeGroup
    console.log(`callbackNodeGroup=`, callbackNodeGroup)
  
    // How to reply when complete
    // assert (typeof(event.onComplete) === 'object')
    // assert (typeof(event.onComplete.nodeGroup) === 'string')
    // assert (typeof(event.onComplete.callback) === 'string')
    // assert (typeof(event.onComplete.context) === 'object')
  

    const myNodeGroup = schedulerForThisNode.getNodeGroup()
    // const myNodeId = schedulerForThisNode.getNodeId()

    // Add the event to the queue
    // event.fromNodeId = this.#nodeId

    // tx.vog_flowRecordStepEnd(stepId, completionStatus, note, output)

    // Can we shortcut the reply, bypassing the queue?
    if (callbackNodeGroup === myNodeGroup) {

      /*
       *  The step completion callback runs on this node group. Run it here.
       */
      if (SHORTCUT_STEP_COMPLETE && workerForShortcut !== null) {

        // Complete the step without queueing, in the current worker thread
        if (Q_VERBOSE) console.log(`YARP stepCompleted - bypasing queues`)
        event.txState = tx
        const rv = await workerForShortcut.processEvent_StepCompleted(event)
        assert(rv === GO_BACK_AND_RELEASE_WORKER)
      } else {

        // Reply goes in this node's dedicated express queue.
        // queueName = Scheduler2.nodeExpressQueueName(myNodeGroup, myNodeId)
        if (Q_VERBOSE) console.log(`YARP stepCompleted - adding to local express memory queue`)
        event.txState = tx
        this.#expressMemoryQueue.add(event)
        // dbLogbook.bulkLogging(event.txId, pipelineStepId, [{ message: `step complete -> express ${queueName}`,  level: dbLogbook.LOG_LEVEL_TRACE, source: dbLogbook.LOG_SOURCE_SYSTEM }])
      }
    } else {

      /*
       *  The completion callback runs in a different group or a different node in this group.
       */
      // console.log(`nodeGroupOfParent=`, nodeGroupOfParent)
      // console.log(`nodeIdOfParent=`, nodeIdOfParent)
      // if (nodeGroupOfParent === myNodeGroup && nodeIdOfParent !== null) {

      //   // Send the reply to the specific node's express queue.
      //   const queueName = Scheduler2.nodeExpressQueueName(nodeGroupOfParent, nodeIdOfParent)
      //   // if (Q_VERBOSE)
      //   console.log(`YARP stepCompleted - adding to node queue ${queueName}`)
      //   event.txState = tx.stringify()
      //   await RedisQueue.enqueue(queueName, event)
      //   // dbLogbook.bulkLogging(event.txId, pipelineStepId, [{ message: `step complete -> express ${queueName}`,  level: dbLogbook.LOG_LEVEL_TRACE, source: dbLogbook.LOG_SOURCE_SYSTEM }])
      // } else {

        // Put the event in the node group's queue
        // const queueName = Scheduler2.groupQueueName(nodeGroupOfParent)


        /*VOG
        const queueName = Scheduler2.groupExpressQueueName(nodeGroupOfParent)
        if (Q_VERBOSE) console.log(`YARP stepCompleted - adding to group queue ${queueName}`)
        event.txState = tx.stringify()
        await RedisQueue.enqueue(queueName, event)
        // dbLogbook.bulkLogging(event.txId, pipelineStepId, [{ message: `step complete -> group ${queueName}`,  level: dbLogbook.LOG_LEVEL_TRACE, source: dbLogbook.LOG_SOURCE_SYSTEM }])
        VOG*/


        const redisLua = await RedisQueue.getRedisLua()
        const pipelineName = null
        // console.log(`\n\nPIPELINE IS ${yarpLuaPipeline}`)
        event.txState = tx
        // console.log(`schedule_StepCompleted: event=`, event)
        redisLua.enqueue('in', pipelineName, event)
      // }
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
  async schedule_TransactionCompleted(tx, txInitNodeGroup, txInitNodeId, workerForShortcut, event) {
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

    // Add to the event queue
    event.eventType = Scheduler2.TRANSACTION_COMPLETED_EVENT
    event.fromNodeId = this.#nodeId

    // Can we shortcut the reply, bypassing the queue?
    if (txInitNodeGroup === myNodeGroup && txInitNodeId === myNodeId) {

      /*
        *  The step will runs on this node.
        */
      if (SHORTCUT_TX_COMPLETION && workerForShortcut !== null) {

        // Complete the step without queueing, in the current worker thread
        // console.log(`Shortcutting TxCompleted !!!!!`)
        if (Q_VERBOSE) console.log(`YARP transactionCompleted - bypass queues`)
        event.txState = tx
        const rv = await workerForShortcut.processEvent_TransactionCompleted(event)
        assert(rv === GO_BACK_AND_RELEASE_WORKER)
        // console.log(`TX COMPLETE - BYPASSING QUEUEING`)
      } else {

        // Place the event in this node's dedicated express queue.
        // queueName = Scheduler2.nodeExpressQueueName(myNodeGroup, myNodeId)
        if (Q_VERBOSE) console.log(`YARP transactionCompleted - adding to express local memory queue`)
        event.txState = tx
        this.#expressMemoryQueue.add(event)
      }
    } else {

      // Put the event in a specific node's express queue.
      // console.log(`TX COMPLETE - SAVING STATE IN REDIS`)
      // console.log(`schedule_TransactionCompleted: ${event.txId} - queue event to different node`)
      const queueName = Scheduler2.nodeExpressQueueName(txInitNodeGroup, txInitNodeId)
      event.txState = tx.stringify()
      await RedisQueue.enqueue(queueName, event)
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
    event.fromNodeId = this.#nodeId
    await RedisQueue.enqueue(queueName, event)

    // Update our statistics
    this.#enqueuePastMinute.add(1)
    this.#enqueuePastHour.add(1)

    return GO_BACK_AND_RELEASE_WORKER
  }//- enqueue_TransactionChange

  async queueLength() {
    return await RedisQueue.queueLength(this.#groupQueue)
  }

  async queueLengths() {
    return await RedisQueue.queueLengths()
  }

  async drainQueue() {
    await RedisQueue.drainQueue(this.#groupQueue)
    await RedisQueue.drainQueue(this.#nodeRegularQueue)
    await RedisQueue.drainQueue(this.#nodeExpressQueue)
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
   * @param {String} nodeGroup
   * @returns Queue name
   */
   static groupExpressQueueName(nodeGroup) {
    assert(nodeGroup)
    return `${Scheduler2.GROUP_EXPRESS_QUEUE_PREFIX}:${nodeGroup}`
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
    const regularQueue = Scheduler2.nodeRegularQueueName(nodeGroup, nodeId)
    const expressQueue = Scheduler2.nodeExpressQueueName(nodeGroup, nodeId)
    const groupQueue = Scheduler2.groupQueueName(nodeGroup)
    let num = await RedisQueue.moveElementsToAnotherQueue(regularQueue, groupQueue)
    num += await RedisQueue.moveElementsToAnotherQueue(expressQueue, groupQueue)
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

    RedisQueue.close()

    // Make this scheduler unusable
    this.#state = Scheduler2.DESTROYED
  }

  async getDetailsOfActiveNodesfromREDIS(withStepTypes=false) {
    // if (VERBOSE) console.log(`Scheduler2.getDetailsOfActiveNodesfromREDIS(withStepTypes=${withStepTypes})`)
    return await RedisQueue.getDetailsOfActiveNodesfromREDIS(withStepTypes)
  }

  async getNodeDetailsFromREDIS(nodeGroup, nodeId) {
    // if (VERBOSE) console.log(`Scheduler2.getNodeDetailsFromREDIS(${nodeGroup}, ${nodeId})`)
    return await RedisQueue.getNodeDetailsFromREDIS(nodeGroup, nodeId)
  }

  async getCurrentNodeDetails({ withStats, withStepTypes }) {
    // if (VERBOSE) console.log(`Scheduler2.getStatus()`)
    // this.#transactionsInPastMinute.display('tx/sec')
    // this.#stepsPastMinute.display('steps/sec')

    // const waiting = await RedisQueue.queueLength(this.#groupQueue)
    const info = {
      nodeGroup: schedulerForThisNode.getNodeGroup(),
      nodeId: schedulerForThisNode.getNodeId(),
      appVersion,
      datpVersion,
      buildTime,
      events: {
        waiting: 987654321,
        processing: 0,
        expressMemoryQueue: this.#expressMemoryQueue.len(),
        regularMemoryQueue: this.#regularMemoryQueue.len(),
      },
      workers: {
        total: this.#workers.length,
        running: 0,
        waiting: 0,
        shuttingDown: 0,
        standby: 0,
        required: this.#requiredWorkers
      },
      stats: { },
      throughput: { },
      stepTypes: [ ],
    }//- info

    if (withStepTypes) {
      info.stepTypes = await StepTypeRegister.myStepTypes()
    }

    if (withStats) {
      info.stats = {
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
      info.throughput = { }
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
      info.transactionsInCache = -1
      info.outstandingLongPolls = await LongPoll.outstandingLongPolls()
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
    // if (VERBOSE) console.log(`getStatus returning`, info)
    return info
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
     await RedisQueue.setTemporaryValue(key, value, duration)
  }

  /**
   * Access a value saved using _setTemporaryValue_. If the expiry duration for
   * the temporary value has passed, null will be returned.
   *
   * @param {string} key
   * @returns The value saved using _setTemporaryValue_.
   */
  async getTemporaryValue(key) {
    return await RedisQueue.getTemporaryValue(key)
  }

  /**
   * @param {Transaction} transaction 
   * @param {number} persistAfter Time before it is written to long term storage (seconds)
   */
  async saveTransactionState_level1(tx) {
    // console.log(`Scheduler2.saveTransactionState_level1()`, tx.toJSON())
    await RedisQueue.saveTransactionState_level1(tx)
  }

  async getTransactionStateFromREDIS(txId) {
    // console.log(`Scheduler2.getTransactionStateFromREDIS(${txId})`)
    const tx = await RedisQueue.getTransactionState(txId)
    return tx
  }

  async cacheStats() {
    const q = await RedisQueue.queueStats()

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
    await RedisQueue.persistTransactionStatesToLongTermStorage()
  }


  async dump() {
    // console.log(``)
    // console.log(``)
    console.log(``)
    console.log(`DUMP (${this.#groupQueue}):`)
    // console.log(`SCHEDULER`)
    // console.log(`---------`)
    console.log(`  - ${await RedisQueue.queueLength(this.#groupQueue)} waiting events.`)

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
