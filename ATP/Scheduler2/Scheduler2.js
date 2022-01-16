// import { Queue } from './Queue'
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

// Debug related
const VERBOSE = 0
require('colors')


export const DEFAULT_QUEUE = ''

export default class Scheduler2 {
  #debugLevel

  /*
   *  External code only interacts with the scheduler via events.
   */
  #nodeGroup // Group of this node
  #fullQueueName // Name of the queue

  #myQueue

  // Worker threads
  #requiredWorkers
  #workers

  // Timeout handler for each clock tick
  // #timeout
  // #tickCounter
  #state

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


  /**
   * Remember the response object for if the user wants long polling.
   */
  #responsesForSynchronousReturn //ZZZZ Separate this out

  constructor(groupName, queueName=null, options= { }) {
    if (VERBOSE) console.log(`Scheduler2.constructor(${groupName}, ${queueName})`)

    this.#debugLevel = 0

    this.#nodeGroup = groupName
    this.#fullQueueName = Scheduler2.standardQueueName(groupName, queueName)
    this.#myQueue = null

    // Allocate some StepWorkers
    this.#requiredWorkers = (options.numWorkers ? options.numWorkers : 1)
    this.#workers = [ ]

    // this.#timeout = null
    // this.#tickCounter = 1
    this.#state = Scheduler2.STOPPED
  }

  async _checkConnectedToQueue() {
    // console.log(`_checkConnectedToQueue`)
    if (!this.#myQueue) {
      if (VERBOSE) console.log(`Getting a new connection`)
      // Get a connection to my queue
      this.#myQueue = await getQueueConnection()
    }
    if (VERBOSE) console.log(`this.#myQueue=`, this.#myQueue)
  }

  async start() {
    if (VERBOSE) console.log(`Scheduler2.start(${this.#fullQueueName})`)
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
      const worker = new Worker2(id, this.#nodeGroup, this.#fullQueueName)
      this.#workers.push(worker)
      await worker.start()
    }
    // console.log(`  - created ${this.#workers.length} workers`)
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
   static async startTransaction(input) {
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
      if (VERBOSE||trace) console.log(`*** Scheduler2.startTransaction()`.bgYellow.black, input)
      if (VERBOSE||trace) console.log(`*** Scheduler2.startTransaction() - transaction type is ${input.metadata.transactionType}`.bgYellow.black, input)

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
            callback: metadata.onComplete.callback,
            context: metadata.onComplete.context
          },
          status: STEP_SUCCESS,
          transactionOutput: { whoopee: 'doo', description }
        })
        // console.log(`tx=`, (await tx).toString())
        const queueName = Scheduler2.standardQueueName(input.metadata.nodeGroup, DEFAULT_QUEUE)
        Scheduler2.enqueue_TransactionCompleted(queueName, {
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
      if (VERBOSE||trace) console.log(`Scheduler2.startTransaction() - pipelineName:`.bgYellow.black, input, pipelineName)
      // console.log(`pipelineName=`, pipelineName)
      //ZZZZ How about the version?  Should we use name:version ?


      // Persist the transaction details
      const tx = await TransactionCache.newTransaction(metadata.owner, metadata.externalId, metadata.transactionType)
      const def = {
        transactionType: metadata.transactionType,
        nodeGroup: metadata.nodeGroup,
        nodeId: metadata.nodeGroup, //ZZZZ Set to current node
        pipelineName,
        status: TransactionIndexEntry.RUNNING,//ZZZZ
        metadata: metadataCopy,
        transactionInput: initialData,
        onComplete: {
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
      const nodeGroupWherePipelineRuns = metadata.nodeGroup
      const queueToPipelineNode = Scheduler2.standardQueueName(nodeGroupWherePipelineRuns, DEFAULT_QUEUE)

      // console.log(`metadataCopy=`, metadataCopy)
      // console.log(`data=`, data)
      if (VERBOSE||trace) console.log(`Scheduler2.startTransaction() - adding to queue ${queueToPipelineNode}`)
      // console.log(`txId=`, txId)
      const fullSequence = txId.substring(3, 9)
      // console.log(`fullSequence=`, fullSequence)
      await Scheduler2.enqueue_StepStart(queueToPipelineNode, {
        txId,
        stepId,
        parentNodeGroup: metadata.nodeGroup,
        parentStepId: '',
        fullSequence,
        stepDefinition: pipelineName,
        metadata: metadataCopy,
        data: initialData,
        level: 0,

        onComplete: {
          callback: ROOT_STEP_COMPLETE_CALLBACK,
          context: { txId, stepId },
          nodeGroup: metadata.nodeGroup,
        }
      })

      return tx
    } catch (e) {
      console.log(`DATP internal error in startTransaction()`, e)
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
  static async enqueue_StepStart(queueName, data) {
    if (VERBOSE) {
      console.log(`\n<<< enqueueStepStart EVENT(${queueName})`.green, data)
      console.log(new Error(`TRACE ONLY 1`).stack.magenta)

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

    // Add to the event queue
    const queue = await getQueueConnection()
    // obj.eventType = Scheduler2.STEP_START_EVENT
    // console.log(`Adding ${obj.eventType} event to queue ${queueName}`.brightGreen)
    await queue.enqueue(queueName, {
      // Just what we need
      eventType: this.STEP_START_EVENT,
      txId: obj.txId,
      stepId: obj.stepId,
      // onComplete: obj.onComplete,
      // completionToken: obj.onComplete.completionToken

    })
  }//- enqueueStepStart

  static async enqueue_StepRestart(queueName, txId, stepId) {
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
    const queue = await getQueueConnection()
    await queue.enqueue(queueName, {
      eventType: this.STEP_START_EVENT,
      txId,
      stepId
    })
  }//- enqueue_StepRestart


  /**
   *
   * @param {string} queueName
   * @param {object} event
   */
   static async enqueue_StepCompleted(queueName, event) {
    if (VERBOSE) {
      console.log(`\n<<< enqueue_StepCompleted(${queueName})`.green, event)
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
    const queue = await getQueueConnection()
    await queue.enqueue(queueName, event)
  }//- enqueue_StepCompleted


  /**
   *
   * @param {string} queueName
   * @param {object} event
   */
  static async enqueue_TransactionCompleted(queueName, event) {
    if (VERBOSE) {
      console.log(`\n<<< enqueue_TransactionCompleted(${queueName})`.green, event)
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
    const queue = await getQueueConnection()
    await queue.enqueue(queueName, event)
  }//- enqueue_TransactionCompleted


  /**
   *
   * @param {string} queueName
   * @param {object} event
   */
   static async enqueue_TransactionChange(queueName, event) {
    if (VERBOSE) {
      console.log(`\n<<< enqueue_TransactionChange(${queueName})`.green, event)
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
    const queue = await getQueueConnection()
    await queue.enqueue(queueName, event)
  }//- enqueue_TransactionCompleted


  /**
   *
   * @param {string} eventType
   * @param {object} data
   */
  static async enqueue_NoOperation(queueName) {
    if (VERBOSE) {
      console.log(`\n<<< enqueue_NoOperation(${queueName})`.green)
    }

    assert(typeof(queueName) === 'string')

    // Add to the event queue
    const event = { eventType: Scheduler2.NULL_EVENT }
    // console.log(`Adding ${event.eventType} event to queue ${queueName}`.brightGreen)
    const queue = await getQueueConnection()
    await queue.enqueue(queueName, event)
  }//- enqueue_NoOperation








  async queueLength() {
    await this._checkConnectedToQueue()
    return await this.#myQueue.queueLength(this.#fullQueueName)
  }

  async drainQueue() {
    await this._checkConnectedToQueue()
    return await this.#myQueue.drainQueue(this.#fullQueueName)
  }

  /**
   *
   * @param {String} nodeGroup
   * @param {String} queue
   * @returns The standard queue for a DATP node
   */
  static standardQueueName(nodeGroup, queue=null) {
    if (!nodeGroup) {
      throw new Error(`standardQueueName(): invalid nodeGroup [${nodeGroup}]`)
    }
    let name = `${nodeGroup}`
    if (queue) {
      name += `:${queue}`
    }
    // console.log(`list=`, list)
    return name
  }

  /**
   * Stop the scheduler from processing ticks.
   */
  async stop() {
    if (this.#debugLevel > 0) {
      console.log(`Scheduler2.shutdown()`)
    }
    this.#state = Scheduler2.STOPPED

    // Set all the workers to "shutting down" mode.
    // When they finish their next event they'll stop looking at the queue.
    for (const worker of this.#workers) {
      await worker.stop()
    }

    // There is a problem, in that the workers won't have noticed they have been
    // stopped yet, because they will be blocking on a read of the event queue. They
    // will need to read (and process) an event before they can go to standby mode
    // again, and this won't happen if there are no events in the queue.
    // To help them clear out we'll send a bunch of null events down the queue.
    for (const worker of this.#workers) {
      // console.log(`Sending null event`)
      await Scheduler2.enqueue_NoOperation(this.#fullQueueName)
    }
  }

  async destroy() {
    if (this.#debugLevel > 0) {
      console.log(`Scheduler2.destroy(${this.#fullQueueName})`)
    }

    // Ask all the workers to disconnect from queues
    for (const worker of this.#workers) {
      await worker.destroy()
    }
    this.#workers = [ ] // Free them up

    if (this.#myQueue) {
      this.#myQueue.close()
    }

    // Make this scheduler unusable
    this.#state = Scheduler2.DESTROYED
  }

  async getStatus() {
    // console.log(`Scheduler2.getStatus()`)
    await this._checkConnectedToQueue()

    const obj = {
      events: {
        waiting: await this.#myQueue.queueLength(this.#fullQueueName),
        processing: 0
      },
      workers: {
        total: this.#workers.length,
        running: 0,
        waiting: 0,
        shuttingDown: 0,
        standby: 0
      }
    }

    // Check the status of the workers
    for (const worker of this.#workers) {
      // console.log(`yah yarp 2`, worker)
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
    // console.log(`yah yarp 2`, obj)
    return obj
  }

  async dump() {
    await this._checkConnectedToQueue()
    // console.log(``)
    // console.log(``)
    console.log(``)
    console.log(`DUMP (${this.#fullQueueName}):`)
    // console.log(`SCHEDULER`)
    // console.log(`---------`)
    console.log(`  - ${await this.#myQueue.queueLength(this.#fullQueueName)} waiting events.`)

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
