import { Queue } from './Queue'
import { Event } from './Event'
import dbTransactionType from '../../database/dbTransactionType'
import TxData from '../TxData'
import TransactionCache from './TransactionCache'
import TransactionIndexEntry from '../TransactionIndexEntry'//ZZZZ YARP2 replace this
import StepWorker from './StepWorker'
import { Job } from './Job'
import CallbackRegister from './CallbackRegister'
import GenerateHash from '../GenerateHash'

export const TICK_INTERVAL = 10


const ROOT_STEP_COMPLETE_CALLBACK = `rootStepComplete`
CallbackRegister.register(ROOT_STEP_COMPLETE_CALLBACK, async (data) => {
  // console.log(`*** Callback ${stepCallbackName}()`)
  // console.log(`data=`, data)

  // Pass control back to the Transaction
  await EVENT(TX_END, { txId: data.txId })

  // const tx = await TransactionCache.findTransaction(data.getTxId(), true)
  // console.log(`tx=`, tx.toString())
  // const obj = tx.asObject()
  // console.log(`obj=`, obj)

  // // Call the transaction's callback
  // await obj.callback(obj.callbackContext)
})

class Scheduler2 {
  #debugLevel

  /*
   *  External code only interacts with the scheduler via events.
   */
  #eventQueue // Queue<Event>

  /*
   *  Jobs (Transaction / Steps)
   */
  #running  // Currently running transactions (txId -> Job)
  #sleeping // Transactiions waiting for something (txId -> Job)
  #waitingToRun // Ready to run (Queue of Job)

  // /*
  //  *  The transaction cache holds transaction details, but only for a while.
  //  *  txId -> TransactionIndexEntry ZZZZ????
  //  */
  // #transactionCache

  // Worker threads
  #workers

  // Timeout handler for each clock tick
  #timeout
  #tickCounter
  #shutdown

  /**
   * Remember the response object for if the user wants long polling.
   */
  #responsesForSynchronousReturn //ZZZZ Separate this out

  constructor() {
    this.#debugLevel = 0
    this.#eventQueue = new Queue()
    this.#running = [ ]
    this.#sleeping = [ ]
    this.#waitingToRun = new Queue()
    // this.#transactionCache = [ ]

    // Allocate some StepWorkers
    this.#workers = [ ]
    const numWorkers = 1
    for (let i = 0; i < numWorkers; i++) {
      this.#workers.push(new StepWorker(`${i}`))
    }

    this.#timeout = null
    this.#tickCounter = 1
    this.#shutdown = false
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
   * @param {string} eventType
   * @param {object} data
   */
  async event(eventType, data) {
    if (this.#debugLevel > 1) {
      console.log(`\nQUEUE EVENT(${eventType})`, data)
    } else if (this.#debugLevel > 0) {
      console.log(`\nQUEUE EVENT(${eventType})`)
    }

    let obj
    if (data instanceof TxData) {
      obj = data.getData()
    } else if (typeof(data) === 'object') {
      obj = data
    } else {
      throw new Error('Scheduler2.event() - data parameter must be TxData or object')
    }

    // Validate the event data before adding the event to the queue.
    switch (eventType) {
      case TX_START:
        if (typeof(obj.metadata) !== 'object') throw new Error(`TX_START requires data.metadata`)
        if (typeof(obj.metadata.owner) !== 'string') { throw new Error(`Invalid metadata.owner`) }
        if (typeof(obj.metadata.nodeId) !== 'string') { throw new Error(`Invalid metadata.nodeId`) }
        if (typeof(obj.metadata.externalId) !== 'string') { throw new Error(`Invalid metadata.externalId`) }
        if (typeof(obj.metadata.transactionType) !== 'string') { throw new Error(`Invalid metadata.transactionType`) }
        if (typeof(obj.metadata.callback) !== 'string') { throw new Error(`Invalid metadata.callback`) }
        if (typeof(obj.metadata.callbackContext) !== 'object') { throw new Error(`Invalid metadata.callbackContext`) }
        if (typeof(obj.data) !== 'object') throw new Error(`TX_START requires data.data`)
        break

      case TX_END:
        if (typeof(obj.txId) !== 'string') { throw new Error(`Invalid txId`) }
        // if (typeof(obj.callback) !== 'string') { throw new Error(`Invalid callback`) }
        // if (typeof(obj.callbackContext) !== 'object') { throw new Error(`Invalid callbackContext`) }
        break

      case STEP_START:
        if (typeof(obj.txId) !== 'string') throw new Error(`STEP_START requires data.txId`)
        if (typeof(obj.stepId) !== 'string') throw new Error(`STEP_START requires data.stepId`)
        if (typeof(obj.parentStepId) !== 'string') throw new Error(`STEP_START requires data.parentStepId`)
        if (typeof(obj.fullSequencePrefix) !== 'string') throw new Error(`STEP_START requires data.fullSequencePrefix`)
        if (typeof(obj.definition) !== 'string') throw new Error(`STEP_START requires data.definition`)
        if (typeof(obj.callback) !== 'string') throw new Error(`STEP_START requires data.callback`)
        if (typeof(obj.callbackContext) !== 'object') throw new Error(`STEP_START requires data.callbackContext`)
        break

      case STEP_END:
        if (typeof(obj.txId) !== 'string') throw new Error(`STEP_END requires data.txId`)
        if (typeof(obj.stepId) !== 'string') throw new Error(`STEP_END requires data.stepId`)
        break
    }

    // Add to the event queue
    const event = new Event(eventType, obj)
    this.#eventQueue.enqueue(event)
  }

  /**
   *
   * @returns
   */
  async tick() {
    if (this.#debugLevel > 0) {
      console.log(``)
      console.log(``)
      console.log(`tick ${this.#tickCounter++}`)
    }

    // Prevent double-running
    if (this.#timeout) {
      clearTimeout(this.#timeout)
      this.#timeout = null
    }
    if (this.#shutdown) {
      console.log(`Scheduler2.tick() - scheduler already shut down`)
      return
    }

    // Now run a scheduler "clock tick"
    try {
      // Read events and update our queues and transaction register
      for ( ; ; ) {
        const event = this.#eventQueue.dequeue()
        if (!event) {
          break
        }
        const type = event.getType()
        switch (type) {
          case TX_START:
            if (this.#debugLevel > 0) {
              console.log(`*** PROCESS EVENT: TX_START - create a step event`)
            }
            await this.tx_start_event(event.getData())
            break

          case TX_END:
            if (this.#debugLevel > 0) {
              console.log(`*** PROCESS EVENT: TX_END - use the transaction's callback`)
            }
            await this.tx_end_event(event.getData())
            break

          case STEP_START:
            if (this.#debugLevel > 0) {
              console.log(`*** PROCESS EVENT: STEP_START - add to ready to run queue`)
            }
            await this.step_start_event(event.getData())
            break

          case STEP_END:
            if (this.#debugLevel > 0) {
              console.log(`*** PROCESS EVENT: STEP_END - call the step's callback`)
            }
            await this.step_end_event(event.getData())
            break

          // case
          default:
            throw new Error(`Unknown event type ${type}`)
        }
        break
      }
    } catch (e) {
      console.log(`Scheduler2: Fatal error processing event queue`, e)
    }

    // Look for waiting steps now ready to run - add them to the run queue

    // Delete old items in the transaction cache

    // Feed the workers
    try {
      for (let i = 0; i < this.#workers.length; i++) {
        const worker = this.#workers[i]
        if (!worker.inUse()) {
          switch (worker.getType()) {
            case 'step-worker':
              const job = this.#waitingToRun.dequeue()
              if (job) {
                console.log(`*** ALLOCATE JOB TO STEP WORKER`)
                worker.processJob(job)
              }
              break

            default:
              throw new Error(`Unknown worker type [${worker.getType()}`)
          }
        }
      }
    } catch (e) {
      console.log(`Scheduler2: Fatal error feeding workers`, e)
    }

    // Show debug stuff
    // await this.dump()
    // console.log(``)
    // console.log(``)

    // Wait a while, then check again
    // console.log(`Wait before next tick`)
    if (!this.#shutdown) {
      this.#timeout = setTimeout(() => {
        this.#timeout = null
        this.tick()
      }, TICK_INTERVAL)
    }

  }//- tick()


  /**
   * Stop the scheduler from processing ticks.
   */
  async shutdown() {
    if (this.#debugLevel > 0) {
      console.log(`Scheduler2.shutdown()`)
    }
    this.#shutdown = true
    if (this.#shutdown) {
      clearTimeout(this.#shutdown)
      this.#shutdown = null
    }
  }

  /**
   *
   * @param {TxData} input
   * @returns
   */
  async tx_start_event(input) {
    if (this.#debugLevel > 1) {
      console.log(`*** tx_start_event()`, input)
    }
    const metadata = input.metadata
    const data = input.data
    const initialTxData = new TxData(data)

    /*
     *  Two transaction types are provided for testing.
     */
    if (metadata.transactionType === 'ping1') {
      // console.log(`Scheduler - TX_START for ping1`)
      await CallbackRegister.call(metadata.callback, metadata.callbackContext)
      return
    }

    if (metadata.transactionType === 'ping2') {
      // console.log(`Scheduler - TX_START for ping2`)

      // Create a new transaction
      // metadata.pipeline = '---' // Never gets used
      const tx = await TransactionCache.newTransaction(metadata.owner, metadata.externalId)
      // console.log(`tx=`, tx)
      tx.delta(null, {
        callback: metadata.callback,
        callbackContext: metadata.callbackContext,
      })
      // console.log(`tx=`, (await tx).toString())
      this.event(TX_END, { txId: tx.getTxId() })
      return
    }

    // Create a version of the metadata that can be passed to steps
    const metadataCopy = JSON.parse(JSON.stringify(metadata))
    // console.log(`metadataCopy 1=`, metadataCopy)
    delete metadataCopy['transactionType']
    delete metadataCopy['owner']
    delete metadataCopy['externalId']
    delete metadataCopy['nodeId']
    delete metadataCopy['callback']
    delete metadataCopy['callbackContext']
    // console.log(`metadataCopy 2=`, metadataCopy)

    // Which pipeline should we use?
    const pipelineDetails = await dbTransactionType.getPipeline(metadata.transactionType)
    // console.log(`initiateTransaction() - pipelineDetails:`, pipelineDetails)
    if (!pipelineDetails) {
      throw new Error(`Unknown transaction type ${transactionType}`)
    }

    const pipelineName = pipelineDetails.pipelineName
    // console.log(`pipelineName=`, pipelineName)
    //ZZZZ How about the version?  Should we use name:version ?



    const tx = await TransactionCache.newTransaction(metadata.owner, metadata.externalId)
    tx.delta(null, {
      transactionType: metadata.transactionType,
      nodeId: metadata.nodeId,
      pipelineName,
      status: TransactionIndexEntry.RUNNING,//ZZZZ
      callback: metadata.callback,
      callbackContext: metadata.callbackContext,
      metadata: metadataCopy,
      data: initialTxData,
    })
    // console.log(`tx=`, (await tx).toString())


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
    const txId = tx.getTxId()
    const stepId = GenerateHash('s')

    await EVENT(STEP_START, {
      txId,
      stepId,
      parentStepId: '',
      fullSequencePrefix: txId.substring(txId.length - 8),
      definition: pipelineName,
      callback: ROOT_STEP_COMPLETE_CALLBACK,
      callbackContext: { txId, stepId }
    })
  }


  /**
   *
   * @param {TxData} input
   * @returns
   */
   async tx_end_event(input) {
    if (this.#debugLevel > 1) {
      console.log(`*** tx_end_event()`, input)
    }
    const tx = await TransactionCache.findTransaction(input.txId, true)
    const transactionData = tx.transactionData()
    await CallbackRegister.call(transactionData.callback, transactionData.callbackContext)
    return
  }


  /**
   *
   * @param {TxData} input
   */
  async step_start_event(input) {
    if (this.#debugLevel > 1) {
      console.log(`*** step_start_event()`, input)
    }

    const txId = input.txId
    // console.log(`txId=`, txId)
    const stepId = input.stepId
    const parentStepId = input.parentStepId
    const fullSequencePrefix = input.fullSequencePrefix
    const definition = input.definition
    // console.log(`definition=`, definition)
    const callback = input.callback
    const callbackContext = input.callbackContext

    const tx = await TransactionCache.findTransaction(txId, true)
    await tx.delta(stepId, {
      parentStepId,
      fullSequencePrefix,
      definition,
      callback,
      callbackContext,
    })

    // Start the step
    if (definition === 'misc/ping3') {
      // console.log(`Step definition is misc/ping3 - return without calling a step`)
      await EVENT(STEP_END, { txId, stepId })
      return
    }

    //ZZZZZZ
    console.log(`DO NOT KNOW HOW TO START A STEP YET`)
    // // console.log(`tx=`, tx)


    // // Add a Job to the ready queue

    // const job = new Job(input)
    // this.#waitingToRun.enqueue(job)
  }

  async step_end_event(input) {
    if (this.#debugLevel > 1) {
      console.log(`*** step_end_event()`, input)
    }

    const txId = input.txId
    const stepId = input.stepId
    const tx = await TransactionCache.findTransaction(txId, true)
    // const obj = tx.asObject()
    // console.log(`obj=`, obj)

    // const step = obj.steps[stepId]
    // console.log(`step=`, step)
    const stepData = tx.stepData(stepId)

    // Call the callback
    await CallbackRegister.call(stepData.callback, stepData.callbackContext)
  }

  async getStatus() {
    return {
      // bull: 'frog',
      events: this.#eventQueue.size(),
      waiting: this.#waitingToRun.size(),
    }
  }

  async dump() {
    // console.log(``)
    // console.log(``)
    // console.log(``)
    console.log(`---------`)
    // console.log(`SCHEDULER`)
    // console.log(`---------`)
    console.log(`${this.#eventQueue.size()} incoming events:`)
    for (const event of this.#eventQueue.items()) {
      console.log(`    - ${event.getType()}`)
    }
    console.log(`${this.#waitingToRun.size()} steps waiting to run:`)
    for (const job of this.#waitingToRun.items()) {
      console.log(`    - ${job.getInput()}`)
    }
    console.log(`Workers:`)
    for (let i = 0; i < this.#workers.length; i++) {
      let worker = this.#workers[i]
      console.log(`    worker #${worker.getId()}: ${worker.inUse() ? 'BUSY' : 'waiting'}`)
    }
    // console.log(`Transaction cache:`)
    TransactionCache.dump()
  }
}


const scheduler = new Scheduler2()
export default scheduler

export const TX_START = 'tx-start'
export const TX_PROGRESS = 'tx-progress'
export const TX_END = 'tx-end'
export const STEP_START = 'step-start'
export const STEP_END = 'step-end'
export const LONG_POLL = 'long-poll'


export const EVENT = (eventType, data) => scheduler.event(eventType, data)

// Only while debugging
export const TICK = () => scheduler.tick()
export const DUMP = () => scheduler.dump()
export const SHUTDOWN = () => scheduler.shutdown()
