import TxData from './TxData'
import TransactionIndexEntry from './TransactionIndexEntry'
import Scheduler from './Scheduler'
import Step from './Step'
import pause from './lib/pause'
import pad from './lib/pad'
import statusString from './lib/statusString'
import ResultReceiver from './ResultReceiver'
import ResultReceiverRegister from './ResultReceiverRegister'
import GenerateHash from "./GenerateHash"


// Hardcoded steps
import DummyStep from './hardcoded/DummyStep'
import SaySomethingStep from './hardcoded/SaySomethingStep'
import Pipeline from './hardcoded/PipelineStep'
import MockStep from './hardcoded/MockStep'
import RandomDelayStep from './hardcoded/RandomDelayStep'
import ExampleStep from './hardcoded/ExampleStep'
import DemoStep from './hardcoded/DemoStep'
import dbTransactionInstance from '../database/dbTransactionInstance'
import dbTransactionType from '../database/dbTransactionType'
import colors from 'colors'

const ANTI_BRUTE_FORCE_DELAY = 2000 // 2 seconds



/*
 *  ResultReceiver for when a transaction is completed.
 */
const ATP_TRANSACTION_COMPLETION_HANDLER_NAME = 'transaction-completion-handler'


/**
 * This ResultReceiver is called when the transaction is complete. It persists
 * our transaction details (i.e. updates the database) before calling the
 * ResultReceiver for the code that called ATP.initiateTransaction() to
 * start the transaction.
 */
class TransactionCompletionHandler extends ResultReceiver {
  constructor() {
    super()
  }
  async haveResult(contextForCompletionHandler, status, note, response) {
    console.log(`<<<<    TransactionCompletionHandler.haveResult()  `.white.bgBlue.bold)
    console.log(`  contextForCompletionHandler=`, JSON.stringify(contextForCompletionHandler, '', 0))
    // console.log(`  - status=`, status)
    console.log(`  response=`, JSON.stringify(response, '', 0))
    // Scheduler.dumpSteps(`\nAfter Completion`)

    // Update the transaction status
    const txId = contextForCompletionHandler.txId


    // console.log(`\n\nkkkkk kkkkk kkkkk kkkkk kkkkk kkkkk kkkkk kkkkk kkkkk kkkkk kkkkk kkkkk kkkkk `)
    console.log(`FINISHED ${txId}`)
    // console.log(`this.#transactionIndex=`, this.#transactionIndex)
    await defaultATE.dumpTransactions('wHEN HAVE RESULT')



    // console.log(`//////////////////////////////////////////////////`)
    // console.log(`txId=`, txId)
    // console.log(`defaultATE=`, defaultATE)
    const txEntry = await defaultATE.getTransactionEntry(txId)
    if (!txEntry) {
      const msg = `INTERNAL ERROR IN TransactionCompletionHandler.haveResult: txEntry not found (${txId})`
      console.log(msg)
      throw new Error(msg)
    }
    // console.log(`txEntry=`, txEntry)
    let transactionStatus
    switch (status) {
      case Step.COMPLETED:
        transactionStatus = TransactionIndexEntry.COMPLETE
        break

      default:
        transactionStatus = TransactionIndexEntry.UNKNOWN
        break
    }
    txEntry.setStatus(transactionStatus)


    // Persist the transaction
    await dbTransactionInstance.saveFinalStatus(txId, transactionStatus, response)

    // Call the user's completion handler
    const handlerName = contextForCompletionHandler.userCompletionHandlerName
    const completionHandlerObj = await ResultReceiverRegister.getHandler(handlerName)
    await completionHandlerObj.haveResult(contextForCompletionHandler, status, note, response)
  }
}


/*
 *  The main entry point to the Transaction Engine.
 */
class AsynchronousTransactionEngine {
  #transactionIndex

  constructor() {
    this.#transactionIndex = [ ] // txId -> TXData
  }

  /**
   * Prepare the engine for use.
   */
  async initialize() {
    await SaySomethingStep.register()
    await DummyStep.register()
    await MockStep.register()
    await RandomDelayStep.register()
    await Pipeline.register()
    await ExampleStep.register()
    await DemoStep.register()

    await ResultReceiver.register(ATP_TRANSACTION_COMPLETION_HANDLER_NAME, new TransactionCompletionHandler())

  }//- initialize()

  async allocateTransactionId() {
    const transactionId = GenerateHash('tx')
    console.log(`New transactionId is ${transactionId}`)
    return transactionId
  }

  /**
   *
   * @param {String} transactionType
   * @param {String} initiatedBy
   * @param {TXData} data
   * @param {String} completionHandlerName
   * @returns
   */
  async initiateTransaction(transactionId, transactionType, initiatedBy, data, completionHandlerName, color) {
    console.error(`>>>>    initiateTransaction(${transactionType})  `.white.bgBlue.bold)
    // console.log(`initiatedBy=`, initiatedBy)
    console.log(`data=`, data)
    // console.log(`completionHandlerName=`, completionHandlerName)
    // console.log(`color=`, color)

    // await registerStepTypes()

    // Which pipeline should we use?
    const pipelineDetails = await dbTransactionType.getPipeline(transactionType)
    // console.log(`initiateTransaction() - pipelineDetails:`, pipelineDetails)
    if (pipelineDetails === null) {
      throw new Error(`Unknown transaction type ${transactionType}`)
    }

    const pipelineName = pipelineDetails.pipelineName
    //ZZZZ How about the version?  Should we use name:version ?
    // console.log(`initiateTransaction() - using pipeline '${pipelineName}''`)

    const status = TransactionIndexEntry.RUNNING
    // console.log(`status=`, status)

    const initialTxData = new TxData(data)
    // console.log(`initialTxData=`, initialTxData)
    // console.log(`initialTxData.getData()=`, await initialTxData.getData())

    // Remember the transaction
    const transactionIndexEntry = new TransactionIndexEntry(transactionId, transactionType, status, initiatedBy, initialTxData)
    // console.log(`WUMBOXI 1`, transactionIndexEntry)
    const txId = await transactionIndexEntry.getTxId()
    // console.log(`WUMBOXI 2`, await transactionIndexEntry.getInitialData())

    // Persist the transaction
    const inquiryToken = await dbTransactionInstance.persist(transactionIndexEntry, pipelineName)
    console.log(`inquiryToken=`, inquiryToken)


    this.#transactionIndex[txId] = transactionIndexEntry

    // console.log(`\n\nkkkkk kkkkk kkkkk kkkkk kkkkk kkkkk kkkkk kkkkk kkkkk kkkkk kkkkk kkkkk kkkkk `)
    // console.log(`ADD ${txId}`)
    // console.log(`this.#transactionIndex=`, this.#transactionIndex)
    await this.dumpTransactions('AFTER ADD')


    // Invoke the step
    const parentInstance = null
    const fullSequencePrefix = txId.substring(txId.length - 8)
    const definition = pipelineName
    const contextForCompletionHandler = {
      txId,
      userCompletionHandlerName: completionHandlerName
    }
    const invokeReply = await Scheduler.invokeStep(txId, parentInstance, fullSequencePrefix, definition, initialTxData, ATP_TRANSACTION_COMPLETION_HANDLER_NAME, contextForCompletionHandler)
    // const invokeReply = await Scheduler.invokeStep(txId, parentInstance, fullSequencePrefix, definition, initialTxData, completionHandlerName, contextForCompletionHandler)


    // Update the transaction with details of this top level step
    // console.log(`invokeReply=`, invokeReply)
    const rootStepId = invokeReply.stepId
    // const status = invokeReply.status
    // console.log(`rootStepId=`, rootStepId)
    await transactionIndexEntry.setRootStepId(rootStepId)
    // await transactionIndexEntry.setStatus(status)
    // await transactionIndexEntry.setTransactionType(transactionType)
    if (color) {
      await transactionIndexEntry.setColor(color)
    }

    // Display the Scheduler
    // await Scheduler.dumpSteps(`\nAfter Starting`)

    return { txId, inquiryToken }
  }//- initiateTransaction

  async getTransactionResult(transactionId) {
    return await dbTransactionInstance.getTransaction(transactionId)
  }

  /**
   * Get details of a transaction.
   *
   * @param {String} txId
   * @returns { txId, status, startTime }
   */
  async getTransactionStatus(txId) {
    // console.log(`getTransactionStatus(${txId})`)
    // console.log(`this.#transactionIndex=`, this.#transactionIndex)

    const transactionIndexEntry = this.#transactionIndex[txId]
    if (!transactionIndexEntry) {
      // This should never happen
      console.log(`Attempt to access unknown transaction ${txId} - possible hack attempt?`)
      await pause(ANTI_BRUTE_FORCE_DELAY) // To prevent brute force attempt to find transaction IDs
      throw new Error(`Unknown transaction (${txId})`)
    }

    // Now get the step details
    const rootStepId = await transactionIndexEntry.getRootStepId()
    // console.log(`rootStepId=`, rootStepId)
    // await Scheduler.dumpSteps(rootStepId)
    const stepEntry = await Scheduler.getStepEntry(rootStepId)
    // console.log(`stepEntry=`, stepEntry)
    // const stepInstance = stepEntry.getStepInstance()
    // console.log(`stepInstance=`, stepInstance)
    return {
      txId,
      status: await stepEntry.getStatus(),
      startTime: stepEntry.getStartTime(),
    }
  }//- getTransactionStatus()

  async getTransactionEntry(txId) {
    console.log(`getTransactionEntry(${txId})`, this.#transactionIndex)
    const txEntry = this.#transactionIndex[txId]
    return txEntry ? txEntry : null
  }


  async dumpTransactions(msg) {
    if (msg) {
      console.log(`${msg}:`)
    }
    // console.log(`registered steps:\n`, this.stepIndex)
    const maxRecords = -1
    const txlist = await this.getTransactions(true, maxRecords)
    // console.log(`registered txlist:\n`, txlist)
    const ul = '----------------------------------------------------------------------'
    console.log(``)
    console.log(`  ${pad('TX', 50)}  ${pad('Status', 10)}  Start time`)
    console.log(`  ${pad(ul, 50)}  ${pad(ul, 10)}  ${pad(ul, 60)}`)
    for (const tx of txlist) {
      const txId = await tx.getTxId()
      const status = statusString(await tx.getStatus())
      const startTime = await tx.getStartTime()
      console.log(`  ${pad(txId, 50)}  ${pad(status, 10)}  ${startTime}`)
    }
    console.log(``)
  }

  async transactionList(msg, includeComplete) {
    if (msg) {
      console.log(`${msg}:`)
    }
    // console.log(`registered steps:\n`, this.stepIndex)
    const maxRecords = 20
    const txlist = await this.getTransactions(includeComplete, maxRecords)
    const reply = [ ]
    for (const tx of txlist) {
      const transactionType = await tx.getTransactionType()
      const txId = await tx.getTxId()
      const status = statusString(await tx.getStatus())
      const startTime = await tx.getStartTime()
      const color = await tx.getColor()
      // console.log(`  ${pad(txId, 50)}  ${pad(status, 10)}  ${startTime}`)
      reply.push({
        txId,
        transactionType,
        status,
        startTime,
        color,
      })
    }
    return reply
  }

  async dumpSteps(msg, transactionId) {
    await Scheduler.dumpSteps(msg, transactionId)
  }

  async stepList(includeCompleted, transactionId) {
    const steps = await Scheduler.stepList(includeCompleted, transactionId)
    return steps
  }

  async getTransactions(includeCompleted, maxRecords) {
    const list = [ ]
    for (const txId in this.#transactionIndex) {
      const txEntry = this.#transactionIndex[txId]
      // console.log(`-> ${schedulerSnapshotOfStepInstance.step.id}`)
      const status = await txEntry.getStatus()
      // console.log(`--> status=`, status)
      if (status === Step.COMPLETED && !includeCompleted) {
        continue
      }
      const sortTime = (await txEntry.getStartTime()).getTime()
      list.push({ txEntry, sortTime })
    }
    list.sort((a, b) => {
      // Newest first
      // console.log(`${a.sortTime} vs ${b.sortTime}`)
      if (a.sortTime < b.sortTime) return +1
      if (a.sortTime > b.sortTime) return -1
      return 0
    })
    const newList = [ ]
    for (let i = 0; i < list.length; i++) {
      if (maxRecords > 0 && i >= maxRecords) {
        break
      }
      newList.push(list[i].txEntry)
    }
    return newList
  }//- getTransactions

  async stepDescription(definition) {
    console.log(`ATP.stepDescription()`)
    return await Step.describe(definition)
  }//- stepDescription


  async stepDefinition(definition) {
    console.log(`ATP.stepDefinition()`)
    // return await Step.describe(definition)

    try {
      definition = await Step.resolveDefinition(definition)
      return definition
    } catch (e) {
      console.log(`Error resolving pipeline definition (${definition}):`, e)
      throw e
    }
    // console.log(`definition=`, definition)

  }//- stepDescription
}

// export default {
//   initialize,
//   initiateTransaction,
//   getTransactionStatus,
// }


const defaultATE = new AsynchronousTransactionEngine()
export default defaultATE
