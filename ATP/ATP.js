/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import XData from './XData'
import Step, { STEP_FAILED } from './Step'
import pause from '../lib/pause'
import pad from '../lib/pad'
import statusString from '../lib/statusString'
import GenerateHash from "./GenerateHash"
import assert from 'assert'


// Hardcoded steps
import DummyStep from './hardcoded-steps/DummyStep'
import SaySomethingStep from './hardcoded-steps/SaySomethingStep'
import PipelineStep from './hardcoded-steps/PipelineStep'
import MockStep from './hardcoded-steps/MockStep'
import RandomDelayStep from './hardcoded-steps/RandomDelayStep'
import ExampleStep from './hardcoded-steps/ExampleStep'
import DemoStep from './hardcoded-steps/DemoStep'
import MandatoryFieldsStep from './hardcoded-steps/MandatoryFieldsStep'
import MapFieldsStep from './hardcoded-steps/MapFieldsStep'
import ConvertDatesStep from './hardcoded-steps/ConvertDatesStep'
import RouterStep from './hardcoded-steps/RouterStep'
import WaitStep from './hardcoded-steps/WaitStep'

// Database stuff
import dbTransactionInstance from '../database/dbTransactionInstance'
import dbTransactionType from '../database/dbTransactionType'
import colors from 'colors'
import Logbook from './Logbook'

const ANTI_BRUTE_FORCE_DELAY = 2000 // 2 seconds

// /*
//  *  ResultReceiver for when a transaction is completed.
//  */
const ATP_TRANSACTION_COMPLETION_HANDLER_NAME = 'transaction-completion-handler'

// /**
//  * This ResultReceiver is called when the transaction is complete. It persists
//  * our transaction details (i.e. updates the database) before calling the
//  * ResultReceiver for the code that called ATP.initiateTransaction() to
//  * start the transaction.
//  */
// class AtpTransactionCompletionHandler extends ResultReceiver {
//   constructor() {
//     super()
//   }
//   async haveResult(contextForCompletionHandler, status, note, response) {
//     assert(response instanceof XData)
//     console.log(`<<<<    AtpTransactionCompletionHandler.haveResult(${status}, ${note})  `.white.bgBlue.bold)
//     console.log(`  contextForCompletionHandler=`, JSON.stringify(contextForCompletionHandler, '', 0))
//     // console.log(`  - status=`, status)
//     // console.log(`  - response=`, response.toString())
//     // Scheduler.dumpSteps(`\nAfter Completion`)

//     try {
//       // Update the transaction status
//       const txId = contextForCompletionHandler.txId
//       console.log(`FINISHED ${txId}`)
//       const txEntry = await defaultATE.getTransactionEntry(txId)
//       if (!txEntry) {
//         const msg = `INTERNAL ERROR IN AtpTransactionCompletionHandler.haveResult: txEntry not found (${txId})`
//         console.log(msg)
//         throw new Error(msg)
//       }
//       let transactionStatus
//       assert(status !== STEP_FAILED) // Should not happen. Pipelines either rollback or abort.
//       switch (status) {
//         case STEP_COMPLETED:
//           console.log(`Step is COMPLETED`)
//           transactionStatus = TransactionIndexEntry.COMPLETE
//           break

//         case STEP_ABORTED:
//           console.log(`Step is ABORT`)
//           transactionStatus = TransactionIndexEntry.TERMINATED
//           break

//         default:
//           console.log(`------- Step completed with an unrecognised status ${status}`)

//           transactionStatus = TransactionIndexEntry.UNKNOWN
//           assert(false)
//           break
//       }
//       txEntry.setStatus(transactionStatus)


//       // Persist the transaction
//       await dbTransactionInstance.saveFinalStatus(txId, transactionStatus, response)

//       // Call the user's completion handler
//       const handlerName = contextForCompletionHandler.userCompletionHandlerName
//       const completionHandlerObj = await ResultReceiverRegister.getHandler(handlerName)
//       await completionHandlerObj.haveResult(contextForCompletionHandler, transactionStatus, note, response)
//     } catch (e) {
//       console.trace(`Error in transaction completion handler`, e)
//       throw e
//     }
//   }//- haveResult
// }


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
    await PipelineStep.register()
    await ExampleStep.register()
    await DemoStep.register()
    await MandatoryFieldsStep.register()
    await MapFieldsStep.register()
    await ConvertDatesStep.register()
    await RouterStep.register()
    await WaitStep.register()


    // await ResultReceiver.register(ATP_TRANSACTION_COMPLETION_HANDLER_NAME, new AtpTransactionCompletionHandler())

  }//- initialize()

  async allocateTransactionId() {
    const transactionId = GenerateHash('tx')
    console.log(`New transactionId is ${transactionId}`)
    return transactionId
  }

  // /**
  //  *
  //  * @param {String} transactionType
  //  * @param {String} initiatedBy
  //  * @param {TXData} data
  //  * @param {String} completionHandlerName
  //  * @returns
  //  */
  // async initiateTransaction(transactionId, transactionType, initiatedBy, data, completionHandlerName, options) {
  //   console.error(`>>>>    initiateTransaction(${transactionType})  `.white.bgBlue.bold)

  //   if (!options) {
  //     options = { }
  //   }

  //   // Which pipeline should we use?
  //   const pipelineDetails = await dbTransactionType.getPipeline(transactionType)
  //   // console.log(`initiateTransaction() - pipelineDetails:`, pipelineDetails)
  //   if (pipelineDetails === null) {
  //     throw new Error(`Unknown transaction type ${transactionType}`)
  //   }

  //   const pipelineName = pipelineDetails.pipelineName
  //   //ZZZZ How about the version?  Should we use name:version ?

  //   const status = TransactionIndexEntry.RUNNING
  //   const initialTxData = new XData(data)

  //   // Remember the transaction
  //   const transactionIndexEntry = new TransactionIndexEntry(transactionId, transactionType, status, initiatedBy, initialTxData)
  //   const txId = await transactionIndexEntry.getTxId()

  //   // Persist the transaction
  //   const inquiryToken = await dbTransactionInstance.persist(transactionIndexEntry, pipelineName)
  //   console.log(`inquiryToken=`, inquiryToken)
  //   this.#transactionIndex[txId] = transactionIndexEntry

  //   // Create a logbook for this transaction/pipeline
  //   const logbook = new Logbook.cls({
  //     transactionId: txId,
  //     description: `Pipeline logbook`
  //   })


  //   // Invoke the step
  //   const parentInstance = null
  //   const sequenceYARP = txId.substring(txId.length - 8)
  //   const definition = pipelineName
  //   const contextForCompletionHandler = {
  //     txId,
  //     userCompletionHandlerName: completionHandlerName,
  //     options
  //   }
  //   const invokeReply = await Scheduler.invokeStep(txId, parentInstance, sequenceYARP, definition, initialTxData, logbook, ATP_TRANSACTION_COMPLETION_HANDLER_NAME, contextForCompletionHandler)

  //   // Update the transaction with details of this top level step
  //   const rootStepId = invokeReply.stepId
  //   await transactionIndexEntry.setRootStepId(rootStepId)
  //   if (options.color) {
  //     await transactionIndexEntry.setColor(options.color)
  //   }

  //   // Display the Scheduler
  //   // await Scheduler.dumpSteps(`\nAfter Starting`)

  //   return { txId, inquiryToken }
  // }//- initiateTransaction

  async getTransactionResult(transactionId) {
    return await dbTransactionInstance.getTransaction(transactionId)
  }

  // /**
  //  * Get details of a transaction.
  //  *
  //  * @param {String} txId
  //  * @returns { txId, status, startTime }
  //  */
  // async getTransactionStatus(txId) {
  //   const transactionIndexEntry = this.#transactionIndex[txId]
  //   if (!transactionIndexEntry) {
  //     // This should never happen
  //     console.log(`Attempt to access unknown transaction ${txId} - possible hack attempt?`)
  //     await pause(ANTI_BRUTE_FORCE_DELAY) // To prevent brute force attempt to find transaction IDs
  //     throw new Error(`Unknown transaction (${txId})`)
  //   }

  //   // Now get the step details
  //   const rootStepId = await transactionIndexEntry.getRootStepId()
  //   const stepEntry = await Scheduler.getStepEntry(rootStepId)
  //   return {
  //     txId,
  //     status: await stepEntry.getStatus(),
  //     startTime: stepEntry.getStartTime(),
  //   }
  // }//- getTransactionStatus()

  async getTransactionEntry(txId) {
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

  // async transactionList(msg, includeComplete) {
  //   if (msg) {
  //     console.log(`${msg}:`)
  //   }
  //   const maxRecords = 20
  //   const txlist = await this.getTransactions(includeComplete, maxRecords)
  //   const reply = [ ]
  //   for (const tx of txlist) {
  //     const transactionType = await tx.getTransactionType()
  //     const txId = await tx.getTxId()
  //     const status = statusString(await tx.getStatus())
  //     const startTime = await tx.getStartTime()
  //     const color = await tx.getColor()
  //     reply.push({
  //       txId,
  //       transactionType,
  //       status,
  //       startTime,
  //       color,
  //     })
  //   }
  //   return reply
  // }

  // async dumpSteps(msg, transactionId) {
  //   await Scheduler.dumpSteps(msg, transactionId)
  // }

  // async stepList(includeCompleted, transactionId) {
  //   const steps = await Scheduler.stepList(includeCompleted, transactionId)
  //   return steps
  // }

  // async getTransactions(includeCompleted, maxRecords) {
  //   const list = [ ]
  //   for (const txId in this.#transactionIndex) {
  //     const txEntry = this.#transactionIndex[txId]
  //     const status = await txEntry.getStatus()
  //     if (status === STEP_COMPLETED && !includeCompleted) {
  //       continue
  //     }
  //     const sortTime = (await txEntry.getStartTime()).getTime()
  //     list.push({ txEntry, sortTime })
  //   }
  //   list.sort((a, b) => {
  //     // Newest first
  //     if (a.sortTime < b.sortTime) return +1
  //     if (a.sortTime > b.sortTime) return -1
  //     return 0
  //   })
  //   const newList = [ ]
  //   for (let i = 0; i < list.length; i++) {
  //     if (maxRecords > 0 && i >= maxRecords) {
  //       break
  //     }
  //     newList.push(list[i].txEntry)
  //   }
  //   return newList
  // }//- getTransactions

  async stepDescription(definition) {
    console.log(`ATP.stepDescription()`)
    return await Step.describe(definition)
  }//- stepDescription


  async stepDefinition(definition) {
    // console.log(`ATP.stepDefinition()`)
    try {
      definition = await Step.resolveDefinition(definition)
      return definition
    } catch (e) {
      console.log(`Error resolving pipeline definition (${definition}):`, e)
      throw e
    }
  }//- stepDescription
}

const defaultATE = new AsynchronousTransactionEngine()
export default defaultATE
