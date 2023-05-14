/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import { serveMondatWebapp, startDatpServer } from './restify/startServer'
import step from './ATP/Step'
import stepInstance from './ATP/StepInstance'
import stepTypeRegister from './ATP/StepTypeRegister'
import conversionHandler from './CONVERSION/lib/ConversionHandler'
import formsAndFields from './CONVERSION/lib/formsAndFields'
import callbackRegister from './ATP/Scheduler2/CallbackRegister'
import dbQuery from './database/query'
import healthcheck from './restify/healthcheck'
import juice from '@tooltwist/juice-client'
import { RouterStep as RouterStepInternal } from './ATP/hardcoded-steps/RouterStep'
import Pause from './lib/pause'
import Scheduler2 from './ATP/Scheduler2/Scheduler2'
import { deepCopy } from './lib/deepCopy'
import LongPoll from './ATP/Scheduler2/LongPoll'
import { generateErrorByName, registerErrorLibrary } from './lib/errorCodes'
import errors_datp_EN from './lib/errors-datp-EN'
import errors_datp_FIL from './lib/errors-datp-FIL'
import { registerReplyConverter, convertReply } from './ATP/Scheduler2/ReplyConverter'
import { WebhookProcessor } from './ATP/Scheduler2/webhooks/WebhookProcessor'
import { requiresWebhookReply, RETURN_TX_STATUS_CALLBACK_ZZZ } from './ATP/Scheduler2/webhooks/tryTheWebhook'
import { DuplicateExternalIdError } from './ATP/Scheduler2/DuplicateExternalIdError'
import { ArchiveProcessor } from './ATP/Scheduler2/archiving/ArchiveProcessor'
import DatpCron from './cron/cron'
import { WakeupProcessor } from './ATP/Scheduler2/WakeupProcessor'

const VERBOSE = 0

export const Step = step
export const StepInstance = stepInstance
export const StepTypes = stepTypeRegister
export const ConversionHandler = conversionHandler
export const FormsAndFields = formsAndFields
export const CallbackRegister = callbackRegister
export const RouterStep = RouterStepInternal

export const pause = Pause
export const query = dbQuery
export let schedulerForThisNode = null
export let webhookProcessor = null
export let archiveProcessor = null
export let wakeupProcessor = null
export let cron = null

async function restifySlaveServer(options) {
  return startDatpServer(options)
}

async function restifyMasterServer(options) {
  return startDatpServer(options)
}

export async function goLive(server) {
  const nodeGroup = await juice.string('datp.nodeGroup', juice.MANDATORY)
  if (!nodeGroup) {
    console.log(`FATAL ERROR: Config does not specify datp.nodeGroup. Shutting down.`)
    process.exit(1)
  }
  const description = await juice.string('datp.description', juice.OPTIONAL)
  const serveMondat = await juice.boolean('datp.serveMondat', false)

  // Register our DATP error codes
  registerErrorLibrary(errors_datp_EN)
  registerErrorLibrary(errors_datp_FIL)

  // Registering the healthcheck will allow the Load Balancer to recognise the server is active.
  healthcheck.registerRoutes(server)

  // Start the master Scheduler
  schedulerForThisNode = new Scheduler2(nodeGroup, { description })
  // await scheduler.drainQueue()
  await schedulerForThisNode.start()

  webhookProcessor = new WebhookProcessor()
  await webhookProcessor.start()

  archiveProcessor = new ArchiveProcessor()
  await archiveProcessor.start()

  wakeupProcessor = new WakeupProcessor()
  await wakeupProcessor.start()

  // Cron
  cron = new DatpCron()
  await cron.start()

  // Perhaps serve up MONDAT
  if (serveMondat) {
    await serveMondatWebapp(server)
  }
}

/**
 * For unit testing, we do not want to start the restify server. This function
 * allows the scheduler to be started without the full DATP server functinoality.
 */
export async function prepareForUnitTesting() {
  // Start the master Scheduler
  const MASTER_NODE_GROUP = 'master'
  schedulerForThisNode = new Scheduler2(MASTER_NODE_GROUP, null)
  await schedulerForThisNode.start()
}


/**
 * Start a new transaction, based on the HTTP request we received.
 *
 * @param {*} req
 * @param {*} res
 * @param {*} next
 * @param {*} tenant
 * @param {*} externalId
 * @param {*} transactionType
 * @param {*} metadata
 * @param {*} data
 */
export async function startTransactionRoute(req, res, next, tenant, transactionType, data=null, metadata=null) {
  if (VERBOSE) console.log(`DATP.startTransactionRoute()`)
  // console.log(`metadata=`, metadata)
  // console.log(`data=`, data)
  // console.log(`req.params=`, req.params)
  // console.log(`req.body=`, req.body)
  // console.log(`req.query=`, req.query)

  // console.log(`metadata=`, metadata)
  if (typeof(tenant) !== 'string') throw new Error(`Invalid tenant`)
  if (typeof(transactionType) !== 'string') throw new Error(`Invalid transactionType`)

  if (data) {
    // Use the supplied value
    // console.log(`Use provided data`)
  } else if (req.body && req.body.data) {
    data = req.body.data
  } else {
    data = { }
  }
  if (metadata) {

    // Use the metadata supplied by the function call
    // console.log(`Use provided metadata`)
  } else if (req.body && req.body.metadata) {

    // Get the metadata from the body (POST or PUT request)
    metadata = req.body.metadata
  } else if (req.query) {

    // Create the metadata from query parameters (GET request)
    metadata = {
      externalId: req.query.externalId ? req.query.externalId : null,
      poll: req.query.poll ? req.query.poll : null,
      webhook: req.query.webhook ? req.query.webhook : null,
      // Deprecated - use poll and webhook instead
      reply: req.query.reply ? req.query.reply : null,
    }
  } else {

    // Nowhere to get the metadata from.
    metadata = { }
  }
  const externalId = metadata.externalId ? metadata.externalId : null
  // console.log(`VOGCED 1 externalId=`, externalId)
  const reply = metadata.reply

  // Let's see how we should reply - shortpoll (default), longpoll, or webhook (http...)
  let callback = RETURN_TX_STATUS_CALLBACK_ZZZ

  // Work out polling and webhook details.
  let pollType = 'short'
  let webhook = null
  if (metadata.reply) {
    // This is deprecated, but supported for a while.
    if (metadata.reply === 'shortpoll' || metadata.reply === 'short') {
      metadata.poll = 'short'
    } else if (metadata.reply === 'longpoll' || metadata.reply === 'long') {
      metadata.poll = 'long'
    } else if (typeof(metadata.reply)==='string' && metadata.reply.startsWith('http')) {
      metadata.webhook = metadata.reply
    } else {
      throw new Error(`Invalid value for metadata.reply [${metadata.reply}]`)
    }
    delete metadata.reply

  } else if (!metadata.poll && !metadata.webhook) {

    // Default with nothing specified
    metadata.poll = 'short'
  } else {

    // Expect metadata.poll and metadata.webhook to be provided if required.
    // metadata.reply not specified.
    if (!metadata.poll) {
      metadata.poll = 'short'
    } else if (metadata.poll !== 'short' && metadata.poll !== 'long') {
      throw new Error(`metadata.poll has invalid value [${metadata.poll}]`)
    }
  }



  const context = { }
  if (requiresWebhookReply(metadata)) {

    // Reply by webhook.
    // Note that this does not preclude polling to get the status.
    if (VERBOSE) console.log(`Will reply with web hook to ${metadata.webhook}`)
    callback = RETURN_TX_STATUS_CALLBACK_ZZZ
    context.webhook = metadata.webhook
    context.progressReports = !!metadata.progressReports
    if (VERBOSE && context.progressReports) console.log(`Will also send progress reports via webhook`)
  }
  
  //ZZZZ This 'else' will be removed once the two callbacks are combined:
  // RETURN_TX_STATUS_WITH_WEBHOOK_CALLBACK
  // RETURN_TX_STATUS_WITH_LONGPOLL_CALLBACK
  // else
  if (metadata.poll  === 'long') {

    // Reply with LONG POLLING.
    // We'll retain the response object for a while and not reply to this API call
    // just yet, in the hope that the transaction completes and we can use the
    // response object to send our reply.
    callback = RETURN_TX_STATUS_CALLBACK_ZZZ
    context.longpoll = true

  } else {
  // } else if (metadata.poll === 'shortpoll' || reply === undefined) {

    // By default we reply with SHORT POLLING.
    // We just reply as soon as the transaction is started.
    // However, we still use the longpoll callback in case the user polls
    // to get the transaction result before it completes, using long polling.
  // } else {

  //   // This should not happen.
  //   throw new Error(`Invalid value for option 'reply' [${reply}]`)
  }

  /*
   *  Sanitize the metadata and data to make sure it contains no mischief (getters/setter, functions, etc)
   */
  const metadataCopy = deepCopy(metadata)
  metadataCopy.owner = tenant
  metadataCopy.nodeGroup = 'master'
  metadataCopy.externalId = externalId
  metadataCopy.transactionType = transactionType
  metadataCopy.onComplete = { callback, context }

  // Sanitize the data.
  const dataCopy = deepCopy(data)
  // dataCopy.tenant = tenant //ZZZZZ Is this needed?

  // console.log(`STARTING TRANSACTION WITH:`)
  // console.log(`metadataCopy=`, metadataCopy)
  // console.log(`dataCopy=`, dataCopy)

  // ZZZ Hack to see if metadata.reply is still used somewhere.
  metadataCopy.reply = {
    get: () => {
      console.trace('Accessing metadata.reply()')
      throw new Error('metadata.reply is being used!!!')
    }
  };

  /*
   *  Start the transaction.
   */
  let tx
  try {
    tx = await schedulerForThisNode.startTransaction({ metadata: metadataCopy, data: dataCopy })
//  console.log(`tx=`, tx.asObject())
  } catch (e) {

    // An error occurred.
    // Return a message to the API caller.
    if (e instanceof DuplicateExternalIdError) {
      res.send({
        metadata: {
          status: 'error',
          message: `A transaction with this externalId already exists`
        }
      })
      return next()
    }
    throw e
  }

  /*
   * Now decide how we reply.
   *  - for short poll, reply immediately. 
   *  - for long polling, wait a while before returning, with the hope
   *    that the completing transaction can grab the reponse object and
   *    return the transaction status before we timeout.
   * These actions are independant of whether a webhook is being used.
   */
  if (metadata.poll === 'long') {

    // Long polling.
    if (VERBOSE) console.log(`DATP.startTransactionRoute() - REPLY AFTER A WHILE`)
    // const cancelWebhook = !!metadata.webhook
    const cancelWebhook = !!metadata.pollReplyCancelsWebhook
    return LongPoll.returnTxStatusAfterDelayWithPotentialEarlyReply(tenant, tx.getTxId(), res, next, cancelWebhook)
  } else {

    // Short polling.
    // console.log(`tx=`, tx.pretty())
    let summary = tx.summaryFromState()
    // console.log(`summary=`, summary)
    if (VERBOSE) console.log(`DATP.startTransactionRoute() - IMMEDIATE REPLY`)

    // Convert the reply as required by the app.
    // ReplyConverter
    // console.log(`ReplyConverter 1`)
    const { httpStatus, reply } = convertReply(summary)
    res.send(httpStatus, reply)
    // res.send(summary)
    next()
  }// !isLongpoll
}//- startTransactionRoute


export async function transactionStatusByTxIdRoute(req, res, next) {
  // console.log(`\n----- transactionStatusByTxIdRoute()`)
  // console.log(`req.params=`, req.params)

  const tenant = await tenantFromCredentials(req)
  const txId = req.params.txId
  // console.log(`txId=`, txId)
  assert(txId)

  const summary = await Transaction.getSummary(tenant, txId)
  // console.log(`summary=`, summary)
  if (summary) {

    // Convert the reply as required by the app.
    // ReplyConverter
    // console.log(`ReplyConverter 2`)
    const { httpStatus, reply } = convertReply(summary)
    res.send(httpStatus, reply)
    // res.send(summary)
    return next()
  } else {
    res.send(new errors.NotFoundError('Unknown transaction'))
    return next()
  }
}

export async function transactionStatusByExternalIdRoute(req, res, next) {
  // console.log(`\n----- transactionStatusByExternalIdRoute()`)
  // console.log(`req.params=`, req.params)

  const tenant = await tenantFromCredentials(req)
  const externalId = req.params.externalId
  assert(externalId)

  const summary = await Transaction.getSummaryByExternalId(tenant, externalId)
  // console.log(`summary=`, summary)
  if (summary) {
    res.send(summary)
    return next()
  } else {
    res.send(new errors.NotFoundError('Unknown transaction'))
    return next()
  }
}



export default {
  restifyMasterServer,
  restifySlaveServer,
  goLive,
  schedulerForThisNode,

  // These are here for convenience for external applications.
  Step,
  StepTypes,
  StepInstance,
  RouterStep,
  ConversionHandler,
  FormsAndFields,
  query: dbQuery,
  // initiateTransaction,
  RouterStep,
  pause: Pause,
  prepareForUnitTesting,

  // v2.0 functions
  startTransactionRoute,
  transactionStatusByTxIdRoute,
  transactionStatusByExternalIdRoute,

  // v2.1.2 functions
  generateErrorByName,
  registerReplyConverter,
}