/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import { serveMondatApi, startDatpServer } from './restify/startServer'
import step from './ATP/Step'
import stepTypeRegister from './ATP/StepTypeRegister'
import conversionHandler from './CONVERSION/lib/ConversionHandler'
import formsAndFields from './CONVERSION/lib/formsAndFields'
import dbQuery from './database/query'
import healthcheck from './restify/healthcheck'
import juice from '@tooltwist/juice-client'
import { RouterStep as RouterStepInternal } from './ATP/hardcoded-steps/RouterStep'
import pause from './lib/pause'
import Scheduler2 from './ATP/Scheduler2/Scheduler2'
import { requiresWebhookReply, RETURN_TX_STATUS_WITH_WEBHOOK_CALLBACK } from './ATP/Scheduler2/returnTxStatusWithWebhookCallback'
import Transaction from './ATP/Scheduler2/Transaction'
import { deepCopy } from './lib/deepCopy'
import LongPoll from './ATP/Scheduler2/LongPoll'
import { RETURN_TX_STATUS_WITH_LONGPOLL_CALLBACK } from './ATP/Scheduler2/returnTxStatusViaLongpollCallback'
import { DuplicateExternalIdError } from './ATP/Scheduler2/TransactionPersistance'
import DatpCron from './cron/cron'

const VERBOSE = 0

export const Step = step
export const StepTypes = stepTypeRegister
export const ConversionHandler = conversionHandler
export const FormsAndFields = formsAndFields
export const RouterStep = RouterStepInternal

export const query = dbQuery
export let schedulerForThisNode = null
export let cron = null

async function restifySlaveServer(options) {
  return startDatpServer(options)
}

async function restifyMasterServer(options) {
  return startDatpServer(options)
}

export async function goLive(server) {
  const nodeGroup = await juice.string('datp.nodeGroup', null)
  if (!nodeGroup) {
    console.log(`FATAL ERROR: Config does not specify datp.nodeGroup. Shutting down.`)
    process.exit(1)
  }
  const name = await juice.string('datp.name', juice.OPTIONAL)
  const description = await juice.string('datp.description', juice.OPTIONAL)
  const serveMondat = await juice.boolean('datp.serveMondat', false)
  // const serveMondatApi = await juice.boolean('datp.serveMondatApi', nodeGroup === 'master')

  // Registering the healthcheck will allow the Load Balancer to recognise the server is active.
  healthcheck.registerRoutes(server)

  // Start the master Scheduler
  schedulerForThisNode = new Scheduler2(nodeGroup, { name, description })
  // await scheduler.drainQueue()
  await schedulerForThisNode.start()

  // Cron
  cron = new DatpCron()
  await cron.start()

  // Perhaps serve up MONDAT
  if (serveMondat) {
    await serveMondatApi(server)
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
    // Use the supplied value
    // console.log(`Use provided metadata`)
  } else if (req.body && req.body.metadata) {
    metadata = req.body.metadata
  } else if (req.query) {
    metadata = {
      externalId: req.query.externalId ? req.query.externalId : null,
      reply: req.query.reply ? req.query.reply : 'shortpoll'
    }
  } else {
    metadata = { }
  }
  const externalId = metadata.externalId ? metadata.externalId : null
  const reply = metadata.reply
  const progressReports = metadata.progressReports ? true : false

  // Let's see how we should reply - shortpoll (default), longpoll, or webhook (http...)
  let callback = RETURN_TX_STATUS_WITH_LONGPOLL_CALLBACK
  let context = { }
  let isLongpoll = false

  if (requiresWebhookReply(metadata)) {

    // Reply by webhook.
    // Note that this does not preclude polling to get the status.
    if (VERBOSE) console.log(`Will reply with web hook to ${reply}`)
    if (VERBOSE && progressReports) console.log(`Will also send progress reports via webhook`)
    callback = RETURN_TX_STATUS_WITH_WEBHOOK_CALLBACK
    context = { webhook: reply, progressReports }
  } else if (reply  === 'longpoll') {

    // Reply with LONG POLLING.
    // We'll retain the response object for a while and not reply to this API call
    // just yet, in the hope that the transaction completes and we can use the
    // response object to send our reply.
    callback = RETURN_TX_STATUS_WITH_LONGPOLL_CALLBACK
    isLongpoll = true
  } else if (reply === 'shortpoll' || reply === undefined) {

    // By default we reply with SHORT POLLING.
    // We just reply as soon as the transaction is started.
  } else {

    // This should not happen.
    throw new Error(`Invalid value for option 'reply' [${reply}]`)
  }

  // Sanitize the metadata to make sure it contains no mischief (getters/setter, functions, etc)
  const metadataCopy = deepCopy(metadata)
  metadataCopy.owner = tenant
  metadataCopy.nodeGroup = 'master'
  metadataCopy.externalId = externalId
  metadataCopy.transactionType = transactionType
  metadataCopy.onComplete = { callback, context }


  const dataCopy = deepCopy(data)
  // dataCopy.tenant = tenant //ZZZZZ Is this needed?

  // console.log(`STARTING TRANSACTION WITH:`)
  // console.log(`metadataCopy=`, metadataCopy)
  // console.log(`dataCopy=`, dataCopy)

  let tx
  try {
    tx = await schedulerForThisNode.startTransaction({ metadata: metadataCopy, data: dataCopy })
  } catch (e) {

    // Return a message to the API caller
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
  // console.log(`tx=`, tx)

  if (isLongpoll) {
    // Wait a while before returning, but allow the completing pipeline
    // to hijack our response object if it finishes.
    if (VERBOSE) console.log(`DATP.startTransactionRoute() - REPLY AFTER A WHILE`)
    return LongPoll.returnTxStatusAfterDelayWithPotentialEarlyReply(tenant, tx.getTxId(), res, next)
  } else {

    // Reply with the current transaction status
    let summary = await Transaction.getSummary(tenant, tx.getTxId())
    if (VERBOSE) console.log(`DATP.startTransactionRoute() - IMMEDIATE REPLY`)
    res.send(summary)
    next()
  }// !isLongpoll
}//- startTransactionRoute


export async function transactionStatusByTxIdRoute(req, res, next) {
  console.log(`\n----- transactionStatusByTxIdRoute()`)
  console.log(`req.params=`, req.params)

  const tenant = await tenantFromCredentials(req)
  const txId = req.params.txId
  console.log(`txId=`, txId)
  assert(txId)

  const summary = await Transaction.getSummary(tenant, txId)
  console.log(`summary=`, summary)
  if (summary) {
    res.send(summary)
    return next()
  } else {
    res.send(new errors.NotFoundError('Unknown transaction'))
    return next()
  }
}

export async function transactionStatusByExternalIdRoute(req, res, next) {
  console.log(`\n----- transactionStatusByExternalIdRoute()`)
  console.log(`req.params=`, req.params)

  const tenant = await tenantFromCredentials(req)
  const externalId = req.params.externalId
  assert(externalId)

  const summary = await Transaction.getSummaryByExternalId(tenant, externalId)
  console.log(`summary=`, summary)
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

  // These are here for convenience for external applications.
  Step,
  StepTypes,
  RouterStep,
  ConversionHandler,
  FormsAndFields,
  query: dbQuery,
  // initiateTransaction,
  RouterStep,
  pause,
  prepareForUnitTesting,

  // New v2.0 functions
  startTransactionRoute,
  transactionStatusByTxIdRoute,
  transactionStatusByExternalIdRoute,
}