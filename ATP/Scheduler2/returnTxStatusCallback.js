/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import assert from 'assert'
import axios from 'axios'
import dbLogbook from '../../database/dbLogbook'
import query from '../../database/query'
import { PIPELINES_VERBOSE } from '../hardcoded-steps/PipelineStep'
import Transaction from './Transaction'
import juice from '@tooltwist/juice-client'
import crypto from 'crypto'
import { GO_BACK_AND_RELEASE_WORKER } from './Worker2'
import { convertReply } from './ReplyConverter'
import LongPoll from './LongPoll'
import dbupdate from '../../database/dbupdate'

require('colors')

const VERBOSE = 0

const MIN_WEBHOOK_RETRY = 10
const RETRY_EXPONENT = 1.4
const MAX_WEBHOOK_RETRY = 600 // 5 minutes

export const WEBHOOK_EVENT_TXSTATUS = 'txstatus'
export const WEBHOOK_EVENT_PROGRESS = 'progressReport'


export const RETURN_TX_STATUS_CALLBACK = `returnTxStatus`

/**
 * This callback handles sending a reply to the invoker of a transaction.
 * 
 * 1. First it checks to see if there is a longpoll waiting for the transaction status.
 * 2. Next, it registers a webhook reply, if that was requested.
 * 
 * There is a special case:
 * If the reply is sent with the lonpoll, and the longpoll is the first for the
 * transaction (i.e. the API request that started the transaction) then we consider
 * the transaction status to have been returned and cancel the need for any webhook.
 * 
 * @param {object} callbackContext 
 * @param {object} data Data returned by the pipeline
 * @param {Worker2} worker Ignored
 * @returns GO_BACK_AND_RELEASE_WORKER
 */
export async function returnTxStatusCallback (tx, callbackContext, data, worker) {
  if (VERBOSE) console.log(`==> returnTxStatusCallback()`.magenta, callbackContext, data)

  // if (VERBOSE) {
  //   console.log(`--------------------------------------------------------`)
  //   console.log(`returnTxStatusCallback()`)
  //   console.log(`callbackContext=`, callbackContext)
  //   console.log(`data=`, data)
  // }
  
  // assert(callbackContext.webhook)
  assert(data.owner)
  assert(data.txId)

  // First, see if a long poll is waiting for this transaction.
  const { sent: replyViaLongpoll, cancelWebhook }  = await LongPoll.tryToReply(data.txId)
  // console.log(`replyViaLongpoll=`, replyViaLongpoll)
  // console.log(`cancelWebhook=`, cancelWebhook)
  if (replyViaLongpoll) {
    if (VERBOSE) console.log(`==> returnTxStatusCallback() - REPLIED BY LONGPOLL`.magenta)
  } else {
    if (VERBOSE) console.log(`==> returnTxStatusCallback() - DID NOT REPLY BY LONGPOLL`.magenta)
  }

  // The status has been sent back via a longpoll that was waiting.
  // If this longpoll was from the transaction initiation API waiting, then
  // cancelWebhook will be true, and we no longer need to reply via the webhook.
  let replyViaWebhook = false
  if (callbackContext.webhook) {
    if (cancelWebhook) {
      if (VERBOSE) console.log(`==> returnTxStatusCallback() - WEBHOOK NOT REQUIRED`.magenta)
    } else {
      if (VERBOSE) console.log(`==> returnTxStatusCallback() - REPLIED BY WEBHOOK`.magenta)
      await sendStatusByWebhook(data.owner, data.txId, callbackContext.webhook, WEBHOOK_EVENT_TXSTATUS)
      replyViaWebhook = true
    }
  }
  if (!replyViaLongpoll && !replyViaWebhook) {
    if (VERBOSE) console.log(`==> returnTxStatusCallback() - NOT REPLYING BY WEBHOOK OR LONGPOLL`.magenta)
  }
  return GO_BACK_AND_RELEASE_WORKER
}

export function requiresWebhookReply(metadata) {
  return (typeof(metadata.webhook) === 'string') && metadata.webhook.startsWith('http')
}

export function requiresWebhookProgressReports(metadata) {
  return requiresWebhookReply(metadata) && metadata.progressReports
}

export async function sendStatusByWebhook(owner, txId, webhookUrl, eventType) {
  // console.log(`sendStatusByWebhook(${owner}, ${txId}, webhookUrl=${webhookUrl}, eventType=${eventType})`)

  // Save this webhook in the database
  const eventTime = new Date()
  try {
    const sql = `INSERT INTO atp_webhook
      (transaction_id, owner, url, event_type, initial_attempt, next_attempt)
      VALUES (?, ?, ?, ?, ?, DATE_ADD(NOW(), INTERVAL ${MIN_WEBHOOK_RETRY} SECOND))`
    const params = [ txId, owner, webhookUrl, eventType, eventTime ]
    // console.log(`sql=`, sql)
    // console.log(`params=`, params)
    const result = await dbupdate(sql, params)
    // console.log(`result=`, result)
  } catch (e) {

    // console.log(`e=`, e)
    // console.log(`e.code=`, e.code)
    if (e.code && e.code === 'ER_DUP_ENTRY') {
      // The record already exists, so we'll update it. This happens when we have progress reports.
      const sql2 = `UPDATE atp_webhook SET event_type=?, initial_attempt=?, next_attempt = DATE_ADD(NOW(), INTERVAL ${MIN_WEBHOOK_RETRY} SECOND) WHERE transaction_id=?`
      const params2 = [ eventType, eventTime, txId ]
      const reply2 = await dbupdate(sql2, params2)
      // console.log(`reply2=`, reply2)
    } else {
      throw e
    }
  }

  await tryTheWebhook(owner, txId, webhookUrl, eventType, eventTime, 0)
}

export async function tryTheWebhook(owner, txId, webhookUrl, eventType, eventTime, retryCount) {
  if (VERBOSE) console.log(`tryTheWebhook(${owner}, ${txId}, ${webhookUrl}, ${eventType}, ${eventTime}, ${retryCount})`)

  // Get the status
  const summary = await Transaction.getSummary(owner, txId)
  // if (VERBOSE) console.log(`summary=`, summary)
  if (summary === null) {
    // The transaction does not exist (this should not happen)
    // Cancel the webhook
    console.log(`Cancelling webhook for unknown transaction ${txId}`)
    const sql2 = `UPDATE atp_webhook SET status='cancelled', next_attempt = NULL WHERE transaction_id=?`
    const params2 = [ txId ]
    const reply2 = await dbupdate(sql2, params2)
    // console.log(`reply2=`, reply2)
    return
  }

  // Convert the reply as required by the app.
  // ReplyConverter
  // console.log(`ReplyConverter 5`)
  const { reply: convertedSummary } = convertReply(summary)

  // Prepare the webhook payload
  const payload = {
    eventType,
    metadata: convertedSummary.metadata,
    progressReport: convertedSummary.progressReport,
    data: convertedSummary.data,
    eventTime,
    deliveryTime: new Date(),
  }
  const json = JSON.stringify(payload, '', 0)
  // console.log(`json=`, json)

  // Add on a signature
  const privateKey = await juice.string('datp.webhook-credentials.privateKey', juice.MANDATORY)
  var signerObject = crypto.createSign("RSA-SHA256")
  signerObject.update(json)
  var signature = signerObject.sign({key: privateKey, padding: crypto.constants.RSA_PKCS1_PSS_PADDING}, "base64")
  // if (VERBOSE) console.info("signature: %s", signature)
  payload.signature = signature

  // Call the webhook
  let errorMsg = ''
  try {
    // console.log(`webhookUrl=`, webhookUrl)
    // console.log(`summary=`, summary)
    // console.log(`HERE WE GO...`)
    const acknowledgement = await axios.post(webhookUrl, payload)
    // console.log(`acknowledgement=`, acknowledgement)

    // Check the reply
    //ZZZZZ
    const problemWithAcknowledgement = false // Do something here

    // Handle either success, or retry.
    if (problemWithAcknowledgement) {
      // We'll log the problem below, then let the retry occur
      errorMsg = 'ZZZZZZZZZ'
    } else {
      // Cancel the webhook
      const message = JSON.stringify(convertedSummary)
      const sql2 = `
        UPDATE atp_webhook
        SET status='complete', next_attempt = NULL, message=?
        WHERE transaction_id=?`
      const params2 = [ message, txId ]
      // console.log(`sql2=`, sql2)
      // console.log(`params2=`, params2)
      const reply2 = await query(sql2, params2)
      // console.log(`reply2=`, reply2)
      if (reply2.affectedRows !== 1) {
        //ZZZZZ
        console.log(`Serious internal error: resetting atp_webhook after complete updated ${reply2.affectedRows} rows.`)
      }

      // Update the transaction log.
      await dbLogbook.bulkLogging(txId, null, [ {
        level: dbLogbook.LOG_LEVEL_INFO,
        source: dbLogbook.LOG_SOURCE_SYSTEM,
        message: 'Webhook called successfully.'
      }])
      if (VERBOSE) console.log(`Webhook delivered for ${txId}`)
      return
    }

  } catch (e) {
    if (e.code === 'ECONNREFUSED') {
      errorMsg = `Webhook failed: ECONNREFUSED`
    } else {
      console.log(`Error calling webhook ${webhookUrl}:`, e)
      errorMsg = JSON.stringify(e)
    }
  }

  // Something didn't go right. Let's log the error, then let the retry occur.
  if (VERBOSE) console.log(errorMsg)
  await dbLogbook.bulkLogging(txId, null, [ {
    level: dbLogbook.LOG_LEVEL_INFO,
    source: dbLogbook.LOG_SOURCE_SYSTEM,
    message: 'Webhook called successfully.'
  }])

  // Work out the retry time, with exponential interval increase
  let interval = MIN_WEBHOOK_RETRY
  for (let i = retryCount; i > 0; i--) {
    interval *= RETRY_EXPONENT
  }
  if (interval > MAX_WEBHOOK_RETRY) {
    interval = MAX_WEBHOOK_RETRY
  }
  interval = Math.round(interval)

  // Let cron know when to try again
  const sql2 = `UPDATE atp_webhook SET
    next_attempt = DATE_ADD(NOW(), INTERVAL ${interval} SECOND),
    retry_count = retry_count + 1,
    message=?
    WHERE transaction_id=?`
  const params2 = [ txId, errorMsg ]
  const reply2 = await dbupdate(sql2, params2)
  // console.log(`reply2=`, reply2)

  const wakeTime = new Date(Date.now() + (interval * 1000))
  await dbLogbook.bulkLogging(txId, null, [ {
    level: dbLogbook.LOG_LEVEL_INFO,
    source: dbLogbook.LOG_SOURCE_SYSTEM,
    message: `Webhook rety in ${interval} seconds, at ${wakeTime.toLocaleTimeString('PST')}.`
  }])
}