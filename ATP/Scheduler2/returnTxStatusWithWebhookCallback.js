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
require('colors')

const VERBOSE = 1

const MIN_WEBHOOK_RETRY = 10
const RETRY_EXPONENT = 1.4
const MAX_WEBHOOK_RETRY = 600 // 5 minutes


export const RETURN_TX_STATUS_WITH_WEBHOOK_CALLBACK = `returnTxStatusWithWebhook`

export async function requiresWebhookReply(metadata) {
  return metadata.reply && (typeof(metadata.reply) === 'string') && metadata.reply.startsWith('http')
}

export async function requiresWebhookProgressReports(metadata) {
  return requiresWebhookReply(metadata) && metadata.progressReports
}

export async function returnTxStatusWithWebhookCallback (callbackContext, data) {
  if (PIPELINES_VERBOSE) console.log(`==> returnTxStatusWithWebhookCallback()`.magenta, callbackContext, data)

  assert(callbackContext.webhook)
  assert(data.owner)
  assert(data.txId)

  await sendStatusByWebhook(data.owner, data.txId, callbackContext.webhook)
}

export async function sendStatusByWebhook(owner, txId, webhookUrl) {

  // Save this webhook in the database
  try {
    const sql = `INSERT into atp_webhook
      (transaction_id, owner, url, next_attempt)
      VALUES (?, ?, ?, DATE_ADD(NOW(), INTERVAL ${MIN_WEBHOOK_RETRY} SECOND))`
    const params = [ txId, owner, webhookUrl ]
    // console.log(`sql=`, sql)
    // console.log(`params=`, params)
    const result = await query(sql, params)
    // console.log(`result=`, result)
  } catch (e) {

    // console.log(`e=`, e)
    // console.log(`e.code=`, e.code)
    if (e.code && e.code === 'ER_DUP_ENTRY') {
      // The record already exists, so we'll update it. This happens when we have progress reports.
      const sql2 = `UPDATE atp_webhook SET next_attempt = DATE_ADD(NOW(), INTERVAL ${MIN_WEBHOOK_RETRY} SECOND) WHERE transaction_id=?`
      const params2 = [ txId ]
      const reply2 = await query(sql2, params2)
      console.log(`reply2=`, reply2)
    } else {
      throw e
    }
  }

  await tryTheWebhook(owner, txId, webhookUrl, 0)
}

export async function tryTheWebhook(owner, txId, webhookUrl, retryCount) {
  console.log(`tryTheWebhook(${owner}, ${txId}, ${webhookUrl}, ${retryCount})`)
  // Get the status
  const summary = await Transaction.getSummary(owner, txId)
  // console.log(`summary=`, summary)
  if (summary === null) {
    // The transaction does not exist (this should not happen)
    // Cancel the webhook
    if (VERBOSE) console.log(`cancelling webhook for unknown transaction ${txId}`)
    const sql2 = `UPDATE atp_webhook SET status='cancelled', next_attempt = NULL WHERE transaction_id=?`
    const params2 = [ txId ]
    const reply2 = await query(sql2, params2)
    // console.log(`reply2=`, reply2)
    return
  }

  // Call the webhook
  let errorMsg = ''
  try {
    // console.log(`webhookUrl=`, webhookUrl)
    // console.log(`summary=`, summary)
    // console.log(`HERE WE GO...`)
    const acknowledgement = await axios.post(webhookUrl, summary)
    // console.log(`acknowledgement=`, acknowledgement)

    // Check the reply
    const problemWithAcknowledgement = false // Do something here

    // Handle either success, or retry.
    if (problemWithAcknowledgement) {
      // We'll log the problem below, then let the retry occur
      errorMsg = 'ZZZZZZZZZ'
    } else {
      // Cancel the webhook
      const message = JSON.stringify(summary)
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
        level: dbLogbook.LOG_LEVEL_TRACE,
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
    level: dbLogbook.LOG_LEVEL_TRACE,
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
  const reply2 = await query(sql2, params2)
  console.log(`reply2=`, reply2)

  const wakeTime = new Date(Date.now() + (interval * 1000))
  await dbLogbook.bulkLogging(txId, null, [ {
    level: dbLogbook.LOG_LEVEL_TRACE,
    source: dbLogbook.LOG_SOURCE_SYSTEM,
    message: `Webhook rety in ${interval} seconds, at ${wakeTime.toLocaleTimeString('PST')}.`
  }])
}