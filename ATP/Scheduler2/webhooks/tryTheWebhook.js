/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import assert from 'assert'
import axios from 'axios'
import juice from '@tooltwist/juice-client'
import crypto from 'crypto'
import { convertReply } from '../ReplyConverter'
import TransactionState from '../TransactionState'

require('colors')

const VERBOSE = 0

// const MIN_WEBHOOK_RETRY = 10
// const RETRY_EXPONENT = 1.4
// const MAX_WEBHOOK_RETRY = 600 // 5 minutes
// let maxWebhookAttempts = -1

export const WEBHOOK_EVENT_TXSTATUS = 'complete'
export const WEBHOOK_EVENT_PROGRESS = 'progressReport'

export const WEBHOOK_RESULT_SUCCESS = 'success'
export const WEBHOOK_RESULT_FAILED = 'failed' // Failed, but retry
export const WEBHOOK_RESULT_ABORTED = 'aborted' // Failed, and don't try again


export const WEBHOOK_STATUS_PENDING = 'PENDING'
export const WEBHOOK_STATUS_PROCESSING = 'PROCESSING'
export const WEBHOOK_STATUS_DELIVERED = 'DELIVERED'
export const WEBHOOK_STATUS_RETRY = 'RETRY'
export const WEBHOOK_STATUS_ABORTED = 'ABORTED'
export const WEBHOOK_STATUS_MAX_RETRIES = 'MAX_RETRIES'


export function requiresWebhookReply(metadata) {
  return (typeof(metadata.webhook) === 'string') && metadata.webhook.startsWith('http')
}

export function requiresWebhookProgressReports(metadata) {
  return requiresWebhookReply(metadata) && metadata.progressReports
}

export async function tryTheWebhook(owner, txId, webhookUrl, eventType, eventTime, retryCount) {
  if (VERBOSE) console.log(`tryTheWebhook(${owner}, ${txId}, ${webhookUrl}, ${eventType}, ${eventTime}, ${retryCount})`)

  // We start with zero
  // retryCount++

  // Get the status
  const summary = await TransactionState.getSummary(owner, txId)
  // if (VERBOSE) console.log(`summary=`, summary)
  if (summary === null) {
    // The transaction does not exist (this should not happen)
    // Cancel the webhook
    console.log(`Cancelling webhook for unknown transaction ${txId}`)
    return { result: WEBHOOK_RESULT_ABORTED, comment: `Unknown transaction ${txid}` }
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
  let comment = ''
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
      //ZZZ YARP DO THIS
      comment = 'ZZZZZZZZZ'
      return { result: WEBHOOK_RESULT_FAILED, comment }
    } else {
      // All good, Cancel the webhook
      if (VERBOSE) console.log(`Webhook delivered for ${txId}`)
      return { result: WEBHOOK_RESULT_SUCCESS, comment }
    }

  } catch (e) {

    // Something didn't go right. Let's log the error, then let the retry occur.
    if (e.code === 'ECONNREFUSED') {
      comment = `Webhook failed: ECONNREFUSED`
    } else if (e.code === 'ECONNRESET') {
      comment = `Webhook failed: ECONNRESET`
    } else if (e.code === 'ETIMEDOUT') {
      comment = `Webhook failed: ETIMEDOUT`
    } else if (e.code === 'EPIPE') {
      comment = `Webhook failed: EPIPE`
    } else if (e.response) {
      console.log(`e.response.status=`, e.response.status)
      console.log(`e.response.statusText=`, e.response.statusText)
      comment = JSON.stringify({ status: e.response.status, statusText: e.response.statusText })
    } else {
      console.log(`Error calling webhook (attempt ${retryCount}, ${webhookUrl}):`, e.message)
      console.log(`e=`, e)
      comment = JSON.stringify(e)
    }
    if (VERBOSE) console.log(comment)
    return { result: WEBHOOK_RESULT_FAILED, comment }
  }
}
