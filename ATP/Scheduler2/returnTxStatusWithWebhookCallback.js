/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import assert from 'assert'
import axios from 'axios'
import { PIPELINES_VERBOSE } from '../hardcoded-steps/PipelineStep'
import Transaction from './Transaction'
require('colors')

export const RETURN_TX_STATUS_WITH_WEBHOOK_CALLBACK = `returnTxStatusWithWebhook`

export async function returnTxStatusWithWebhookCallback (callbackContext, data) {
  if (PIPELINES_VERBOSE) console.log(`==> returnTxStatusWithWebhookCallback()`.magenta, callbackContext, data)

  assert(callbackContext.webhook)
  assert(data.owner)
  assert(data.txId)

  const summary = await Transaction.getSummary(data.owner, data.txId)
  console.log(`summary=`, summary)


  const url = callbackContext.webhook
  console.log(`url=`, url)

  try {
    await axios.post(url, summary)

    // The reply has been received - update the transaction table.
    // ZZZZZ
  } catch (e) {
    if (e.code === 'ECONNREFUSED') {
      console.log(`Webhook failed, could not call ${url}`)
    } else {
      console.log(`Error in webhook:`, e)
    }

    // Schedule this webhook for retry
    console.log(`Scheduling webhook for retry`)
    // ZZZZZ
  }
}
