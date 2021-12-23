import assert from 'assert'
import axios from 'axios'
import Transaction from './Transaction'

export const RETURN_TX_STATUS_WITH_WEBHOOK = `returnTxStatusWithWebhook`

export async function returnTxStatusWithWebhookCallback (callbackContext, data) {
  console.log(`==> returnTxStatusWithWebhookCallback()`, callbackContext, data)

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
