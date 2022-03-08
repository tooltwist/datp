/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import { schedulerForThisNode } from ".."
import { tryTheWebhook } from "../ATP/Scheduler2/returnTxStatusWithWebhookCallback"
import { STEP_SLEEPING } from "../ATP/Step"
import query from "../database/query"

export const CRON_INTERVAL = 15 // seconds
const VERBOSE = 0

export default class DatpCron {

  #running

  constructor() {
    this.#running = false
  }

  async start() {
    const eachLoop = async () => {
      this.#running = true
      // Start them in parallel
      const p1 = schedulerForThisNode.keepAlive()
      const p2 = this.moveScheduledEventsToEventQueue()
      const p3 = this.retryWebhooks()
      const p4 = this.tidyTransactionCache()

      // Wait till they all finish
      await p1
      await p2
      await p3
      await p4
      this.#running = false

      // Prepare to run it again.
      // unref() allows the process to shut down if required,
      // even though the timeout is still waiting.
      setTimeout(eachLoop, CRON_INTERVAL * 1000).unref()
    }
    eachLoop()
  }

  async moveScheduledEventsToEventQueue() {
    // Find all the 
    // console.log(`moveScheduledEventsToEventQueue()`)

    // We want to find the sleeping transactions where the wake time is before now. However,
    // short sleeps will be handled by setTimeout() so we want to leave them alone - We'll give
    // them 10 seconds extra after their pause to change their status in the transaction record.
    const earliestTime = new Date()
    const buffer = 10
    earliestTime.setSeconds(earliestTime.getSeconds() - buffer);

    // Find the sleeping transactions
    const sql = `SELECT
      transaction_id AS txId,
      wake_step_id AS wakeStepId,
      status,
      wake_node_group AS nodeGroup,
      wake_time AS wakeTime
    FROM atp_transaction2 WHERE
    status = ? AND
    wake_node_group = ? AND
    wake_time IS NOT NULL AND wake_time < ?`
    const status = STEP_SLEEPING
    const nodeGroup = schedulerForThisNode.getNodeGroup()
    const params = [
      status,
      nodeGroup,
      earliestTime,
    ]
    // console.log(`sql=`, sql)
    // console.log(`params=`, params)
    const rows = await query(sql, params)
    if (VERBOSE && rows.length > 0) {
      console.log(`rows=`, rows)
    }

    // Move these to the queue
    for (const tx of rows) {
      try {
        // console.log(`Restarting transaction [${tx.txId}]`)
        await schedulerForThisNode.enqueue_StepRestart(nodeGroup, tx.txId, tx.wakeStepId)  
      } catch (e) {
        // Log this and potentially cancel the sleep info in the transaction.
        //ZZZZZ
        console.log(`Error while waking transaction:`)
        console.log(`txId: ${tx.txId}`)
        console.log(e)
      }
    }
  }

  async retryWebhooks() {
    if (VERBOSE) console.log(`Cron checking webhooks`)
    // Find the webhooks ready to be tried again
    const sql = `
      SELECT transaction_id, owner, url, retry_count
      FROM atp_webhook
      WHERE status = 'outstanding' AND next_attempt < NOW()`
    const rows = await query(sql)

    for (const row of rows) {
      const owner = row.owner
      const txId = row.transaction_id
      const webhookUrl = row.url
      const retryCount = row.retry_count
      if (VERBOSE) console.log(`Cron retrying webhook for ${txId}`)
      await tryTheWebhook(owner, txId, webhookUrl, retryCount)
    }
  }

  async tidyTransactionCache () {

  }

  isRunning() {
    return this.#running
  }
}