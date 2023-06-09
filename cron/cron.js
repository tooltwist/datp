/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import { schedulerForThisNode } from ".."
import { STEP_SLEEPING } from "../ATP/Step"
import query from "../database/query"
import TransactionCacheAndArchive from '../ATP/Scheduler2/TransactionCacheAndArchive'
import dbupdate from "../database/dbupdate"

export const CRON_INTERVAL = 5 // seconds
const VERBOSE = 0

export default class DatpCron {

  #running

  constructor() {
    this.#running = false
  }

  async start() {

    //
    const eachLoop = async () => {
      if (schedulerForThisNode.shuttingDown()) {
        console.log(`Skipping cron jobs - currently shutting down.`)
      } else {
        this.#running = true
        // Start them in parallel
        const p1 = schedulerForThisNode.keepAlive()
        const p2 = this.moveScheduledEventsToEventQueue()
        // const p3 = this.retryWebhooks()

        // Wait till they all finish
        await p1
        await p2
        // await p3
        this.#running = false
      }

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
    FROM atp_transaction2
    WHERE status = ?
      AND wake_node_group = ?
      AND wake_time IS NOT NULL
      AND wake_time < ?`
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
      console.log(`cron: rows=`, rows)
    }

    // Move these to the queue
    for (const tx of rows) {
      try {
        // Clear the wake time, so we don't rerun it a second time.
        // If required, the step will specify to rerun itself.
        // If multiple processes are trying to restart this transaction at the same time,
        // only one will succeed in making this change to this record.
        const sql2 = `UPDATE atp_transaction2
          SET wake_time = NULL
          WHERE transaction_id=?
            AND status=?
            AND wake_time=?`
        const params2 = [ tx.txId, tx.status, tx.wakeTime ]
        // console.log(`sql2=`, sql2)
        // console.log(`params2=`, params2)
        const result = await dbupdate(sql2, params2)
        // console.log(`result of cron's update=`, result)

        // This prevents multiple crons restarting the transaction
        if (result.affectedRows === 1) {

          const { state: txState } = await TransactionCacheAndArchive.getTransactionStateStatus(tx.txId)
          if (VERBOSE) console.log(`Restarting transaction [${tx.txId}]`)
          await schedulerForThisNode.enqueue_StepRestart(txState, nodeGroup, tx.txId, tx.wakeStepId)
        } else {
          if (VERBOSE) console.log(`cron: some other thread restarted the transaction`)
        }

        //
      } catch (e) {
        // Log this and potentially cancel the sleep info in the transaction.
        //ZZZZZ
        console.log(`e.message=`, e.message)
        if (e.message === `Unknown transaction ${tx.txId}`) {
          //ZZZZZ This should notify the administrator
          console.log(`---------------------------------------------------------------------------------------------------`)
          console.log(`SERIOUS ERROR:`)
          console.log(`Transaction was put to sleep, but when we try to re-awake it the transaction state has gone missing.`)
          console.log(`Please investigate transaction ${tx.txId}.`)
          console.log(`We will not try again.`)
          console.log(`---------------------------------------------------------------------------------------------------`)
        
        } else {
          //ZZZZZ This should notify the administrator
          console.log(`Error while waking transaction:`)
          console.log(`txId: ${tx.txId}`)
          console.log(e)
        }
      }

      // If we are shutting down now, quit immediately.
      if (schedulerForThisNode.shuttingDown()) {
        return
      }
    }//- next tx
  }

  isRunning() {
    return this.#running
  }
}