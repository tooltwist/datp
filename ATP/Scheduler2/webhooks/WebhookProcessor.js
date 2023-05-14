/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import { schedulerForThisNode } from "../../.."
import { getNodeGroup } from "../../../database/dbNodeGroup"
import { RedisLua } from "../queuing/redis-lua"
import { luaGetWebhooksToProcess } from "../queuing/redis-webhooks"
import { tryTheWebhook } from "./tryTheWebhook"

const VERBOSE = 0
const IDLE_VERBOSE = 0

const IDLE_EMPTY_BATCHES = 100 // We switch to idle mode after this many empty batches

const WEBHOOK_WAITING = 'waiting'
const WEBHOOK_PROCESSING = 'processing'
const WEBHOOK_HAVE_RESULT = 'haveResult'

export class WebhookProcessor {
  // Config params
  #webhookProcessing // Are we doing this here?
  #requiredWorkers // Reload config if negative
  #webhookPause
  #webhookPauseBusy // When all workers are in use
  #webhookPauseIdle

  // The workers
  #workers

  // We've go to idle mode after IDLE_EMPTY_BATCHES empty batches
  #emptyBatchCount

  constructor() {
    // console.log(`WebhookProcessor.contructor()`)
    // Config
    this.#webhookProcessing = 0
    this.#requiredWorkers = -1

    // The workers
    this.#workers = [ ]
    this.#emptyBatchCount = 0
  }

  async checkConfig() {
    const nodeGroup = schedulerForThisNode.getNodeGroup()
    const group = await getNodeGroup(nodeGroup)
    // console.log(`this.#nodeGroup=`, this.#nodeGroup)
    // console.log(`group=`, group)
    if (!group) {
      // Node group is not in the database
      console.log(`Fatal error: node group '${nodeGroup}' is not defined in the database.`)
      console.log(`This error is too dangerous to contine. Shutting down now.`)
      process.exit(1)
    }
    const newWebhookProcessing = group.webhookProcessing
    const newRequiredWorkers = group.webhookWorkers
    const newWebhookPause = group.webhookPause
    const newWebhookPauseBusy = group.webhookPauseBusy
    const newWebhookPauseIdle = group.webhookPauseIdle
    if (this.#requiredWorkers < 0) {
      this.#requiredWorkers = 0
    }

    if (
      this.#webhookProcessing !== newWebhookProcessing ||
      this.#requiredWorkers !== newRequiredWorkers ||
      this.#webhookPause !== newWebhookPause ||
      this.#webhookPauseBusy !== newWebhookPauseBusy ||
      this.#webhookPauseIdle !== newWebhookPauseIdle
    ) {
      this.#webhookProcessing = newWebhookProcessing
      this.#requiredWorkers = newRequiredWorkers
      this.#webhookPause = newWebhookPause
      this.#webhookPauseBusy = newWebhookPauseBusy
      this.#webhookPauseIdle = newWebhookPauseIdle
      if (this.#webhookProcessing && this.#requiredWorkers > 0) {
        console.log(` ✔ `.brightGreen + `webhook processing`)
        // console.log(`WebhookProcessor:`)
        console.log(`        workers:`, this.#requiredWorkers)
        console.log(`          pause:`, this.#webhookPause)
        console.log(`      pauseBusy:`, this.#webhookPauseBusy)
        console.log(`      pauseIdle:`, this.#webhookPauseIdle)
      } else {
        console.log(` ✖ `.red + `webhook processing`)
      }
    }
  }

  async start() {
    // console.log(`WebhookProcessor.start()`)
    await RedisLua._checkLoaded()

    const loop = async () => {
      // console.log(`-----------------------`.red)
      // console.log(`WebhookProcessor.loop()`)

      if (schedulerForThisNode.shuttingDown()) {
        return
      }

      await this.checkConfig()

      // Start additional workers if required
      while (this.#workers.length < this.#requiredWorkers) {
        this.#workers.push({
          i: this.#workers.length,
          status: WEBHOOK_WAITING,
          txId: null,
          webhook: null,
          retryCount: 0,
          result: null,
          comment: null,
        })
      }
      // console.log(`this.#workers=`, this.#workers)

      // See how many workers are available.
      // Also take note of any available results from webhook calls.
      const available = [ ]
      let previousResults = [ ]
      for (const worker of this.#workers) {
        if (worker.status === WEBHOOK_WAITING) {
          available.push(worker)

        } else if (worker.status === WEBHOOK_HAVE_RESULT) {
          // console.log(`- worker ${worker.i} has a result`)
          worker.status = WEBHOOK_WAITING
          previousResults.push({
            txId: worker.txId,
            // webhook: worker.webhook,
            result: worker.result,
            comment: worker.comment
          })
          available.push(worker)
        }
      }
      let numRequired = Math.min(this.#requiredWorkers, available.length)
      // console.log(`previousResults=`.bgCyan, previousResults)
      // console.log(`available=`, available)
// previousResults = [ ]

      if (available.length === 0) {
        // console.log(`WebhookProcessor.loop(): full house`)
        setTimeout(loop, this.#webhookPauseBusy)
        return
      }

      /*
       *  Assign each webhook to one of the available workers.
       */
// console.log(`numRequired=`, numRequired)
// numRequired = Math.min(1, numRequired)
      let numStarted = 0
      if (numRequired > 0) {
        const batch = await luaGetWebhooksToProcess(previousResults, numRequired)

        // console.log(`batch.length=`, batch.length)
        if (batch.length === 0) {
          // Nothing to do
          if (this.#emptyBatchCount === IDLE_EMPTY_BATCHES && IDLE_VERBOSE) console.log(`WebhookProcessor entering idle mode`)
          if (this.#emptyBatchCount++ > IDLE_EMPTY_BATCHES) {
            setTimeout(loop, this.#webhookPauseIdle)
          } else {
            setTimeout(loop, this.#webhookPause)
          }
          return
        }
        if (this.#emptyBatchCount > IDLE_EMPTY_BATCHES && IDLE_VERBOSE) console.log(`WebhookProcessor exiting idle mode`)
        this.#emptyBatchCount = 0
        // console.log(`batch=`, batch)
 
        for (let i = 0; i < batch.length; i++) {
          const item = batch[i]
          const worker = available[i]
          worker.status = WEBHOOK_PROCESSING
          worker.txId = item.txId
          worker.webhook = item.webhook
          const retryCount = item.retryCount
          const eventType = item.webhookType
          const eventTime = item.completionTime // When the transaction completed
          // console.log(`---> ${worker.txId} ${item.retryCount}  ${item.retryTime}  ${item.nextRetryTime}`)

          setImmediate(async () => {

            // Try calling the webhook
            const owner = 'acme' //ZZZZ
            const txId = worker.txId
            const webhookUrl = worker.webhook
            if (VERBOSE) console.log(`WebhookProcessor trying webhook for ${txId} (attempt ${retryCount})`)

            // console.log(`await tryTheWebhook(${owner}, ${txId}, ${webhookUrl}, ${eventType}, ${eventTime}, ${retryCount})`)
            const { result, comment } = await tryTheWebhook(owner, txId, webhookUrl, eventType, eventTime, retryCount)
            // console.log(`result=`, result)
            // console.log(`comment=`, comment)
            
            // If we are shutting down now, quit immediately.
            if (schedulerForThisNode.shuttingDown()) {
              return
            }
      
            // const { result, comment } = await dummy(worker.i, worker.txId)
            if (result === 'failure') {
              console.log(`FAILED. RetryCount = ${retryCount}`)
            }
            worker.result = result
            worker.comment = comment
            worker.status = WEBHOOK_HAVE_RESULT
          })
          numStarted++
        }
      }

      if (numStarted === numRequired) {
        // console.log(`WebhookProcessor.loop(): full house`)
        setTimeout(loop, this.#webhookPauseBusy)
        return
      } else {
        setTimeout(loop,  this.#webhookPause)
        return
      }
    }//- loop


    // Start the loop.
    setImmediate(loop)
  }//- start()

  /**
   * Reload the config next time through the archiving loop
   */
  clearConfig() {
    this.#workers = -1
  }

}
