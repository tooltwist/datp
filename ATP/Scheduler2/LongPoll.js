/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import dbupdate from "../../database/dbupdate"
import query from "../../database/query"
import { luaGetCachedState } from "./queuing/redis-cachedState"
import { RedisQueue } from "./queuing/RedisQueue-ioredis"
import { convertReply } from "./ReplyConverter"
import TransactionState from "./TransactionState"

const LONGPOLL_TIMEOUT = 15

const VERBOSE = 0


export default class LongPoll {

  static index = { } // txId => { response, next, tenant, timer }

  /**
   * Save a response object for a period of time, so it can be used to
   * immediately return the transaction summary if the specified transaction
   * completes.
   * 
   * If the transaction does not complete in time, the current status of
   * the transaction is returned.
   * 
   * If `cancelWebhook` is true then a webhook reply for the transaction will
   * be prevented from occurring. This should only be used when the transaction
   * is being initiated, with both a webhook and a longpoll specified.
   * 
   * @param {string} tenant
   * @param {string} txId The transaction who's status will be returned.
   * @param {HttpResponse} response Restify response object.
   * @param {function} next Restify next() function.
   * @param {boolean} cancelWebhook
   * @param {number} duration How many seconds we hold on to the response object before replying.
   */
  static async returnTxStatusAfterDelayWithPotentialEarlyReply(tenant, txId, response, next, cancelWebhook=false, duration=LONGPOLL_TIMEOUT) {
    // if (VERBOSE) console.log(`LongPoll:returnTxStatusAfterDelayWithPotentialEarlyReply(tenant=${tenant}, txId=${txId}, response, next, cancelWebhook=${cancelWebhook}, duration=${duration}`)

    // Remove this after a while
    const timer = setTimeout(async () => {
      if (VERBOSE) console.log(`LongPoll:returnTxStatusAfterDelayWithPotentialEarlyReply - timeout activated`)
      // Use the responswe to return the current transaction status
      delete LongPoll.index[txId]

      // console.log(`- long poll timeout`)

      let summary = await TransactionState.getSummary(tenant, txId)
      if (VERBOSE) console.log(`LongPoll:returnTxStatusAfterDelayWithPotentialEarlyReply - reply after timeout`, summary)

      // Convert the reply as required by the app.
      // ReplyConverter
      // console.log(`ReplyConverter 3`)
      const { httpStatus, reply } = convertReply(summary)
      response.send(httpStatus, reply)
      // response.send(summary)
      return next()
    }, duration * 1000)

    // Remember this response object, so tryToReplyToLongPoll can find it
    LongPoll.index[txId] = {
      response,
      next,
      tenant,
      timer,
      cancelWebhook
    }
    if (VERBOSE) console.log(`LongPoll:returnTxStatusAfterDelayWithPotentialEarlyReply - timeout initiated`)
  }

  /**
   * If a long poll is still waiting to return, grab the response
   * object here and use it to send the transaction status.
   *
   * @param {string} txId Transaction ID
   * @returns True if the reply was sent via a long poll.
   */
  static async tryToReply(txId) {
    if (VERBOSE) console.log(`LongPoll:tryToReply(${txId})`)

    // Do we have a response object still?
    const entry = LongPoll.index[txId]
    if (entry) {
      if (VERBOSE) console.log(`LongPoll:tryToReply - entry found`)
      clearTimeout(entry.timer)
      delete LongPoll.index[txId]

      // // Remember that the reply has been sent
      // if (VERBOSE) console.log(`LongPoll:tryToReply - set response_acknowledge_time`)
      // const sql = `UPDATE atp_transaction2 SET response_acknowledge_time = NOW() WHERE transaction_id=? AND response_acknowledge_time IS NULL`
      // const params = [ txId ]
      // const result = await dbupdate(sql, params)
      // // console.log(`result=`, result)


      // We can assume the transaction state is still in Lua, because something
      // recently changed the transaction status, and that is why we are here.
      const withMondatDetails = false
      const markAsCompleted = true
      const cancelWebhook = !!entry.cancelWebhook
      const result2 = await luaGetCachedState(txId, withMondatDetails, markAsCompleted, cancelWebhook)
      const txState = result2.txState
      const txStateObject = txState.asObject()
      const summary2 = {
        "metadata": {
          "owner": txStateObject.owner,
          "txId": txStateObject.txId,
          "externalId": txStateObject.externalId ? txStateObject.externalId : null,
          "transactionType": txStateObject.transactionData.transactionType,
          "status": txStateObject.transactionData.status,
          "sequenceOfUpdate": txStateObject.delta,
          "completionTime": txStateObject.transactionData.completionTime,
          "lastUpdated": txStateObject.transactionData.lastUpdated,
          "notifiedTime": txStateObject.transactionData.notifiedTime
        },
        "progressReport": txStateObject.progressReport,
        "data": txStateObject.transactionData.transactionOutput
      }

      // Send the reply
      let summary = summary2
      if (VERBOSE) {
        let json = JSON.stringify(summary, '', 2)
        if (json.length > 500) json = json.substring(0, 500) + ' [truncated]'
        // console.log(`LongPoll:tryToReply - send summary`, json)
      }

      // Convert the reply as required by the app.
      const { httpStatus, reply } = convertReply(summary)
      entry.response.send(httpStatus, reply)
      entry.next()

      // // Perhaps cancel any pending webhook. This should only happen
      // // when the longpoll is for the transaction initiation request
      // // and a webhook reply has been requested.
      // if (entry.cancelWebhook) {
      //   const sql2 = `
      //     UPDATE atp_webhook
      //     SET status='complete', next_attempt = NULL, message=?
      //     WHERE transaction_id=?`
      //   const message = JSON.stringify({
      //     message: `Replied via long poll in transaction initiation API call`
      //   })
      //   const params2 = [ message, txId ]
      //   // console.log(`sql2=`, sql2)
      //   // console.log(`params2=`, params2)
      //   const result2 = await query(sql2, params2)
      //   // console.log(`result2=`, result2)
      //   if (result2.affectedRows > 0) {
      //     console.log(`Webhook cancelled for transaction ${txId}.`)
      //   }
      // }

      return { sent: true, cancelWebhook: entry.cancelWebhook }
    }

    // Could not send the reply via long poll
    if (VERBOSE) console.log(`LongPoll:tryToReply - entry not found`)
    return { sent: false, cancelWebhook: false }
  }

  /**
   * Return the number of long polls currently waiting to reply.
   * @returns
   */
  static async outstandingLongPolls() {
    const num = Object.keys(LongPoll.index).length
    if (VERBOSE) console.log(`LongPoll.outstandingLongPolls() - ${num}`)
    return num
  }
}//- LongPoll