/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import query from "../../database/query"
import Transaction from "./Transaction"

const LONGPOLL_TIMEOUT = 15

const VERBOSE = 0


export default class LongPoll {

  static index = { } // txId => { response, next, tenant, timer }

  /**
   *
   * @param {*} tenant
   * @param {*} txId
   * @param {*} response
   * @param {*} next
   * @param {*} duration
   */
  static async returnTxStatusAfterDelayWithPotentialEarlyReply(tenant, txId, response, next, duration=LONGPOLL_TIMEOUT) {
    if (VERBOSE) console.log(`LongPoll:returnTxStatusAfterDelayWithPotentialEarlyReply(tenant=${tenant}, txId=${txId}, response, next, duration=${duration}`)

    // Remove this after a while
    const timer = setTimeout(async () => {
      if (VERBOSE) console.log(`LongPoll:returnTxStatusAfterDelayWithPotentialEarlyReply - timeout activated`)
      // Use the responswe to return the current transaction status
      delete LongPoll.index[txId]

      let summary = await Transaction.getSummary(tenant, txId)
      if (VERBOSE) console.log(`LongPoll:returnTxStatusAfterDelayWithPotentialEarlyReply - reply after timeout`, summary)
      response.send(summary)
      return next()
    }, duration * 1000)

    // Remember this response object, so tryToReplyToLongPoll can find it
    LongPoll.index[txId] = {
      response,
      next,
      tenant,
      timer
    }
    if (VERBOSE) console.log(`LongPoll:returnTxStatusAfterDelayWithPotentialEarlyReply - timeout initiated`)
  }

  /**
   * If a long poll is waiting to return, grab the response object here
   * and use it to send the transaction status.
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

      // Remember that the reply has been sent
      if (VERBOSE) console.log(`LongPoll:tryToReply - set response_acknowledge_time`)
      const sql = `UPDATE atp_transaction2 SET response_acknowledge_time = NOW() WHERE transaction_id=? AND response_acknowledge_time IS NULL`
      const params = [ txId ]
      const result = await query(sql, params)
      // console.log(`result=`, result)

      // Send the reply
      let summary = await Transaction.getSummary(entry.tenant, txId)
      if (VERBOSE) {
        const json = JSON.stringify(summary, '', 2)
        if (json.length > 500) json = json.substring(0, 500)
        console.log(`LongPoll:tryToReply - send summary`, json)
      }
      entry.response.send(summary)
      entry.next()

      return true
    }

    // Could not send the reply via long poll
    if (VERBOSE) console.log(`LongPoll:tryToReply - entry not found`)
    return false
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