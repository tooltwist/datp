/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import { LogbookHandler } from './Logbook'
const VERBOSE = 1

export default class NullLogbook extends LogbookHandler {

  /**
   * Get the log entries for a transaction.
   *
   * @param {string} txId Transaction ID
   * @returns A list of { stepId, level, source, message, created }
   */
  async getLog(txId) {
    if (VERBOSE) console.log(`NullLogbook.getLog(${txId})`)
    return [ ]
  }


  /**
   * Save a list of log entries
   *
   * @param {string} txId Transaction ID
   * @param {string} stepId If null, the log message applies to the transaction
   * @param {*} array Array of { level, source, message }
   */
  async bulkLogging(txId, stepId, array) {
    if (VERBOSE) console.log(`NullLogbook.bulkLogging(${txId}, ${stepId})`, array)
  }
}