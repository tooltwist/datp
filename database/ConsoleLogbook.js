/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import { LogbookHandler } from './Logbook'
import dbLogbook from './dbLogbook'

const VERBOSE = 0

export default class ConsoleLogbook extends LogbookHandler {

  /**
   * Get the log entries for a transaction.
   *
   * @param {string} txId Transaction ID
   * @returns A list of { stepId, level, source, message, created }
   */
  async getLog(txId) {
    if (VERBOSE) console.log(`ConsoleLogbook.getLog(${txId})`)
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
    if (VERBOSE) console.log(`ConsoleLogbook.bulkLogging(${txId}, ${stepId})`, array)

    for (const entry of array) {

      // What was the source of the log entry
      let source = entry.source
      switch (source) {
        case dbLogbook.LOG_SOURCE_DEFINITION:
        case dbLogbook.LOG_SOURCE_EXCEPTION:
        case dbLogbook.LOG_SOURCE_INVOKE:
        case dbLogbook.LOG_SOURCE_PROGRESS_REPORT:
        case dbLogbook.LOG_SOURCE_ROLLBACK:
        case dbLogbook.LOG_SOURCE_SYSTEM:
          break
        default:
          source = dbLogbook.LOG_SOURCE_UNKNOWN
      }

      // What logging level?
      let level = entry.level
      switch (level) {
        case dbLogbook.LOG_LEVEL_TRACE:
        case dbLogbook.LOG_LEVEL_DEBUG:
        case dbLogbook.LOG_LEVEL_INFO:
        case dbLogbook.LOG_LEVEL_WARNING:
        case dbLogbook.LOG_LEVEL_ERROR:
        case dbLogbook.LOG_LEVEL_FATAL:
          break
        default:
          level = dbLogbook.LOG_LEVEL_UNKNOWN
      }

      // Use JSON as the message if necessary
      let message = entry.message
      if (typeof(message) === 'object') {
        message = JSON.stringify(message, '', 0)
      }

      // const sequence = entry.fullSequence ? entry.fullSequence : txId.substring(3, 9)
      //ZZZZ VOG Use vogPath
      // const sequence = entry.vogPath ? entry.vogPath : txId.substring(3, 9)
      console.log(`${txId}|${stepId}|${level}|${source}|${sequence}|${message}`)
    }// for
  }
}