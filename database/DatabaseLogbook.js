/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import query from './query'
import { DEBUG_DB_ATP_LOGBOOK, logDestination } from '../datp-constants'
// import { schedulerForThisNode } from '..'
import dbupdate from './dbupdate'
import { LogbookHandler } from './Logbook'
import dbLogbook from './dbLogbook'

let hackCount = 0
let countDbAtpLogbook = 0
const VERBOSE = 0

export default class DatabaseLogbook extends LogbookHandler{

  /**
   * Get the log entries for a transaction.
   *
   * @param {string} txId Transaction ID
   * @returns A list of { stepId, level, source, message, created }
   */
  async getLog(txId) {
    if (VERBOSE) console.log(`DatabaseLogbook.getLog(${txId})`)
    const sql = `SELECT
      step_id AS stepId,
      level,
      source,
      message,
      created
    FROM atp_logbook WHERE transaction_id = ?`
    const params = [ txId ]
    const rows = await query(sql, params)
    return rows
  }


  /**
   * Save a list of log entries
   *
   * @param {string} txId Transaction ID
   * @param {string} stepId If null, the log message applies to the transaction
   * @param {*} array Array of { level, source, message }
   */
  async bulkLogging(txId, stepId, array) {
    if (VERBOSE) console.log(`DatabaseLogbook.bulkLogging(${txId}, ${stepId})`, array)
    if (array.length < 1) {
      return
    }
    if (DEBUG_DB_ATP_LOGBOOK) console.log(`atp_logbook INSERT ${countDbAtpLogbook++}`)
    let sql = `INSERT INTO atp_logbook (transaction_id, step_id, level, source, message) VALUES`
    const params = [ ]
    let sep = '\n'
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

      // Add to the SQL statement and parameters.
      sql += `${sep}(?,?,?,?,?)`
      params.push(txId)
      params.push(stepId)
      params.push(level)
      params.push(source)
      params.push(message)
      sep = ',\n'
    }// for
    // console.log(`sql=`, sql)
    // console.log(`params=`, params)
    const result = await dbupdate(sql, params)
    // console.log(`result=`, result)
    if (result.affectedRows !== array.length) {
      console.log(`**************************************************************************`)
      console.log(`INTERNAL ERROR: Could not save atp_logbook records - logging not occurring`)
      console.log(`NEEED TO SAVE ${array.length} RECORDS BUT ONLY SAVED ${result.affectedRows}`)
      console.log(`**************************************************************************`)
    }
  }
}