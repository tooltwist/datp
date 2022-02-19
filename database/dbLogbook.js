/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import query from './query'

export default class dbLogbook {

  static LOG_LEVEL_DEBUG = 'debug'
  static LOG_LEVEL_TRACE = 'trace'
  static LOG_LEVEL_WARNING = 'warning'
  static LOG_LEVEL_ERROR = 'error'
  static LOG_LEVEL_UNKNOWN = 'unknown'

  static LOG_SOURCE_INVOKE = 'invoke'
  static LOG_SOURCE_ROLLBACK = 'rollback'
  static LOG_SOURCE_EXCEPTION = 'exception'
  static LOG_SOURCE_DEFINITION = 'definition'
  static LOG_SOURCE_SYSTEM = 'system'
  static LOG_SOURCE_PROGRESS_REPORT = 'progReport'
  static LOG_SOURCE_UNKNOWN = 'unknown'

  /**
   *
   * @param {String} msg
   */
  static async log(transactionId, stepId, sequence, msg) {
    console.log(`log(${transactionId}, ${stepId}, ${sequence}, ${msg})`)

    if (typeof(json) !== 'string') {
      throw new Error('log() requires string value for msg parameter')
    }
    const sql = `INSERT INTO atp_logbook (transaction_id, step_id, sequence, message) VALUES (?, ?, ?, ?)`
    const params = [ transactionId, stepId, sequence, msg ]
    // console.log(`sql=`, sql)
    // console.log(`params=`, params)
    const result = await query(sql, params)
    // console.log(`result=`, result)
  }

  // static async logbookEntries(transactionId) {
  //   console.log(`logbookEntries(${transactionId})`)
  //   const sql = `SELECT step_id AS stepId, sequence, message FROM atp_logbook WHERE transaction_id=? ORDER BY sequence`
  //   const params = [ trasactionId ]
  //   // console.log(`sql=`, sql)
  //   // console.log(`params=`, params)
  //   const result = await query(sql, params)
  //   // console.log(`result=`, result)
  //   return result
  // }

  /**
   * Get the log entries for a transaction.
   *
   * @param {string} txId Transaction ID
   * @returns A list of { stepId, level, source, message, created }
   */
  static async getLog(txId) {
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
  static async bulkLogging(txId, stepId, array) {
    // console.log(`bulkLogging(${txId}, ${stepId})`, array)
    if (array.length < 1) {
      return
    }
    let sql = `INSERT INTO atp_logbook (transaction_id, step_id, level, source, message) VALUES`
    const params = [ ]
    let sep = '\n'
    for (const entry of array) {
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
      let level = entry.level
      switch (level) {
        case dbLogbook.LOG_LEVEL_DEBUG:
        case dbLogbook.LOG_LEVEL_ERROR:
        case dbLogbook.LOG_LEVEL_TRACE:
        case dbLogbook.LOG_LEVEL_WARNING:
          break
        default:
          level = dbLogbook.LOG_LEVEL_UNKNOWN
      }
      let message = entry.message
      if (typeof(message) === 'object') {
        message = JSON.stringify(message, '', 0)
      }
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
    const result = await query(sql, params)
    // console.log(`result=`, result)
    if (result.affectedRows !== array.length) {
      console.log(`**************************************************************************`)
      console.log(`INTERNAL ERROR: Could not save atp_logbook records - logging not occurring`)
      console.log(`NEEED TO SAVE ${array.length} RECORDS BUT ONLY SAVED ${result.affectedRows}`)
      console.log(`**************************************************************************`)
    }
  }
}