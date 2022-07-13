/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import query from './query'
import { DEBUG_DB_ATP_LOGBOOK } from '../datp-constants'
import { schedulerForThisNode } from '..'
import { logger } from '../lib/pino-logbook'
import juice from '@tooltwist/juice-client'
import dbupdate from './dbupdate'

let hackCount = 0
let countDbAtpLogbook = 0
let _logDestination = null

export default class dbLogbook {

  static LOG_LEVEL_TRACE = 'trace'
  static LOG_LEVEL_DEBUG = 'debug'
  static LOG_LEVEL_INFO = 'info'
  static LOG_LEVEL_WARNING = 'warning'
  static LOG_LEVEL_ERROR = 'error'
  static LOG_LEVEL_FATAL = 'fatal'
  static LOG_LEVEL_UNKNOWN = 'unknown'

  static LOG_SOURCE_INVOKE = 'invoke'
  static LOG_SOURCE_ROLLBACK = 'rollback'
  static LOG_SOURCE_EXCEPTION = 'exception'
  static LOG_SOURCE_DEFINITION = 'definition'
  static LOG_SOURCE_SYSTEM = 'system'
  static LOG_SOURCE_PROGRESS_REPORT = 'progReport'
  static LOG_SOURCE_UNKNOWN = 'unknown'

  static async logDestination() {
    if (!_logDestination) {
      const dest = await juice.string('datp.logDestination', 'db')
      switch (dest) {
        case 'pico':
        case 'db':
          _logDestination = dest
          break

        case 'none':
          if (hackCount++ == 0) {
            console.log(`WARNING!!!!!`)
            console.log(`Not saving log entries`)
          }  
          _logDestination = dest
          break
          
        default:
          console.log(`Error: unknown datp.logDestination [${dest}]`)
          console.log(`Should be pico | db | none`)
          console.log(`Will proceed with 'db'.`)
          _logDestination = 'db'
      }
    }
    return _logDestination
  }


  // /**
  //  *
  //  * @param {String} msg
  //  */
  // static async log(transactionId, stepId, sequence, msg) {
  //   console.log(`log(${transactionId}, ${stepId}, ${sequence}, ${msg})`)

  //   if (typeof(json) !== 'string') {
  //     throw new Error('log() requires string value for msg parameter')
  //   }
  //   if (HACK_TO_BYPASS_LOGGING_WHILE_TESTING) {
  //     if (hackCount++ == 0) {
  //       console.log(`WARNING!!!!!`)
  //       console.log(`Not saving log entries (HACK_TO_BYPASS_LOGGING_WHILE_TESTING=true)`)
  //     }
  //   }
  //   if (DEBUG_DB_ATP_LOGBOOK) console.log(`atp_logbook INSERT ${countDbAtpLogbook++}`)
  //   const sql = `INSERT INTO atp_logbook (transaction_id, step_id, sequence, message) VALUES (?, ?, ?, ?)`
  //   const params = [ transactionId, stepId, sequence, msg ]
  //   // console.log(`sql=`, sql)
  //   // console.log(`params=`, params)
  //   const result = await dbupdate(sql, params)
  //   // console.log(`result=`, result)
  // }

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

    // Where will we send these logs?
    const dest = await dbLogbook.logDestination()
    switch (dest) {
      case 'none':
        // Nothing to do
        return

      case 'db':
        return await dbLogbook.bulkLogging_database(txId, stepId, array)

      case 'pico':
        return await dbLogbook.bulkLogging_pico(txId, stepId, array)
    }
  }


  static async bulkLogging_database(txId, stepId, array) {
    // console.log(`bulkLogging_database(${txId}, ${stepId})`, array)
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


  /**
   * Save a list of log entries to the pico logstream.
   *
   * @param {string} txId Transaction ID
   * @param {string} stepId If null, the log message applies to the transaction
   * @param {*} array Array of { level, source, message }
   */
   static async bulkLogging_pico(txId, stepId, array) {
    // console.log(`bulkLogging_pico(${txId}, ${stepId})`, array)

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

      let message = entry.message
      if (typeof(message) === 'object') {
        message = JSON.stringify(message, '', 0)
      }

      const basicInfo = {
        txId,
        stepId,
        source,
        nodeGroup: schedulerForThisNode.getNodeGroup(),
        nodeId: schedulerForThisNode.getNodeId(),
      }

      switch (entry.level) {
        case dbLogbook.LOG_LEVEL_TRACE:
          logger.trace(basicInfo, message)
          break
        case dbLogbook.LOG_LEVEL_DEBUG:
          logger.debug(basicInfo, message)
          break
        case dbLogbook.LOG_LEVEL_WARNING:
          logger.warn(basicInfo, message)
          break
        case dbLogbook.LOG_LEVEL_ERROR:
          logger.error(basicInfo, message)
          break
        case dbLogbook.LOG_LEVEL_FATAL:
          logger.fatal(basicInfo, message)
          break
          
        case dbLogbook.LOG_LEVEL_INFO:
        default:
          logger.info(basicInfo, message)
          break
      }
    }//- for
  }
}
