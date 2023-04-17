/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */

// See https://github.com/DataDog/datadog-api-client-typescript


// // See https://brettlangdon.github.io/node-dogapi/
// var dogapi = require("dogapi");
// var options = {
//   api_key: "6d7339e5df2a7f892e6eb10cd0198fe0",
//   app_key: "22189a6e9ccd70d87dbb4295339438bf3cf0a3d5"
// };

// // See https://docs.datadoghq.com/logs/log_collection/nodejs/?tab=winston30
// const { createLogger, format, transports } = require('winston');
// const httpTransportOptions = {
//   host: 'http-intake.logs.datadoghq.com',
//   path: `/api/v2/logs?dd-api-key=6d7339e5df2a7f892e6eb10cd0198fe0&ddsource=nodejs&service=datp`,
//   ssl: true
// };
// const logger = createLogger({
//   level: 'info',
//   exitOnError: false,
//   format: format.json(),
//   transports: [
//     new transports.Http(httpTransportOptions),
//   ],
// });

import axios from 'axios'
import { DEBUG_DB_ATP_LOGBOOK, logDestination } from '../datp-constants'
import { LogbookHandler } from './Logbook'
import dbLogbook from './dbLogbook'
import { schedulerForThisNode } from '..';
import juice, { MANDATORY } from '@tooltwist/juice-client';

// let hackCount = 0
let countDbAtpLogbook = 0
const VERBOSE = 0


// See https://github.com/winstonjs/winston/blob/master/docs/transports.md#datadog-transport
var winston = require('winston')
var DatadogWinston = require('datadog-winston')




export default class DatadogLogbook extends LogbookHandler {
  #initialized
  #winstonLogger
  #apikey
  #appkey

  constructor () {
    super()
    this.#initialized = false
    this.#winstonLogger = null
    this.#apikey = null
    this.#appkey = null
    }

  async _checkInitialized() {
    if (!this.#initialized) {
      // dogapi.initialize(options)

      // Initialize Winston
      console.log(`Initializing Winston for DATP.`)
      this.#apikey = await juice.string('datp.datadog.apikey', MANDATORY)
      this.#appkey = await juice.string('datp.datadog.appkey', MANDATORY)
  
      this.#winstonLogger = winston.createLogger({
        // Whatever options you need
        // Refer https://github.com/winstonjs/winston#creating-your-own-logger
      })

      this.#winstonLogger.add(
        new DatadogWinston({
          apiKey: this.#apikey,
          hostname: schedulerForThisNode.getNodeId(),
          // hostname: 'fred',
          // service: 'datp',
          // ddsource: 'nodejs',
          // ddtags: `nodeGroup:${schedulerForThisNode.getNodeGroup()}`,
        })
      )
      if (VERBOSE) this.#winstonLogger.add(new winston.transports.Console({ }));

      this.#initialized = true
    }
  }

  /**
   * Get the log entries for a transaction.
   *
   * @param {string} txId Transaction ID
   * @returns A list of { stepId, level, source, message, created }
   */
  async getLog(txId) {
    if (VERBOSE) console.log(`DatadogLogbook.getLog(${txId})`)

    await this._checkInitialized()
    // See https://docs.datadoghq.com/logs/guide/access-your-log-data-programmatically/
    const url = 'https://api.datadoghq.com/api/v2/logs/events/search'
    const data = {
      filter: {
        query: `@txId:${txId}`,
      },
      page: { limit: 10 }
    }
    console.log(`data=`, data)

    try {
      const reply = await axios.post(url, data, {
        headers: {
          "Accept": "application/json",
          "Content-Type": "application/json",
          "DD-API-KEY": this.#apikey,
          "DD-APPLICATION-KEY": this.#appkey
        },
      })
      // console.log(`reply=`, reply)
      // console.log(`reply.data=`, reply.data)
      // console.log(`reply.data.data=`, reply.data.data)

      // Sort by timestamp
      reply.data.data.sort((a, b) => {
        // Look at the timestamp
        const t1 = a.attributes.attributes.ts
        const t2 = b.attributes.attributes.ts
        // console.log(`${t1} vs ${t2}`)
        if (t1 < t2) return -1
        if (t1 > t2) return +1

        // Look at the counter, within the same timestamp
        const c1 = a.attributes.attributes.c
        const c2 = b.attributes.attributes.c
        // console.log(`${c1} vs ${c2}`)
        if (c1 < c2) return -1
        if (c1 > c2) return +1
        return 0
      })

      const logs = [ ]
      for (const rec of reply.data.data) {
        // console.log(`log.attributes=`, rec.attributes)

        logs.push({
          txId: rec.attributes.attributes.stepId,
          stepId: rec.attributes.attributes.stepId,
          level: rec.attributes.attributes.level,
          source: rec.attributes.attributes.source,
          message: rec.attributes.message,
          created: new Date(rec.attributes.attributes.ts)
        })
      }
      // console.log(`logs=`, logs)
      // console.log(`logs.length=`, logs.length)
      return logs
    }
    catch (e) {
      console.log(`e=`, e)
      console.log(`e.response.data=`, e.response.data)
      return [ ]
    }
  }


  /**
   * Save a list of log entries
   *
   * @param {string} txId Transaction ID
   * @param {string} stepId If null, the log message applies to the transaction
   * @param {*} array Array of { level, source, message }
   */
  async bulkLogging(txId, stepId, array) {
    if (VERBOSE) console.log(`DatadogLogbook.bulkLogging(${txId}, ${stepId})`, array)
    await this._checkInitialized()

    if (array.length < 1) {
      return
    }
    if (DEBUG_DB_ATP_LOGBOOK) console.log(`atp_logbook INSERT ${countDbAtpLogbook++}`)
    let cnt = 0
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
      const sequence = entry.fullSequence ? entry.fullSequence : txId.substring(3, 9)
      //ZZZZ VOG Use vogPath?

      // Use JSON as the message if necessary
      let message
      if (typeof(entry.message) === 'object') {
        message = JSON.stringify(entry.message, '', 0)
        // message = entry.fullSequence + ' ' + JSON.stringify(entry.message, '', 0)
      } else {
        message = entry.message
        // message = entry.fullSequence + ' ' + entry.message
      }

      let title = `${txId}`
      if (stepId) {
        title += `:${stepId}`
      }
      // const text = message
      var properties = {
        sequence,
        ts: entry.ts,
        // tags: [
        //   `txId:${txId}`,
        //   `stepId:${stepId}`
        // ],
        // alert_type: "error",
        txId,
        stepId,
        source,
        level: entry.level,
        nodeId: schedulerForThisNode.getNodeId(),
        nodeGroup: schedulerForThisNode.getNodeGroup(),
        c: cnt++,
      };
      // dogapi.event.create(title, text, properties, function(err, res){
      //   console.dir(res);
      // });

      // See logger.info('Hello log with metas',{color: 'blue' });
      
      // What logging level?
      switch (entry.level) {
        case dbLogbook.LOG_LEVEL_TRACE:
        case dbLogbook.LOG_LEVEL_DEBUG:
        case dbLogbook.LOG_LEVEL_INFO:
          this.#winstonLogger.info(message, properties)
          break

        case dbLogbook.LOG_LEVEL_WARNING:
          this.#winstonLogger.warn(message, properties)
          break

        case dbLogbook.LOG_LEVEL_ERROR:
        case dbLogbook.LOG_LEVEL_FATAL:
          this.#winstonLogger.error(message, properties)
          break

        default:
          properties.level =  dbLogbook.LOG_LEVEL_UNKNOWN
          this.#winstonLogger.error(message, properties)
          break
      }

    }// for
  }//- bulkLogging
}