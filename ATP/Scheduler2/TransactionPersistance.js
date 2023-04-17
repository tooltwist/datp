/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import query from "../../database/query";
import TransactionIndexEntry from "../TransactionIndexEntry";
import TransactionState from "./TransactionState";
import { DEBUG_DB_ATP_TRANSACTION, DEBUG_DB_ATP_TRANSACTION_DELTA } from '../../datp-constants'
// import { logger } from '../../lib/pino-deltas'
// import dogapi from 'dogapi'
import juice from '@tooltwist/juice-client'
import { schedulerForThisNode } from '../..'
import AWS from 'aws-sdk'
import dbupdate from "../../database/dbupdate";


const VERBOSE = 0

let hackCount = 0
let countDbAtpTransactionDelta = 0
let countDbAtpTransactionInsert = 0
let _deltaDestination = null
let _datadogInitialized = false
const MAX_DATADOG_JSON_SIZE = 4000
let _dynamoDB = null



export default class TransactionPersistance {
  /**
   *
   * @param {TransactionState} tx
   */
  // static async saveNewTransaction(tx) {
  //   const txId = tx.getTxId()
  //   const owner = tx.getOwner()
  //   const externalId = tx.getExternalId()
  //   const transactionType = tx.getTransactionType()

  //   console.log(`saveNewTransaction(): NOT SAVING TX TO DB`.cyan)
  //   return

  //   try {
  //     if (DEBUG_DB_ATP_TRANSACTION) console.log(`TX INSERT ${countDbAtpTransactionInsert++}`)
  //     const sql = `INSERT INTO atp_transaction2 (transaction_id, owner, external_id, transaction_type, status) VALUES (?,?,?,?,?)`
  //     const status = TransactionIndexEntry.RUNNING //ZZZZ YARP2
  //     const params = [ txId, owner, externalId, transactionType, status ]
  //     await dbupdate(sql, params)
  //     // console.log(`result=`, result)
  //   } catch (e) {
  //     // See if there was a problem with the externalId
  //     if (e.code === 'ER_DUP_ENTRY') {
  //       // A transaction already exists with this externalId
  //       console.log(`TransactionPersistance:DuplicateExternalIdError - detected duplicate externalId during DB insert`)
  //       throw new DuplicateExternalIdError()
  //     }
  //     throw e
  //   }
  // }//- saveNewTransaction()


  static async deltaDestination() {
    if (!_deltaDestination) {
      const dest = await juice.string('datp.deltaDestination', 'db')
      switch (dest) {
        case 'pico':
        case 'db':
        case 'datadog':
        case 'dynamodb':
          _deltaDestination = dest
          break

        case 'none':
          if (hackCount++ == 0) {
            console.log(`WARNING!!!!!`)
            console.log(`Not saving transaction deltas`)
          }
          _deltaDestination = dest
          break
          
        default:
          console.log(`Error: unknown datp.deltaDestination [${dest}]`)
          console.log(`Should be pico | db | none`)
          console.log(`Will proceed with 'db'.`)
          _deltaDestination = 'db'
      }
    }
    return _deltaDestination
  }


  static async persistDelta(owner, txId, delta) {
    if (VERBOSE) console.log(`TransactionPersistance.persistDelta()`, delta)

    const dest = await TransactionPersistance.deltaDestination()
    switch (dest) {
      case 'none':
        // Nothing to do
        return

      case 'db':
        return await TransactionPersistance.persistDelta_database(owner, txId, delta)

      // case 'pico':
      //   return await TransactionPersistance.persistDelta_pico(owner, txId, delta)

      // case 'datadog':
      //   return await TransactionPersistance.persistDelta_datadog(owner, txId, delta)

      case 'dynamodb':
        return await TransactionPersistance.persistDelta_dynamodb(owner, txId, delta)
    }
  }


  static async persistDelta_database(owner, txId, delta) {
    if (VERBOSE) console.log(`TransactionPersistance.persistDelta_database()`, delta)

    const json = JSON.stringify(delta.data)

    // Save the deltas
    if (DEBUG_DB_ATP_TRANSACTION_DELTA) console.log(`persistDelta_database ${countDbAtpTransactionDelta++}`)
    let sql = `INSERT INTO atp_transaction_delta (owner, transaction_id, sequence, step_id, data, event_time) VALUES (?,?,?,?,?,?)`
    let params = [
      owner,
      txId,
      delta.sequence,
      delta.stepId,
      json,
      delta.time
    ]
    // console.log(`sql=`, sql)
    // console.log(`params=`, params)
    const result = await dbupdate(sql, params)
    // console.log(`result=`, result)
    if (result.affectedRows !== 1) {
      // THIS IS A SERIOUS PROBLEM
      //ZZZZ Handle this better
      console.log(``)
      console.log(``)
      console.log(``)
      console.log(`SERIOUS ERROR: Unable to save to transaction journal`)
      console.log(``)
      console.log(``)
      console.log(``)
      console.log(``)
      throw new Error(`Unable to write to atp_transaction_delta`)
    }
  }

  // static async persistDelta_pico(owner, txId, delta) {
  //   if (VERBOSE) console.log(`TransactionPersistance.persistDelta_pico()`, delta)

  //   const json = JSON.stringify(delta.data)
  //   logger.info({ owner, txId, json })
  // }

  // static async persistDelta_datadog(owner, txId, delta) {
  //   if (VERBOSE) console.log(`TransactionPersistance.persistDelta_datadog()`)

  //   if (!_datadogInitialized) {
  //     const options = {
  //       api_key: "6d7339e5df2a7f892e6eb10cd0198fe0",
  //       app_key: "22189a6e9ccd70d87dbb4295339438bf3cf0a3d5"
  //     };
  //     dogapi.initialize(options);
  //     _datadogInitialized = true
  //   }

  //   let json = JSON.stringify(delta.data)
  //   // logger.info({ owner, txId, json })
  //   while (json) {
  //     console.log(`Saving ${json.length} bytes to datadog`)
  //     // See https://brettlangdon.github.io/node-dogapi/#event-create
  //     let title = `Delta for ${txId}`
  //     let text
  //     if (json.length > MAX_DATADOG_JSON_SIZE) {
  //       text = json.substring(0, MAX_DATADOG_JSON_SIZE)
  //       json = json.substring(MAX_DATADOG_JSON_SIZE)
  //     } else {
  //       text = json
  //       json = null
  //     }
  //     const myNodeGroup = await schedulerForThisNode.getNodeGroup()
  //     const myNodeId = await schedulerForThisNode.getNodeId()

  //     const properties = {
  //       tags: ["some:tag"],
  //       alert_type: "error",
  //       date_happened: Math.floor(delta.time.getTime() / 1000),
  //       //priority
  //       host: myNodeId,
  //       tags: [
  //         `owner:${owner}`,
  //         `txId:${txId}`,
  //         `stepId:${delta.stepId}`,
  //         `sequence:${delta.sequence}`,
  //         `nodeGroup:${myNodeGroup}`,
  //         `nodeId:${myNodeId}`
  //       ],
  //       alert_type: "error",
  //       aggregation_key: txId,
  //       // source_type_name: 'user',

  //       // delta fields:
  //       // owner,
  //       // txId,
  //       // delta.sequence,
  //       // delta.stepId,
  //       // json,
  //       // delta.time
  //     };
  //     console.log(`------------------------------`)
  //     console.log(`properties=`, properties)
  //     dogapi.event.create(title, text, properties, function(err, res){
  //       if (err) {
  //         console.log(`Error:`, err)
  //       } else {
  //         console.log(`res=`, res)
  //       }
  //     });
  //   }
  // }

  static async initializeDynamoDB() {
    if (!_dynamoDB) {

      AWS.config.update({
        region: "local", // replace with your region in AWS account
        endpoint: "http://localhost:8000"
      });
      _dynamoDB = new AWS.DynamoDB();
    }
  }

  static async persistDelta_dynamodb(owner, txId, delta) {
    if (VERBOSE) console.log(`TransactionPersistance.persistDelta_dynamodb()`, delta)

    const json = JSON.stringify(delta.data)

    // Save the deltas
    if (DEBUG_DB_ATP_TRANSACTION_DELTA) console.log(`persistDelta_dynamodb ${countDbAtpTransactionDelta++}`)

    const unixTimeNow = Math.floor(Date.now() / 1000)
    const params = {
      TableName: "transaction_delta",
      Item: {
        owner: { S: owner },
        transaction_id: { S: txId },
        sequence_no: { N: `${delta.sequence}` },
        step_id: { S: delta.stepId ? delta.stepId : '' },
        data: { S: json },
        event_time: { S: delta.time.toISOString() },
        log_time: { N: `${unixTimeNow}` },
        // yarp: { S: null }
        // ttl: { N: expiryTime }
      }
    }
    // console.log(`params=`, params)

    try {
      await TransactionPersistance.initializeDynamoDB()
      await _dynamoDB.putItem(params).promise()
    } catch (err) {
      console.log(``)
      console.log(``)
      console.log(``)
      console.log(`SERIOUS ERROR: Unable to save to transaction journal`, err)
      console.log(`params=`, params)
      console.log(``)
      console.log(``)
      console.log(``)
      console.log(``)
      throw new Error(`Unable to write transaction_delta to dynamoDB`)
    }
    // console.log(`Added transaction_delta for tx ${txId}, ${delta.sequence}`);
  }


  static async reconstructTransaction(owner, txId, delta) {
    if (VERBOSE) console.log(`TransactionPersistance.reconstructTransaction()`, delta)

    const dest = await TransactionPersistance.deltaDestination()
    switch (dest) {
      case 'none':
        // Nothing to do
        return

      case 'db':
        return await TransactionPersistance.reconstructTransaction_database(owner, txId, delta)

      // case 'pico':
      //   return await TransactionPersistance.reconstructTransaction_pico(owner, txId, delta)

      // case 'datadog':
      //   return await TransactionPersistance.reconstructTransaction_datadog(owner, txId, delta)

      case 'dynamodb':
        return await TransactionPersistance.reconstructTransaction_dynamodb(owner, txId, delta)
    }
  }

  /**
   *
   * @param {string} txId Transaction ID
   * @returns Promise<TransactionState>
   */
  static async reconstructTransaction_database(txId) {
    if (VERBOSE) console.log(`reconstructTransaction(${txId})`)
    const sql = `SELECT * from atp_transaction2 WHERE transaction_id=?`
    const params = [ txId ]
    const rows = await query(sql, params)
    // console.log(`rows=`, rows)

    if (rows.length < 1) {
      // Transaction not found
      return null
    }
    const tx = new TransactionState({
      txId,
      owner: rows[0].owner,
      externalId: rows[0].external_id,
      transactionData: {
        transactionType: rows[0].transaction_type
      }
    })

    // Now add the deltas
    const sql2 = `SELECT * from atp_transaction_delta WHERE transaction_id=? ORDER BY sequence`
    const params2 = [ txId ]
    const rows2 = await query(sql2, params2)
    // console.log(`rows2=`, rows2)
    if (rows2.length < 1) {
      return null
    }
    for (const row of rows2) {
      const data = JSON.parse(row.data)
      if (data.completionTime) {
        data.completionTime = new Date(data.completionTime)
      }
      const replaying = true // Prevent DB being updated (i.e. duplicating the journal)
      await tx.delta(row.step_id, data, 'TransactionPersistance.reconstructTransaction()', replaying)
    }
    return tx
  }

  /**
   *
   * @param {string} txId Transaction ID
   * @returns Promise<TransactionState>
   */
   static async reconstructTransaction_dynamodb(txId) {
    if (VERBOSE) console.log(`reconstructTransaction_dynamodb(${txId})`)

    // Get the transaction summary first, to check it's a valid transaction ID.
    const sql = `SELECT * from atp_transaction2 WHERE transaction_id=?`
    const params = [ txId ]
    const rows = await query(sql, params)
    // console.log(`rows=`, rows)
    if (rows.length < 1) {
      // Transaction not found
      return null
    }
    const tx = new TransactionState({
      txId,
      owner: rows[0].owner,
      externalId: rows[0].external_id,
      transactionData: {
        transactionType: rows[0].transaction_type
      }
    })

    // Now add the deltas
    try {
      await TransactionPersistance.initializeDynamoDB()
      // await _dynamoDB.putItem(params).promise()
      // const data = await _dynamoDB.getItem(params2).promise()
      const params2 = {
        ExpressionAttributeValues: {
          ":txId": { S: txId }
        },
        KeyConditionExpression: "transaction_id = :txId", 
        // ProjectionExpression: "sequence_no, step_id, data",
        TableName: "transaction_delta",
      };
      const reply = await _dynamoDB.query(params2).promise()
      // console.log(`reply=`, reply)
      // console.log(`reply.Items=`, reply.Items)

      if (reply.Count > 0) {

        // Sort the items
        reply.Items.sort((a, b) => {
          const numA = parseInt(a.sequence_no.N)
          const numB = parseInt(b.sequence_no.N)
          if (numA < numB) return -1
          if (numA > numB) return +1
          return 0
        })

        // Apply the deltas to the transaction
        for (const item of reply.Items) {
          const data = JSON.parse(item.data.S)
          const stepId = item.step_id.S
          // console.log(`data=`, data)
          // if (data.completionTime) {
          //   data.completionTime = new Date(data.completionTime)
          // }
          const replaying = true // Prevent DB being updated (i.e. duplicating the journal)
          await tx.delta(stepId, data, 'TransactionPersistance.reconstructTransaction()', replaying)
        }//- for
        // console.log(`tx=`, tx.asJSON(true))
        return tx
      }
    } catch (err) {
      console.log(``)
      console.log(``)
      console.log(``)
      console.log(`SERIOUS ERROR: Unable to select transaction_delta records (txId=${txId})`, err)
      console.log(``)
      console.log(``)
      console.log(``)
      console.log(``)
      throw new Error(`Unable to read transaction_delta from dynamoDB`)
    }

    return null
  }
}
