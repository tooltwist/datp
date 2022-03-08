/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import query from "../../database/query";
import TransactionIndexEntry from "../TransactionIndexEntry";
import Transaction from "./Transaction";
import { HACK_TO_BYPASS_TXDELTAS_WHILE_TESTING } from '../../datp-constants'

const VERBOSE = 0
let hackCount = 0

export class DuplicateExternalIdError extends Error {
  constructor() {
    super('Transaction with this externalId already exists')
  }
}


export default class TransactionPersistance {
  /**
   *
   * @param {Transaction} tx
   */
  static async saveNewTransaction(tx) {
    const txId = tx.getTxId()
    // const type = tx.getType()
    const owner = tx.getOwner()
    const externalId = tx.getExternalId()
    const transactionType = tx.getTransactionType()
    // const input = tx.getInput()
    // const nodeId = tx.getNodeId()
    // const pipeline = tx.getPipeline()


    try {
      const sql = `INSERT INTO atp_transaction2 (transaction_id, owner, external_id, transaction_type, status) VALUES (?,?,?,?,?)`
      const status = TransactionIndexEntry.RUNNING //ZZZZ YARP2
      const params = [ txId, owner, externalId, transactionType, status ]
      await query(sql, params)
      // console.log(`result=`, result)
    } catch (e) {
      // See if there was a problem with the externalId
      if (e.code === 'ER_DUP_ENTRY') {
        // A transaction already exists with this externalId
        console.log(`TransactionPersistance:DuplicateExternalIdError - detected duplicate externalId during DB insert`)
        throw new DuplicateExternalIdError()
      }
      throw e
    }
}


  static async persistDelta(owner, txId, delta) {
    if (VERBOSE) console.log(`TransactionPersistance.persistDelta()`, delta)

    if (HACK_TO_BYPASS_TXDELTAS_WHILE_TESTING) {
      if (hackCount++ == 0) {
        console.log(`WARNING!!!!!`)
        console.log(`Not saving transaction deltas (HACK_TO_BYPASS_TXDELTAS_WHILE_TESTING=true)`)
      }
      return
    }

    const json = JSON.stringify(delta.data)

    // Save the deltas
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
    const result = await query(sql, params)
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

  /**
   *
   * @param {string} txId Transaction ID
   * @returns Promise<Transaction>
   */
  static async reconstructTransaction(txId) {
    if (VERBOSE) console.log(`reconstructTransaction(${txId})`)
    const sql = `SELECT * from atp_transaction2 WHERE transaction_id=?`
    const params = [ txId ]
    const rows = await query(sql, params)
    // console.log(`rows=`, rows)

    if (rows.length < 1) {
      // Transaction not found
      return null
    }
    const tx = new Transaction(txId, rows[0].owner, rows[0].external_id, rows[0].transaction_type)

    // Now add the deltas
    const sql2 = `SELECT * from atp_transaction_delta WHERE transaction_id=? ORDER BY sequence`
    const params2 = [ txId ]
    const rows2 = await query(sql2, params2)
    // console.log(`rows2=`, rows2)
    for (const row of rows2) {
      const data = JSON.parse(row.data)
      if (data.completionTime) {
        data.completionTime = new Date(data.completionTime)
      }
      const replaying = true // Prevent DB being updated (i.e. duplicating the journal)
      await tx.delta(row.step_id, data, replaying)
    }
    return tx
  }
}