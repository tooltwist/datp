import query from "../../database/query";
import TransactionIndexEntry from "../TransactionIndexEntry";
import TxData from "../TxData";
import Transaction from "./Transaction";


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
    // const input = tx.getInput()
    // const nodeId = tx.getNodeId()
    // const pipeline = tx.getPipeline()

    // const sql = `INSERT INTO atp_transaction2 (
    //   transaction_id, external_id, transaction_type, transaction_status, owner, initial_data, node_id, pipeline
    // ) VALUES (?,?,?,?,?,?,?,?)`
    // const status = TransactionIndexEntry.RUNNING //ZZZZ YARP2
    // const params = [ txId, externalId, type, status, owner, input.getJson(), nodeId, pipeline ]
    const sql = `INSERT INTO atp_transaction2 (transaction_id, owner, external_id, status) VALUES (?,?,?,?)`
    const status = TransactionIndexEntry.RUNNING //ZZZZ YARP2
    const params = [ txId, owner, externalId, status ]
    await query(sql, params)
    // console.log(`result=`, result)
  }


  static async persistDeltas(tx) {
    // console.log(`TransactionPersistance.persistDeltas(tx)`)

    const deltas = tx.getDeltas()
    // console.log(`deltas=`, deltas)

    if (deltas.length < 1) {
      return
    }

    // Save the deltas
    let sql = `INSERT INTO atp_transaction_delta (owner, transaction_id, sequence, step_id, data, event_time) VALUES `
    let params = [ ]
    let sep = ''
    for (let i = 0; i < deltas.length; i++) {
      const delta = deltas[i]
      sql += `${sep}(?,?,?,?,?,?)`
      sep = ','
      params.push(tx.getOwner())
      params.push(tx.getTxId())
      params.push(delta.sequence)
      params.push(delta.stepId)
      params.push(delta.data)
      params.push(delta.time)

      if (!delta.stepId) {
        if (delta.status) {
          newStatus = delta.status
        }
        if (delta.status) {
          newCompletionTime = delta.completionTime
        }
        if (delta.status) {
          newResponseAcknowledgeTime = delta.responseAcknowledgeTime
        }
      }
    }
    // console.log(`sql=`, sql)
    // console.log(`params=`, params)

    const result = await query(sql, params)
    // console.log(`result=`, result)


    // Look for changes to atp_transaction2 fields.
    let newStatus = tx.getStatus()
    let newCompletionTime = null
    let newResponseAcknowledgeTime = null
    for (let i = 0; i < deltas.length; i++) {
      const delta = deltas[i]
      if (delta.stepId === null) {
        if (delta.status) {
          newStatus = delta.status
        }
        if (delta.status) {
          newCompletionTime = delta.completionTime
        }
        if (delta.status) {
          newResponseAcknowledgeTime = delta.responseAcknowledgeTime
        }
      }
    }
    if (newStatus !== tx.getStatus() || newCompletionTime || newResponseAcknowledgeTime) {
      const sql2 = `UPDATE atp_transaction2 SET status=?, completion_time=?, response_acknowledge_time=? WHERE transaction_id=?`
      const params2 = [ newStatus, newCompletionTime, newResponseAcknowledgeTime, tx.getTxId() ]
      const result2 = await query(sql2, params2)
      console.log(`result2=`, result2)
    }

  }


  static async reconstructTransaction(txId) {
    const sql = `SELECT * from atp_transaction2 WHERE transaction_id=?`
    const params = [ txId ]
    const rows = await query(sql, params)
    // console.log(`rows=`, rows)

    if (rows.length < 1) {
      // Transaction not found
      return null
    }
    const tx = new Transaction(txId, rows[0].owner, rows[0].external_id)

    // Now add the deltas
    const sql2 = `SELECT * from atp_transaction_delta WHERE transaction_id=? ORDER BY sequence`
    const params2 = [ txId ]
    const rows2 = await query(sql2, params2)
    // console.log(`rows2=`, rows2)
    for (const row of rows2) {
      const data = JSON.parse(row.data)
      tx.delta(row.step_id, data)
    }
    return tx
  }
}