import me from '../ATP/me'
import query from './query'
import GenerateHash from "../ATP/GenerateHash"
import TransactionIndexEntry from '../ATP/TransactionIndexEntry'

export default {
  persist,
  saveFinalStatus,
  getTransaction,
}

/**
 *
 * @param {TransactionIndexEntry} transactionIndexEntry
 * @param {String} pipelineName
 */
async function persist(transactionIndexEntry, pipelineName) {
  // See if the transaction already exists
  // console.log(`dbTransactionInstance.persist()`, transactionIndexEntry)

  const transactionId = await transactionIndexEntry.getTxId()
  const initiatedBy = await transactionIndexEntry.getInitiatedBy()
  const transactionType = await transactionIndexEntry.getTransactionType()
  const transactionStatus = await transactionIndexEntry.getTransactionStatus()
  const nodeId = await me.getNodeId()
  const nodeName = await me.getName()
  const txData = await transactionIndexEntry.getInitialData()
  // console.log(`txData=`, txData)
  // console.log(`typeof(txData)=`, typeof(txData))
  // const data = await txData.getData()
  const json = await txData.getJson()
  // console.log(`json=`, json)
  const inquiryToken = GenerateHash('it')


  const sql = `INSERT INTO atp_transaction_instance (transaction_id, transaction_type, transaction_status, initiated_by,  node_id, pipeline_name, initial_data, inquiry_token) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`
  const params = [ transactionId, transactionType, transactionStatus, initiatedBy, nodeId, pipelineName, json, inquiryToken ]
  // console.log(`sql=`, sql)
  // console.log(`params=`, params)
  const result = await query(sql, params)
  // console.log(`result=`, result)

  return inquiryToken
}

async function saveFinalStatus(txId, finalStatus, response) {

  try {
    // const zzz = Math.round((Math.random() * 1000) % 1000)
    // console.log(`\n\nWHAMMO 1 - ${zzz}\n\n`)

    // Check the status has not already been set. If it has, then the step is
    // running two thread, most likely because it did not 'await' when completing.
    let sql1 = `SELECT transaction_status FROM atp_transaction_instance WHERE transaction_id=?`
    let params1 = [ txId ]
    const result1 = await query(sql1, params1)
    if (result1.length < 1) {
      const msg = `Internal Error: trying to set status of transaction not in the database [${txId}]`
      console.trace(msg)
      throw new Error(msg)
    }
    if (result1[0].transaction_status !== TransactionIndexEntry.RUNNING) {
      console.trace(`BIG PROBLEM - attempt to set transaction completion status twice. Step might not be using 'await' when completing.`)
      throw new Error(`Fatal error in transaction. Attempt to set transaction completion status twice. Step might not be using 'await' when completing.`)
    }


    // Set the completion status and the response from the transaction
    const json = response.getJson()
    let sql = `UPDATE atp_transaction_instance SET transaction_status=?, completion_time=NOW(3), response=? WHERE transaction_id=?`
    let params = [ finalStatus, json, txId ]
    // console.log(`\n\nWHAMMO 2 - ${zzz}\n\n`)

    // console.log(`sql=`, sql)
    // console.log(`params=`, params)
    const result = await query(sql, params)
    // console.log(`\n\nWHAMMO 3 - ${zzz}\n\n`)
    // console.log(`result=`, result)
    // console.log(`\n\nWHAMMO 4 - ${zzz}\n\n`)
  } catch (e) {
    // console.log(`\n\nWHAMMO 999\n\n`)
    console.trace(e)
    throw e
  }
}

async function getTransaction(txId) {
  const sql = `SELECT
    transaction_id AS transactionId,
    transaction_type AS transactionType,
    transaction_status AS status,
    node_id AS nodeId,
    initiated_by AS initiatedBy,
    pipeline_name AS pipelineName,
    initial_data AS initialData,
    start_time AS startTime,
    completion_time AS completionTime,
    response,
    inquiry_token AS inquiryToken,
    response_method AS responseMethod
  FROM atp_transaction_instance WHERE transaction_id=?`
  const params = [ txId ]

  // console.log(`sql=`, sql)
  // console.log(`params=`, params)
  const result = await query(sql, params)
  // console.log(`result=`, result)
  if (result.length > 0) {
    return result[0]
  }
  return null
}