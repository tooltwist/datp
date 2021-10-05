import me from '../ATP/me'
import query from './query'
import GenerateHash from "../ATP/GenerateHash"

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
  const json = response.getJson()

  let sql = `UPDATE atp_transaction_instance SET transaction_status=?, completion_time=NOW(3), response=? WHERE transaction_id=?`
  let params = [ finalStatus, json, txId ]

  // console.log(`sql=`, sql)
  // console.log(`params=`, params)
  const result = await query(sql, params)
  // console.log(`result=`, result)
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