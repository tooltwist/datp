import query from './query'

export default {
  getPipeline,
}

// /**
//  *
//  * @param {TransactionIndexEntry} transactionIndexEntry
//  */
// async function save(transactionIndexEntry) {
//   // See if the transaction already exists
//   console.log(`dbTransactionType.save()`, transactionIndexEntry)

//   const transactionId = transactionIndexEntry.getTxId()
//   const transactionType = transactionIndexEntry.getType()
//   const nodeId = me.getNodeId()
//   const nodeName = me.getName()
// }





async function getPipeline(transactionType) {
  // console.log(`getPipeline(${transactionType})`)
  const sql = `
      SELECT description, pipeline_name AS pipelineName, node_name AS nodeName, pipeline_version AS pipelineVersion
      FROM atp_transaction_type
      WHERE transaction_type=?`
  const params = [ transactionType ]
  // console.log(`sql=`, sql)
  // console.log(`params=`, params)
  const result = await query(sql, params)
  // console.log(`result=`, result)
  if (result.length !== 1) {
    return null
  }
  return result[0]
}