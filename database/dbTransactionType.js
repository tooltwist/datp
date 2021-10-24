/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
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