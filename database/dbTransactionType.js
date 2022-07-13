/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import dbupdate from './dbupdate'
import query from './query'

export async function getPipelineType(pipelineType) {
  // console.log(`getPipelineType(${pipelineType})`)
  const sql = `
      SELECT transaction_type AS pipelineName, is_transaction_type AS isTransactionType, description, node_group AS nodeGroup, pipeline_version AS pipelineVersion, notes
      FROM atp_transaction_type
      WHERE transaction_type=?`
  const params = [ pipelineType ]
  // console.log(`sql=`, sql)
  // console.log(`params=`, params)
  const result = await query(sql, params)
  // console.log(`result=`, result)
  if (result.length !== 1) {
    return null
  }
  result[0].isTransactionType = (result[0].isTransactionType != 0)
  return result[0]
}


export async function db_updatePipelineType(pipelineName, updates) {
  // console.log(`updatePipelineType(${pipelineName})`, updates)
  let sql = `UPDATE atp_transaction_type SET `
  let params = [ ]
  let sep = ''
  if (typeof(updates.description) != 'undefined') {
    sql += `${sep}description=?`
    params.push(updates.description)
    sep = ', '
  }
  if (typeof(updates.isTransactionType) != 'undefined') {
    sql += `${sep}is_transaction_type=?`
    params.push(updates.isTransactionType)
    sep = ', '
  }

  if (typeof(updates.nodeGroup) != 'undefined') {
    sql += `${sep}node_group=?`
    params.push(updates.nodeGroup)
    sep = ', '
  }

  if (typeof(updates.pipelineVersion) != 'undefined') {
    sql += `${sep}pipeline_version=?`
    params.push(updates.pipelineVersion)
    sep = ', '
  }

  if (typeof(updates.notes) != 'undefined') {
    sql += `${sep}notes=?`
    params.push(updates.notes)
    sep = ', '
  }

  sql += ` WHERE transaction_type=?`
  params.push(pipelineName)
  // console.log(`sql=`, sql)
  // console.log(`params=`, params)
  const result = await dbupdate(sql, params)
  // console.log(`result=`, result)
  if (result.affectedRows !== 1) {
    throw new Error('Unknown pipelineType')
  }
}