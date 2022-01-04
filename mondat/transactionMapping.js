/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import query from '../database/query'
import errors from 'restify-errors';

export async function getTransactionMappingsV1(req, res, next) {
  // console.log(`getTransactionMappingsV1()`)

  const sql = `SELECT
    transaction_type AS transactionType,
    pipeline_name AS pipelineName,
    pipeline_version AS pipelineVersion
    FROM atp_transaction_type
    ORDER BY transactionType`
  const reply =  await query(sql)
  // console.log(`reply=`, reply)

  res.send(reply)
  return next();
}

export async function saveTransactionMappingsV1(req, res, next) {
  // console.log(`saveTransactionMappingsV1()`)
  // console.log(`req.params=`, req.params)
  // console.log(`req.body=`, req.body)
  const node = 'master'

  try {
    // Try updating first
    let sql = `UPDATE atp_transaction_type SET pipeline_name=?, pipeline_version=?, node_name=?`
    let params = [ req.body.pipelineName, req.body.pipelineVersion, node ]
    if (req.body.description) {
      sql += `, description`
      params.push(req.body.description)
    }
    sql += ` WHERE transaction_type=?`
    params.push(req.body.transactionType)
    // console.log(`sql=`, sql)
    // console.log(`params=`, params)
    let result = await query(sql, params)
    // console.log(`result=`, result)
    if (result.affectedRows === 1) {
      res.send({ status: 'ok' })
      return next()
    }

    // Update failed, must be a new transaction type.
    let sql2a = `INSERT INTO atp_transaction_type (transaction_type, pipeline_name, pipeline_version, node_name`
    let sql2b = `) VALUES (?, ?, ?, ?`
    let sql2c = ` )`
    let params2 = [ req.body.transactionType, req.body.pipelineName, req.body.pipelineVersion, node ]
    if (req.body.description) {
      sql2a += `, description`
      sql2b += `, ?`
      params.push(req.body.description)
    }
    let sql2 = `${sql2a}${sql2b}${sql2c}`
    // console.log(`sql2=`, sql2)
    // console.log(`params2=`, params2)
    result = await query(sql2, params2)
    // console.log(`result=`, result)

    if (result.affectedRows === 1) {
      res.send({ status: 'ok' })
      return next()
    }
  } catch (e) {
    console.log(`saveTransactionMappingsV1(): Exception saving mapping:`, e)
  }
  res.send(new errors.InternalServerError(`Unable to save new mapping`))
  return next()
}

export async function deleteTransactionMappingsV1(req, res, next) {
  console.log(`deleteTransactionMappingsV1()`)
  // console.log(`req.params=`, req.params)

  try {
    const sql = `DELETE FROM atp_transaction_type WHERE transaction_type=?`
    const params = [ req.params.transactionType ]
    // console.log(`sql=`, sql)
    // console.log(`params=`, params)
    const reply =  await query(sql, params)
    // console.log(`reply=`, reply)
    if (reply.affectedRows === 1) {
      res.send({ status: 'ok' })
      return next()
    }
  } catch (e) {
    console.log(`deleteTransactionMappingsV1(): Exception deleting mapping:`, e)
  }
  res.send(new errors.InternalServerError(`Unable to delete mapping`))
  return next()
}
