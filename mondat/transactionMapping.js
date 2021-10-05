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

  try {
    // Try updating first
    let sql = `UPDATE atp_transaction_type SET pipeline_name=?, pipeline_version=? WHERE transaction_type=?`
    let params = [ req.body.pipelineName, req.body.pipelineVersion, req.body.transactionType ]
    // console.log(`sql=`, sql)
    // console.log(`params=`, params)
    let result = await query(sql, params)
    // console.log(`result=`, result)
    if (result.affectedRows === 1) {
      res.send({ status: 'ok' })
      return next()
    }

    // Update failed, must be a new transaction type.
    sql = `INSERT INTO atp_transaction_type (transaction_type, pipeline_name, pipeline_version) VALUES (?, ?, ?)`
    params = [ req.body.transactionType, req.body.pipelineName, req.body.pipelineVersion ]
    // console.log(`sql=`, sql)
    // console.log(`params=`, params)
    result = await query(sql, params)
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
