/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import query from '../database/query'
import errors from 'restify-errors';
import dbupdate from '../database/dbupdate';

export async function mondatRoute_getTestCasesV1(req, res, next) {
  // console.log(`mondatRoute_getTestCasesV1()`)

  const sql = `SELECT
    name,
    description,
    transaction_type AS transactionType,
    input_data AS inputData
    FROM m_test_case
    ORDER BY name`
  const reply =  await query(sql)
  // console.log(`reply=`, reply)

  res.send(reply)
  return next();
}

export async function mondatRoute_saveTestCasesV1(req, res, next) {
  // console.log(`mondatRoute_saveTestCasesV1()`)
  // console.log(`req.params=`, req.params)
  // console.log(`req.body=`, req.body)
  // res.send(new errors.InternalServerError(`Not yet`))
  // return next()


  try {
    // Try updating first
    let sql = `UPDATE m_test_case SET name=?, description=?, transaction_type=?, input_data=? WHERE name=?`
    const name = req.body.originalName ? req.body.originalName : req.body.name
    let params = [ req.body.name, req.body.description, req.body.transactionType, req.body.inputData, name ]
    // console.log(`sql=`, sql)
    // console.log(`params=`, params)
    let result = await dbupdate(sql, params)
    // console.log(`result=`, result)
    if (result.affectedRows === 1) {
      res.send({ status: 'ok' })
      return next()
    }

    // Update failed, must be a new transaction type.
    sql = `INSERT INTO m_test_case (name, description, transaction_type, input_data) VALUES (?, ?, ?, ?)`
    params = [ req.body.name, req.body.description, req.body.transactionType, req.body.inputData ]
    // console.log(`sql=`, sql)
    // console.log(`params=`, params)
    result = await dbupdate(sql, params)
    // console.log(`result=`, result)

    if (result.affectedRows === 1) {
      res.send({ status: 'ok' })
      return next()
    }
  } catch (e) {
    console.log(`mondatRoute_saveTestCasesV1(): Exception saving m_test_case:`, e)
  }
  res.send(new errors.InternalServerError(`Unable to save new test case`))
  return next()
}

export async function mondatRoute_deleteTestCasesV1(req, res, next) {
  console.log(`mondatRoute_deleteTestCasesV1()`)
  // console.log(`req.params=`, req.params)
  // res.send(new errors.InternalServerError(`Not yet`))
  // return next()

  try {
    const sql = `DELETE FROM m_test_case WHERE name=?`
    const params = [ req.params.name ]
    // console.log(`sql=`, sql)
    // console.log(`params=`, params)
    const reply =  await query(sql, params)
    // console.log(`reply=`, reply)
    if (reply.affectedRows === 1) {
      res.send({ status: 'ok' })
      return next()
    }
  } catch (e) {
    console.log(`mondatRoute_deleteTestCasesV1(): Exception deleting test case:`, e)
  }
  res.send(new errors.InternalServerError(`Unable to delete mapping`))
  return next()
}
