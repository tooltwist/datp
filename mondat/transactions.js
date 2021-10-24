/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import ATP from '../ATP/ATP'

export async function dumpAllTransactionsV1(req, res, next) {
  console.log(`dumpAllTransactionsV1()`)

  await ATP.dumpTransactions(`All transactions`)

  res.send({ status: 'done' })
  return next();
}

export async function dumpTransactionV1(req, res, next) {
  console.log(`dumpTransactionV1()`)

  const transactionId = req.params.txId
  // await Scheduler.dumpSteps(`Transaction ${transactionId}`, transactionId)
  await ATP.dumpSteps(`Transaction ${transactionId}`, transactionId)

  res.send({ status: 'done' })
  return next();
}

export async function listAllTransactionsV1(req, res, next) {
  // console.log(`listAllTransactionsV1()`)
  const includeCompleted = true
  const txlist = await ATP.transactionList(null, includeCompleted)
  res.send(txlist)
  return next();
}

export async function transactionStatusV1(req, res, next) {
  console.log(`transactionStatusV1()`)

  const transactionId = req.params.txId
  // await Scheduler.dumpSteps(`Transaction ${transactionId}`, transactionId)
  const steps = await ATP.stepList(true, transactionId)

  res.send(steps)
  return next();
}
