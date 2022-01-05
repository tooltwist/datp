/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import ATP from '../ATP/ATP'
import Transaction from '../ATP/Scheduler2/Transaction';
import TransactionCache from '../ATP/Scheduler2/TransactionCache';

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
  const txlist = await Transaction.findTransactions({ })
  res.send(txlist)
  return next();
}

export async function transactionStatusV1(req, res, next) {
  // console.log(`transactionStatusV1()`)


  const txId = req.params.txId
  const tx = await TransactionCache.findTransaction(txId, true)
  // console.log(`tx=`, tx)

  // Check that the current user has access to this transaction
  //ZZZZZ

  // Create a list of steps
  const ids = tx.stepIds()
  const steps = [ ]
  const index = { }
  for (const stepId of ids) {
    const stepData = await tx.stepData(stepId)
    stepData.logs = [ ]
    steps.push(stepData)
    index[stepId] = stepData
  }

  // Load the log entries for this transaction
  const logEntries = await Transaction.getLog(txId)
  // console.log(`logEntries=`, logEntries)
  for (const entry of logEntries) {
    const step = index[entry.stepId]
    if (step) {
      step.logs.push(entry)
      delete entry.stepId
    } else {
      //ZZZZZ Write this to the system error log
      console.log(`INTERNAL ERROR: Found log entry for unknown step in transaction [ ${entry.stepId} in ${txId}]`)
    }
  }

  // Send the reply
  res.send(steps)
  return next();
}
