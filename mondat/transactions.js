/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import ATP from '../ATP/ATP'
import Transaction from '../ATP/Scheduler2/Transaction';
import TransactionCache from '../ATP/Scheduler2/txState-level-1';
import { STEP_ABORTED, STEP_FAILED, STEP_SUCCESS } from '../ATP/Step';
import errors from 'restify-errors'
import dbLogbook from '../database/dbLogbook';

export async function dumpAllTransactionsV1(req, res, next) {
  console.log(`dumpAllTransactionsV1()`)

  await ATP.dumpTransactions(`All transactions`)

  res.send({ status: 'done' })
  return next();
}

export async function dumpTransactionV1(req, res, next) {
  console.log(`dumpTransactionV1()`)

  const transactionId = req.params.txId
  await ATP.dumpSteps(`Transaction ${transactionId}`, transactionId)

  res.send({ status: 'done' })
  return next();
}

export async function mondatRoute_transactionsV1(req, res, next) {
  // console.log(`mondatRoute_transactionsV1()`)
  // console.log(`req.query=`, req.query)
  // console.log(`req.params=`, req.params)

  let pagesize = 20
  // console.log(`req.query.pagesize=`, req.query.pagesize)
  try { pagesize = parseInt(req.query.pagesize) } catch (e) { }
  let offset = 0
  // console.log(`req.query.offset=`, req.query.offset)
  try { offset = parseInt(req.query.offset) } catch (e) { }
  const filter = req.query.filter ? req.query.filter : null
  let status = [STEP_SUCCESS, STEP_FAILED, STEP_ABORTED] // Finished transactions
  if (req.query.status) {
    status = req.query.status.split(',')
  }

  // console.log(`pagesize=`, pagesize)
  // console.log(`page=`, page)
  // console.log(`filter=`, filter)
  // console.log(`status=`, status)
  const txlist = await Transaction.findTransactions(pagesize, offset, filter, status)
  res.send(txlist)
  return next();
}

export async function mondatRoute_transactionStatusV1(req, res, next) {
  // console.log(`mondatRoute_transactionStatusV1()`)

  //ZZZZZZZ This should get the transaction summary from the database, and leave the state alone.

  const txId = req.params.txId
  const tx = await TransactionCache.getTransactionState(txId)
  if (!tx) {
    console.log(`Transaction state not available: ${txId}`)
    res.send(new errors.NotFoundError(`Unknown transaction`))
    return next()
  }

  // Check that the current user has access to this transaction
  //ZZZZZ

  // Get transaction details
  const txData = tx.txData()

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
  const logEntries = await dbLogbook.getLog(txId)
  const brokenSteps = new Set() // stepIds we've already complained about
  for (const entry of logEntries) {
    if (entry.stepId) {
      const step = index[entry.stepId]
      if (step) {
        step.logs.push(entry)
        delete entry.stepId
      } else {
        //ZZZZZ Write this to the system error log
        if (!brokenSteps.has(entry.stepId)) {
          console.log(`INTERNAL ERROR: Found log entry for unknown step in transaction [ ${entry.stepId} in ${txId}]`)
  console.log(`typeof(entry.stepId)=`, typeof(entry.stepId))
          brokenSteps.add(entry.stepId)
        }
      }
    } else {
      // Transaction level log entry
      //ZZZZZ
      // console.log(`YARP TX LOG:`, entry)
    }
  }

  // Send the reply
  res.send({
    status: txData.status,
    transactionType: txData.transactionType,
    nodeGroup: txData.nodeGroup,
    nodeId: txData.nodeId,
    pipelineName: txData.pipelineName,
    steps
  })
  return next();
}
