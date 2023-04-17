/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import ATP from '../ATP/ATP'
import TransactionState from '../ATP/Scheduler2/TransactionState';
import TransactionCacheAndArchive from '../ATP/Scheduler2/TransactionCacheAndArchive';
import { STEP_ABORTED, STEP_FAILED, STEP_SUCCESS } from '../ATP/Step';
import errors from 'restify-errors'
import dbLogbook from '../database/dbLogbook'
import { RedisQueue } from '../ATP/Scheduler2/queuing/RedisQueue-ioredis';
import { findArchivedTransactions } from '../ATP/Scheduler2/archiving/ArchiveProcessor';
import { luaTransactionsInPlay, luaTransactionsSleeping } from '../ATP/Scheduler2/queuing/redis-transactions';
import { luaTransactionsWithException } from '../ATP/Scheduler2/queuing/redis-exceptions';

const VERBOSE = 0

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

export async function mondatRoute_recentTransactionsV1(req, res, next) {
  // console.log(`mondatRoute_recentTransactionsV1()`)
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
    status = [ ]
    req.query.status.split(',').forEach(s => { if (s) status.push(s) })
  }
  const archived = (req.query.archived ? true : false)

  // console.log(`pagesize=`, pagesize)
  // console.log(`page=`, page)
  // console.log(`filter=`, filter)
  // console.log(`status=`, status)
  const txlist = await TransactionState.findTransactions(archived, pagesize, offset, filter, status)
  res.send(txlist)
  return next();
}

export async function mondatRoute_transactionsInPlayV1(req, res, next) {
  // console.log(`mondatRoute_transactionsInPlayV1()`)
  // console.log(`req.query=`, req.query)
  // console.log(`req.params=`, req.params)

  let pagesize = 20
  try { pagesize = parseInt(req.query.pagesize) } catch (e) { }
  let page = 1
  // console.log(`req.query.page=`, req.query.page)
  try { page = parseInt(req.query.page) } catch (e) { }
  const offset = (page - 1) * pagesize

  const redisLua = await RedisQueue.getRedisLua()
  const transactions = await luaTransactionsInPlay(offset, pagesize)
  res.send(transactions)
  return next()
}

export async function mondatRoute_transactionsSleepingV1(req, res, next) {
  // console.log(`mondatRoute_transactionsSleepingV1()`)
  // console.log(`req.query=`, req.query)
  // console.log(`req.params=`, req.params)

  let pagesize = 20
  try { pagesize = parseInt(req.query.pagesize) } catch (e) { }
  let page = 1
  // console.log(`req.query.page=`, req.query.page)
  try { page = parseInt(req.query.page) } catch (e) { }
  const offset = (page - 1) * pagesize

  const redisLua = await RedisQueue.getRedisLua()
  const transactions = await luaTransactionsSleeping(offset, pagesize)
  res.send(transactions)
  return next()
}//- mondatRoute_transactionsSleepingV1

export async function mondatRoute_archivedTransactionsV1(req, res, next) {
  // console.log(`mondatRoute_archivedTransactionsV1()`)
  // console.log(`req.query=`, req.query)
  // console.log(`req.params=`, req.params)

  let pagesize = 20
  // console.log(`req.query.pagesize=`, req.query.pagesize)
  try { pagesize = parseInt(req.query.pagesize) } catch (e) { }
  let page = 1
  // console.log(`req.query.offset=`, req.query.offset)
  try { page = parseInt(req.query.page) } catch (e) { }
  const filter = req.query.filter ? req.query.filter : null
  let status = [STEP_SUCCESS, STEP_FAILED, STEP_ABORTED] // Finished transactions
  if (req.query.status) {
    status = [ ]
    req.query.status.split(',').forEach(s => { if (s) status.push(s) })
  }
  // const archived = (req.query.archived ? true : false)
  const offset = (page - 1) * pagesize

  // console.log(`pagesize=`, pagesize)
  // console.log(`offset=`, offset)
  // console.log(`filter=`, filter)
  // console.log(`status=`, status)
  const txlist = await findArchivedTransactions(offset, pagesize, status, filter)
  // console.log(`txlist=`, txlist)
  res.send(txlist)
  return next();
}//- mondatRoute_archivedTransactionsV1

export async function mondatRoute_exceptionTransactionsV1(req, res, next) {
  // console.log(`mondatRoute_exceptionTransactionsV1()`)
  // console.log(`req.query=`, req.query)
  // console.log(`req.params=`, req.params)

  let pagesize = 20
  try { pagesize = parseInt(req.query.pagesize) } catch (e) { }
  let page = 1
  // console.log(`req.query.page=`, req.query.page)
  try { page = parseInt(req.query.page) } catch (e) { }
  const offset = (page - 1) * pagesize

  const redisLua = await RedisQueue.getRedisLua()
  const transactions = await luaTransactionsWithException(offset, pagesize)
  res.send(transactions)
  return next()
}//- mondatRoute_exceptionTransactionsV1


export async function mondatRoute_transactionStatusV1(req, res, next) {
  // if (VERBOSE)
  // console.log(`mondatRoute_transactionStatusV1()`.yellow)

  //ZZZZZZZ This should get the transaction summary from the database, and leave the state alone.

  const txId = req.params.txId
  const tx = await TransactionCacheAndArchive.getTransactionState(txId)
  if (!tx) {
    console.log(`Transaction state not available: ${txId}`)
    res.send(new errors.NotFoundError(`Unknown transaction`))
    return next()
  }

  // Check that the current user has access to this transaction
  //ZZZZZ
  // console.log(`tx=`, tx)
  // console.log(`typeof tx=`, typeof tx)

  // Get transaction details
  const txData = tx.transactionData()

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


//ZZZZZ THIS DOES NOT WORK
// TransactionCacheAndArchive.getTransactionState(txId) returns a TransactionState, not an object.
export async function mondatRoute_transactionStateV1(req, res, next) {
  if (VERBOSE) console.log(`mondatRoute_transactionStateV1()`.yellow)

  //ZZZZZZZ This should get the transaction summary from the database, and leave the state alone.

  const txId = req.params.txId
  const { state } = await TransactionCacheAndArchive.getTransactionState(txId)
  if (!state) {
    console.log(`Transaction state not available: ${txId}`)
    res.send(new errors.NotFoundError(`Unknown transaction`))
    return next()
  }
  console.log(`tx=`.yellow, tx)
  res.send(state.asObject())
  return next()
}//- mondatRoute_transactionStateStatusV1

export async function mondatRoute_transactionStateStatusV1(req, res, next) {
  if (VERBOSE) console.log(`mondatRoute_transactionStateStatusV1()`.yellow)

  const txId = req.params.txId
  const stateStatus = await TransactionCacheAndArchive.getTransactionStateStatus(txId)
  // console.log(`mondatRoute_transactionStateStatusV1: stateStatus=`, stateStatus)
  if (!stateStatus) {
    console.log(`Transaction stateStatus not available: ${txId}`)
    res.send(new errors.NotFoundError(`Unknown transaction`))
    return next()
  }
  // console.log(`stateStatus=`.magenta, stateStatus)
  res.send(stateStatus)
  return next()
}//- mondatRoute_transactionStateStatusV1

