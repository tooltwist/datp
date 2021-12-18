import test from 'ava'
import Transaction from '../../../ATP/Scheduler2/Transaction'
import TransactionCache from '../../../ATP/Scheduler2/TransactionCache'
import { STEP_FAILED, STEP_QUEUED, STEP_RUNNING, STEP_SLEEPING, STEP_SUCCESS } from '../../../ATP/Step'
import createTestTransaction from '../helpers/createTestTransaction'

const OWNER = 'fred'

// https://github.com/avajs/ava/blob/master/docs/01-writing-tests.md
test.beforeEach(async t => {
})

test.serial('Get summary by transaction ID', async t => {

  // Create the transaction with an external ID
  const num = Math.round(Math.random() * 100000000000)
  const externalId = `e-${num}`
  const tx = await TransactionCache.newTransaction(OWNER, externalId)
  const txId = tx.getTxId()

  // Prior to running
  await tx.delta(null, {
    status: STEP_QUEUED
  })

  let summary = await Transaction.getSummary(OWNER, txId)
  // console.log(`summary=`, summary)
  t.is(summary.txId, txId)
  t.is(summary.owner, OWNER)
  t.is(summary.externalId, externalId)
  t.is(summary.status, STEP_QUEUED)
  t.is(summary.sequenceOfUpdate, 1)
  t.is(summary.progressReport, '{}')
  t.is(summary.transactionOutput, '{}')
  t.is(summary.completionTime, null)

  // Go to sleep
  await tx.delta(null, {
    status: STEP_SLEEPING,
    progressReport: { description: 'Waiting for dinner' }
    // transactionOutput: { }
  })
  summary = await Transaction.getSummary(OWNER, txId)
  t.is(summary.txId, txId)
  t.is(summary.owner, OWNER)
  t.is(summary.externalId, externalId)
  t.is(summary.status, STEP_SLEEPING)
  t.is(summary.sequenceOfUpdate, 2)
  t.is(summary.progressReport, '{"description":"Waiting for dinner"}')
  t.is(summary.transactionOutput, '{}')
  t.is(summary.completionTime, null)


  // Go to sleep
  await tx.delta(null, {
    status: STEP_SUCCESS,
    progressReport: {  },
    transactionOutput: { wonderful: 'result'}
  })
  summary = await Transaction.getSummary(OWNER, txId)
  t.is(summary.txId, txId)
  t.is(summary.owner, OWNER)
  t.is(summary.externalId, externalId)
  t.is(summary.status, STEP_SUCCESS)
  t.is(summary.sequenceOfUpdate, 3)
  t.is(summary.progressReport, '{}')
  t.is(summary.transactionOutput, '{"wonderful":"result"}')
  t.is(summary.completionTime, null)
})


test.serial('Get summary by external ID', async t => {

  // Create the transaction with an external ID
  const num = Math.round(Math.random() * 100000000000)
  const externalId = `e-${num}`
  const tx = await TransactionCache.newTransaction(OWNER, externalId)
  const txId = tx.getTxId()

  // Prior to running
  await tx.delta(null, {
    status: STEP_QUEUED
  })

  let summary = await Transaction.getSummaryByExternalId(OWNER, externalId)
  // console.log(`summary=`, summary)
  t.is(summary.txId, txId)
  t.is(summary.owner, OWNER)
  t.is(summary.externalId, externalId)
  t.is(summary.status, STEP_QUEUED)
  t.is(summary.sequenceOfUpdate, 1)
  t.is(summary.progressReport, '{}')
  t.is(summary.transactionOutput, '{}')
  t.is(summary.completionTime, null)

  // Go to sleep
  await tx.delta(null, {
    status: STEP_SLEEPING,
    progressReport: { description: 'Waiting for breakfast' }
    // transactionOutput: { }
  })
  summary = await Transaction.getSummaryByExternalId(OWNER, externalId)
  t.is(summary.txId, txId)
  t.is(summary.owner, OWNER)
  t.is(summary.externalId, externalId)
  t.is(summary.status, STEP_SLEEPING)
  t.is(summary.sequenceOfUpdate, 2)
  t.is(summary.progressReport, '{"description":"Waiting for breakfast"}')
  t.is(summary.transactionOutput, '{}')
  t.is(summary.completionTime, null)


  // Go to sleep
  await tx.delta(null, {
    status: STEP_SUCCESS,
    progressReport: {  },
    transactionOutput: { wonderful: 'pretty good'}
  })
  summary = await Transaction.getSummaryByExternalId(OWNER, externalId)
  t.is(summary.txId, txId)
  t.is(summary.owner, OWNER)
  t.is(summary.externalId, externalId)
  t.is(summary.status, STEP_SUCCESS)
  t.is(summary.sequenceOfUpdate, 3)
  t.is(summary.progressReport, '{}')
  t.is(summary.transactionOutput, '{"wonderful":"pretty good"}')
  t.is(summary.completionTime, null)
})
