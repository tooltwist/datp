import test from 'ava'
import Transaction from '../../../ATP/Scheduler2/Transaction'
import TransactionCache from '../../../ATP/Scheduler2/TransactionCache'
import { STEP_FAILED, STEP_QUEUED, STEP_RUNNING, STEP_SLEEPING, STEP_SUCCESS } from '../../../ATP/Step'
import createTestTransaction from '../helpers/createTestTransaction'

const OWNER = 'fred'
const TRANSACTION_TYPE = 'example'

// https://github.com/avajs/ava/blob/master/docs/01-writing-tests.md
test.before(async t => {
})

test.serial('Get summary by transaction ID', async t => {

  // Create the transaction with an external ID
  const num = Math.round(Math.random() * 100000000000)
  const externalId = `e-${num}`
  const tx = await TransactionCache.newTransaction(OWNER, externalId, TRANSACTION_TYPE)
  const txId = tx.getTxId()

  // Prior to running
  await tx.delta(null, {
    status: STEP_QUEUED
  })

  let summary = await Transaction.getSummary(OWNER, txId)
  // console.log(`summary=`, summary)
  t.is(summary.metadata.txId, txId)
  t.is(summary.metadata.owner, OWNER)
  t.is(summary.metadata.externalId, externalId)
  t.is(summary.metadata.status, STEP_QUEUED)
  t.is(summary.metadata.sequenceOfUpdate, 1)
  t.is(summary.metadata.completionTime, null)
  t.is(typeof summary.progressReport, 'object')
  t.is(typeof summary.data, 'undefined')

  // Go to sleep
  await tx.delta(null, {
    status: STEP_SLEEPING,
    progressReport: { description: 'Waiting for dinner' }
    // transactionOutput: { }
  })
  summary = await Transaction.getSummary(OWNER, txId)
  t.is(summary.metadata.txId, txId)
  t.is(summary.metadata.owner, OWNER)
  t.is(summary.metadata.externalId, externalId)
  t.is(summary.metadata.status, STEP_SLEEPING)
  t.is(summary.metadata.sequenceOfUpdate, 2)
  t.is(summary.metadata.completionTime, null)
  t.is(typeof summary.progressReport, 'object')
  t.is(summary.progressReport.description, 'Waiting for dinner')
  t.is(typeof summary.data, 'undefined')


  // Go to sleep
  await tx.delta(null, {
    status: STEP_SUCCESS,
    progressReport: { },
    transactionOutput: { wonderful: 'result'}
  })
  summary = await Transaction.getSummary(OWNER, txId)
  // console.log(`summary=`, summary)
  t.is(summary.metadata.txId, txId)
  t.is(summary.metadata.owner, OWNER)
  t.is(summary.metadata.externalId, externalId)
  t.is(summary.metadata.status, STEP_SUCCESS)
  t.is(summary.metadata.sequenceOfUpdate, 3)
  t.is(summary.metadata.completionTime, null)
  t.is(JSON.stringify(summary.progressReport), '{}')
  t.is(JSON.stringify(summary.data), '{"wonderful":"result"}')
})


test.serial('Get summary by external ID', async t => {

  // Create the transaction with an external ID
  const num = Math.round(Math.random() * 100000000000)
  const externalId = `e-${num}`
  const tx = await TransactionCache.newTransaction(OWNER, externalId, TRANSACTION_TYPE)
  const txId = tx.getTxId()

  // Prior to running
  await tx.delta(null, {
    status: STEP_QUEUED
  })

  let summary = await Transaction.getSummaryByExternalId(OWNER, externalId)
  // console.log(`summary=`, summary)
  t.is(summary.metadata.txId, txId)
  t.is(summary.metadata.owner, OWNER)
  t.is(summary.metadata.externalId, externalId)
  t.is(summary.metadata.status, STEP_QUEUED)
  t.is(summary.metadata.sequenceOfUpdate, 1)
  t.is(summary.metadata.completionTime, null)
  t.is(JSON.stringify(summary.progressReport), '{}')
  t.is(typeof summary.data, 'undefined')

  // Go to sleep
  await tx.delta(null, {
    status: STEP_SLEEPING,
    progressReport: { description: 'Waiting for breakfast' }
    // transactionOutput: { }
  })
  summary = await Transaction.getSummaryByExternalId(OWNER, externalId)
  // console.log(`summary=`, summary)
  t.is(summary.metadata.txId, txId)
  t.is(summary.metadata.owner, OWNER)
  t.is(summary.metadata.externalId, externalId)
  t.is(summary.metadata.status, STEP_SLEEPING)
  t.is(summary.metadata.sequenceOfUpdate, 2)
  t.is(summary.metadata.completionTime, null)
  t.is(JSON.stringify(summary.progressReport), '{"description":"Waiting for breakfast"}')
  t.is(typeof summary.data, 'undefined')

  // Go to sleep
  await tx.delta(null, {
    status: STEP_SUCCESS,
    progressReport: {  },
    transactionOutput: { wonderful: 'pretty good'}
  })
  summary = await Transaction.getSummaryByExternalId(OWNER, externalId)
  // console.log(`summary=`, summary)
  t.is(summary.metadata.completionTime, null)
  t.is(summary.metadata.txId, txId)
  t.is(summary.metadata.owner, OWNER)
  t.is(summary.metadata.externalId, externalId)
  t.is(summary.metadata.status, STEP_SUCCESS)
  t.is(summary.metadata.sequenceOfUpdate, 3)
  t.is(JSON.stringify(summary.progressReport), '{}')
  t.is(JSON.stringify(summary.data), '{"wonderful":"pretty good"}')
})
