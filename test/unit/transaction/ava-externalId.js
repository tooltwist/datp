import test from 'ava'
import TransactionCache from '../../../ATP/Scheduler2/TransactionCache'
import createTestTransaction from '../helpers/createTestTransaction'

const OWNER = 'fred'

// https://github.com/avajs/ava/blob/master/docs/01-writing-tests.md
test.beforeEach(async t => {
})


/*
 *  Tests
 */
test.serial('External Id is saved in transaction', async t => {

  // Create the transaction with an external ID
  const num = Math.round(Math.random() * 100000000000)
  const externalId = `e-${num}`
  const tx = await TransactionCache.newTransaction(OWNER, externalId)
  const txId = tx.getTxId()

  // Check the external ID is saved
  t.is(tx.getExternalId(), externalId)

  // Check the values were persisted
  await TransactionCache.removeFromCache(txId)
  let tx2 = await TransactionCache.findTransaction(txId, false)
  t.falsy(tx2)
  tx2 = await TransactionCache.findTransaction(txId, true)
  t.truthy(tx2)
  t.is(tx2.getExternalId(), externalId)
})


test.serial('Access transaction via external ID', async t => {

  // Create the transaction with an external ID
  const num = Math.round(Math.random() * 100000000000)
  const externalId = `e-${num}`
  const tx = await TransactionCache.newTransaction(OWNER, externalId)

  // Find the transaction
  const tx2 = await TransactionCache.findTransactionByExternalId(OWNER, externalId, false)
  t.truthy(tx2)
  t.is(tx2.getExternalId(), externalId)
})


test.serial('Check externalId is saved', async t => {

  // Create the transaction with an external ID
  const num = Math.round(Math.random() * 100000000000)
  const externalId = `e-${num}`
  const tx = await TransactionCache.newTransaction(OWNER, externalId)
  const txId = tx.getTxId()

  // Find the transaction
  let tx2 = await TransactionCache.findTransactionByExternalId(OWNER, externalId, false)
  t.truthy(tx2)
  t.is(tx2.getExternalId(), externalId)

  // Check the externalId was persisted
  await TransactionCache.removeFromCache(txId)
  tx2 = await TransactionCache.findTransactionByExternalId(OWNER, externalId, false)
  t.falsy(tx2)
  tx2 = await TransactionCache.findTransactionByExternalId(OWNER, externalId, true)
  t.truthy(tx2)
  t.is(tx2.getTxId(), txId)
  t.is(tx2.getExternalId(), externalId)
})
