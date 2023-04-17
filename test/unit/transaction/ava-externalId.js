/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import test from 'ava'
import TransactionCacheAndArchive from '../../../ATP/Scheduler2/TransactionCacheAndArchive'
import createTestTransaction from '../helpers/createTestTransaction'

const OWNER = 'fred'
const TRANSACTION_TYPE = 'example'


// https://github.com/avajs/ava/blob/master/docs/01-writing-tests.md
test.before(async t => {
})


/*
 *  Tests
 */
test.serial('External Id is saved in transaction', async t => {

  // Create the transaction with an external ID
  const num = Math.round(Math.random() * 100000000000)
  const externalId = `e-${num}`
  const tx = await TransactionCacheAndArchive.newTransaction(OWNER, externalId, TRANSACTION_TYPE)
  const txId = tx.getTxId()

  // Check the external ID is saved
  t.is(tx.getExternalId(), externalId)

  // Check the values were persisted
  await TransactionCacheAndArchive.removeFromCache(txId)
  let tx2 = await TransactionCacheAndArchive.getTransactionState(txId, false)
  t.falsy(tx2)
  tx2 = await TransactionCacheAndArchive.getTransactionState(txId, true)
  t.truthy(tx2)
  t.is(tx2.getExternalId(), externalId)
})


test.serial('Access transaction via external ID', async t => {

  // Create the transaction with an external ID
  const num = Math.round(Math.random() * 100000000000)
  const externalId = `e-${num}`
  const tx = await TransactionCacheAndArchive.newTransaction(OWNER, externalId, TRANSACTION_TYPE)

  // Find the transaction
  const tx2 = await TransactionCacheAndArchive.findTransactionByExternalId(OWNER, externalId, false)
  t.truthy(tx2)
  t.is(tx2.getExternalId(), externalId)
})


test.serial('Check externalId is saved', async t => {

  // Create the transaction with an external ID
  const num = Math.round(Math.random() * 100000000000)
  const externalId = `e-${num}`
  const tx = await TransactionCacheAndArchive.newTransaction(OWNER, externalId, TRANSACTION_TYPE)
  const txId = tx.getTxId()

  // Find the transaction
  let tx2 = await TransactionCacheAndArchive.findTransactionByExternalId(OWNER, externalId, false)
  t.truthy(tx2)
  t.is(tx2.getExternalId(), externalId)

  // Check the externalId was persisted
  await TransactionCacheAndArchive.removeFromCache(txId)
  tx2 = await TransactionCacheAndArchive.findTransactionByExternalId(OWNER, externalId, false)
  t.falsy(tx2)
  tx2 = await TransactionCacheAndArchive.findTransactionByExternalId(OWNER, externalId, true)
  t.truthy(tx2)
  t.is(tx2.getTxId(), txId)
  t.is(tx2.getExternalId(), externalId)
})
