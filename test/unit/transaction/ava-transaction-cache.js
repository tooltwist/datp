/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import test from 'ava'
import TransactionCacheAndArchive from '../../../ATP/Scheduler2/TransactionCacheAndArchive'
import createTestTransaction from '../helpers/createTestTransaction'


// https://github.com/avajs/ava/blob/master/docs/01-writing-tests.md
test.before(async t => {
})


/*
 *  Check the code is valid
 */
test.serial('Exists and has unit tests', async t => {
  t.truthy(TransactionCacheAndArchive)
});


/*
 *  Tests
 */
test.serial('allocate new transaction', async t => {
  const { tx, txId } = await createTestTransaction()

  t.truthy(tx)
  t.is(tx.getTxId(), txId)
});


test.serial('allocate then fetch tx', async t => {
  const { tx, txId } = await createTestTransaction()
  t.truthy(tx)
  t.is(tx.getTxId(), txId)

  const tx2 = await TransactionCacheAndArchive.getTransactionState(txId)
  t.truthy(tx2)
  t.is(tx2.getTxId(), txId)
});


test.serial('fetch a non-existant transaction', async t => {
  const tx = await TransactionCacheAndArchive.getTransactionState('xyz123')
  t.is(tx, null)
})


test.serial('allocate, remove, then fetch tx (without loading)', async t => {
  const { tx, txId } = await createTestTransaction()
  t.truthy(tx)
  t.is(tx.getTxId(), txId)

  const tx2 = await TransactionCacheAndArchive.getTransactionState(txId)
  t.truthy(tx2)
  t.is(tx2.getTxId(), txId)

  await TransactionCacheAndArchive.removeFromCache(txId)

  const tx3 = await TransactionCacheAndArchive.getTransactionState(txId)
  t.is(tx3, null)
});
