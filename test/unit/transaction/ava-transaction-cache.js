import test from 'ava'
import TransactionCache from '../../../ATP/Scheduler2/TransactionCache'
import createTestTransaction from '../helpers/createTestTransaction'


// https://github.com/avajs/ava/blob/master/docs/01-writing-tests.md
test.beforeEach(async t => {
})


/*
 *  Check the code is valid
 */
test.serial('Exists and has unit tests', async t => {
  t.truthy(TransactionCache)
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

  const tx2 = await TransactionCache.findTransaction(txId, false)
  t.truthy(tx2)
  t.is(tx2.getTxId(), txId)
});


test.serial('fetch a non-existant transaction', async t => {
  const tx = await TransactionCache.findTransaction('xyz123', false)
  t.is(tx, null)
})


test.serial('allocate, remove, then fetch tx (without loading)', async t => {
  const { tx, txId } = await createTestTransaction()
  t.truthy(tx)
  t.is(tx.getTxId(), txId)

  const tx2 = await TransactionCache.findTransaction(txId, false)
  t.truthy(tx2)
  t.is(tx2.getTxId(), txId)

  await TransactionCache.removeFromCache(txId)

  const tx3 = await TransactionCache.findTransaction(txId, false)
  t.is(tx3, null)
});
