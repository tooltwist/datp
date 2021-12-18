import test from 'ava'
import { STEP_QUEUED, STEP_RUNNING, STEP_SUCCESS } from '../../../ATP/Step'
import TransactionCache from '../../../ATP/Scheduler2/TransactionCache'
import createTestTransaction from '../helpers/createTestTransaction'

/*
 *  We need to use a different node name for each test file, as they run in different
 *  processes (with different CallbackRegisters). If multiple test files have the same nodeId
 *  then they draw from the same queue, but the worker might not know the callback handler.
 */
// const OWNER = 'fred'
// const NODE_ID = 'succeed-workflow'


// https://github.com/avajs/ava/blob/master/docs/01-writing-tests.md
test.beforeEach(async t => {
})



test.serial('Attempt to set invalid transaction status', async t => {
  const { tx } = await createTestTransaction()

  t.is(tx.getStatus(), 'running')
  await t.throwsAsync(async() => {
    await tx.delta(null, {
      status: 'blahblah'
    })
  }, { instanceOf: Error, message: 'Invalid status [blahblah]'})
})


test.serial('Changing status updates transaction', async t => {
  const { tx, txId } = await createTestTransaction()
  t.is(tx.getStatus(), STEP_RUNNING)
  t.is(tx.getDeltaCounter(), 0)
  t.is(tx.getSequenceOfUpdate(), 0)
  t.is(JSON.stringify(tx.getProgressReport()), '{}')
  t.is(JSON.stringify(tx.getTransactionOutput()), '{}')
  t.is(tx.getCompletionTime(), null)

  await tx.delta(null, { status: STEP_QUEUED })
  t.is(tx.getDeltaCounter(), 1)
  t.is(tx.getSequenceOfUpdate(), 1)
  t.is(tx.getStatus(), STEP_QUEUED)
  t.is(JSON.stringify(tx.getProgressReport()), '{}')
  t.is(JSON.stringify(tx.getTransactionOutput()), '{}')
  t.is(tx.getCompletionTime(), null)

  await tx.delta(null, { status: STEP_RUNNING })
  t.is(tx.getDeltaCounter(), 2)
  t.is(tx.getSequenceOfUpdate(), 2)
  t.is(tx.getStatus(), STEP_RUNNING)
  t.is(JSON.stringify(tx.getProgressReport()), '{}')
  t.is(JSON.stringify(tx.getTransactionOutput()), '{}')
  t.is(tx.getCompletionTime(), null)

  await tx.delta(null, { status: STEP_SUCCESS })
  t.is(tx.getDeltaCounter(), 3)
  t.is(tx.getSequenceOfUpdate(), 3)
  t.is(tx.getStatus(), STEP_SUCCESS)
  t.is(JSON.stringify(tx.getProgressReport()), '{}')
  t.is(JSON.stringify(tx.getTransactionOutput()), '{}')
  t.is(tx.getCompletionTime(), null)

  // Check the values were persisted
  await TransactionCache.removeFromCache(txId)
  let tx2 = await TransactionCache.findTransaction(txId, false)
  t.falsy(tx2)
  tx2 = await TransactionCache.findTransaction(txId, true)
  t.truthy(tx2)
  t.is(tx.getDeltaCounter(), 3)
  t.is(tx.getSequenceOfUpdate(), 3)
  t.is(tx2.getStatus(), STEP_SUCCESS)
  t.is(JSON.stringify(tx2.getProgressReport()), '{}')
  t.is(JSON.stringify(tx2.getTransactionOutput()), '{}')
  t.is(tx2.getCompletionTime(), null)
})


test.serial('Changing progressReport updates transaction', async t => {
  const { tx, txId } = await createTestTransaction()
  t.is(tx.getDeltaCounter(), 0)
  t.is(tx.getSequenceOfUpdate(), 0)
  t.is(tx.getStatus(), STEP_RUNNING)
  t.is(JSON.stringify(tx.getProgressReport()), '{}')
  t.is(JSON.stringify(tx.getTransactionOutput()), '{}')
  t.is(tx.getCompletionTime(), null)

  await tx.delta(null, { progressReport: { it: 'went okay' } })
  t.is(tx.getDeltaCounter(), 1)
  t.is(tx.getSequenceOfUpdate(), 1)
  t.is(tx.getStatus(), STEP_RUNNING)
  t.is(JSON.stringify(tx.getProgressReport()), '{"it":"went okay"}')
  t.is(JSON.stringify(tx.getTransactionOutput()), '{}')
  t.is(tx.getCompletionTime(), null)

  // Check the values were persisted
  await TransactionCache.removeFromCache(txId)
  let tx2 = await TransactionCache.findTransaction(txId, false)
  t.falsy(tx2)
  tx2 = await TransactionCache.findTransaction(txId, true)
  t.truthy(tx2)
  t.is(tx2.getDeltaCounter(), 1)
  t.is(tx2.getSequenceOfUpdate(), 1)
  t.is(tx2.getStatus(), STEP_RUNNING)
  t.is(JSON.stringify(tx2.getProgressReport()), '{"it":"went okay"}')
  t.is(JSON.stringify(tx2.getTransactionOutput()), '{}')
  t.is(tx2.getCompletionTime(), null)
})


test.serial('Changing transactionOutput updates transaction', async t => {
  const { tx, txId } = await createTestTransaction()
  t.is(tx.getDeltaCounter(), 0)
  t.is(tx.getSequenceOfUpdate(), 0)
  t.is(tx.getStatus(), STEP_RUNNING)
  t.is(JSON.stringify(tx.getProgressReport()), '{}')
  t.is(JSON.stringify(tx.getTransactionOutput()), '{}')
  t.is(tx.getCompletionTime(), null)

  await tx.delta(null, { transactionOutput: { some: 'stuff' } })
  t.is(tx.getDeltaCounter(), 1)
  t.is(tx.getSequenceOfUpdate(), 1)
  t.is(tx.getStatus(), STEP_RUNNING)
  t.is(JSON.stringify(tx.getProgressReport()), '{}')
  t.is(JSON.stringify(tx.getTransactionOutput()), '{"some":"stuff"}')
  t.is(tx.getCompletionTime(), null)

  // Check the values were persisted
  await TransactionCache.removeFromCache(txId)
  let tx2 = await TransactionCache.findTransaction(txId, false)
  t.falsy(tx2)
  tx2 = await TransactionCache.findTransaction(txId, true)
  t.truthy(tx2)
  t.is(tx2.getDeltaCounter(), 1)
  t.is(tx2.getSequenceOfUpdate(), 1)
  t.is(tx2.getStatus(), STEP_RUNNING)
  t.is(JSON.stringify(tx2.getProgressReport()), '{}')
  t.is(JSON.stringify(tx2.getTransactionOutput()), '{"some":"stuff"}')
  t.is(tx2.getCompletionTime(), null)
})


test.serial('Changing completionTime updates transaction', async t => {
  const { tx, txId } = await createTestTransaction()
  t.is(tx.getDeltaCounter(), 0)
  t.is(tx.getSequenceOfUpdate(), 0)
  t.is(tx.getStatus(), STEP_RUNNING)
  t.is(JSON.stringify(tx.getProgressReport()), '{}')
  t.is(JSON.stringify(tx.getTransactionOutput()), '{}')
  t.is(tx.getCompletionTime(), null)

  const completionTime = new Date()
  await tx.delta(null, { completionTime })
  t.is(tx.getDeltaCounter(), 1)
  t.is(tx.getSequenceOfUpdate(), 1) // Not changed
  t.is(tx.getStatus(), STEP_RUNNING)
  t.is(JSON.stringify(tx.getProgressReport()), '{}')
  t.is(JSON.stringify(tx.getTransactionOutput()), '{}')
  t.is(tx.getCompletionTime(), completionTime)

  // Check the values were persisted
  await TransactionCache.removeFromCache(txId)
  let tx2 = await TransactionCache.findTransaction(txId, false)
  t.falsy(tx2)
  tx2 = await TransactionCache.findTransaction(txId, true)
  t.truthy(tx2)
  t.is(tx2.getDeltaCounter(), 1)
  t.is(tx2.getSequenceOfUpdate(), 1) // Not changed
  t.is(tx2.getStatus(), STEP_RUNNING)
  t.is(JSON.stringify(tx2.getProgressReport()), '{}')
  t.is(JSON.stringify(tx2.getTransactionOutput()), '{}')
  t.is(tx2.getCompletionTime().getTime(), completionTime.getTime())
})


test.serial('Reject invalid completionTime', async t => {
  let txId
  await t.throwsAsync(async() => {

    const { tx, txId: newTxId } = await createTestTransaction()
    txId = newTxId
    t.is(tx.getDeltaCounter(), 0)
    t.is(tx.getSequenceOfUpdate(), 0)
    t.is(tx.getCompletionTime(), null)

    await tx.delta(null, { completionTime: 12345 })
  }, { instanceOf: Error, message: 'data.completionTime parameter must be of type Date'})

  // Check the values were persisted
  await TransactionCache.removeFromCache(txId)
  let tx2 = await TransactionCache.findTransaction(txId, false)
  t.falsy(tx2)
  tx2 = await TransactionCache.findTransaction(txId, true)
  t.truthy(tx2)
  t.is(tx2.getDeltaCounter(), 0)
  t.is(tx2.getSequenceOfUpdate(), 0)
  t.is(tx2.getCompletionTime(), null)
})


test.serial('Changing other stuff does not update transaction', async t => {
  const { tx, txId } = await createTestTransaction()
  t.is(tx.getStatus(), 'running')
  t.is(tx.getDeltaCounter(), 0)
  t.is(tx.getSequenceOfUpdate(), 0)

  await tx.delta(null, { stuff: 'to not save transaction' })
  t.is(tx.getDeltaCounter(), 1)
  t.is(tx.getSequenceOfUpdate(), 0) // Not changed
  t.is(JSON.stringify(tx.txData()), '{"status":"running","stuff":"to not save transaction"}')

  // Check the values were persisted
  await TransactionCache.removeFromCache(txId)
  let tx2 = await TransactionCache.findTransaction(txId, false)
  t.falsy(tx2)
  tx2 = await TransactionCache.findTransaction(txId, true)
  t.truthy(tx2)
  t.is(tx2.getDeltaCounter(), 1)
  t.is(tx2.getSequenceOfUpdate(), 0) // Not changed
  t.is(JSON.stringify(tx2.txData()), '{"status":"running","stuff":"to not save transaction"}')
})
