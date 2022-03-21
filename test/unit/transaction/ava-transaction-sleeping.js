import test from 'ava'
import createTestTransaction from '../helpers/createTestTransaction'
import query from '../../../database/query'
import TransactionPersistance from '../../../ATP/Scheduler2/TransactionPersistance'
import TransactionCache from '../../../ATP/Scheduler2/TransactionCache'
import { STEP_RUNNING, STEP_SLEEPING, STEP_SUCCESS } from '../../../ATP/Step'


// https://github.com/avajs/ava/blob/master/docs/01-writing-tests.md
test.before(async t => {
})


/*
 *  Tests
 */
test.serial('Sleep counter before sleeping', async t => {
  const { tx, txId, externalId } = await createTestTransaction()

  t.truthy(tx)
  t.is(tx.getTxId(), txId)
  t.is(tx.getRetryCounter(), 0)
  t.is(tx.getSleepingSince(), null)
  t.is(tx.getWakeTime(), null)
  t.is(tx.getWakeSwitch(), null)
})


test.serial('Sleep counter starts with status change to sleep', async t => {
  const { tx, txId, externalId } = await createTestTransaction()

  t.truthy(tx)
  t.is(tx.getTxId(), txId)
  t.is(tx.getRetryCounter(), 0)
  t.is(tx.getSleepingSince(), null)
  t.is(tx.getWakeTime(), null)
  t.is(tx.getWakeSwitch(), null)

  await tx.delta(null, {
    status: STEP_SLEEPING,
    sleepDuration: 60,
    wakeSwitch: 'abc'
  })
  t.is(tx.getRetryCounter(), 1)
  t.not(tx.getSleepingSince(), null)
  t.truthy(tx.getSleepingSince() instanceof Date)
  t.not(tx.getWakeTime(), null)
  t.truthy(tx.getWakeTime() instanceof Date)
  t.is(tx.getWakeSwitch(), 'abc')
})


test.serial('Sleep counter increments after sleep-run-sleep', async t => {
  const { tx, txId, externalId } = await createTestTransaction()

  t.truthy(tx)
  t.is(tx.getTxId(), txId)
  t.is(tx.getRetryCounter(), 0)
  t.is(tx.getSleepingSince(), null)
  t.is(tx.getWakeTime(), null)
  t.is(tx.getWakeSwitch(), null)

  await tx.delta(null, {
    status: STEP_SLEEPING,
    sleepDuration: 60,
    wakeSwitch: 'abc'
  })
  t.is(tx.getRetryCounter(), 1)
  t.not(tx.getSleepingSince(), null)
  t.truthy(tx.getSleepingSince() instanceof Date)
  t.not(tx.getWakeTime(), null)
  t.truthy(tx.getWakeTime() instanceof Date)
  t.is(tx.getWakeSwitch(), 'abc')

  await tx.delta(null, {
    status: STEP_RUNNING,
    sleepDuration: 60,
    wakeSwitch: 'abc'
  })
  t.is(tx.getRetryCounter(), 1)
  t.not(tx.getSleepingSince(), null)
  t.truthy(tx.getSleepingSince() instanceof Date)
  t.not(tx.getWakeTime(), null)
  t.truthy(tx.getWakeTime() instanceof Date)
  t.is(tx.getWakeSwitch(), 'abc')

  await tx.delta(null, {
    status: STEP_SLEEPING,
    sleepDuration: 60,
    wakeSwitch: 'abc'
  })
  t.is(tx.getRetryCounter(), 2)
  t.not(tx.getSleepingSince(), null)
  t.truthy(tx.getSleepingSince() instanceof Date)
  t.not(tx.getWakeTime(), null)
  t.truthy(tx.getWakeTime() instanceof Date)
  t.is(tx.getWakeSwitch(), 'abc')
})


test.serial('Sleep counter does not increment with repeat setting to sleep status', async t => {
  const { tx, txId, externalId } = await createTestTransaction()

  t.truthy(tx)
  t.is(tx.getTxId(), txId)
  t.is(tx.getRetryCounter(), 0)
  t.is(tx.getSleepingSince(), null)
  t.is(tx.getWakeTime(), null)
  t.is(tx.getWakeSwitch(), null)

  await tx.delta(null, {
    status: STEP_SLEEPING,
    sleepDuration: 60,
    wakeSwitch: 'abc'
  })
  t.is(tx.getRetryCounter(), 1)
  t.not(tx.getSleepingSince(), null)
  t.truthy(tx.getSleepingSince() instanceof Date)
  t.not(tx.getWakeTime(), null)
  t.truthy(tx.getWakeTime() instanceof Date)
  t.is(tx.getWakeSwitch(), 'abc')


  await tx.delta(null, {
    status: STEP_SLEEPING,
    sleepDuration: 60,
    wakeSwitch: 'abc'
  })
  t.is(tx.getRetryCounter(), 1)
  t.not(tx.getSleepingSince(), null)
  t.truthy(tx.getSleepingSince() instanceof Date)
  t.not(tx.getWakeTime(), null)
  t.truthy(tx.getWakeTime() instanceof Date)
  t.is(tx.getWakeSwitch(), 'abc')
})

test.serial('Sleep fields reset on success status', async t => {
  const { tx, txId, externalId } = await createTestTransaction()

  t.truthy(tx)
  t.is(tx.getTxId(), txId)
  t.is(tx.getRetryCounter(), 0)
  t.is(tx.getSleepingSince(), null)
  t.is(tx.getWakeTime(), null)
  t.is(tx.getWakeSwitch(), null)

  await tx.delta(null, {
    status: STEP_SLEEPING,
    sleepDuration: 60,
    wakeSwitch: 'abc'
  })
  t.is(tx.getRetryCounter(), 1)
  t.not(tx.getSleepingSince(), null)
  t.truthy(tx.getSleepingSince() instanceof Date)
  t.not(tx.getWakeTime(), null)
  t.truthy(tx.getWakeTime() instanceof Date)
  t.is(tx.getWakeSwitch(), 'abc')


  await tx.delta(null, {
    status: STEP_SUCCESS,
  })
  t.is(tx.getRetryCounter(), 0)
  t.is(tx.getSleepingSince(), null)
  t.is(tx.getWakeTime(), null)
  t.is(tx.getWakeSwitch(), null)
})

test.serial('Sleep start time does not change with sleep-run-sleep', async t => {
  const { tx, txId, externalId } = await createTestTransaction()

  t.truthy(tx)
  t.is(tx.getTxId(), txId)
  t.is(tx.getRetryCounter(), 0)
  t.is(tx.getSleepingSince(), null)
  t.is(tx.getWakeTime(), null)
  t.is(tx.getWakeSwitch(), null)

  await tx.delta(null, {
    status: STEP_SLEEPING,
    sleepDuration: 60,
    wakeSwitch: 'abc'
  })
  t.is(tx.getRetryCounter(), 1)
  const sleepingSince1 = tx.getSleepingSince()

  await tx.delta(null, {
    status: STEP_RUNNING,
    sleepDuration: 60,
    wakeSwitch: 'abc'
  })

  await tx.delta(null, {
    status: STEP_SLEEPING,
    sleepDuration: 60,
    wakeSwitch: 'abc'
  })
  const sleepingSince2 = tx.getSleepingSince()
  t.is(sleepingSince1, sleepingSince2)
})


test.serial('Persist sleep fields to database as delta', async t => {
  const { tx, txId, externalId } = await createTestTransaction()

  t.truthy(tx)
  t.is(tx.getTxId(), txId)
  t.is(tx.getRetryCounter(), 0)
  t.is(tx.getSleepingSince(), null)
  t.is(tx.getWakeTime(), null)
  t.is(tx.getWakeSwitch(), null)

  await tx.delta(null, {
    status: STEP_SLEEPING,
    sleepDuration: 60,
    wakeSwitch: 'abc'
  })
  t.is(tx.getRetryCounter(), 1)
  t.not(tx.getSleepingSince(), null)
  t.truthy(tx.getSleepingSince() instanceof Date)
  t.not(tx.getWakeTime(), null)
  t.truthy(tx.getWakeTime() instanceof Date)
  t.is(tx.getWakeSwitch(), 'abc')

  // Check the database
  const sql = `SELECT * FROM atp_transaction_delta WHERE transaction_id=?`
  const params = [ txId ]
  const rows = await query(sql, params)
  // console.log(`rows=`, rows)
  t.is(rows.length, 1)

  // Check row 0
  t.is(rows[0].transaction_id, txId)
  t.is(rows[0].sequence, 1)
  t.is(rows[0].step_id, null)
  const data = JSON.parse(rows[0].data)
  // console.log(`data=`, data)
  t.is(Object.keys(data).length, 3)
  t.is(data.status, STEP_SLEEPING)
  t.is(data.sleepDuration, 60)
  t.is(data.wakeSwitch, 'abc')
})


test.serial('Persist sleep fields to database in atp_transaction2', async t => {
  const { tx, txId, externalId } = await createTestTransaction()

  t.truthy(tx)
  t.is(tx.getTxId(), txId)
  t.is(tx.getRetryCounter(), 0)
  t.is(tx.getSleepingSince(), null)
  t.is(tx.getWakeTime(), null)
  t.is(tx.getWakeSwitch(), null)

  await tx.delta(null, {
    status: STEP_SLEEPING,
    sleepDuration: 60,
    wakeSwitch: 'abc'
  })
  t.is(tx.getRetryCounter(), 1)
  t.not(tx.getSleepingSince(), null)
  t.truthy(tx.getSleepingSince() instanceof Date)
  t.not(tx.getWakeTime(), null)
  t.truthy(tx.getWakeTime() instanceof Date)
  t.is(tx.getWakeSwitch(), 'abc')

  // Check the database
  const sql = `SELECT * FROM atp_transaction2 WHERE transaction_id=?`
  const params = [ txId ]
  const rows = await query(sql, params)
  // console.log(`rows=`, rows)
  t.is(rows.length, 1)
  const record = rows[0]

  // Check row 0
  t.is(record.transaction_id, txId)
  t.is(record.status, STEP_SLEEPING)
  t.is(record.sleep_counter, 1)
  t.not(record.sleeping_since, null)
  t.truthy(record.sleeping_since instanceof Date)
  t.not(record.wake_time, null)
  t.truthy(record.wake_time instanceof Date)
  t.is(record.wake_switch, 'abc')
})


test.serial('Reconstruct sleep fields from database', async t => {
  const { tx, txId, externalId } = await createTestTransaction()

  t.truthy(tx)
  t.is(tx.getTxId(), txId)
  t.is(tx.getRetryCounter(), 0)
  t.is(tx.getSleepingSince(), null)
  t.is(tx.getWakeTime(), null)
  t.is(tx.getWakeSwitch(), null)

  await tx.delta(null, {
    status: STEP_SLEEPING,
    sleepDuration: 60,
    wakeSwitch: 'abc'
  })
  t.is(tx.getRetryCounter(), 1)
  t.not(tx.getSleepingSince(), null)
  t.truthy(tx.getSleepingSince() instanceof Date)
  t.not(tx.getWakeTime(), null)
  t.truthy(tx.getWakeTime() instanceof Date)
  t.is(tx.getWakeSwitch(), 'abc')

  // See if it reconstructs the same
  let tx2 = await TransactionPersistance.reconstructTransaction(txId)
  // console.log(`tx2=`, tx2)
  t.is(tx2.getRetryCounter(), 1)
  t.not(tx2.getSleepingSince(), null)
  t.truthy(tx2.getSleepingSince() instanceof Date)
  t.not(tx2.getWakeTime(), null)
  t.truthy(tx2.getWakeTime() instanceof Date)
  t.is(tx2.getWakeSwitch(), 'abc')

  // Remove it from the cache and check again
  await TransactionCache.removeFromCache(txId)
  tx2 = await TransactionCache.getTransactionState(txId, false)
  t.falsy(tx2)
  tx2 = await TransactionCache.getTransactionState(txId, true)
  t.truthy(tx2)
  t.is(tx2.getRetryCounter(), 1)
  t.not(tx2.getSleepingSince(), null)
  t.truthy(tx2.getSleepingSince() instanceof Date)
  t.not(tx2.getWakeTime(), null)
  t.truthy(tx2.getWakeTime() instanceof Date)
  t.is(tx2.getWakeSwitch(), 'abc')
})
