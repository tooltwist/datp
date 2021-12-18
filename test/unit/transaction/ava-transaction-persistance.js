import test from 'ava'
import createTestTransaction from '../helpers/createTestTransaction'
import query from '../../../database/query'
import TransactionIndexEntry from '../../../ATP/TransactionIndexEntry'
import TransactionPersistance from '../../../ATP/Scheduler2/TransactionPersistance'
import TransactionCache from '../../../ATP/Scheduler2/TransactionCache'


// https://github.com/avajs/ava/blob/master/docs/01-writing-tests.md
test.beforeEach(async t => {
})


/*
 *  Check the code is valid
 */
test.serial('Exists and has unit tests', async t => {
  t.truthy(TransactionPersistance)
});

/*
 *  Tests
 */
test.serial('allocate new transaction', async t => {
  const { tx, txId, externalId } = await createTestTransaction()

  t.truthy(tx)
  t.is(tx.getTxId(), txId)
  // const input = tx.getInput().getData()

  // Check the database
  const sql = `SELECT * FROM atp_transaction2 WHERE transaction_id=?`
  const params = [ txId ]
  const rows = await query(sql, params)
  // console.log(`rows=`, rows)

  t.is(rows.length, 1)
  const row = rows[0]
  t.is(row.transaction_id, txId)
  t.is(row.external_id, externalId)
  // t.is(row.transaction_type, input.metadata.transactionType)
  t.is(row.status, TransactionIndexEntry.RUNNING)
  t.is(row.owner, tx.getOwner())
  // t.is(row.initial_data, externalId)
  // t.is(row.node_id, tx.getNodeId())
  // t.is(row.pipeline, tx.getPipeline())
})



test.serial('persist transaction details', async t => {
  const { tx, txId, externalId } = await createTestTransaction()

  tx.delta(null, {
    chicken: 'soup',
    wallace: 'grommet'
  })
  tx.delta(null, {
    value1: 'aaa',
  })
  tx.delta(null, {
    value2: 'bbb',
  })

  await TransactionCache.persist(txId)

  // Check the database
  const sql = `SELECT * FROM atp_transaction_delta WHERE transaction_id=?`
  const params = [ txId ]
  const rows = await query(sql, params)
  // console.log(`rows=`, rows)

  t.is(rows.length, 3)

  // Check row 0
  t.is(rows[0].transaction_id, txId)
  t.is(rows[0].sequence, 1)
  t.is(rows[0].step_id, null)
  const data0 = JSON.parse(rows[0].data)
  t.is(Object.keys(data0).length, 2)
  t.is(data0.chicken, 'soup')
  t.is(data0.wallace, 'grommet')

  // Check row 1
  t.is(rows[1].transaction_id, txId)
  t.is(rows[1].sequence, 2)
  t.is(rows[1].step_id, null)
  const data1 = JSON.parse(rows[1].data)
  t.is(Object.keys(data1).length, 1)
  t.is(data1.value1, 'aaa')

  // Check row 2
  t.is(rows[2].transaction_id, txId)
  t.is(rows[2].sequence, 3)
  t.is(rows[2].step_id, null)
  const data2 = JSON.parse(rows[2].data)
  t.is(Object.keys(data2).length, 1)
  t.is(data2.value2, 'bbb')
})



test.serial('reconstruct transaction details', async t => {
  const { tx, txId, externalId, owner } = await createTestTransaction()

  // Add some tx and step deltas
  const stepId1 = 'yarp-1'
  const stepId2 = 'yarp-2'
  tx.delta(null, { a: 'value-a' })
  tx.delta(stepId1, { b: 123 })
  tx.delta(stepId1, { c: 'value-c' })
  tx.delta(null, { happy: 'days' })
  tx.delta(null, { sun: 'set' })
  tx.delta(stepId2, { fast: 'chicken' })
  tx.delta(stepId1, { small: 0.0001 })
  tx.delta(stepId2, { nested: { more: { andmore: { value: 'abc' }}} })
  tx.delta(stepId1, { big: 999999999.999 })


  await TransactionCache.persist(txId)

  const tx2 = await TransactionPersistance.reconstructTransaction(txId)

  // const tx2
  // console.log(`tx 2=`, tx2.toString())

  // Check the returned object
  const obj = tx2.asObject()
  t.truthy(obj)
  t.is(Object.keys(obj).length, 5)
  t.is(typeof(obj.owner), 'string')
  t.is(typeof(obj.txId), 'string')
  t.is(typeof(obj.externalId), 'string')
  t.is(typeof(obj.transactionData), 'object')
  t.is(typeof(obj.steps), 'object')

  const steps = obj.steps


  // Check the data
  const data = obj.transactionData
  t.is(Object.keys(data).length, 4)
  t.is(data.status, 'running')
  t.is(data.a, 'value-a')
  t.is(data.happy, 'days')
  t.is(data.sun, 'set')

  // Check the steps
  t.is(Object.keys(steps).length, 2)

  const step1 = steps[stepId1]
  t.truthy(step1)
  t.is(Object.keys(step1).length, 4)
  t.is(step1.b, 123)
  t.is(step1.c, 'value-c')
  t.is(step1.small, 0.0001)
  t.is(step1.big, 999999999.999)

  const step2 = steps[stepId2]
  t.truthy(step2)
  t.is(Object.keys(step2).length, 2)

  t.is(typeof(step2.fast), 'string')
  t.is(step2.fast, 'chicken')
  t.is(typeof(step2.nested), 'object')
  t.is(Object.keys(step2.nested).length, 1)
  t.is(typeof(step2.nested.more), 'object')
  t.is(Object.keys(step2.nested.more).length, 1)
  t.is(typeof(step2.nested.more.andmore), 'object')
  t.is(Object.keys(step2.nested.more.andmore).length, 1)
  t.is(typeof(step2.nested.more.andmore.value), 'string')
  t.is(step2.nested.more.andmore.value, 'abc')
})



test.serial('find hibernated transaction', async t => {
  const { tx, txId, externalId, owner } = await createTestTransaction()

  // Add some tx and step deltas
  const stepId1 = 'yarp-1'
  const stepId2 = 'yarp-2'
  tx.delta(null, { a: 'value-a' })
  tx.delta(stepId1, { b: 123 })
  tx.delta(stepId1, { c: 'value-c' })
  tx.delta(null, { happy: 'days' })
  tx.delta(null, { sun: 'set' })
  tx.delta(stepId2, { fast: 'chicken' })
  tx.delta(stepId1, { small: 0.0001 })
  tx.delta(stepId2, { nested: { more: { andmore: { value: 'abc' }}} })
  tx.delta(stepId1, { big: 999999999.999 })

  // Persist the transaction
  await TransactionCache.persist(txId)

  // Check it is no longer in the cache
  let tx2 = await TransactionCache.findTransaction(txId, false)
  t.falsy(tx2)


  tx2 = await TransactionCache.findTransaction(txId, true)
  t.truthy(tx2)

  // console.log(`tx 2=`, tx2.toString())

  // Check the returned object
  const obj = tx2.asObject()
  t.truthy(obj)
  t.is(Object.keys(obj).length, 5)
  t.is(typeof(obj.owner), 'string')
  t.is(typeof(obj.txId), 'string')
  t.is(typeof(obj.externalId), 'string')
  t.is(typeof(obj.transactionData), 'object')
  t.is(typeof(obj.steps), 'object')

  // Check the transaction data
  const data = obj.transactionData
  t.is(Object.keys(data).length, 4)
  t.is(data.status, 'running')
  t.is(data.a, 'value-a')
  t.is(data.happy, 'days')
  t.is(data.sun, 'set')

  // Check the steps
  const steps = obj.steps
  t.is(Object.keys(steps).length, 2)

  const step1 = steps[stepId1]
  t.truthy(step1)
  t.is(Object.keys(step1).length, 4)
  t.is(step1.b, 123)
  t.is(step1.c, 'value-c')
  t.is(step1.small, 0.0001)
  t.is(step1.big, 999999999.999)

  const step2 = steps[stepId2]
  t.truthy(step2)
  t.is(Object.keys(step2).length, 2)

  t.is(typeof(step2.fast), 'string')
  t.is(step2.fast, 'chicken')
  t.is(typeof(step2.nested), 'object')
  t.is(Object.keys(step2.nested).length, 1)
  t.is(typeof(step2.nested.more), 'object')
  t.is(Object.keys(step2.nested.more).length, 1)
  t.is(typeof(step2.nested.more.andmore), 'object')
  t.is(Object.keys(step2.nested.more.andmore).length, 1)
  t.is(typeof(step2.nested.more.andmore.value), 'string')
  t.is(step2.nested.more.andmore.value, 'abc')
})


test.serial.only('persist transaction values changed by a delta', async t => {
  const { tx, txId, externalId, owner } = await createTestTransaction()

  // Add some tx and step deltas
  const stepId1 = 'yarp-1'
  const stepId2 = 'yarp-2'
  tx.delta(null, { a: 'value-a' })
  tx.delta(stepId1, { b: 123 })
  tx.delta(stepId1, { c: 'value-c' })
  tx.delta(null, { happy: 'days' })
  tx.delta(null, { sun: 'set' })
  tx.delta(stepId2, { fast: 'chicken' })
  tx.delta(stepId1, { small: 0.0001 })
  tx.delta(stepId2, { nested: { more: { andmore: { value: 'abc' }}} })
  tx.delta(stepId1, { big: 999999999.999 })

  // Persist the transaction
  await TransactionCache.persist(txId)

  // Check it is no longer in the cache
  let tx2 = await TransactionCache.findTransaction(txId, false)
  t.falsy(tx2)


  tx2 = await TransactionCache.findTransaction(txId, true)
  t.truthy(tx2)
  // console.log(`tx 2=`, tx2.toString())

  // Check the returned object
  const obj = tx2.asObject()
  // console.log(`obj=`, obj)
  t.truthy(obj)
  t.is(Object.keys(obj).length, 5)
  t.is(typeof(obj.owner), 'string')
  t.is(typeof(obj.txId), 'string')
  t.is(typeof(obj.externalId), 'string')
  t.is(typeof(obj.transactionData), 'object')
  t.is(typeof(obj.steps), 'object')

  const data = obj.transactionData
  const steps = obj.steps


  // Check the data
  t.is(Object.keys(data).length, 4)
  // console.log(`data=`, data)
  t.is(data.status, 'running')
  t.is(data.a, 'value-a')
  t.is(data.happy, 'days')
  t.is(data.sun, 'set')

  // Check the steps
  t.is(Object.keys(steps).length, 2)

  const step1 = steps[stepId1]
  t.truthy(step1)
  t.is(Object.keys(step1).length, 4)
  t.is(step1.b, 123)
  t.is(step1.c, 'value-c')
  t.is(step1.small, 0.0001)
  t.is(step1.big, 999999999.999)

  const step2 = steps[stepId2]
  t.truthy(step2)
  t.is(Object.keys(step2).length, 2)

  t.is(typeof(step2.fast), 'string')
  t.is(step2.fast, 'chicken')
  t.is(typeof(step2.nested), 'object')
  t.is(Object.keys(step2.nested).length, 1)
  t.is(typeof(step2.nested.more), 'object')
  t.is(Object.keys(step2.nested.more).length, 1)
  t.is(typeof(step2.nested.more.andmore), 'object')
  t.is(Object.keys(step2.nested.more.andmore).length, 1)
  t.is(typeof(step2.nested.more.andmore.value), 'string')
  t.is(step2.nested.more.andmore.value, 'abc')
})
