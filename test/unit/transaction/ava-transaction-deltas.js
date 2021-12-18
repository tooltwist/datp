import test from 'ava'
import Transaction from '../../../ATP/Scheduler2/Transaction'
import createTestTransaction from '../helpers/createTestTransaction'

// https://github.com/avajs/ava/blob/master/docs/01-writing-tests.md
test.beforeEach(async t => {
  const num = Math.round(Math.random() * 100000000000)
  t.context.txId = `t-${num}`
})


/*
 *  Check the code is valid
 */
test.serial('Exists and has unit tests', async t => {
  t.truthy(Transaction)
});


/*
 *  Tests
 */
test.serial('allocate', async t => {
  const { tx, txId } = await createTestTransaction()
  t.truthy(tx)
  t.is(tx.getTxId(), txId)
});

test.serial('stream - empty', async t => {
  const { tx, txId } = await createTestTransaction()

  const obj = tx.asObject()
  t.truthy(obj)
  t.is(obj.txId, txId)
  const data = obj.transactionData
  t.truthy(data)
  t.is(Object.keys(data).length, 1)
  t.is(data.status, 'running')
  t.truthy(obj.steps)
  t.is(Object.keys(obj.steps).length, 0)
})


test.serial('stream - simple tx values', async t => {
  // const tx = new Transaction(t.context.txId)
  const { tx, txId } = await createTestTransaction()
  await tx.delta(null, {
    a: 'aaa',
    b: 123,
    c: {
      d: 'ddd'
    }
  })

  const obj = tx.asObject()
  t.truthy(obj)
  t.is(obj.txId, txId)
  t.truthy(obj.steps)
  t.is(Object.keys(obj.steps).length, 0)

  const data = obj.transactionData
  t.truthy(data)
  t.is(Object.keys(data).length, 4)
  t.is(data.status, 'running')
  t.is(data.a, 'aaa')
  t.is(data.b, 123)
  t.is(typeof(data.c), 'object')
  t.is(Object.keys(data.c).length, 1)
  t.is(typeof(data.c.d), 'string')
  t.is(data.c.d, 'ddd')
})


test.serial('stream - overwrite tx values', async t => {
  const { tx, txId } = await createTestTransaction()

  await tx.delta(null, {
    a: 'aaa',
    b: 123,
    c: {
      d: 'ddd'
    }
  })
  await tx.delta(null, {
    a: 'zzz',
    b: 999,
    c: {
      d: 'pqr'
    }
  })

  // Check the returned object
  const obj = tx.asObject()
  t.truthy(obj)
  t.is(Object.keys(obj).length, 5)
  t.is(typeof(obj.owner), 'string')
  t.is(typeof(obj.txId), 'string')
  t.is(typeof(obj.externalId), 'string')
  t.is(typeof(obj.transactionData), 'object')
  t.is(typeof(obj.steps), 'object')

  // Check the transaction ID
  t.is(obj.txId, txId)

  // Check the data
  const data = obj.transactionData
  t.is(Object.keys(data).length, 4)
  t.is(data.status, 'running')
  t.is(data.a, 'zzz')
  t.is(data.b, 999)
  t.is(typeof(data.c), 'object')
  t.is(Object.keys(data.c).length, 1)
  t.is(typeof(data.c.d), 'string')
  t.is(data.c.d, 'pqr')

  // Check the steps
  t.is(Object.keys(obj.steps).length, 0)
})


test.serial('stream - delete simple tx value', async t => {
  const { tx, txId } = await createTestTransaction()
  await tx.delta(null, {
    a: 'aaa',
    b: 123,
    c: {
      d: 'ddd',
      e: 'eee'
    }
  })
  await tx.delta(null, {
    '-a': null,
  })

  const obj = tx.asObject()
  t.truthy(obj)
  t.is(obj.txId, txId)
  t.truthy(obj.steps)
  t.is(Object.keys(obj.steps).length, 0)

  const data = obj.transactionData
  t.truthy(data)
  t.is(Object.keys(data).length, 3)
  t.is(data.status, 'running')
  t.is(data.b, 123)
  t.is(typeof(data.c), 'object')
  t.is(Object.keys(data.c).length, 2)
  t.is(typeof(data.c.d), 'string')
  t.is(data.c.d, 'ddd')
  t.is(typeof(data.c.e), 'string')
  t.is(data.c.e, 'eee')
})


test.serial('stream - delete nested tx value', async t => {
  const { tx, txId } = await createTestTransaction()
  await tx.delta(null, {
    a: 'aaa',
    b: 123,
    c: {
      d: 'ddd',
      e: 'eee'
    }
  })
  await tx.delta(null, {
    c: {
      '-d': null,
    }
  })

  const obj = tx.asObject()
  t.truthy(obj)
  t.is(obj.txId, txId)
  t.truthy(obj.steps)
  t.is(Object.keys(obj.steps).length, 0)

  const data = obj.transactionData
  t.truthy(data)
  t.is(Object.keys(data).length, 4)
  t.is(data.status, 'running')
  t.is(typeof(data.a), 'string')
  t.is(data.a, 'aaa')
  t.is(typeof(data.b), 'number')
  t.is(data.b, 123)

  t.is(typeof(data.c), 'object')
  t.is(Object.keys(data.c).length, 1)
  t.is(typeof(data.c.e), 'string')
  t.is(data.c.e, 'eee')
})



test.serial('stream - simple step', async t => {
  const { tx, txId } = await createTestTransaction()

  const stepId = 'yarp'
  await tx.delta(stepId, {
    a: 'aaa',
    b: 123,
    c: {
      d: 'ddd'
    }
  })

  // Check the returned object
  const obj = tx.asObject()
  t.truthy(obj)
  t.is(Object.keys(obj).length, 5)
  t.is(typeof(obj.owner), 'string')
  t.is(typeof(obj.txId), 'string')
  t.is(typeof(obj.externalId), 'string')
  t.is(typeof(obj.transactionData), 'object')
  t.is(typeof(obj.steps), 'object')

  // Check the transaction ID
  t.is(obj.txId, txId)

  // Check the data
  t.is(Object.keys(obj.transactionData).length, 1)
  t.is(obj.transactionData.status, 'running')

  // Check the steps
  t.is(Object.keys(obj.steps).length, 1)
  const step = tx.stepData(stepId)
  t.truthy(step)
  t.is(typeof(step.a), 'string')
  t.is(step.a, 'aaa')
  t.is(typeof(step.b), 'number')
  t.is(step.b, 123)
  t.is(typeof(step.c), 'object')
  t.is(Object.keys(step.c).length, 1)
  t.is(typeof(step.c.d), 'string')
  t.is(step.c.d, 'ddd')
})


test.serial('stream - overwrite step values', async t => {
  const { tx, txId } = await createTestTransaction()

  const stepId = 'yarp'
  await tx.delta(stepId, {
    a: 'aaa',
    b: 123,
    c: {
      d: 'ddd'
    }
  })
  await tx.delta(stepId, {
    a: 'zzz',
    b: 999,
    c: {
      d: 'pqr'
    }
  })

  // Check the returned object
  const obj = tx.asObject()
  t.truthy(obj)
  t.is(Object.keys(obj).length, 5)
  t.is(typeof(obj.owner), 'string')
  t.is(typeof(obj.txId), 'string')
  t.is(typeof(obj.externalId), 'string')
  t.is(typeof(obj.transactionData), 'object')
  t.is(typeof(obj.steps), 'object')

  // Check the transaction ID
  t.is(obj.txId, txId)

  // Check the data
  const transactionData = tx.transactionData()
  t.is(Object.keys(transactionData).length, 1)
  t.is(transactionData.status, 'running')

  // Check the steps
  t.is(Object.keys(obj.steps).length, 1)
  const step = tx.stepData(stepId)
  t.truthy(step)
  t.is(Object.keys(step).length, 3)
  t.is(typeof(step.a), 'string')
  t.is(step.a, 'zzz')
  t.is(typeof(step.b), 'number')
  t.is(step.b, 999)
  t.is(typeof(step.c), 'object')
  t.is(Object.keys(step.c).length, 1)
  t.is(typeof(step.c.d), 'string')
  t.is(step.c.d, 'pqr')
})


test.serial('stream - delete step values', async t => {
  const { tx, txId } = await createTestTransaction()

  const stepId = 'yarp'
  await tx.delta(stepId, {
    a: 'aaa',
    b: 123,
    c: {
      d: 'ddd',
      e: 'eee'
    }
  })
  await tx.delta(stepId, {
    '-a': null,
    c: {
      '-e': null
    }
  })

  // Check the returned object
  const obj = tx.asObject()
  t.truthy(obj)
  t.is(Object.keys(obj).length, 5)

  t.is(typeof(obj.owner), 'string')
  t.is(typeof(obj.txId), 'string')
  t.is(typeof(obj.externalId), 'string')
  t.is(typeof(obj.transactionData), 'object')
  t.is(typeof(obj.steps), 'object')

  // Check the transaction ID
  // const { txId, data, steps } = obj
  t.is(obj.txId, txId)

  // Check the data
  const transactionData = tx.transactionData()
  t.is(Object.keys(transactionData).length, 1)
  t.is(transactionData.status, 'running')

  // Check the steps
  t.is(Object.keys(obj.steps).length, 1) // Only one step
  const step = tx.stepData(stepId)
  t.truthy(step)
  // console.log(`step=`, step)
  t.is(Object.keys(step).length, 2) // 2 values in step
  t.is(typeof(step.b), 'number')
  t.is(step.b, 123)
  t.is(typeof(step.c), 'object')
  t.is(Object.keys(step.c).length, 1)
  t.is(typeof(step.c.d), 'string')
  t.is(step.c.d, 'ddd')
})



test.serial('stream - build up values', async t => {
  const { tx, txId } = await createTestTransaction()

  const stepId1 = 'yarp-1'
  const stepId2 = 'yarp-2'
  await tx.delta(null, { a: 'value-a' })
  await tx.delta(stepId1, { b: 123 })
  await tx.delta(stepId1, { c: 'value-c' })
  await tx.delta(null, { happy: 'days' })
  await tx.delta(null, { sun: 'set' })
  await tx.delta(stepId2, { fast: 'chicken' })
  await tx.delta(stepId1, { small: 0.0001 })
  await tx.delta(stepId2, { nested: { more: { andmore: { value: 'abc' }}} })
  await tx.delta(stepId1, { big: 999999999.999 })

  // Check the returned object
  const obj = tx.asObject()
  // console.log(`obj=`, obj)
  t.truthy(obj)
  t.is(Object.keys(obj).length, 5)
  t.is(typeof(obj.owner), 'string')
  t.is(typeof(obj.txId), 'string')
  t.is(typeof(obj.externalId), 'string')
  t.is(typeof(obj.transactionData), 'object')
  t.is(typeof(obj.steps), 'object')


  // Check the transaction ID
  t.is(obj.txId, txId)

  // Check the data
  const transactionData = tx.transactionData()
  t.is(Object.keys(transactionData).length, 4)
  t.is(transactionData.status, 'running')
  t.is(transactionData.a, 'value-a')
  t.is(transactionData.happy, 'days')
  t.is(transactionData.sun, 'set')

  // Check the steps
  t.is(Object.keys(obj.steps).length, 2)
  const step1 = tx.stepData(stepId1)
  t.truthy(step1)
  t.is(Object.keys(step1).length, 4)
  t.is(step1.b, 123)
  t.is(step1.c, 'value-c')
  t.is(step1.small, 0.0001)
  t.is(step1.big, 999999999.999)

  const step2 = tx.stepData(stepId2)
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
