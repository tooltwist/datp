import test from 'ava'
import Scheduler2 from '../../../ATP/Scheduler2/Scheduler2'
import CallbackRegister from '../../../ATP/Scheduler2/CallbackRegister'
import pause from '../../../lib/pause'

/*
 *  We need to use a different node name for each test file, as they run in different
 *  processes (with different CallbackRegisters). If multiple test files have the same nodeId
 *  then they draw from the same queue, but the worker might not know the callback handler.
 */
const OWNER = 'fred'
const NODE_ID = 'many-ping1'


// https://github.com/avajs/ava/blob/master/docs/01-writing-tests.md
test.beforeEach(async t => { })


test.serial('Large number of ping1 transactions, single worker', async t => {

  // Prepare a list of transactions to run
  const NUM = 100
  const transactionList = [ ]
  for (let i = 0; i < NUM; i++) {
    transactionList.push({
      i,
      value: `yarp${i}`,
      completed: 0
    })
  }

  let completionCounter = 0
  const startTime = Date.now()
  let endTime = 0

  // Define a callback
  const handlerName = `test-callback-many-ping1-a-${Math.random()}`
  await CallbackRegister.register(handlerName, (context, transactionOutput) => {
    // console.log(`- ping1 callback:`, context, transactionOutput)
    transactionList[context.i].completionOrder = completionCounter++
    transactionList[context.i].completed++

    if (completionCounter === NUM) {
      endTime = Date.now()
    }
  })

  // Start the scheduler and give it time to work
  const scheduler = new Scheduler2(NODE_ID, null, { numWorkers: 1 })
  await scheduler.drainQueue()
  await scheduler.start()

  // Start the test transaction
  for (const tx of transactionList) {
    // console.log(`=> add ${tx.i} to queue`)
    await Scheduler2.startTransaction({
      metadata: {
        owner: OWNER,
        nodeId: NODE_ID,
        externalId: `extref-${Math.random()}`,
        transactionType: 'ping1',
        callback: handlerName,
        callbackContext: tx
      },
      data: {
      }
    })
  }

  // await scheduler.dump()
  await pause(2000)
  await scheduler.stop()

  // Look for any duplicated, or incomplete transactions.
  for (const tx of transactionList) {
    t.is(tx.completed, 1)
  }

  // Check that the callback was called
  t.is(completionCounter, NUM)
  await scheduler.destroy()

  // Check they completed in order
  for (const tx of transactionList) {
    t.is(tx.completionOrder, tx.i)
  }

  if (endTime) {
    const elapsed = endTime - startTime
    const each = elapsed / NUM
    console.log(`Completed ${NUM} ping1 transactions in ${elapsed}ms  (${each}ms per ping, single threaded)`)
  }
})



test.serial('Large number of ping1 transactions, multiple workers', async t => {
  const NUM = 100
  const NUM_WORKERS = 1
  const transactionList = [ ]
  for (let i = 0; i < NUM; i++) {
    transactionList.push({
      i,
      value: `yarp${i}`,
      completed: 0
    })
  }

  let completionCounter = 0
  const startTime = Date.now()
  let endTime

  // Define a callback
  const handlerName = `test-callback-many-ping1-b-${Math.random()}`
  await CallbackRegister.register(handlerName, (data) => {
    // console.log(`- ping1 callback:`, data)
    transactionList[data.i].completionOrder = completionCounter++
    transactionList[data.i].completed++

    if (completionCounter === NUM) {
      endTime = Date.now()
    }
  })

  // Start the scheduler and give it time to work
  const scheduler = new Scheduler2(NODE_ID, null, { numWorkers: NUM_WORKERS })
  await scheduler.drainQueue()
  await scheduler.start()
  // await scheduler.dump()

  // Start the test transaction
  for (const tx of transactionList) {
    // console.log(`=> add ${tx.i} to queue`)
    await Scheduler2.startTransaction({
      metadata: {
        owner: OWNER,
        nodeId: NODE_ID,
        externalId: `extref-${Math.random()}`,
        transactionType: 'ping1',
        callback: handlerName,
        callbackContext: tx
      },
      data: {
      }
    })
  }

  // await scheduler.dump()
  await pause(200)
  await scheduler.stop()

  // Look for any duplicated, or incomplete transactions.
  for (const tx of transactionList) {
    t.is(tx.completed, 1)
  }

  // Check they completed in order
  for (const tx of transactionList) {
    t.is(tx.completionOrder, tx.i)
  }

  // Check that the callback was called
  t.is(completionCounter, NUM)
  await scheduler.destroy()

  const elapsed = endTime - startTime
  const each = elapsed / NUM
  console.log(`Completed ${NUM} ping1 transactions in ${elapsed}ms  (${each}ms per ping, ${NUM_WORKERS} workers)`)
})