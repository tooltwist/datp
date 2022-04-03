/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import test from 'ava'
import CallbackRegister from '../../../ATP/Scheduler2/CallbackRegister'
import pause from '../../../lib/pause'
import { schedulerForThisNode, prepareForUnitTesting } from '../../..'

/*
 *  We need to use a different node name for each test file, as they run in different
 *  processes (with different CallbackRegisters). If multiple test files have the same nodeId
 *  then they draw from the same queue, but the worker might not know the callback handler.
 */
const OWNER = 'fred'
const NODE_GROUP = 'master'
const NUM_TESTS = 100


// https://github.com/avajs/ava/blob/master/docs/01-writing-tests.md
test.before(async t => {
  await prepareForUnitTesting()
})


test.serial('Warm up', async t => {

  // Prepare a list of transactions to run
  const transactionList = [ ]
  for (let i = 0; i < 5; i++) {
    transactionList.push({ i, value: `yarp${i}`, completed: 0 })
  }

  // Define a callback
  const handlerName = `test-callback-${NODE_GROUP}-a-${Math.random()}`
  await CallbackRegister.register(handlerName, (context, transactionOutput) => {
    // Do nothing
  })

  await schedulerForThisNode.drainQueue()

  // Start the test transaction
  for (const tx of transactionList) {
    // console.log(`=> add ${tx.i} to queue`)
    await schedulerForThisNode.startTransaction({
      metadata: {
        owner: OWNER,
        nodeGroup: NODE_GROUP,
        externalId: `extref-${Math.random()}`,
        transactionType: 'ping2',
        onComplete: {
          callback: handlerName,
          context: tx
        }
      },
      data: {
      }
    })
  }

  // await scheduler.dump()
  await pause(1000)
  // await scheduler.stop()
  t.truthy(true)
})


test.serial('Large number of ping2 transactions, single worker', async t => {

  // Prepare a list of transactions to run
  const transactionList = [ ]
  for (let i = 0; i < NUM_TESTS; i++) {
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
  const handlerName = `test-callback-${NODE_GROUP}-a-${Math.random()}`
  await CallbackRegister.register(handlerName, (context, transactionOutput) => {
    // console.log(`- ping2 callback:`, context, transactionOutput)
    transactionList[context.i].completionOrder = completionCounter++
    transactionList[context.i].completed++

    if (completionCounter === NUM_TESTS) {
      endTime = Date.now()
    }
  })

  await schedulerForThisNode.drainQueue()

  // Start the test transaction
  for (const tx of transactionList) {
    // console.log(`=> add ${tx.i} to queue`)
    await schedulerForThisNode.startTransaction({
      metadata: {
        owner: OWNER,
        nodeGroup: NODE_GROUP,
        externalId: `extref-${Math.random()}`,
        transactionType: 'ping2',
        onComplete: {
          callback: handlerName,
          context: tx
        }
      },
      data: {
      }
    })
  }

  // await scheduler.dump()
  await pause(2000)
  // await scheduler.stop()

  // Look for any duplicated, or incomplete transactions.
  for (const tx of transactionList) {
    t.is(tx.completed, 1)
  }

  // Check that the callback was called
  t.is(completionCounter, NUM_TESTS)
  // await scheduler.destroy()

  // Check they completed in order
  for (const tx of transactionList) {
    t.is(tx.completionOrder, tx.i)
  }

  if (endTime) {
    const elapsed = endTime - startTime
    const each = elapsed / NUM_TESTS
    // console.log(`Completed ${NUM_TESTS} ping2 transactions in ${elapsed}ms  (${each}ms per ping, single threaded)`)
  }
})



test.serial('Large number of ping2 transactions, multiple workers', async t => {
  const NUM_WORKERS = 10
  const transactionList = [ ]
  for (let i = 0; i < NUM_TESTS; i++) {
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
  const handlerName = `test-callback-${NODE_GROUP}-b-${Math.random()}`
  await CallbackRegister.register(handlerName, (data) => {
    // console.log(`- ping2 callback:`, data)
    transactionList[data.i].completionOrder = completionCounter++
    transactionList[data.i].completed++

    if (completionCounter === NUM_TESTS) {
      endTime = Date.now()
    }
  })

  await schedulerForThisNode.drainQueue()

  // Start the test transaction
  for (const tx of transactionList) {
    // console.log(`=> add ${tx.i} to queue`)
    await schedulerForThisNode.startTransaction({
      metadata: {
        owner: OWNER,
        nodeGroup: NODE_GROUP,
        externalId: `extref-${Math.random()}`,
        transactionType: 'ping2',
        onComplete: {
          callback: handlerName,
          context: tx
        }
      },
      data: {
      }
    })
  }

  // await scheduler.dump()
  await pause(200)
  // await scheduler.stop()

  // Look for any duplicated, or incomplete transactions.
  for (const tx of transactionList) {
    t.is(tx.completed, 1)
  }

  // Check they completed in order
  for (const tx of transactionList) {
    t.is(tx.completionOrder, tx.i)
  }

  // Check that the callback was called
  t.is(completionCounter, NUM_TESTS)
  // await scheduler.destroy()

  const elapsed = endTime - startTime
  const each = elapsed / NUM_TESTS
  // console.log(`Completed ${NUM_TESTS} ping2 transactions in ${elapsed}ms  (${each}ms per ping, ${NUM_WORKERS} workers)`)
})
