/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import test from 'ava'
import CallbackRegister from '../../../ATP/Scheduler2/CallbackRegister'
import pause from '../../../lib/pause'
import PipelineStep from '../../../ATP/hardcoded-steps/PipelineStep'
import ExampleStep from '../../../ATP/hardcoded-steps/ExampleStep'
import RandomDelayStep from '../../../ATP/hardcoded-steps/RandomDelayStep'
import { schedulerForThisNode, prepareForUnitTesting } from '../../..'

/*
 *  We need to use a different node name for each test file, as they run in different
 *  processes (with different CallbackRegisters). If multiple test files have the same nodeId
 *  then they draw from the same queue, but the worker might not know the callback handler.
 */
const OWNER = 'fred'
const NODE_GROUP = 'master'
const NUM_TESTS = 10


// https://github.com/avajs/ava/blob/master/docs/01-writing-tests.md
test.before(async t => {
  await PipelineStep.register()
  await ExampleStep.register()
  await RandomDelayStep.register()
  await prepareForUnitTesting()
})


test.serial.only('Large number of ping4 transactions', async t => {

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
    // console.log(`- ping4 callback:`, context, transactionOutput)
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
        transactionType: 'ping4',
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

  // // Check they completed in order
  // for (const tx of transactionList) {
  //   t.is(tx.completionOrder, tx.i)
  // }

  if (endTime) {
    const elapsed = endTime - startTime
    const each = elapsed / NUM_TESTS
    //console.log(`Completed ${NUM_TESTS} ping4 transactions in ${elapsed}ms  (${each}ms per ping, single threaded)`)
  }
})
