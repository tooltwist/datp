/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import test from 'ava'
import CallbackRegister from '../../../ATP/Scheduler2/CallbackRegister'
import pause from '../../../lib/pause'
import { STEP_SUCCESS } from '../../../ATP/Step'
import { schedulerForThisNode, prepareForUnitTesting } from '../../..'

/*
 *  We need to use a different node name for each test file, as they run in different
 *  processes (with different CallbackRegisters). If multiple test files have the same nodeId
 *  then they draw from the same queue, but the worker might not know the callback handler.
 */
const OWNER = 'fred'
const NODE_GROUP = 'master'


// https://github.com/avajs/ava/blob/master/docs/01-writing-tests.md
test.before(async t => {
  await prepareForUnitTesting()
})


test.serial('Call ping1 test transaction', async t => {
  let returnedContext = null
  let returnedStatus = null
  let transactionOutput = null
  const startTime = Date.now()
  let endTime = 0

  // Define a callback
  const handlerName = `test-ping1-callback-${Math.random()}`
  await CallbackRegister.register(handlerName, (context, info) => {
    // console.log(`Test harness ping1 callback:`, context, info)
    returnedContext = context
    returnedStatus = info.status
    transactionOutput = info.transactionOutput
    endTime = Date.now()
  })

  await schedulerForThisNode.drainQueue()

  // Start the test transaction
  await schedulerForThisNode.startTransaction({
    metadata: {
      owner: OWNER,
      nodeGroup: NODE_GROUP,
      externalId: `extref-${Math.random()}`,
      transactionType: 'ping1',
      onComplete: {
        callback: handlerName,
        context: {
          ping: 'yippee!'
        }
      }
    },
    data: {
    }
  })

  // await scheduler.dump()
  await pause(500)
  // await schedulerForThisNode.stop()

  // Check that the callback was called
  t.truthy(returnedContext)
  t.is(returnedContext.ping, 'yippee!')
  t.truthy(returnedStatus)
  t.is(returnedStatus, STEP_SUCCESS)
  t.truthy(transactionOutput)
  t.is(transactionOutput.foo, 'bar')
  t.is(transactionOutput.description, 'ping1 - Scheduler2.startTransaction() immediately invoked the callback, without processing')
  //description: 'startTransaction() immediately invoked the callback, without processing'

  if (endTime !== 0) {
    const elapsed = endTime - startTime
    // console.log(`ping1 completed in ${elapsed}ms`)
  }
  // await scheduler.destroy()
})

