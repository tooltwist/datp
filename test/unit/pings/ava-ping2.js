/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
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


test.serial('Call ping2 test transaction', async t => {
  let returnedContext = null
  let status = null
  let transactionOutput = null
  const startTime = Date.now()
  let endTime = 0

  // Define a callback
  const handlerName = `test-callback-${NODE_GROUP}-a-${Math.random()}`
  await CallbackRegister.register(handlerName, (context, result) => {
    // console.log(`Test harness ping2 callback:`, context, result)
    returnedContext = context
    status = result.status
    transactionOutput = result.transactionOutput
    endTime = Date.now()
  })

  // Prepare the scheduler and ensure the queue is empty
  // const scheduler = new Scheduler2(NODE_GROUP, null)
  await schedulerForThisNode.drainQueue()

  // Start the test transaction
  await schedulerForThisNode.startTransaction({
    metadata: {
      owner: OWNER,
      nodeGroup: NODE_GROUP,
      externalId: `extref-${Math.random()}`,
      transactionType: 'ping2',
      onComplete: {
        callback: handlerName,
        context: {
          wallace: 'grommet'
        }
      }
    },
    data: {
    }
  })

  await pause(500)

  // Check that the callback was called
  t.truthy(returnedContext)
  t.is(returnedContext.wallace, 'grommet')
  t.truthy(status)
  t.is(status, STEP_SUCCESS)
  t.truthy(transactionOutput)
  t.is(transactionOutput.whoopee, 'doo')
  t.is(transactionOutput.description, 'ping2 - Scheduler2.startTransaction() returning without processing step')

  if (endTime !== 0) {
    const elapsed = endTime - startTime
    // console.log(`ping2 completed in ${elapsed}ms`)
  }
  // await scheduler.destroy()
})
