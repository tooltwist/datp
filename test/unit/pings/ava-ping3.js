/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import test from 'ava'
import CallbackRegister from '../../../ATP/Scheduler2/CallbackRegister'
import PipelineStep from '../../../ATP/hardcoded-steps/PipelineStep'
import pause from '../../../lib/pause'
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
  await PipelineStep.register()
  await prepareForUnitTesting()
})



test.serial('Call ping3 test transaction', async t => {
  let returnedContext = null
  let status = null
  let transactionOutput = null
  const startTime = Date.now()
  let endTime = 0

  // Define a callback
  const handlerName = `test-callback-${NODE_GROUP}-a-${Math.random()}`
  await CallbackRegister.register(handlerName, (context, data) => {
    // console.log(`Test harness ping3 callback:`, context, data)
    returnedContext = context
    status = data.status
    transactionOutput = data.transactionOutput
    endTime = Date.now()
  })

  // Prepare the scheduler and ensure the queue is empty
  await schedulerForThisNode.drainQueue()

  // Start the test transaction
  await schedulerForThisNode.startTransaction({
    metadata: {
      owner: OWNER,
      nodeGroup: NODE_GROUP,
      externalId: `extref-${Math.random()}`,
      transactionType: 'ping3',
      onComplete: {
        callback: handlerName,
        context: {
          yarp: 'whammo'
        }
      },
      traceLevel: 0
    },
    data: {
      some: 'stuff'
    }
  })

  await pause(500)

  // Check that the callback was called
  t.truthy(returnedContext)
  t.is(returnedContext.yarp, 'whammo')
  t.truthy(status)
  t.is(status, 'success')
  t.truthy(transactionOutput)
  t.is(transactionOutput.happy, 'dayz')
  t.is(transactionOutput.description, 'processEvent_StepStart() - util.ping3 - returning via STEP_COMPLETED, without processing step')

  if (endTime !== 0) {
    const elapsed = endTime - startTime
    // console.log(`ping3 completed in ${elapsed}ms`)
  }
})
