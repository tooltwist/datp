import test from 'ava'
import Scheduler2 from '../../../ATP/Scheduler2/Scheduler2'
import CallbackRegister from '../../../ATP/Scheduler2/CallbackRegister'
import pause from '../../../lib/pause'
import PipelineStep from '../../../ATP/hardcoded-steps/PipelineStep'
import ExampleStep from '../../../ATP/hardcoded-steps/ExampleStep'
import RandomDelayStep from '../../../ATP/hardcoded-steps/RandomDelayStep'
import { STEP_ABORTED } from '../../../ATP/Step'
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
  await ExampleStep.register()
  await RandomDelayStep.register()

  await prepareForUnitTesting()
})


test.serial('Run a sucessful transaction', async t => {
  let returnedContext = null
  let status = null
  let transactionOutput = null
  const startTime = Date.now()
  let endTime = 0

  // Define a callback
  const handlerName = `test-${NODE_GROUP}-callback-${Math.random()}`
  await CallbackRegister.register(handlerName, (context, data) => {
    // console.log(`Test harness ping4 callback:`, context, data)
    returnedContext = context
    status = data.status
    transactionOutput = data.transactionOutput
    endTime = Date.now()
  })

  await schedulerForThisNode.drainQueue()

  // Start the test transaction
  await schedulerForThisNode.startTransaction({
    metadata: {
      owner: OWNER,
      nodeGroup: NODE_GROUP,
      externalId: `extref-${Math.random()}`,
      transactionType: 'ping4',
      onComplete: {
        callback: handlerName,
        context: {
          glee: 'yippee!'
        }
      }
    },
    data: {
      instruction: 'abort'
    }
  })

  // await scheduler.dump()
  await pause(500)
  // await scheduler.stop()


  // Check that the callback was called
  t.truthy(returnedContext)
  t.is(returnedContext.glee, 'yippee!')
  t.truthy(status)
  t.is(status, STEP_ABORTED)
  t.truthy(transactionOutput)
  t.is(transactionOutput.foo, 'bar')
  t.is(transactionOutput.alert, 'Catastrophic failure!')

  if (endTime !== 0) {
    const elapsed = endTime - startTime
    // console.log(`ping4 completed in ${elapsed}ms`)
  }
  // await scheduler.destroy()
})

