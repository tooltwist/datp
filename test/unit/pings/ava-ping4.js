import test from 'ava'
import Scheduler2 from '../../../ATP/Scheduler2/Scheduler2'
import CallbackRegister from '../../../ATP/Scheduler2/CallbackRegister'
import pause from '../../../lib/pause'
import PipelineStep from '../../../ATP/hardcoded-steps/PipelineStep'
import ExampleStep from '../../../ATP/hardcoded-steps/ExampleStep'
import RandomDelayStep from '../../../ATP/hardcoded-steps/RandomDelayStep'

/*
 *  We need to use a different node name for each test file, as they run in different
 *  processes (with different CallbackRegisters). If multiple test files have the same nodeId
 *  then they draw from the same queue, but the worker might not know the callback handler.
 */
const OWNER = 'fred'
const NODE_ID = 'ping4'


// https://github.com/avajs/ava/blob/master/docs/01-writing-tests.md
test.beforeEach(async t => {
  await PipelineStep.register()
  await ExampleStep.register()
  await RandomDelayStep.register()
})


test.serial('Call ping4 test transaction', async t => {
  let returnedContext = null
  let status = null
  let transactionOutput = null
  const startTime = Date.now()
  let endTime = 0

  // Define a callback
  const handlerName = `test-ping4-callback-${Math.random()}`
  await CallbackRegister.register(handlerName, (context, data) => {
    // console.log(`Test harness ping4 callback:`, context, data)
    returnedContext = context
    status = data.status
    transactionOutput = data.transactionOutput
    endTime = Date.now()
  })

  // Start the scheduler and give it time to work
  const scheduler = new Scheduler2(NODE_ID, null)
  await scheduler.drainQueue()
  await scheduler.start()

  // Start the test transaction
  await Scheduler2.startTransaction({
    metadata: {
      owner: OWNER,
      nodeId: NODE_ID,
      externalId: `extref-${Math.random()}`,
      transactionType: 'ping4',
      callback: handlerName,
      callbackContext: {
        glee: 'yippee!'
      }
    },
    data: {
      good: 'times'
    }
  })

  // await scheduler.dump()
  await pause(5000)
  await scheduler.stop()


  // Check that the callback was called
  t.truthy(returnedContext)
  t.is(returnedContext.glee, 'yippee!')
  t.truthy(status)
  t.is(status, 'success')
  t.truthy(transactionOutput)
  t.is(transactionOutput.foo, 'bar')

  if (endTime !== 0) {
    const elapsed = endTime - startTime
    console.log(`ping4 completed in ${elapsed}ms`)
  }
  await scheduler.destroy()
})

