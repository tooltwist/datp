import test from 'ava'
import Scheduler2 from '../../../ATP/Scheduler2/Scheduler2'
import CallbackRegister from '../../../ATP/Scheduler2/CallbackRegister'
import PipelineStep from '../../../ATP/hardcoded-steps/PipelineStep'
import pause from '../../../lib/pause'

/*
 *  We need to use a different node name for each test file, as they run in different
 *  processes (with different CallbackRegisters). If multiple test files have the same nodeId
 *  then they draw from the same queue, but the worker might not know the callback handler.
 */
const OWNER = 'fred'
const NODE_ID = 'ping3'


// https://github.com/avajs/ava/blob/master/docs/01-writing-tests.md
test.beforeEach(async t => {
  await PipelineStep.register()
})



test.serial('Call ping3 test transaction', async t => {
  let returnedContext = null
  let status = null
  let transactionOutput = null
  const startTime = Date.now()
  let endTime = 0

  // Define a callback
  const handlerName = `test-callback-ping3-a-${Math.random()}`
  await CallbackRegister.register(handlerName, (context, data) => {
    // console.log(`Test harness ping3 callback:`, context, data)
    returnedContext = context
    status = data.status
    transactionOutput = data.transactionOutput
    endTime = Date.now()
  })

  // Prepare the scheduler and ensure the queue is empty
  const scheduler = new Scheduler2(NODE_ID, null)
  await scheduler.drainQueue()

  // Start the test transaction
  await Scheduler2.startTransaction({
    metadata: {
      owner: OWNER,
      nodeId: NODE_ID,
      externalId: `extref-${Math.random()}`,
      transactionType: 'ping3',
      callback: handlerName,
      callbackContext: {
        yarp: 'whammo'
      },
      traceLevel: 0
    },
    data: {
      some: 'stuff'
    }
  })

  // Start the scheduler and give it time to work
  await scheduler.start()
  await pause(500)
  await scheduler.stop()

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
    console.log(`ping3 completed in ${elapsed}ms`)
  }
  await scheduler.destroy()
})

