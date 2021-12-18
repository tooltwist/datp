import test from 'ava'
import Scheduler2 from '../../../ATP/Scheduler2/Scheduler2'
import CallbackRegister from '../../../ATP/Scheduler2/CallbackRegister'
import pause from '../../../lib/pause'
import { STEP_SUCCESS } from '../../../ATP/Step'

/*
 *  We need to use a different node name for each test file, as they run in different
 *  processes (with different CallbackRegisters). If multiple test files have the same nodeId
 *  then they draw from the same queue, but the worker might not know the callback handler.
 */
const OWNER = 'fred'
const NODE_ID = 'ping1'


// https://github.com/avajs/ava/blob/master/docs/01-writing-tests.md
test.beforeEach(async t => { })


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
      transactionType: 'ping1',
      callback: handlerName,
      callbackContext: {
        ping: 'yippee!'
      }
    },
    data: {
    }
  })

  // await scheduler.dump()
  await pause(500)
  await scheduler.stop()

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
    console.log(`ping1 completed in ${elapsed}ms`)
  }
  await scheduler.destroy()
})

