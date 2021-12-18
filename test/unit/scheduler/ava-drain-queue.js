import test from 'ava'
import Scheduler2 from '../../../ATP/Scheduler2/Scheduler2'
import CallbackRegister from '../../../ATP/Scheduler2/CallbackRegister'

/*
 *  We need to use a different node name for each test file, as they run in different
 *  processes (with different CallbackRegisters). If multiple test files have the same nodeId
 *  then they draw from the same queue, but the worker might not know the callback handler.
 */
const OWNER = 'fred'
const NODE_ID = 'drain-queue'


// https://github.com/avajs/ava/blob/master/docs/01-writing-tests.md
test.beforeEach(async t => { })

/*
 *  This test doesn't actually test anything specific, but checks that the
 *  scheduler actually stops, and doesn't run in the background and stop
 *  the tests from existing when complete.
 */
test.serial('Drain a queue (DANGEROUS OPERATION!!!)', async t => {

  const handlerName = `test-callback-drain-${Math.random()}`
  await CallbackRegister.register(handlerName, (data) => { })


  const addToQueue = async () => {
    await Scheduler2.startTransaction({
      metadata: {
        owner: OWNER,
        nodeId: NODE_ID,
        externalId: `extref-${Math.random()}`,
        transactionType: 'ping3',
        callback: handlerName,
        callbackContext: { }
      },
      data: {
      }
    })
  }//- addToQueue

  // Start the scheduler and give it time to work
  const scheduler = new Scheduler2(NODE_ID, null)

  await scheduler.drainQueue()
  t.is(await scheduler.queueLength(), 0)
  await addToQueue()
  t.is(await scheduler.queueLength(), 1)
  await addToQueue()
  await addToQueue()
  await addToQueue()
  await addToQueue()
  t.is(await scheduler.queueLength(), 5)

  await scheduler.drainQueue()
  t.is(await scheduler.queueLength(), 0)
  await addToQueue()
  await addToQueue()
  t.is(await scheduler.queueLength(), 2)
  await scheduler.drainQueue()

  // // Let the workers drain the remainder
  // await scheduler.start()
  // await pause(500)
  // t.is(await scheduler.queueLength(), 0)
  // await scheduler.stop()
  // await scheduler.destroy()
})
