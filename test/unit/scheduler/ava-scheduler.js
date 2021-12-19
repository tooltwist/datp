import test from 'ava'
import Scheduler2 from '../../../ATP/Scheduler2/Scheduler2'
import CallbackRegister from '../../../ATP/Scheduler2/CallbackRegister'
import pause from '../../../lib/pause'

/*
 *  We need to use a different node name for each test file, as they run in different
 *  processes (with different CallbackRegisters). If multiple test files have the same nodeId
 *  then they draw from the same queue, but the worker might not know the callback handler.
 */
const OWNER = 'fred'
const NODE_GROUP = 'scheduler'


// https://github.com/avajs/ava/blob/master/docs/01-writing-tests.md
test.beforeEach(async t => { })


/*
 *  This test doesn't actually test anything specific, but checks that the
 *  scheduler actually stops, and doesn't run in the background and stop
 *  the tests from existing when complete.
 */
test.serial('Start and stop scheduler', async t => {

  // Start the scheduler and give it time to work
  const scheduler = new Scheduler2(NODE_GROUP, null)
  await scheduler.drainQueue()
  const status0 = await scheduler.getStatus()
  // console.log(`status0=`, status0)
  t.is(status0.events.waiting, 0)
  t.is(status0.events.processing, 0)
  t.is(status0.workers.total, 0)
  t.is(status0.workers.running, 0)
  t.is(status0.workers.waiting, 0)
  t.is(status0.workers.shuttingDown, 0)
  t.is(status0.workers.standby, 0)

  // await pause(2000)
  await scheduler.start()
  const status1 = await scheduler.getStatus()
  // console.log(`status1=`, status1)
  await pause(500)
  t.is(status1.events.waiting, 0)
  t.is(status1.events.processing, 0)
  t.is(status1.workers.total, 1)
  t.is(status1.workers.running, 0)
  t.is(status1.workers.waiting, 1)
  t.is(status1.workers.shuttingDown, 0)
  t.is(status1.workers.standby, 0)

  await scheduler.stop()
  await pause(500)
  const status2 = await scheduler.getStatus()
  // console.log(`status2=`, status2)
  t.is(status2.events.waiting, 0)
  t.is(status2.events.processing, 0)
  t.is(status2.workers.total, 1)
  t.is(status2.workers.running, 0)
  t.is(status2.workers.waiting, 0)
  t.is(status2.workers.shuttingDown, 0)
  t.is(status2.workers.standby, 1)

  await scheduler.destroy()
})


test.serial('Add event before starting scheduler', async t => {
  let complete = false

  // Define a callback
  const handlerName = `test-callback-${NODE_GROUP}-a-${Math.random()}`
  await CallbackRegister.register(handlerName, (data) => {
    // console.log(`Test harness ping1 callback:`, data)
    complete = true
  })

  // Start the scheduler and give it time to work
  const scheduler = new Scheduler2(NODE_GROUP, null)
  await scheduler.drainQueue()
  // await scheduler.dump()
  const status1 = await scheduler.getStatus()
  // console.log(`status1=`, status1)
  t.is(status1.events.waiting, 0)
  t.is(status1.events.processing, 0)
  t.is(status1.workers.total, 0)
  t.is(status1.workers.running, 0)
  t.is(status1.workers.waiting, 0)
  t.is(status1.workers.shuttingDown, 0)
  t.is(status1.workers.standby, 0)

  // Add the transaction event
  await Scheduler2.startTransaction({
    metadata: {
      owner: OWNER,
      nodeGroup: NODE_GROUP,
      externalId: `extref-${Math.random()}`,
      transactionType: 'ping3',
      onComplete: {
        callback: handlerName,
        context: {
          ping: 'yipee!'
        }
      }
    },
    data: {
    }
  })
  await pause(500)
  const status2 = await scheduler.getStatus()
  // console.log(`status2=`, status2)

  t.is(status2.events.waiting, 1)
  t.is(status2.events.processing, 0)
  t.is(status2.workers.total, 0)
  t.is(status2.workers.running, 0)
  t.is(status2.workers.waiting, 0)
  t.is(status2.workers.shuttingDown, 0)
  t.is(status2.workers.standby, 0)

  // Start the scheduler
  await scheduler.start()
  await pause(500)
  const status3 = await scheduler.getStatus()
  // console.log(`status3=`, status3)
  t.is(status3.events.waiting, 0)
  t.is(status3.events.processing, 0)
  t.is(status3.workers.total, 1)
  t.is(status3.workers.running, 0)
  t.is(status3.workers.waiting, 1)
  t.is(status3.workers.shuttingDown, 0)
  t.is(status3.workers.standby, 0)


  // Stop the scheduler
  // await scheduler.dump()
  await scheduler.stop()
  await pause(500)
  const status4 = await scheduler.getStatus()
  // console.log(`status4=`, status4)
  t.is(status4.events.waiting, 0)
  t.is(status4.events.processing, 0)
  t.is(status4.workers.total, 1)
  t.is(status4.workers.running, 0)
  t.is(status4.workers.waiting, 0)
  t.is(status4.workers.shuttingDown, 0)
  t.is(status4.workers.standby, 1)

  // Check that the callback was called
  t.truthy(complete)

  await scheduler.destroy()
})


test.serial('Start scheduler before first event', async t => {
  let complete = false

  // Define a callback
  const handlerName = `test-callback-${NODE_GROUP}-b-${Math.random()}`
  await CallbackRegister.register(handlerName, (data) => {
    // console.log(`Test harness ping1 callback:`, data)
    complete = true
  })

  // Get the Queuing initialized
  // await Queue2.checkRunning()

  // Start the scheduler and give it time to work
  const scheduler = new Scheduler2(NODE_GROUP, null)
  await scheduler.drainQueue()

  // await scheduler.dump()
  const status1 = await scheduler.getStatus()
  // console.log(`status1=`, status1)
  t.is(status1.events.waiting, 0)
  t.is(status1.events.processing, 0)
  t.is(status1.workers.total, 0)
  t.is(status1.workers.running, 0)
  t.is(status1.workers.waiting, 0)
  t.is(status1.workers.shuttingDown, 0)
  t.is(status1.workers.standby, 0)

  await scheduler.start()
  const status2 = await scheduler.getStatus()
  // console.log(`status2=`, status2)
  t.is(status2.events.waiting, 0)
  t.is(status2.events.processing, 0)
  t.is(status2.workers.total, 1)
  t.is(status2.workers.running, 0)
  t.is(status2.workers.waiting, 1)
  t.is(status2.workers.shuttingDown, 0)
  t.is(status2.workers.standby, 0)

  // Start the test transaction
  await Scheduler2.startTransaction({
    metadata: {
      owner: OWNER,
      nodeGroup: NODE_GROUP,
      externalId: `extref-${Math.random()}`,
      transactionType: 'ping3',
      onComplete: {
        callback: handlerName,
        context: {
          ping: 'yipee!'
        }
      }
    },
    data: {
    }
  })

  // Give it time to complete
  await pause(1000)
  const status3 = await scheduler.getStatus()
  // console.log(`status3=`, status3)
  t.is(status3.events.waiting, 0)
  t.is(status3.events.processing, 0)
  t.is(status3.workers.total, 1)
  t.is(status3.workers.running, 0)
  t.is(status3.workers.waiting, 1)
  t.is(status3.workers.shuttingDown, 0)
  t.is(status3.workers.standby, 0)


  // Stop the scheduler
  // await scheduler.dump()
  await scheduler.stop()
  await pause(500)
  const status4 = await scheduler.getStatus()
  // console.log(`status4=`, status4)
  // console.log(`status1=`, status1)
  t.is(status4.events.waiting, 0)
  t.is(status4.events.processing, 0)
  t.is(status4.workers.total, 1)
  t.is(status4.workers.running, 0)
  t.is(status4.workers.waiting, 0)
  t.is(status4.workers.shuttingDown, 0)
  t.is(status4.workers.standby, 1)

  // Check that the callback was called
  t.truthy(complete)

  await scheduler.destroy()
})
