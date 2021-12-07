import test from 'ava'
// import createTestTransaction from '../helpers/createTestTransaction'
// import query from '../../../database/query'
// import TransactionIndexEntry from '../../../ATP/TransactionIndexEntry'
// import TransactionPersistance from '../../../ATP/Scheduler2/TransactionPersistance'
// import TransactionCache from '../../../ATP/Scheduler2/TransactionCache'
import { EVENT, SHUTDOWN, TICK, TICK_INTERVAL, TX_START } from '../../../ATP/Scheduler2/Scheduler2'
import CallbackRegister from '../../../ATP/Scheduler2/CallbackRegister'
import pause from '../../../lib/pause'


// https://github.com/avajs/ava/blob/master/docs/01-writing-tests.md
test.beforeEach(async t => {
})


/*
 *  Check the code is valid
 */
// test.serial('Exists and has unit tests', async t => {
//   t.truthy(TransactionPersistance)
// });

const OWNER = 'fred'
const NODE_ID = 'node1'

/*
 *  Tests
 */
test.serial('Call ping1 test transaction', async t => {
  let complete = false

  // Define a callback
  const handlerName = `test-callback-${Math.random()}`
  await CallbackRegister.register(handlerName, (data) => {
    console.log(`Test harness ping1 callback:`, data)
    complete = true
  })

  // Start the test transaction
  EVENT(TX_START, {
    metadata: {
      owner: OWNER,
      nodeId: NODE_ID,
      externalId: `extref-${Math.random()}`,
      transactionType: 'ping1',
      callback: handlerName,
      callbackContext: {
        ping: 'yipee!'
      }
    },
    data: {
    }
  })

  // Start the scheduler and give it time to work
  await TICK()
  await pause(TICK_INTERVAL * 2)
  await SHUTDOWN()

  // Check that the callback was called
  t.truthy(complete)
})



test.serial('Call ping2 test transaction', async t => {
  let complete = false

  // Define a callback
  const handlerName = `test-callback-${Math.random()}`
  await CallbackRegister.register(handlerName, (data) => {
    console.log(`Test harness ping2 callback:`, data)
    complete = true
  })

  // Start the test transaction
  EVENT(TX_START, {
    metadata: {
      owner: OWNER,
      nodeId: NODE_ID,
      externalId: `extref-${Math.random()}`,
      transactionType: 'ping2',
      callback: handlerName,
      callbackContext: {
        yarp: 'whammo'
      }
    },
    data: {
    }
  })

  // Start the scheduler and give it time to work
  await TICK()
  await pause(TICK_INTERVAL * 2)
  await SHUTDOWN()

  // Check that the callback was called
  t.truthy(complete)
})




test.serial('Call ping3 test transaction', async t => {
  let complete = false

  // Define a callback
  const startTime = Date.now()
  let endTime

  const handlerName = `test-callback-${Math.random()}`
  await CallbackRegister.register(handlerName, (data) => {
    console.log(`Test harness ping3 callback:`, data)
    complete = true
    endTime = Date.now()
  })

  // Start the test transaction
  EVENT(TX_START, {
    metadata: {
      owner: OWNER,
      nodeId: NODE_ID,
      externalId: `extref-${Math.random()}`,
      transactionType: 'ping3',
      callback: handlerName,
      callbackContext: {
        yarp: 'whammo'
      }
    },
    data: {
      some: 'stuff'
    }
  })

  // Start the scheduler and give it time to work
  await TICK()
  await pause(TICK_INTERVAL * 6)
  await SHUTDOWN()

  // Check that the callback was called
  t.truthy(complete)

  if (complete) {
    const elapsed = endTime - startTime
    console.log(`ping3 completed in ${elapsed}ms`)
  }
})

