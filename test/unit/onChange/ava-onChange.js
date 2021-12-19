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
const NODE_GROUP = 'onChange'


// https://github.com/avajs/ava/blob/master/docs/01-writing-tests.md
test.beforeEach(async t => {
  await PipelineStep.register()
  await ExampleStep.register()
  await RandomDelayStep.register()
})


test.serial('Watch for changes on ping4 transaction', async t => {
  let completionContext = null
  let completionData = null
  let changes = [ ]
  let changeContext = null
  const externalId = `extref-${Math.random()}`


  // Define a completion callback
  const handlerName = `test-${NODE_GROUP}-callback-${Math.random()}`
  await CallbackRegister.register(handlerName, (context, data) => {
    // console.log(`Completion callback:`, context, data)
    completionContext = context
    completionData = data
  })
  // Define an onChange callback
  const changeHandlerName = `test-${NODE_GROUP}-change-callback-${Math.random()}`
  await CallbackRegister.register(changeHandlerName, (context, data) => {
    // console.log(`Change callback:`, context, data)
    changes.push(data)
    changeContext = context
  })

  // Start the scheduler and give it time to work
  const scheduler = new Scheduler2(NODE_GROUP, null)
  await scheduler.drainQueue()
  await scheduler.start()

  // Start the test transaction
  const myTx = await Scheduler2.startTransaction({
    metadata: {
      owner: OWNER,
      nodeGroup: NODE_GROUP,
      externalId,
      transactionType: 'ping4',
      onComplete: {
        callback: handlerName,
        context: {
          glee: 'yippee!'
        }
      },
      onChange: {
        callback: changeHandlerName,
        context: {
          very: 'cool'
        },
      }
    },
    data: {
      good: 'times'
    }
  })

  // await scheduler.dump()
  await pause(5000)
  await scheduler.stop()

  // Check that the onChange callback was called
  // console.log(`completionContext=`, completionContext)
  t.truthy(completionContext)
  t.is(completionContext.glee, 'yippee!')

  // console.log(`completionData=`, completionData)
  t.truthy(completionData)
  t.is(completionData.status, 'success')
  t.is(completionData.transactionOutput.good, 'times')
  t.is(completionData.transactionOutput.foo, 'bar')

  // console.log(`changeContext=`, changeContext)
  t.truthy(changeContext)
  t.is(changeContext.very, 'cool')

  // console.log(`changes=`, changes)
  t.is(changes.length, 1)
  t.is(changes[0].txId, myTx.getTxId())
  t.is(changes[0].owner, OWNER)
  t.is(changes[0].externalId, externalId)
  t.is(changes[0].status, 'success')
  t.is(changes[0].transactionInput.good, 'times')
  t.is(changes[0].transactionOutput.good, 'times')
  t.is(changes[0].transactionOutput.foo, 'bar')

  await scheduler.destroy()
})

