import test from 'ava'
import CallbackRegister from '../../../ATP/Scheduler2/CallbackRegister'
import pause from '../../../lib/pause'
import PipelineStep from '../../../ATP/hardcoded-steps/PipelineStep'
import ExampleStep from '../../../ATP/hardcoded-steps/ExampleStep'
import RandomDelayStep from '../../../ATP/hardcoded-steps/RandomDelayStep'
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
  await PipelineStep.register()
  await ExampleStep.register()
  await RandomDelayStep.register()
  await prepareForUnitTesting()
})


test.serial('Call ping4 test transaction', async t => {
  // let returnedContext = null
  // let status = null
  // let transactionOutput = null
  // const startTime = Date.now()
  // let endTime = 0
  const externalId = `extref-${Math.random()}`

  // Define a callback
  const handlerName = `test-txdetails-callback-${Math.random()}`
  await CallbackRegister.register(handlerName, (context, data) => {
    // console.log(`Test harness ping4 callback:`, context, data)
    // returnedContext = context
    // status = data.status
    // transactionOutput = data.transactionOutput
    // endTime = Date.now()
  })

  await schedulerForThisNode.drainQueue()

  // Start the test transaction
  const tx = await schedulerForThisNode.startTransaction({
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
      }
    },
    data: {
      good: 'times'
    }
  })

  // await scheduler.dump()
  // Give it time to complete
  await pause(3000)
  // await scheduler.stop()


  // Okay, we've finished running, now check we can get the details
  const txId = tx.getTxId()
  const withSteps = true
  let details = await tx.getDetails(withSteps)
  // console.log(`details=`, details)

  // console.log(`details=`, details)
  t.is(details.txId, txId)
  t.is(details.owner, OWNER)
  t.is(details.externalId, externalId)
  t.is(details.status, STEP_SUCCESS)
  t.is(details.sequenceOfUpdate, 19)
  t.is(JSON.stringify(details.progressReport), '{}')
  t.is(details.transactionOutput.good, 'times')
  t.is(details.transactionOutput.foo, 'bar')
  t.is(details.completionTime, null)

  // Check the steps
  t.is(details.steps.length, 3)

  // The pipeline will be first
  // console.log(`details.steps[0]=`, details.steps[0])
  t.is(details.steps[0].isPipeline, true)
  t.is(details.steps[0].parentStepId, '')
  t.is(details.steps[0].pipelineName, 'util.ping4')
  t.is(details.steps[0].status, 'success')
  t.is(typeof(details.steps[0].stepInput), 'object')
  t.is(typeof(details.steps[0].stepOutput), 'object')
  t.is(details.steps[0].children.length, 2)

  // First child
  t.is(details.steps[1].isPipeline, false)
  t.is(details.steps[1].parentStepId, details.steps[0].stepId)
  t.is(details.steps[1].status, 'success')
  t.is(typeof(details.steps[1].stepInput), 'object')
  t.is(typeof(details.steps[1].stepOutput), 'object')

  // Second child
  t.is(details.steps[2].isPipeline, false)
  t.is(details.steps[2].parentStepId, details.steps[0].stepId)
  t.is(details.steps[2].status, 'success')
  t.is(typeof(details.steps[2].stepInput), 'object')
  t.is(typeof(details.steps[2].stepOutput), 'object')




  // if (endTime !== 0) {
  //   const elapsed = endTime - startTime
  //   console.log(`ping4 completed in ${elapsed}ms`)
  // }
  // await scheduler.destroy()
})

