import test from 'ava'
import TransactionCache from '../../../ATP/Scheduler2/TransactionCache'
import { STEP_FAILED, STEP_QUEUED, STEP_RUNNING, STEP_SLEEPING, STEP_SUCCESS } from '../../../ATP/Step'

const OWNER = 'fred'

// https://github.com/avajs/ava/blob/master/docs/01-writing-tests.md
test.beforeEach(async t => {
})

test.serial.skip('Missing await for tx.delta()', async t => {

  await t.throwsAsync(async() => {
    // Create the transaction with an external ID
    const num = Math.round(Math.random() * 100000000000)
    const externalId = `e-${num}`
    const tx = await TransactionCache.newTransaction(OWNER, externalId)

    // Start one before the previous has finished
    const p1 = tx.delta(null, { status: STEP_QUEUED })
    const p2 = tx.delta(null, { status: STEP_RUNNING })
    // const p3 = tx.delta(null, { status: STEP_SUCCESS })
    // const p4 = tx.delta(null, { status: STEP_FAILED })
    // const p5 = tx.delta(null, { status: STEP_QUEUED })
    // const p6 = tx.delta(null, { status: STEP_QUEUED })
    await p1
    await p2
    // await p3
    // await p4
    // await p5
    // await p6
  }, { instanceOf: Error, message: `delta() was called again before it completed. Missing 'await'?`})
})
