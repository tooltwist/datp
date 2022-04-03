/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import test from 'ava'
import TransactionCache from '../../../ATP/Scheduler2/TransactionCache'
import { STEP_FAILED, STEP_QUEUED, STEP_RUNNING, STEP_SLEEPING, STEP_SUCCESS } from '../../../ATP/Step'

const OWNER = 'fred'
const TRANSACTION_TYPE = 'example'

// https://github.com/avajs/ava/blob/master/docs/01-writing-tests.md
test.before(async t => {
})

test.serial.skip('Missing await for tx.delta()', async t => {

  await t.throwsAsync(async() => {
    // Create the transaction with an external ID
    const num = Math.round(Math.random() * 100000000000)
    const externalId = `e-${num}`
    const tx = await TransactionCache.newTransaction(OWNER, externalId, TRANSACTION_TYPE)

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
