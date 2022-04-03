/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import test from 'ava'
import Transaction from '../../../ATP/Scheduler2/Transaction'
import TransactionCache from '../../../ATP/Scheduler2/TransactionCache'

const OWNER = 'fred'
const TRANSACTION_TYPE = 'example'


// https://github.com/avajs/ava/blob/master/docs/01-writing-tests.md
test.before(async t => { })


test.serial('Selecting switches', async t => {

  // Create the transaction with an external ID
  const num = Math.round(Math.random() * 100000000000)
  const externalId = `e-${num}`
  const tx = await TransactionCache.newTransaction(OWNER, externalId, TRANSACTION_TYPE)
  const txId = tx.getTxId()
  // console.log(`txId=`, txId)

  const { switches, sequenceOfUpdate } = await Transaction.getSwitches(OWNER, txId)
  // console.log(`switches=`, switches)
  // console.log(`sequenceOfUpdate=`, sequenceOfUpdate)

  t.is(typeof(switches), 'object')
  t.is(Object.keys(switches).length, 0)
  t.is(sequenceOfUpdate, 0)
})


test.serial('Set a boolean switch to true', async t => {

  // Create the transaction with an external ID
  const num = Math.round(Math.random() * 100000000000)
  const externalId = `e-${num}`
  const tx = await TransactionCache.newTransaction(OWNER, externalId, TRANSACTION_TYPE)
  const txId = tx.getTxId()
  // console.log(`txId=`, txId)

  // Check the initial switches (i.e. none)
  {
    const { switches, sequenceOfUpdate } = await Transaction.getSwitches(OWNER, txId)
    t.is(typeof(switches), 'object')
    t.is(Object.keys(switches).length, 0)
    t.is(sequenceOfUpdate, 0)
  }

  // Set the switch
  await Transaction.setSwitch(OWNER, txId, 'power-on', true)

  // Check the initial switches (i.e. none)
  {
    const { switches, sequenceOfUpdate } = await Transaction.getSwitches(OWNER, txId)
    t.is(typeof(switches), 'object')
    t.is(Object.keys(switches).length, 1)
    t.is(sequenceOfUpdate, 1)
    t.is(switches['power-on'], true)
  }
})


test.serial('Set a boolean switch to off', async t => {

  // Create the transaction with an external ID
  const num = Math.round(Math.random() * 100000000000)
  const externalId = `e-${num}`
  const tx = await TransactionCache.newTransaction(OWNER, externalId, TRANSACTION_TYPE)
  const txId = tx.getTxId()
  // console.log(`txId=`, txId)

  // Check the initial switches (i.e. none)
  {
    const { switches, sequenceOfUpdate } = await Transaction.getSwitches(OWNER, txId)
    t.is(typeof(switches), 'object')
    t.is(Object.keys(switches).length, 0)
    t.is(sequenceOfUpdate, 0)
  }

  // Set the switch
  await Transaction.setSwitch(OWNER, txId, 'dogsHaveBeaks', false)

  // Check the initial switches (i.e. none)
  const { switches, sequenceOfUpdate } = await Transaction.getSwitches(OWNER, txId)
  t.is(typeof(switches), 'object')
  t.is(Object.keys(switches).length, 1)
  t.is(sequenceOfUpdate, 1)
  t.is(switches.dogsHaveBeaks, false)
})


test.serial('Set a string switch', async t => {

  // Create the transaction with an external ID
  const num = Math.round(Math.random() * 100000000000)
  const externalId = `e-${num}`
  const tx = await TransactionCache.newTransaction(OWNER, externalId, TRANSACTION_TYPE)
  const txId = tx.getTxId()
  // console.log(`txId=`, txId)

  // Check the initial switches (i.e. none)
  {
    const { switches, sequenceOfUpdate } = await Transaction.getSwitches(OWNER, txId)
    t.is(typeof(switches), 'object')
    t.is(Object.keys(switches).length, 0)
    t.is(sequenceOfUpdate, 0)
  }

  // Set the switch
  await Transaction.setSwitch(OWNER, txId, 'name', 'Eugene')

  // Check the initial switches (i.e. none)
  {
    const { switches, sequenceOfUpdate } = await Transaction.getSwitches(OWNER, txId)
    t.is(typeof(switches), 'object')
    t.is(Object.keys(switches).length, 1)
    t.is(sequenceOfUpdate, 1)
    t.is(switches.name, 'Eugene')
  }

  // Set the switch again
  await Transaction.setSwitch(OWNER, txId, 'name', 'Norris')

  // Check the initial switches (i.e. none)
  {
    const { switches, sequenceOfUpdate } = await Transaction.getSwitches(OWNER, txId)
    t.is(typeof(switches), 'object')
    t.is(Object.keys(switches).length, 1)
    t.is(sequenceOfUpdate, 2)
    t.is(switches.name, 'Norris')
  }
})


test.serial('Set multiple switches', async t => {

  // Create the transaction with an external ID
  const num = Math.round(Math.random() * 100000000000)
  const externalId = `e-${num}`
  const tx = await TransactionCache.newTransaction(OWNER, externalId, TRANSACTION_TYPE)
  const txId = tx.getTxId()
  // console.log(`txId=`, txId)

  // Check the initial switches (i.e. none)
  {
    const { switches, sequenceOfUpdate } = await Transaction.getSwitches(OWNER, txId)
    t.is(typeof(switches), 'object')
    t.is(Object.keys(switches).length, 0)
    t.is(sequenceOfUpdate, 0)
  }

  // Set the switch
  {
    await Transaction.setSwitch(OWNER, txId, 'number.of.feet', 3)
    await Transaction.setSwitch(OWNER, txId, 'name', 'Harold')
    await Transaction.setSwitch(OWNER, txId, 'isSunday', false)
    await Transaction.setSwitch(OWNER, txId, 'daytime', true)
    const { switches, sequenceOfUpdate } = await Transaction.getSwitches(OWNER, txId)
    t.is(typeof(switches), 'object')
    t.is(Object.keys(switches).length, 4)
    t.is(sequenceOfUpdate, 4)
    t.is(switches['number.of.feet'], 3)
    t.is(switches['name'], 'Harold')
    t.is(switches['isSunday'], false)
    t.is(switches['daytime'], true)
  }

  // Set the switch again
  {
    await Transaction.setSwitch(OWNER, txId, 'number.of.feet', 5)
    const { switches, sequenceOfUpdate } = await Transaction.getSwitches(OWNER, txId)
    t.is(typeof(switches), 'object')
    t.is(Object.keys(switches).length, 4)
    t.is(sequenceOfUpdate, 5)
    t.is(switches['number.of.feet'], 5)
    t.is(switches['name'], 'Harold')
    t.is(switches['isSunday'], false)
    t.is(switches['daytime'], true)
  }
})


test.serial('Set switch to invalid type', async t => {

  // Create the transaction with an external ID
  const num = Math.round(Math.random() * 100000000000)
  const externalId = `e-${num}`
  const tx = await TransactionCache.newTransaction(OWNER, externalId, TRANSACTION_TYPE)
  const txId = tx.getTxId()
  // console.log(`txId=`, txId)

  // Check the initial switches (i.e. none)
  {
    const { switches, sequenceOfUpdate } = await Transaction.getSwitches(OWNER, txId)
    t.is(typeof(switches), 'object')
    t.is(Object.keys(switches).length, 0)
    t.is(sequenceOfUpdate, 0)
  }

  // Set the switch
  await t.throwsAsync(async() => {
    await Transaction.setSwitch(OWNER, txId, 'details', { abc: 123 })
  }, { instanceOf: Error, message: 'Switch can only be boolean, number, or string (< 32 chars)'})
})



test.serial('Set switch with excessive length string value', async t => {

  // Create the transaction with an external ID
  const num = Math.round(Math.random() * 100000000000)
  const externalId = `e-${num}`
  const tx = await TransactionCache.newTransaction(OWNER, externalId, TRANSACTION_TYPE)
  const txId = tx.getTxId()
  // console.log(`txId=`, txId)

  // Check the initial switches (i.e. none)
  {
    const { switches, sequenceOfUpdate } = await Transaction.getSwitches(OWNER, txId)
    t.is(typeof(switches), 'object')
    t.is(Object.keys(switches).length, 0)
    t.is(sequenceOfUpdate, 0)
  }

  // Set the switch
  await t.throwsAsync(async() => {
    await Transaction.setSwitch(OWNER, txId, 'name', 'Wallace Jonathan Hinklebottom-Smythe the second')
  }, { instanceOf: Error, message: 'Switch exceeds maximum length (name: Wallace Jonathan Hinklebottom-Smythe the second)'})
})



test.serial('Delete switches', async t => {

  // Create the transaction with an external ID
  const num = Math.round(Math.random() * 100000000000)
  const externalId = `e-${num}`
  const tx = await TransactionCache.newTransaction(OWNER, externalId, TRANSACTION_TYPE)
  const txId = tx.getTxId()
  // console.log(`txId=`, txId)

  // Check the initial switches (i.e. none)
  {
    const { switches, sequenceOfUpdate } = await Transaction.getSwitches(OWNER, txId)
    t.is(typeof(switches), 'object')
    t.is(Object.keys(switches).length, 0)
    t.is(sequenceOfUpdate, 0)
  }

  // Set the switch
  {
    await Transaction.setSwitch(OWNER, txId, 'number.of.feet', 3)
    await Transaction.setSwitch(OWNER, txId, 'name', 'Harold')
    await Transaction.setSwitch(OWNER, txId, 'isSunday', false)
    await Transaction.setSwitch(OWNER, txId, 'daytime', true)
    const { switches, sequenceOfUpdate } = await Transaction.getSwitches(OWNER, txId)
    t.is(typeof(switches), 'object')
    t.is(Object.keys(switches).length, 4)
    t.is(sequenceOfUpdate, 4)
    t.is(switches['number.of.feet'], 3)
    t.is(switches['name'], 'Harold')
    t.is(switches['isSunday'], false)
    t.is(switches['daytime'], true)
  }

  // Delete switch using null value
  {
    await Transaction.setSwitch(OWNER, txId, 'isSunday', null)
    const { switches, sequenceOfUpdate } = await Transaction.getSwitches(OWNER, txId)
    t.is(typeof(switches), 'object')
    t.is(Object.keys(switches).length, 3)
    t.is(sequenceOfUpdate, 5)
    t.is(switches['number.of.feet'], 3)
    t.is(switches['name'], 'Harold')
    t.is(typeof(switches['isSunday']), 'undefined')
    t.is(switches['daytime'], true)
  }

  // Delete switch using undefined
  {
    await Transaction.setSwitch(OWNER, txId, 'daytime') // Missing value
    const { switches, sequenceOfUpdate } = await Transaction.getSwitches(OWNER, txId)
    t.is(typeof(switches), 'object')
    t.is(Object.keys(switches).length, 2)
    t.is(sequenceOfUpdate, 6)
    t.is(switches['number.of.feet'], 3)
    t.is(switches['name'], 'Harold')
    t.is(typeof(switches['isSunday']), 'undefined')
    t.is(typeof(switches['daytime']), 'undefined')
  }
})
