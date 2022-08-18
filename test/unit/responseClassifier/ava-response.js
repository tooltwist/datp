/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import test from 'ava'
import { responseClassifier, RESPONSE_CLASSIFICATION_ERROR, RESPONSE_CLASSIFICATION_OFFLINE, RESPONSE_CLASSIFICATION_OK, RESPONSE_RULE_DEFAULT, RESPONSE_RULE_TIMEOUT, RESPONSE_RULE_VALUE } from '../../../lib/responseClassifier'

const myResponse = {
  status: 'good',
  person: {
    age: 12,
    name: {
      first: 'Harry',
      middle: 'John',
      last: 'Bloggs'
    },
    address: {
      street: 'Wilson Avenue',
      zipcode: 1234
    },
  }
}


// https://github.com/avajs/ava/blob/master/docs/01-writing-tests.md
test.before(async t => {
})


test.serial('Simple field value matches', async t => {
  const rules = [
    { type: RESPONSE_RULE_VALUE, field: 'status', value: 'good', classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 200, myResponse)
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_OK)
})


test.serial('Simple field value does not match', async t => {
  const rules = [
    { type: RESPONSE_RULE_VALUE, field: 'status', value: 'wonderful', classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 200, myResponse)
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_ERROR)
})


test.serial('Simple field value does not exist', async t => {
  const rules = [
    { type: RESPONSE_RULE_VALUE, field: 'statusZZ', value: 'good', classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 200, myResponse)
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_ERROR)
})


test.serial('Nested field value matches', async t => {
  const rules = [
    { type: RESPONSE_RULE_VALUE, field: 'person.age', value: 12, classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 200, myResponse)
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_OK)
})

test.serial('Nested field value does not match', async t => {
  const rules = [
    { type: RESPONSE_RULE_VALUE, field: 'person.age', value: 34, classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 200, myResponse)
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_ERROR)
})

test.serial('Nested field value does not exist', async t => {
  const rules = [
    { type: RESPONSE_RULE_VALUE, field: 'person.shoeSize', value: 12, classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 200, myResponse)
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_ERROR)
})


test.serial('Deeper nested field value matches', async t => {
  const rules = [
    { type: RESPONSE_RULE_VALUE, field: 'person.name.first', value: 'Harry', classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 200, myResponse)
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_OK)
})

test.serial('Deeper nested field value does not match', async t => {
  const rules = [
    { type: RESPONSE_RULE_VALUE, field: 'person.name.first', value: 'Mary', classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 200, myResponse)
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_ERROR)
})

test.serial('Deeper nested field value does not exist', async t => {
  const rules = [
    { type: RESPONSE_RULE_VALUE, field: 'person.name.salutation', value: 'Mary', classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 200, myResponse)
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_ERROR)
})
