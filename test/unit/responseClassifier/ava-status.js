/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import test from 'ava'
import { responseClassifier, RESPONSE_CLASSIFICATION_ERROR, RESPONSE_CLASSIFICATION_OFFLINE, RESPONSE_CLASSIFICATION_OK, RESPONSE_RULE_STATUS, RESPONSE_RULE_TIMEOUT } from '../../../lib/responseClassifier'


test.serial('Default for status 200 = ok', async t => {
  const rules = [
    // { type: RESPONSE_RULE_STATUS, value: 301, classification: RESPONSE_CLASSIFICATION_OK }
  ]
  const reply = await responseClassifier(rules, false, 200, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_OK)
  t.is(reply.subClassification, null)
  t.is(reply.status, 200)
  t.is(reply.data.hello, 'there')
})

test.serial('Default for status 300 = ok', async t => {
  const rules = [
    // { type: RESPONSE_RULE_STATUS, value: 301, classification: RESPONSE_CLASSIFICATION_OK }
  ]
  const reply = await responseClassifier(rules, false, 300, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_OK)
  t.is(reply.subClassification, null)
  t.is(reply.status, 300)
  t.is(reply.data.hello, 'there')
})

test.serial('Default for status 400 = error', async t => {
  const rules = [
    // { type: RESPONSE_RULE_STATUS, value: 301, classification: RESPONSE_CLASSIFICATION_OK }
  ]
  const reply = await responseClassifier(rules, false, 400, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_ERROR)
  t.is(reply.subClassification, null)
  t.is(reply.status, 400)
  t.is(reply.data.hello, 'there')
})

test.serial('Default for status 500 = error', async t => {
  const rules = [
    // { type: RESPONSE_RULE_STATUS, value: 301, classification: RESPONSE_CLASSIFICATION_OK }
  ]
  const reply = await responseClassifier(rules, false, 500, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_ERROR)
  t.is(reply.subClassification, null)
  t.is(reply.status, 500)
  t.is(reply.data.hello, 'there')
})

test.serial('Default for status 590', async t => {
  const rules = [
    // { type: RESPONSE_RULE_STATUS, value: 301, classification: RESPONSE_CLASSIFICATION_OK }
  ]
  const reply = await responseClassifier(rules, false, 590, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_ERROR)
  t.is(reply.subClassification, null)
  t.is(reply.status, 590)
  t.is(reply.data.hello, 'there')
})

test.serial('Override status 200 => error', async t => {
  const rules = [
    { type: RESPONSE_RULE_STATUS, value: 200, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 200, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_ERROR)
  t.is(reply.subClassification, null)
  t.is(reply.status, 200)
  t.is(reply.data.hello, 'there')
})

test.serial('Rule for status 301 => ok', async t => {
  const rules = [
    { type: RESPONSE_RULE_STATUS, value: 301, classification: RESPONSE_CLASSIFICATION_OK }
  ]
  const reply = await responseClassifier(rules, false, 301, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_OK)
  t.is(reply.subClassification, null)
  t.is(reply.status, 301)
  t.is(reply.data.hello, 'there')
})


test.serial('Rule for status 301 => error', async t => {
  const rules = [
    { type: RESPONSE_RULE_STATUS, value: 301, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 301, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_ERROR)
  t.is(reply.subClassification, null)
  t.is(reply.status, 301)
  t.is(reply.data.hello, 'there')
})


test.serial('Multiple rules, #1 is error', async t => {
  const rules = [
    { type: RESPONSE_RULE_STATUS, value: 301, classification: RESPONSE_CLASSIFICATION_ERROR },
    { type: RESPONSE_RULE_STATUS, value: 302, classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_STATUS, value: 303, classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_STATUS, value: 304, classification: RESPONSE_CLASSIFICATION_OK },
  ]
  const reply = await responseClassifier(rules, false, 301, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_ERROR)
  t.is(reply.subClassification, null)
  t.is(reply.status, 301)
  t.is(reply.data.hello, 'there')
})


test.serial('Multiple rules, #1 is not an error', async t => {
  const rules = [
    { type: RESPONSE_RULE_STATUS, value: 301, classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_STATUS, value: 302, classification: RESPONSE_CLASSIFICATION_ERROR },
    { type: RESPONSE_RULE_STATUS, value: 303, classification: RESPONSE_CLASSIFICATION_ERROR },
    { type: RESPONSE_RULE_STATUS, value: 304, classification: RESPONSE_CLASSIFICATION_ERROR },
  ]
  const reply = await responseClassifier(rules, false, 301, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_OK)
  t.is(reply.subClassification, null)
  t.is(reply.status, 301)
  t.is(reply.data.hello, 'there')
})


test.serial('Multiple rules, #2 is error', async t => {
  const rules = [
    { type: RESPONSE_RULE_STATUS, value: 301, classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_STATUS, value: 302, classification: RESPONSE_CLASSIFICATION_ERROR },
    { type: RESPONSE_RULE_STATUS, value: 303, classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_STATUS, value: 304, classification: RESPONSE_CLASSIFICATION_OK },
  ]
  const reply = await responseClassifier(rules, false, 302, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_ERROR)
  t.is(reply.subClassification, null)
  t.is(reply.status, 302)
  t.is(reply.data.hello, 'there')
})

test.serial('Multiple rules, #2 is not an error', async t => {
  const rules = [
    { type: RESPONSE_RULE_STATUS, value: 301, classification: RESPONSE_CLASSIFICATION_ERROR },
    { type: RESPONSE_RULE_STATUS, value: 302, classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_STATUS, value: 303, classification: RESPONSE_CLASSIFICATION_ERROR },
    { type: RESPONSE_RULE_STATUS, value: 304, classification: RESPONSE_CLASSIFICATION_ERROR },
  ]
  const reply = await responseClassifier(rules, false, 302, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_OK)
  t.is(reply.subClassification, null)
  t.is(reply.status, 302)
  t.is(reply.data.hello, 'there')
})

test.serial('Multiple rules, #3 is error', async t => {
  const rules = [
    { type: RESPONSE_RULE_STATUS, value: 301, classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_STATUS, value: 302, classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_STATUS, value: 303, classification: RESPONSE_CLASSIFICATION_ERROR },
    { type: RESPONSE_RULE_STATUS, value: 304, classification: RESPONSE_CLASSIFICATION_OK },
  ]
  const reply = await responseClassifier(rules, false, 303, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_ERROR)
  t.is(reply.subClassification, null)
  t.is(reply.status, 303)
  t.is(reply.data.hello, 'there')
})


test.serial('Multiple rules, #3 is not an error', async t => {
  const rules = [
    { type: RESPONSE_RULE_STATUS, value: 301, classification: RESPONSE_CLASSIFICATION_ERROR },
    { type: RESPONSE_RULE_STATUS, value: 302, classification: RESPONSE_CLASSIFICATION_ERROR },
    { type: RESPONSE_RULE_STATUS, value: 303, classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_STATUS, value: 304, classification: RESPONSE_CLASSIFICATION_ERROR },
  ]
  const reply = await responseClassifier(rules, false, 303, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_OK)
  t.is(reply.subClassification, null)
  t.is(reply.status, 303)
  t.is(reply.data.hello, 'there')
})
