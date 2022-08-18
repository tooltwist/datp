/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import test from 'ava'
import { responseClassifier, RESPONSE_CLASSIFICATION_ERROR, RESPONSE_CLASSIFICATION_OFFLINE, RESPONSE_CLASSIFICATION_OK, RESPONSE_RULE_DEFAULT, RESPONSE_RULE_STATUS, RESPONSE_RULE_TIMEOUT } from '../../../lib/responseClassifier'

// https://github.com/avajs/ava/blob/master/docs/01-writing-tests.md
test.before(async t => {
})


test.serial('Rule for status 2XX=ok, default=error (with 123 is error)', async t => {
  const rules = [
    { type: RESPONSE_RULE_STATUS, value: '2XX', classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 123, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_ERROR)
  t.is(reply.subClassification, null)
  t.is(reply.status, 123)
  t.is(reply.data.hello, 'there')
})


test.serial('Rule for status 2XX=ok, default=error (with 200 is ok)', async t => {
  const rules = [
    { type: RESPONSE_RULE_STATUS, value: '2XX', classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 200, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_OK)
  t.is(reply.subClassification, null)
  t.is(reply.status, 200)
  t.is(reply.data.hello, 'there')
})

test.serial('Rule for status 2XX=ok, default=error (with 201 is ok)', async t => {
  const rules = [
    { type: RESPONSE_RULE_STATUS, value: '2XX', classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 201, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_OK)
  t.is(reply.subClassification, null)
  t.is(reply.status, 201)
  t.is(reply.data.hello, 'there')
})

test.serial('Rule for status 2XX=ok, default=error (with 210 is ok)', async t => {
  const rules = [
    { type: RESPONSE_RULE_STATUS, value: '2XX', classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 210, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_OK)
  t.is(reply.subClassification, null)
  t.is(reply.status, 210)
  t.is(reply.data.hello, 'there')
})

test.serial('Rule for status 2XX=ok, default=error (with 299 is ok)', async t => {
  const rules = [
    { type: RESPONSE_RULE_STATUS, value: '2XX', classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 299, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_OK)
  t.is(reply.subClassification, null)
  t.is(reply.status, 299)
  t.is(reply.data.hello, 'there')
})

test.serial('Rule for status 2XX=ok, default=error (with 300 is error)', async t => {
  const rules = [
    { type: RESPONSE_RULE_STATUS, value: '2XX', classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 300, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_ERROR)
  t.is(reply.subClassification, null)
  t.is(reply.status, 300)
  t.is(reply.data.hello, 'there')
})

test.serial('Rule for status 2XX=ok, default=error (with 345 is error)', async t => {
  const rules = [
    { type: RESPONSE_RULE_STATUS, value: '2XX', classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 345, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_ERROR)
  t.is(reply.subClassification, null)
  t.is(reply.status, 345)
  t.is(reply.data.hello, 'there')
})

test.serial('Rule for status 2XX=ok, default=error (with 1000 is error)', async t => {
  const rules = [
    { type: RESPONSE_RULE_STATUS, value: '2XX', classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 1000, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_ERROR)
  t.is(reply.subClassification, null)
  t.is(reply.status, 1000)
  t.is(reply.data.hello, 'there')
})



test.serial('Rule for status 20X=ok, default=error (with 123 is error)', async t => {
  const rules = [
    { type: RESPONSE_RULE_STATUS, value: '2XX', classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 123, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_ERROR)
  t.is(reply.subClassification, null)
  t.is(reply.status, 123)
  t.is(reply.data.hello, 'there')
})

test.serial('Rule for status 20X=ok, default=error (with 200 is ok)', async t => {
  const rules = [
    { type: RESPONSE_RULE_STATUS, value: '2XX', classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 200, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_OK)
  t.is(reply.subClassification, null)
  t.is(reply.status, 200)
  t.is(reply.data.hello, 'there')
})

test.serial('Rule for status 20X=ok, default=error (with 205 is ok)', async t => {
  const rules = [
    { type: RESPONSE_RULE_STATUS, value: '20X', classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 205, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_OK)
  t.is(reply.subClassification, null)
  t.is(reply.status, 205)
  t.is(reply.data.hello, 'there')
})

test.serial('Rule for status 20X=ok, default=error (with 210 is error)', async t => {
  const rules = [
    { type: RESPONSE_RULE_STATUS, value: '20X', classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 210, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_ERROR)
  t.is(reply.subClassification, null)
  t.is(reply.status, 210)
  t.is(reply.data.hello, 'there')
})

test.serial('Rule for status 20X=ok, default=error (with 300 is error)', async t => {
  const rules = [
    { type: RESPONSE_RULE_STATUS, value: '20X', classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 123, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_ERROR)
  t.is(reply.subClassification, null)
  t.is(reply.status, 123)
  t.is(reply.data.hello, 'there')
})

test.serial('Rule for status 20X=ok, default=error (with 1000 is error)', async t => {
  const rules = [
    { type: RESPONSE_RULE_STATUS, value: '20X', classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 1000, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_ERROR)
  t.is(reply.subClassification, null)
  t.is(reply.status, 1000)
  t.is(reply.data.hello, 'there')
})


test.serial('Rule for status 2*=ok, default=error (with 1 is error)', async t => {
  const rules = [
    { type: RESPONSE_RULE_STATUS, value: '2*', classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 1, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_ERROR)
  t.is(reply.subClassification, null)
  t.is(reply.status, 1)
  t.is(reply.data.hello, 'there')
})

test.serial('Rule for status 2*=ok, default=error (with 2 is ok)', async t => {
  const rules = [
    { type: RESPONSE_RULE_STATUS, value: '2*', classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 2, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_OK)
  t.is(reply.subClassification, null)
  t.is(reply.status, 2)
  t.is(reply.data.hello, 'there')
})

test.serial('Rule for status 2*=ok, default=error (with 3 is error)', async t => {
  const rules = [
    { type: RESPONSE_RULE_STATUS, value: '2*', classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 3, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_ERROR)
  t.is(reply.subClassification, null)
  t.is(reply.status, 3)
  t.is(reply.data.hello, 'there')
})

test.serial('Rule for status 2*=ok, default=error (with 10 is error)', async t => {
  const rules = [
    { type: RESPONSE_RULE_STATUS, value: '2*', classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 10, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_ERROR)
  t.is(reply.subClassification, null)
  t.is(reply.status, 10)
  t.is(reply.data.hello, 'there')
})

test.serial('Rule for status 2*=ok, default=error (with 20 is ok)', async t => {
  const rules = [
    { type: RESPONSE_RULE_STATUS, value: '2*', classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 20, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_OK)
  t.is(reply.subClassification, null)
  t.is(reply.status, 20)
  t.is(reply.data.hello, 'there')
})

test.serial('Rule for status 2*=ok, default=error (with 30 is error)', async t => {
  const rules = [
    { type: RESPONSE_RULE_STATUS, value: '2*', classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 30, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_ERROR)
  t.is(reply.subClassification, null)
  t.is(reply.status, 30)
  t.is(reply.data.hello, 'there')
})

test.serial('Rule for status 2*=ok, default=error (with 100 is error)', async t => {
  const rules = [
    { type: RESPONSE_RULE_STATUS, value: '2*', classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 100, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_ERROR)
  t.is(reply.subClassification, null)
  t.is(reply.status, 100)
  t.is(reply.data.hello, 'there')
})

test.serial('Rule for status 2*=ok, default=error (with 200 is ok)', async t => {
  const rules = [
    { type: RESPONSE_RULE_STATUS, value: '2*', classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 200, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_OK)
  t.is(reply.subClassification, null)
  t.is(reply.status, 200)
  t.is(reply.data.hello, 'there')
})

test.serial('Rule for status 2*=ok, default=error (with 300 is error)', async t => {
  const rules = [
    { type: RESPONSE_RULE_STATUS, value: '2*', classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 300, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_ERROR)
  t.is(reply.subClassification, null)
  t.is(reply.status, 300)
  t.is(reply.data.hello, 'there')
})

test.serial('Rule for status 2*=ok, default=error (with 2000 is ok)', async t => {
  const rules = [
    { type: RESPONSE_RULE_STATUS, value: '2*', classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 2000, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_OK)
  t.is(reply.subClassification, null)
  t.is(reply.status, 2000)
  t.is(reply.data.hello, 'there')
})

test.serial('Rule for status 20*=ok, default=error (with 100 is error)', async t => {
  const rules = [
    { type: RESPONSE_RULE_STATUS, value: '20*', classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 100, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_ERROR)
  t.is(reply.subClassification, null)
  t.is(reply.status, 100)
  t.is(reply.data.hello, 'there')
})

test.serial('Rule for status 20*=ok, default=error (with 200 is ok)', async t => {
  const rules = [
    { type: RESPONSE_RULE_STATUS, value: '20*', classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 200, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_OK)
  t.is(reply.subClassification, null)
  t.is(reply.status, 200)
  t.is(reply.data.hello, 'there')
})

test.serial('Rule for status 20*=ok, default=error (with 205 is ok)', async t => {
  const rules = [
    { type: RESPONSE_RULE_STATUS, value: '20*', classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 205, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_OK)
  t.is(reply.subClassification, null)
  t.is(reply.status, 205)
  t.is(reply.data.hello, 'there')
})

test.serial('Rule for status 20*=ok, default=error (with 220 is error)', async t => {
  const rules = [
    { type: RESPONSE_RULE_STATUS, value: '20*', classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 220, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_ERROR)
  t.is(reply.subClassification, null)
  t.is(reply.status, 220)
  t.is(reply.data.hello, 'there')
})

test.serial('Rule for status 20*=ok, default=error (with 300 is error)', async t => {
  const rules = [
    { type: RESPONSE_RULE_STATUS, value: '20*', classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 300, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_ERROR)
  t.is(reply.subClassification, null)
  t.is(reply.status, 300)
  t.is(reply.data.hello, 'there')
})

test.serial('Rule for status 20*=ok, default=error (with 2098 is ok)', async t => {
  const rules = [
    { type: RESPONSE_RULE_STATUS, value: '20*', classification: RESPONSE_CLASSIFICATION_OK },
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 2098, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_OK)
  t.is(reply.subClassification, null)
  t.is(reply.status, 2098)
  t.is(reply.data.hello, 'there')
})
