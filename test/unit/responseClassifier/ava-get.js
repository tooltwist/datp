/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import test from 'ava'
import { axiosGET } from '../../../lib/axiosClassification'
import { RESPONSE_CLASSIFICATION_ERROR, RESPONSE_CLASSIFICATION_OFFLINE, RESPONSE_CLASSIFICATION_OK, RESPONSE_RULE_DEFAULT, RESPONSE_RULE_TIMEOUT, RESPONSE_RULE_VALUE } from '../../../lib/responseClassifier'


test('Get - timeout', async t => {
  const url = 'http://tooltwist.com'
  const options = { timeout: 2 } // 2ms
  const rules = [
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_OK }
  ]
  const reply = await axiosGET(url, options, rules)
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_OFFLINE)
  t.is(reply.subClassification, null)
  t.is(reply.status, 599)
  t.is(typeof(reply.data), 'object')
})


test('Get - bad URL domain', async t => {
  const url = 'http://a.b.c.d.e.f.tooltwist.com'
  const options = { } // 2ms
  const rules = [
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_OK }
  ]
  const reply = await axiosGET(url, options, rules)
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_ERROR)
  t.is(reply.subClassification, null)
  t.is(reply.status, 404)
  t.is(typeof(reply.data), 'object')
})


test('Get - bad URL path', async t => {
  // See https://jsonplaceholder.typicode.com/
  const url = 'https://jsonplaceholder.typicode.com/dunno'
  const options = { } // 2ms
  const rules = [
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_OK }
  ]
  const reply = await axiosGET(url, options, rules)
  // console.log(`reply=`, reply)
  // console.log(`reply.status=`, reply.status)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_ERROR)
  t.is(reply.subClassification, null)
  t.is(reply.status, 404)
  t.is(typeof(reply.data), 'object')
})


test.serial('Get - valid JSON', async t => {
  // See https://www.jsontest.com/
  const url = `http://echo.jsontest.com/foo/bar/hello/there`
  const rules = [
    // { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await axiosGET(url, {}, rules)
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_OK)
  t.is(reply.subClassification, null)
  t.is(reply.status, 200)
  t.is(typeof(reply.data), 'object')
  t.is(reply.data.foo, 'bar')
  t.is(reply.data.hello, 'there')
})

test.serial('Get - data value found', async t => {
  // See https://www.jsontest.com/
  const url = `http://echo.jsontest.com/transactionStatus/processing`
  const goodClassification = 'ifoundIt'
  const rules = [
    { type: RESPONSE_RULE_VALUE, field: 'transactionStatus', value: 'processing', classification: goodClassification },
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await axiosGET(url, {}, rules)
  // console.log(`reply=`, reply)
  t.is(reply.classification, goodClassification)
  t.is(reply.subClassification, null)
  t.is(reply.status, 200)
  t.is(typeof(reply.data), 'object')
  t.is(reply.data.transactionStatus, 'processing')
})


test.serial('Get - data value not found', async t => {
  // See https://www.jsontest.com/
  const url = `http://echo.jsontest.com/transactionStatus/FINISHED`
  const goodClassification = 'ifoundIt'
  const badClassification = 'whereIsIt'
  const rules = [
    { type: RESPONSE_RULE_VALUE, field: 'transactionStatus', value: 'processing', classification: goodClassification },
    { type: RESPONSE_RULE_DEFAULT, classification: badClassification }
  ]
  const reply = await axiosGET(url, {}, rules)
  // console.log(`reply=`, reply)
  t.is(reply.classification, badClassification)
  t.is(reply.subClassification, null)
  t.is(reply.status, 200)
  t.is(typeof(reply.data), 'object')
  t.is(reply.data.transactionStatus, 'FINISHED')
})

