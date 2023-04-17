/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import test from 'ava'
import { responseClassifier, RESPONSE_CLASSIFICATION_ERROR, RESPONSE_CLASSIFICATION_OFFLINE, RESPONSE_CLASSIFICATION_OK, RESPONSE_RULE_DEFAULT, RESPONSE_RULE_TIMEOUT } from '../../../lib/responseClassifier'


test('Override default => ok (test with 505)', async t => {
  const rules = [
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_OK }
  ]
  const reply = await responseClassifier(rules, false, 505, { hello: 'there' })
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_OK)
  t.is(reply.subClassification, null)
  t.is(reply.status, 505)
  t.is(typeof(reply.data), 'object')
  t.is(reply.data.hello, 'there')
})

test('Override default => error (test with 200)', async t => {
  const rules = [
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, false, 200, { hello: 'there' })
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_ERROR)
  t.is(reply.subClassification, null)
  t.is(reply.status, 200)
  t.is(typeof(reply.data), 'object')
  t.is(reply.data.hello, 'there')
})


test('Override default => offline (refused)', async t => {
  const rules = [
    { type: RESPONSE_RULE_DEFAULT, classification: RESPONSE_CLASSIFICATION_OFFLINE }
  ]
  const reply = await responseClassifier(rules, false, 505, { hello: 'there' })
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_ERROR)
  t.is(reply.subClassification, null)
  t.is(reply.status, 505)
  t.is(typeof(reply.data), 'object')
  t.is(reply.data.hello, 'there')
})