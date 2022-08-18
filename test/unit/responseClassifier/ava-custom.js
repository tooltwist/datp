/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import test from 'ava'
import { responseClassifier, RESPONSE_CLASSIFICATION_ERROR, RESPONSE_CLASSIFICATION_OFFLINE, RESPONSE_CLASSIFICATION_OK, RESPONSE_RULE_DEFAULT, RESPONSE_RULE_STATUS, RESPONSE_RULE_TIMEOUT } from '../../../lib/responseClassifier'

test('Default to custom status', async t => {
  const customClassification = 'a-custom-classification'
  const rules = [
    { type: RESPONSE_RULE_DEFAULT, classification: customClassification }
  ]
  const reply = await responseClassifier(rules, false, 200, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, customClassification)
  t.is(reply.subClassification, null)
  t.is(reply.status, 200)
  t.is(reply.data.hello, 'there')
})

test('Custom classification for status 200', async t => {
  const customClassification = 'a-custom-classification'
  const rules = [
    { type: RESPONSE_RULE_STATUS, value: 200, classification: customClassification }
  ]
  const reply = await responseClassifier(rules, false, 200, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, customClassification)
  t.is(reply.subClassification, null)
  t.is(reply.status, 200)
  t.is(reply.data.hello, 'there')
})
