/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import test from 'ava'
import { responseClassifier, RESPONSE_CLASSIFICATION_ERROR, RESPONSE_CLASSIFICATION_OFFLINE, RESPONSE_CLASSIFICATION_OK, RESPONSE_RULE_STATUS, RESPONSE_RULE_TIMEOUT } from '../../../lib/responseClassifier'

test.serial('have sub-classification for status 200', async t => {
  const subClassification = 'this is nice'
  const rules = [
    { type: RESPONSE_RULE_STATUS, value: 200, classification: RESPONSE_CLASSIFICATION_OK, subClassification }
  ]
  const reply = await responseClassifier(rules, false, 200, { hello: 'there'})
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_OK)
  t.is(reply.subClassification, subClassification)
  t.is(reply.status, 200)
  t.is(reply.data.hello, 'there')
})
