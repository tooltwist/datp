/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import test from 'ava'
import { responseClassifier, RESPONSE_CLASSIFICATION_ERROR, RESPONSE_CLASSIFICATION_OFFLINE, RESPONSE_RULE_TIMEOUT } from '../../../lib/responseClassifier'


test.serial('Timeout (default) => offline', async t => {
  const rules = [ ]
  const reply = await responseClassifier(rules, true)
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_OFFLINE)
  t.is(reply.subClassification, null)
  t.is(reply.status, 599)
  t.is(typeof(reply.data), 'object')
})

test.serial('Rule for timeout => offline', async t => {
  const rules = [
    { type: RESPONSE_RULE_TIMEOUT, classification: RESPONSE_CLASSIFICATION_OFFLINE }
  ]
  const reply = await responseClassifier(rules, true)
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_OFFLINE)
  t.is(reply.subClassification, null)
  t.is(reply.status, 599)
  t.is(typeof(reply.data), 'object')
})

test.serial('Rule for timeout => error', async t => {
  const rules = [
    { type: RESPONSE_RULE_TIMEOUT, classification: RESPONSE_CLASSIFICATION_ERROR }
  ]
  const reply = await responseClassifier(rules, true)
  // console.log(`reply=`, reply)
  t.is(reply.classification, RESPONSE_CLASSIFICATION_ERROR)
  t.is(reply.subClassification, null)
  t.is(reply.status, 599)
  t.is(typeof(reply.data), 'object')
})
