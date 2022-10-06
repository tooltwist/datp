/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import assert from 'assert'
import XData from '../XData'

// export function validateEvent_StepStart(event) {
//   assert (typeof(event.txId) === 'string')
//   assert (typeof(event.stepId) === 'string')

//   assert (typeof(event.fullSequence) === 'string')
//   assert (typeof(event.vogPath) === 'string')
//   assert (typeof(event.stepDefinition) !== 'undefined')
//   assert (typeof(event.data) === 'object')
//   assert ( !(event.data instanceof XData))
//   assert (typeof(event.metadata) === 'object')
//   assert (typeof(event.level) === 'number')

//   // How to reply when complete
//   assert (typeof(event.onComplete) === 'object')
//   assert (typeof(event.onComplete.nodeGroup) === 'string')
//   assert (typeof(event.onComplete.callback) === 'string')
//   assert (typeof(event.onComplete.context) === 'object')
// }

// export function validateEvent_StepCompleted(event) {
//   assert(typeof(event) == 'object')
//   assert (typeof(event.txId) === 'string')
//   // assert (typeof(event.parentStepId) === 'string')
//   assert (typeof(event.stepId) === 'string')
//   // assert (typeof(event.completionToken) === 'string')

//   // DO NOT try to reply stuff
//   assert (typeof(event.status) === 'undefined')
//   assert (typeof(event.stepOutput) === 'undefined')

//   // How to reply when complete
//   assert (typeof(event.onComplete) === 'object')
//   assert (typeof(event.onComplete.nodeGroup) === 'string')
//   assert (typeof(event.onComplete.callback) === 'string')
//   assert (typeof(event.onComplete.context) === 'object')
// }

export function validateEvent_TransactionCompleted(event) {
  assert(typeof(event) == 'object')
  assert (typeof(event.txId) === 'string')
  assert (typeof(event.transactionOutput) === 'undefined')
}

export function validateEvent_TransactionChange(event) {
  assert(typeof(event) == 'object')
  assert (typeof(event.txId) === 'string')
  assert (typeof(event.transactionOutput) === 'undefined')
}
