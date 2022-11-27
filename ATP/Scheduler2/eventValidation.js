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

/**
 * Check that an event object contains only fields of the specifed types.
 * 
 * @param {object} event 
 * @param {Array.<{ field: string, type: Object, optional: boolean }>} definition 
 */
export function validateStandardObject(msg, event, definition, mayHaveExtraFields=false) {

  // Create a list of required fields
  const errors = [ ]
  const found = { }
  for (const field in definition) {
    found[field] = false
  }

  // Check the definition of each field
  for (let field in event) {
    const value = event[field]
    found[field] = true

    const def = definition[field]
    if (def) {
      if (objectMatchesDefinition(value, def)) {
        // All okay
      } else {
        errors.push(`Invalid field type for ${field} (${typeof(value)} should be ${def.type})`)
      }
    } else {
      if (!mayHaveExtraFields) {
        errors.push(`Invalid field ${field}`)
      }
    }
  }

  // Check that all the required fields are provided
  for (const field in definition) {
    if (!definition[field].optional && !found[field]) {
      errors.push(`Mandatory field ${field} is missing`)
    }
  }

  // Report any errors
  if (errors.length > 0) {
    console.log(`------------------------------------------------------------`)
    console.log(`ERROR`)
    console.log(`-----`)
    console.log(`${msg} invalid:`)
    for (const error of errors) {
      console.log(`  - ${error}`)
    }
    console.log(event)
    console.log(`------------------------------------------------------------`)
    throw new Error('Validation failed')
  }
}

function objectMatchesDefinition(value, definition) {
  // console.log(`objectMatchesDefinition(${value})`, definition)
  if (value === null && definition.nullable) {
    return true
  }

  const arr = definition.type.split(',')
  for (const type of arr) {
    switch (type.trim().toLowerCase()) {
      case 'string':
        if (typeof(value) === 'string') {
          return true
        }
        break

      case 'number':
        if (typeof(value) === 'number') {
          return true
        }
        break


      case 'object':
        if (typeof(value) === 'object') {
          return true
        }
        break
      
      default:
        console.log(`Invalid definition:`, type)
        return false
    }
  }
  return false
}


export const EVENT_DEFINITION_STEP_START_SCHEDULED = {
  eventType: { type: 'string' },
  txId: { type: 'string' },
  parentNodeGroup: { type: 'string' },
  // fullSequence: { type: 'string' },
  // vogPath: { type: 'string' },
  // _tmpPath: { type: 'string' },
  stepDefinition: { type: 'string,object', optional: true },
  metadata: { type: 'object' },
  data: { type: 'object' },
  level: { type: 'number' }, // YARP Is this required???
  f2i: { type: 'number', optional: true },//ZZZZZ Should not be optional
}


// export const EVENT_DEFINITION_STEP_START_ENQUEUE = {
//   eventType: { type: 'string' },
//   txId: { type: 'string' },
//   flowIndex: { type: 'number' },
//   parentNodeGroup: { type: 'string' },
//   stepDefinition: { type: 'string,object', optional: true },
//   // fullSequence: { type: 'string' },
//   // vogPath: { type: 'string' },
//   // _tmpPath: { type: 'string' },
//   // stepDefinition: { type: 'string' },
//   metadata: { type: 'object' },
//   data: { type: 'object' },
//   level: { type: 'number' }, // YARP Is this required???
//   // txState: { type: 'object' },
// }


export const DEFINITION_PROCESS_STEP_START_EVENT = {
  eventType: { type: 'string' },
  txId: { type: 'string' },
  flowIndex: { type: 'number' },
  f2i: { type: 'number', optional: true },//ZZZZZ Should not be optional
  parentNodeGroup: { type: 'string' },
  stepDefinition: { type: 'string,object', optional: true },
  // fullSequence: { type: 'string' },
  // vogPath: { type: 'string' },
  // _tmpPath: { type: 'string' },
  // stepDefinition: { type: 'string' },
  metadata: { type: 'object' },
  data: { type: 'object' },
  level: { type: 'number' }, // YARP Is this required???
  ts: { type: 'number', optional: true }, // Not set for memory queue?
  // txState: { type: 'object' },
}

export const DEFINITION_MATERIALIZE_STEP_EVENT = {
  eventType: { type: 'string' },
  txId: { type: 'string' },
  flowIndex: { type: 'number' },
  parentNodeGroup: { type: 'string' },
  // fullSequence: { type: 'string' },
  // vogPath: { type: 'string' },
  // _tmpPath: { type: 'string' },
  // stepDefinition: { type: 'string' },
  stepDefinition: { type: 'string,object', optional: true },
  metadata: { type: 'object' },
  data: { type: 'object' },
  level: { type: 'number' }, // YARP Is this required???
  ts: { type: 'number', optional: true }, // Not set for memory queue?
  // ts: { type: 'number' },
  nodeGroup: { type: 'string' },
  nodeId: { type: 'string' },
  f2i: { type: 'number' },
}

export const DEFINITION_STEP_COMPLETE_EVENT = {
  eventType: { type: 'string' },
  txId: { type: 'string' },
  flowIndex: { type: 'number', optional: true },
  parentFlowIndex: { type: 'number' },
  childFlowIndex: { type: 'number' },
  completionToken: { type: 'string,object', optional: true },
  ts: { type: 'number', optional: true }, // Not set for memory queue?
  f2i: { type: 'number' },
}

export const FLOW_DEFINITION = {
  i: { type: 'number' },
  p: { type: 'number', optional: true }, // parent
  // vogPath: { type: 'string' },
  _tmpPath: { type: 'string' },
  ts1: { type: 'number' },
  ts2: { type: 'number' },
  ts3: { type: 'number' },
  stepId: { type: 'string' },
  nodeId: { type: 'string', nullable: true },
  input: { type: 'object' },
  onComplete: { type: 'object' },
    // "nodeGroup": "master",
    // "callback": "txComplete"
  note: { type: 'string', nullable: true },
  completionStatus: { type: 'string', nullable: true },
  output: { type: 'object', nullable: true },
  // Used by pipelines
  vog_nodeGroup: { type: 'string', optional: true },
  vog_currentPipelineStep: { type: 'number', optional: true },
}

export const STEP_DEFINITION = {
  vogPath: { type: 'string' },
  vogP: { type: 'string', nullable: true }, // Parent step
  vogI: { type: 'number' }, // Index of child
  vogPath: { type: 'string' },
  // vogPipeline: { type: 'string' },
  level: { type: 'number' },
  fullSequence: { type: 'string' },
  vogAddedBy: { type: 'string' },
  stepDefinition: { type: 'string,object' },
  // stepInput: { type: 'object' },
  status: { type: 'string' },
  vogStepDefinition: { type: 'object', optional: true }, // YARP Is this required???
  // For pipelines
  pipelineSteps: { type: 'object', optional: true },
  childStepIds: { type: 'object', optional: true },
}

