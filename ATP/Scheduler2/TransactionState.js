/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import query from "../../database/query"
import { deepCopy } from "../../lib/deepCopy"
import {
  STEP_RUNNING,
  STEP_QUEUED,
  STEP_SUCCESS,
  STEP_FAILED,
  STEP_ABORTED,
  STEP_SLEEPING,
  STEP_TIMEOUT,
  STEP_INTERNAL_ERROR,
  STEP_INITIAL
} from "../Step"
import XData from "../XData"
import TransactionPersistance from "./TransactionPersistance"
import Scheduler2 from "./Scheduler2"
import { schedulerForThisNode } from "../.."
import { DEBUG_DB_ATP_TRANSACTION } from "../../datp-constants"
import dbupdate from "../../database/dbupdate"
import me from "../../lib/me"
import GenerateHash from "../GenerateHash"
import assert from 'assert'
import TransactionCache from "./txState-level-1"
import { STEP_DEFINITION, validateStandardObject } from "./eventValidation"
import { RedisQueue } from "./queuing/RedisQueue-ioredis"


export const TX_STATUS_RUNNING = 'running'
export const TX_STATUS_QUEUED = 'queued'
export const TX_STATUS_SUCCESS = 'success'
export const TX_STATUS_FAILED = 'failed'
export const TX_STATUS_ABORTED = 'aborted'
export const TX_STATUS_SLEEPING = 'sleeping'
export const TX_STATUS_TIMEOUT = 'timeout'
export const TX_STATUS_INTERNAL_ERROR = 'internal-error'

export const FIELD_FLOW_INDEX = 'i'
export const FIELD_PARENT_FLOW_INDEX = 'p'

export const F2_TRANSACTION = 'TX_START'
export const F2_TRANSACTION_CH = 'TX_CALLBACK'
export const F2_STEP = 'STEP'
export const F2_PIPELINE = 'PIPELINE_START'
export const F2_PIPELINE_CH = 'PIPELINE_CALLBACK'

// Flow2 attributes are defined this way, so we can use shortened versions when not debugging.
export const F2ATTR_TYPE = '__type'
export const F2ATTR_PARENT = '__parent'
export const F2ATTR_SIBLING = '__sibling'
export const F2ATTR_LEVEL = '__level'
export const F2ATTR_NODEGROUP = '__nodeGroup'
export const F2ATTR_NODEID = '__nodeId'
export const F2ATTR_CALLBACK = '__callback'
export const F2ATTR_STEPID = '__stepId'
export const F2ATTR_DESCRIPTION = '__description'
export const F2ATTR_TRANSACTION_TYPE = '__transactionType'
export const F2ATTR_PIPELINE = '__pipeline'
export const F2ATTR_CURRENT_PIPELINE_STEP = '__currentStep'

export const F2_VERBOSE = 0


// Debug stuff
const VERBOSE = 0
const SUPPRESS = true
require('colors')

let countTxCoreUpdate = 0

export default class TransactionState {
  #me // The transaction object

  // Deltas (changes to this transaction state)
  #deltaCounter
  #deltas

  // We use this as a check that the user is not trying to process
  // a delta before the previous delta has been completed.
  #processingDelta

  /**
   *  Instantiate the transaction state object
   * @param {XData} json Object or JSON
   */
  constructor(json) {
    // console.log(`TransactionState.constructor()`, json)

    const obj = (typeof(json) === 'string') ? JSON.parse(json) : json

    // Core transaction information
    this.#me = {
      owner: obj.owner,
      txId: obj.txId,
      externalId: obj.externalId,
      webhook: obj.webhook ? obj.webhook : { },
      
      progressReport: obj.progressReport ? obj.progressReport : null,

      steps: obj.steps ? obj.steps : { }, // stepId => { }
      flow: obj.flow ? obj.flow : [ ], // Progress through the pipelines / steps
      f2: obj.f2 ? obj.f2 : [ ],
    }

    // transactionData
    if (obj.transactionData) {
      this.#me.transactionData = obj.transactionData
    } else {
      this.#me.transactionData = {
        transactionType: obj.transactionType,
        status: STEP_RUNNING,
        completionTime: 0,
        lastUpdated: 0,
        notifiedTime: 0,
        transactionInput: {},
        transactionOutput: {},
        metadata: { }
      }//- transactionData
    }

    // retry
    if (obj.retry) {
      this.#me.retry = obj.retry
    } else {
      this.#me.retry = {
        sleepingSince: 0,
        sleepCounter: 0,
        wakeTime: 0,
        wakeSwitch: null,
        wakeNodeGroup: null,
        //wakeStepId: null //ZZZZ Shouldn't we just use flowIndex???
      }
    }

    // Counters (are these used?)
    this.#me.deltaCounter = obj.deltaCounter ? obj.deltaCounter : 0
    this.#me.sequenceOfUpdate = obj.sequenceOfUpdate ? obj.sequenceOfUpdate : 0


    // Obsolete ZZZZZ
    this.#deltaCounter = 0
    this.#deltas = [ ]
    this.#processingDelta = false
  }//- constructor

  getTxId() {
    return this.#me.txId
  }

  getExternalId() {
    return this.#me.externalId
  }

  getOwner() {
    return this.#me.owner
  }

  getTransactionType() {
    return this.#me.transactionData.transactionType
  }

  getStatus() {
    return this.#me.transactionData.status
  }

  // Do not use this. It is exclusively a hack while getting events via a LUA script.
  _patchInStatus(status) {
    this.#me.transactionData.status = status
  }

  getSequenceOfUpdate() {
    return this.#me.sequenceOfUpdate
  }

  getDeltaCounter() {
    return this.#deltaCounter
  }

  getProgressReport() {
    return this.#me.progressReport
  }

  getTransactionOutput() {
    return this.#me.transactionData.transactionOutput
  }

  getTransactionOutputAsJSON() {
    if (this.#me.transactionData.transactionOutput) {
      return JSON.stringify(this.#me.transactionData.transactionOutput)
    }
    return null    
  }

  getCompletionTime() {
    return this.#me.transactionData.completionTime
  }

  // vogGetStep(stepId) {
  //   let step = this.#me.steps[stepId]
  //   if (step === undefined) {
  //     step = { }
  //     this.#me.steps[stepId] = step
  //   }
  //   return step
  // }

  async addInitialStep(pipelineName) {
    const stepId = GenerateHash('s')

    const fullSequence = this.#me.txId.substring(3, 9)
    const vogPath = `${this.#me.txId.substring(3, 9)}=${pipelineName}`

    await this.delta(stepId, {
      vogPath: vogPath,
      vogI: 0,
      vogP: null,
      // vogPipeline: pipelineName,
      stepDefinition: pipelineName,
      // vogI: index,
      // vogP: parentStepId,
      level: 0,
      fullSequence,
      status: STEP_INITIAL,
    }, 'Transaction.js')

    // console.log(`this.vog_getStepDefinition(${childStepId})=`, this.vog_getStepDefinition(childStepId))

    return stepId
  }

  /**
   * Add a child step that is a pipeline.
   * This may run on a different node or nodeGroup, as it will be queued.
   * The LUA script plugs in the pipeline definition before the step runs.
   * 
   * @param {string} parentStepId 
   * @param {Integer} index 
   * @param {string} pipelineName 
   * @returns 
   */
  async addPipelineStep(parentStepId, index, pipelineName) {
    const childStepId = GenerateHash('s')
    const parentStep = this.#me.steps[parentStepId]

    const childFullSequence = `${parentStep.fullSequence}.${index}` // Start sequence at 1
    let childVogPath = `${parentStep.vogPath?parentStep.vogPath:'???vog???'},${index}` // Start sequence at 1
    const stepDefinition = this.vog_getStepDefinitionFromParent(parentStepId, index)
    // console.log(`stepDefinition=`, stepDefinition)
    if (stepDefinition && stepDefinition.stepType) {
      childVogPath += `=${stepDefinition.stepType}`
    }

    await this.delta(childStepId, {
      vogPath: childVogPath,
      vogI: index,
      vogP: parentStepId,
      // vogPipeline: pipelineName,
      level: parentStep.level + 1,
      fullSequence: childFullSequence,
      status: STEP_INITIAL,
    }, 'Transaction.js')

    // console.log(`this.vog_getStepDefinition(${childStepId})=`, this.vog_getStepDefinition(childStepId))

    return childStepId
  }
    
  async addChildStep(parentStepId, index) {
    const childStepId = GenerateHash('s')
    // await this.setChildIndex(childStepId, index)
    // await this.setChildParent(childStepId, parentStepId)

    // console.log(`parentStepId=`, parentStepId)
    const parentStep = this.#me.steps[parentStepId]
    // console.log(`parentStep=`, parentStep)

    validateStandardObject('addChildStep() parent step', parentStep, STEP_DEFINITION)

    const childFullSequence = `${parentStep.fullSequence}.${index}` // Start sequence at 1
    // const childVogPath = `${pipelineInstance.getVogPath()},1=P.${stepType}` // Start sequence at 1
    let childVogPath = `${parentStep.vogPath?parentStep.vogPath:'???vog???'},${index}` // Start sequence at 1
    const stepDefinition = this.vog_getStepDefinitionFromParent(parentStepId, index)
    // console.log(`stepDefinition=`, stepDefinition)
    if (stepDefinition && stepDefinition.stepType) {
      childVogPath += `=${stepDefinition.stepType}`
    }

    await this.delta(childStepId, {
      vogPath: childVogPath,
      vogI: index,
      vogP: parentStepId,
      level: parentStep.level + 1,
      fullSequence: childFullSequence,
      status: STEP_INITIAL,
    }, 'Transaction.js')

    // console.log(`this.vog_getStepDefinition(${childStepId})=`, this.vog_getStepDefinition(childStepId))

    return childStepId
  }

  vog_flowRecordStep_scheduled(parentIndex, stepId, input, onComplete) {
    console.log(`vog_flowRecordStep_scheduled - NOT CREATING FLOW RECORD`)
    return

    if (VERBOSE) console.log(`vog_flowRecordStep_scheduled(parentIndex=${parentIndex}, ${stepId}, input, onComplete)`)
    // console.log(`this.#me.flow=`, this.#me.flow)

    const step = this.stepData(stepId)
    assert(step)

    const index = this.#me.flow.length
    const entry = {
      i: index
    }
    if (typeof(parentIndex) === 'number') {
      entry.p = parentIndex
    }
    // entry.vogPath = vogPath
    entry._tmpPath = step.vogPath
    entry.ts1 = Date.now()
    entry.ts2 = 0
    entry.ts3 = 0
    entry.stepId = stepId
    entry.nodeId = null,
    entry.input = input
    entry.onComplete = onComplete
    entry.note = null
    entry.completionStatus = null
    entry.output = null
    // console.log(`vog_flowRecordStep_scheduled() - new flow entry: ${JSON.stringify(entry,'',2)}`.magenta)
    this.#me.flow.push(entry)
    return this.#me.flow.length - 1
  }

  vog_flowRecordStep_invoked(stepId, nodeId) {
    console.log(`vog_flowRecordStep_invoked() - NOT CHECKING FLOW RECORD`)
    return

    // console.log(`vog_flowRecordStep_invoked()`.red)
    assert(this.#me.flow.length >= 1)
    const latestEntry = this.#me.flow[this.#me.flow.length - 1]
    // console.log(`latestEntry=`, latestEntry)
    assert(latestEntry.stepId === stepId)
    latestEntry.ts2 = Date.now()
    latestEntry.nodeId = schedulerForThisNode.getNodeId()
    latestEntry.vog_nodeGroup = schedulerForThisNode.getNodeGroup()
  }

  vog_flowRecordStep_sleep(stepId, wakeSwitch, duration) {
    assert(this.#me.flow.length >= 1)
    const latestEntry = this.#me.flow[this.#me.flow.length - 1]
    assert(latestEntry.stepId === stepId)
    latestEntry.ts3 = Date.now()
    latestEntry.wakeSwitch = wakeSwitch
    latestEntry.duration = duration
  }

  // vog_flowRecordStep_complete(flowIndex, completionStatus, note, output) {
  //   // console.log(`vog_flowRecordStep_complete(${flowIndex}, ${completionStatus}, ${note}, ${output})`.brightYellow)
  //   assert(flowIndex >= 0 && flowIndex < this.#me.flow.length)
  //   const entry = this.#me.flow[flowIndex]
  //   // assert(entry.stepId === stepId)
  //   entry.ts3 = Date.now()
  //   entry.completionStatus = completionStatus
  //   entry.note = note
  //   entry.output = output
  //   // console.log(`entry=`, entry)
  // }

  // vog_getFlow() {
  //   return this.#me.flow
  // }

  // vog_getFlowLength() {
  //   return this.#me.flow.length
  // }

  // vog_getFlowRecord(index) {
  //   if (index < 0) {
  //     index = this.#me.flow.length - 1
  //   }
  //   assert(index >= 0 && index < this.#me.flow.length)
  //   return this.#me.flow[index]
  // }

  v2f_setF2Transaction(pipelineName, metadata, input) {
    assert(this.#me.f2.length === 0)
    const entry = {
      // description: this.vf2_typeDescription(F2_TRANSACTION),
      // t: F2_TRANSACTION,
      // l: 0,
      // p: -1,
      // transactionType: pipelineName,
      // metadata,
      // input,
      ts1: Date.now(),
      ts2: 0,
      ts3: 0
    }
    entry[F2ATTR_DESCRIPTION] = this.vf2_typeDescription(F2_TRANSACTION)
    entry[F2ATTR_TRANSACTION_TYPE] = pipelineName
    entry[F2ATTR_TYPE] = F2_TRANSACTION
    entry[F2ATTR_LEVEL] = 0
    entry[F2ATTR_PARENT] = -1
    this.#me.f2.push(entry)

    if (F2_VERBOSE > 1) console.log(`F2: v2f_setF2Transaction: ${this.#me.f2.length}: ADDING TRANSACTION`.bgBrightRed.black)
    if (F2_VERBOSE > 2) this.dumpFlow2()

    return { f2i: 0, f2: entry }
  }

  vf2_addF2sibling(f2i, type, addedBy) {
    // console.log(`vf2_addF2sibling(${f2i}, ${type})`)
    assert(f2i >= 0 && f2i < this.#me.f2.length)
    const thisF2 = this.#me.f2[f2i]
    // console.log(`thisF2=`, thisF2)

    // If this F2 record has a sibling itself, we'll use the same one.
    const siblingF2i = thisF2[F2ATTR_SIBLING] ? thisF2[F2ATTR_SIBLING] : f2i

    // Skip over any children
    let newPos = f2i + 1
    while (newPos < this.#me.f2.length && this.#me.f2[newPos][F2ATTR_PARENT] === f2i) {
      newPos++
    }
    // console.log(`newPos=`, newPos)
    const newF2 = {
      // description: this.vf2_typeDescription(type),
      // t: type,
      // p: parent.p,
      // l: thisF2.l,
      // s: siblingF2i,
      ts1: Date.now(),
      ts2: 0,
      ts3: 0
    }
    newF2[F2ATTR_DESCRIPTION] = this.vf2_typeDescription(type)
    newF2[F2ATTR_TYPE] = type
    newF2[F2ATTR_LEVEL] = thisF2[F2ATTR_LEVEL]
    newF2[F2ATTR_SIBLING] = siblingF2i
    // newF2[F2ATTR_SIBLING] = siblingF2i
    newF2._addedBy = addedBy
    this.#me.f2.splice(newPos, 0, newF2)

    if (F2_VERBOSE > 1) console.log(`F2: vf2_addF2sibling: ${newPos}/${this.#me.f2.length}: ADDING SIBLING: ${type}`.bgBrightRed.black + ` by ${addedBy}`)
    if (F2_VERBOSE > 2) this.dumpFlow2()

    return { f2i: newPos, f2: newF2 }
  }

  vf2_addF2child(parentF2i, type, addedBy) {
    assert(parentF2i >= 0 && parentF2i < this.#me.f2.length)
    const parent = this.#me.f2[parentF2i]

    // Skip over any children
    let childF2i = parentF2i + 1
    while (childF2i < this.#me.f2.length && this.#me.f2[childF2i][F2ATTR_PARENT] === parentF2i) {
      childF2i++
    }
    const childF2 = {
      // description: this.vf2_typeDescription(type),
      // t: type,
      // p: parentF2i,
      // l: parent.l + 1,
      ts1: Date.now(),
      ts2: 0,
      ts3: 0
    }
    childF2[F2ATTR_DESCRIPTION] = this.vf2_typeDescription(type)
    childF2[F2ATTR_TYPE] = type
    childF2[F2ATTR_PARENT] = parentF2i
    childF2[F2ATTR_LEVEL] = parent[F2ATTR_LEVEL] + 1
    childF2._addedBy = addedBy
    this.#me.f2.splice(childF2i, 0, childF2)

    if (F2_VERBOSE > 1) console.log(`F2: vf2_addF2child: ${childF2i}/${this.#me.f2.length}: ADDING CHILD ${type}`.bgBrightRed.black + ` by ${addedBy}`)
    if (F2_VERBOSE > 2) this.dumpFlow2()

    return { f2i: childF2i, f2: childF2 }
  }

  vf2_typeDescription(type) {
    switch (type) {
      case F2_TRANSACTION:
        return 'Start transaction'
      case F2_TRANSACTION_CH:
        return 'Transaction after pipeline'
      case F2_PIPELINE:
        return 'Start pipeline'
      case F2_PIPELINE_CH:
        return 'Pipeline after step'
      case F2_STEP:
        return 'Run step'
      default:
        return `Unknown type ${type}`
    }
  }

  vf2_getF2Length() {
    return this.#me.f2.length
  }

  vf2_getF2(f2i) {
    assert(f2i >= 0 && f2i < this.#me.f2.length)
    return this.#me.f2[f2i]
  }

  /**
   * Return a flow entry.
   * If a flow entry is for a callback, called after running a step,
   * pipeline or transaction, then it will be associated with the
   * original pipeline or transaction flow entry (it's 'sibling'). In
   * that case, return the flow entry for the sibling.
   * @param {integer} f2i Flow index
   * @returns Flow entry
   */
  vf2_getF2OrSibling(f2i) {
    assert(f2i >= 0 && f2i < this.#me.f2.length)
    const f2 = this.#me.f2[f2i]
    const siblingF2i = f2[F2ATTR_SIBLING]
    // console.log(`siblingF2i=`, siblingF2i)
    if (typeof(siblingF2i) !== 'undefined') {
      assert(siblingF2i >= 0 && siblingF2i < this.#me.f2.length)
      return this.#me.f2[siblingF2i]
    }
    return f2
  }

  // vog_getStepRecord(index) {
  //   // console.log(`this.#me.steps=`, this.#me.steps)
  //   return this.#me.steps[index]
  // }

  /**
   * Run back through the flow entries until we find a status.
   * 
   * @param {*} index 
   * @returns 
   */
  vf2_getStatus(index) {
    assert(index >= 0 && index < this.#me.f2.length)
    for ( ; index > 0; index--) {
      const f2 = this.#me.f2[index]
      if (typeof(f2.status) !== 'undefined') {
        // console.log(`f2@${index}.status=`, f2.status)
        return f2.status
      }
    }
    throw new Error(`Internal error: could not get status of f2 [${index}]`)
  }

  /**
   * Get the stepId related to a flow entry.
   * If this is a step then the flow entry will contain the stepId. If the
   * entry is for a callback then we get the stepId from the matching flow
   * entry (it's sibling pipeline).
   * @param {integer} f2i Flow index
   * @returns 
   */
  vf2_getStepId(f2i) {
    // console.log(`vf2_getStepId(${f2i})`.magenta)
    assert(f2i >= 0 && f2i < this.#me.f2.length)
    const f2 = this.#me.f2[f2i]

    // If this is related to another f2, for example if this is
    // a callback, get the step from that sibling.
    const siblingF2i = f2[F2ATTR_SIBLING]
    // console.log(`siblingF2i=`, siblingF2i)
    if (typeof(siblingF2i) !== 'undefined') {
      // console.log(`USING stepId from sibling`)
      assert(siblingF2i >= 0 && siblingF2i < this.#me.f2.length)
      const siblingF2 = this.#me.f2[siblingF2i]
      // console.log(`siblingF2=`, siblingF2)
      return siblingF2[F2ATTR_STEPID]
    }
// console.log(`USING original f2 stepId`)
    return f2[F2ATTR_STEPID]
  }

  vf2_getNote(index) {
    assert(index >= 0 && index < this.#me.f2.length)
    for ( ; index > 0; index--) {
      const f2 = this.#me.f2[index]
      if (typeof(f2.note) !== 'undefined') {
        // console.log(`f2@${index}.note=`, f2.note)
        return f2.note
      }
    }
    throw new Error(`Internal error: could not get status of f2 [${index}]`)
  }

  vf2_getOutput(index) {
    assert(index >= 0 && index < this.#me.f2.length)
    for ( ; index > 0; index--) {
      const f2 = this.#me.f2[index]
      if (typeof(f2.output) !== 'undefined') {
        // console.log(`f2@${index}.output=`, f2.output)
        return f2.output
      }
    }
    throw new Error(`Internal error: could not get status of f2 [${index}]`)
  }

  // vog_flowPath(index) {
  //   assert(index >= 0 && index < this.#me.flow.length)
  //   const flow = this.#me.flow[index]
  //   const step = this.#me.steps[flow.stepId]
  //   return step.vogPath
  // }

  // vog_getParentFlowIndex(flowIndex) {
  //   assert(flowIndex >= 0 && flowIndex < this.#me.flow.length)
  //   const pIndex = this.#me.flow[flowIndex][FIELD_PARENT_FLOW_INDEX]
  //   return (pIndex === undefined) ? -1 : pIndex
  // }

  vog_getMetadata() {
    assert(typeof(this.#me.transactionData.metadata) === 'object')
    const metadata = this.#me.transactionData.metadata
    return metadata
  }


  vog_setProgressReport(object) {
    this.#me.progressReport = object
  }

  vog_setStatusToSleeping(wakeSwitch, sleepDuration, stepId) {
    this.#me.transactionData.status = STEP_SLEEPING
    this.#me.retry.wakeSwitch = wakeSwitch
    if (!SUPPRESS) console.log(`vog_setStatusToSleeping() - Not sure what to do with the sleepDuration`.magenta)
    this.#me.retry.wakeStepId = wakeStepId
  }

  vog_setNextStepId(stepId) {
    if (!SUPPRESS) console.log(`vog_setNextStepId() - Not sure what to do with the nextStepId`.magenta)
  }

  vog_setTransactionType(transactionType) {
    this.#me.transactionData.transactionType = transactionType
  }

  vog_setNodeGroup(nodeGroup) {
    if (!SUPPRESS) console.log(`vog_setNodeGroup() - Not sure what to do with the node group`.magenta)
  }
  vog_setNodeId(nodeId) {
    if (!SUPPRESS) console.log(`vog_setNodeId() - Not sure what to do with the nodeId`.magenta)
  }
  vog_setPipelineName(pipelineName) {
    if (!SUPPRESS) console.log(`vog_setNodeId() - Not sure what to do with the nodeId`.magenta)
  }
  vog_setStatusToQueued() {
    this.#me.transactionData.status = TX_STATUS_QUEUED
  }
  vog_setStatusToRunning() {
    this.#me.transactionData.status = TX_STATUS_RUNNING
  }
  vog_setMetadata(metadata) {
    assert(typeof(metadata) === 'object')
    this.#me.transactionData.metadata = metadata
  }
  vog_setTransactionInput(initialData) {
    this.#me.transactionData.transactionInput = initialData
  }
  vog_setOnComplete(context) {
    this.#me.onComplete = context
  }
  vog_setOnChange(context) {
    this.#me.onChange = context
  }

  vog_setToComplete(statusOfFinalStep, note, transactionOutput) {
    this.#me.transactionData.status = statusOfFinalStep
    this.#me.transactionData.transactionOutput = transactionOutput
    this.#me.transactionData.completionNote = note
    const now = Date.now()
    this.#me.transactionData.completionTime = now
    this.#me.transactionData.lastUpdated = now
    this.#me.progressReport = null
  }



  vog_getStepDefinitionFromParent(parentStepId, childIndex) {
    // console.log(`vog_getStepDefinitionFromParent(${parentStepId}, ${childIndex})`)
    assert(parentStepId)
    assert(typeof(childIndex) === 'number')

    // Get the parent step
    const parent = this.#me.steps[parentStepId]
    assert(parent)
    // console.log(`parent=`, parent)
    // assert(parent.vogStepDefinition)
    const parentDefinition = parent.vogStepDefinition
    // console.log(`parentDefinition=`, parentDefinition)
    if (!parentDefinition) {
      // This will happen in a pipeline step, before it has passed through
      // the queueing and LUA has patched in the pipeline definition.
      return null
    }
    // console.log(`parentDefinition=`, parentDefinition)

    // Get the step definition
    // console.log(`childIndex=`, childIndex)
    // console.log(`parentDefinition.steps.length=`, parentDefinition.steps.length)
    assert(childIndex >= 0 && childIndex < parentDefinition.steps.length)
    const childDefinition = parentDefinition.steps[childIndex].definition
    assert(childDefinition)
    return childDefinition
  }

  vog_getStepDefinition(stepId) {
    const step = this.#me.steps[stepId]
    assert(step)
    if (step.stepDefinition) {
      return step.stepDefinition.definition
    }
    // Get the definition from the parent
    return this.vog_getStepDefinitionFromParent(step.vogP, step.vogI)
  }

  vog_getWebhook() {
    if (this.#me.transactionData && this.#me.transactionData.metadata.webhook) {
      return this.#me.transactionData.metadata.webhook
    }
    return null
  }


  vog_validateSteps() {
    let errors = 0
    for (const stepId in this.#me.steps) {
      try {
        const step = this.#me.steps[stepId]
        validateStandardObject('vog.validateSteps()', step, STEP_DEFINITION)
      } catch (e) {
        errors++
      }
    }
    if (errors) {
      throw new Error(`Internal error in step definitions`)
    }
  }

  async vog_setStepCompletionHandler(stepId, flowIndex, nodeGroup, callback, context={}, completionToken=null) {
    const step = this.#me.steps[stepId]
    assert(step)
    step.onComplete = {
      flowIndex,
      nodeGroup,
      callback,
      context,
      completionToken
    }
  }
  
  vog_setStepStatus(stepId, status) {
    const step = this.#me.steps[stepId]
    assert(step)
    step.status = status
  }

  JnodeGroupWhereStepRuns(childId) { return this.#me.steps[childId].nodeGroupWhereStepRuns }


  // async setChildIndex(childStepId, stepIndex) {
  //   // this.vogGetStep(stepId).childStepIndex = stepIndex
  //   await this.delta(childStepId, {
  //     vogI: stepIndex,
  //   }, 'Transaction.js')
  // }

  // async setChildParent(childStepId, parentStepId) {
  //   // this.vogGetStep(stepId).vogP = parentStepId
  //   await this.delta(childStepId, {
  //     vogP: parentStepId,
  //   }, 'Transaction.js')
  // }


  asObject() {
    return this.#me
  }

  /**
   * Return transaction state in it's JSON form.
   * 
   * @param {boolean} pretty Display with indenting
   * @returns 
   */
  asJSON(pretty=false) {
    return JSON.stringify(this.asObject(), '', pretty ? 2 : 0)
  }

  // txData() {
  //   return this.#me
  // }

  transactionData() {
    return this.#me.transactionData
  }

  stepData(stepId) {
    const d = this.#me.steps[stepId]
    if (d) {
      return d
    }
    return null
  }

  /**
   * Record data changes to this transaction or a step within this transaction.
   *
   * When the following fields are set on the transaction, they also get persisted to the
   * atp_transaction2 table:
   * status, progressReport, transactionOutput, completionTime.
   * 
   * 
   * 
   * TODO, Phil March 18 2022:
   * This would work better if we:
   *    1. Take a copy of the core values.
   *    2. Update the in-memory copy.
   *    3. Compare the copies of the core values with the new in-memory values.
   * Why is this better? The copying process is not straightforward if only some values
   * within an object are changed. It is actually an overlay, unless "!objectName" is
   * used in the delta instructions. Similarly, "-property" could be used, but is not
   * currently checked. Are these notations currently used? Probably not.
   * 
   *
   * @param {string} stepId If null, the data changes will be saved against the transaction.
   * @param {object} data An object containing values to be saved. e.g. { color: 'red' }
   * @param {boolean} replayingPastDeltas Set to true only when reloading from database (do not use this).
   */
  async delta(stepId, data, note='', replayingPastDeltas=false) {
    let id = stepId ? stepId.substring(2, 10) : 'tx'
    // console.log(`delta - ${note} - ${id}`)
    if (VERBOSE) console.log(`\n*** delta(${stepId}, data, replayingPastDeltas=${replayingPastDeltas})`, data)
// console.log(`YARP delta - #${this.#deltaCounter}`)

    // Check that this function is not already in action, because someone forgot an 'await'.
    if (this.#processingDelta) {
      //ZZZZ Raise an alarm
      console.log(`VERY SERIOUS ERROR: delta() was called again before it completed. Missing 'await'?`)
      throw new Error(`delta() was called again before it completed. Missing 'await'?`)
    }
    this.#processingDelta = true
    try {

      // Next sequence number
      this.#deltaCounter++

      // See if any of the core transaction values are changing
      let coreValuesChanged = false
      if (stepId === null) {
        if (data.status) {
          // Check the status is valid
          switch (data.status) {
            // These are temporary ststuses that do not impact sleeping values
            case STEP_QUEUED:
            case STEP_RUNNING:
              if (this.#me.transactionData.status !== data.status) {
                if (VERBOSE) console.log(`Setting transaction status to ${data.status}`)
                coreValuesChanged = true
                this.#me.transactionData.status = data.status
              }
              break

            // These statuses need to reset the sleeping values
            case STEP_SUCCESS:
            case STEP_FAILED:
            case STEP_ABORTED:
            case STEP_TIMEOUT:
            case STEP_INTERNAL_ERROR:
              if (this.#me.transactionData.status !== data.status) {
                if (VERBOSE) console.log(`Setting transaction status to ${data.status}`)
                coreValuesChanged = true
                this.#me.transactionData.status = data.status
              }
              // We are no longer in a sleep loop
              if (this.#me.retry.counter!==0 || this.#me.retry.sleepingSince!==null || this.#me.retry.wakeTime!==null || this.#me.retry.wakeSwitch!==null) {
                coreValuesChanged = true
                this.#me.retry.counter = 0
                this.#me.retry.sleepingSince = null
                this.#me.retry.wakeTime = null
                this.#me.retry.wakeSwitch = null
                this.#me.retry.wakeNodeGroup = null
                this.#me.retry.wakeStepId = null
                if (VERBOSE) console.log(`Resetting sleep values`)
              }
              break

            case STEP_SLEEPING:
              if (this.#me.transactionData.status !== data.status) {
                // Was not already in sleep mode
                if (VERBOSE) console.log(`Setting transaction status to ${data.status}`)
                coreValuesChanged = true
                this.#me.transactionData.status = data.status
                if (this.#me.retry.sleepingSince === null) {
                  if (VERBOSE) console.log(`Initializing sleep fields`)
                  this.#me.retry.counter = 1
                  this.#me.retry.sleepingSince = new Date()
                  this.#me.retry.wakeNodeGroup = schedulerForThisNode.getNodeGroup()
                  this.#me.retry.wakeStepId = data.wakeStepId
                } else {
                  if (VERBOSE) console.log(`Incrementing sleep counter`)
                  this.#me.retry.counter++
                }
              }
              // if (typeof(data.wakeTime) !== 'undefined' && this.#me.retry.wakeTime !== data.wakeTime) {
              //   if (VERBOSE) console.log(`Setting wakeTime to ${data.wakeTime}`)
              //   if (data.wakeTime === null || data.wakeTime instanceof Date) {
              //     coreValuesChanged = true
              //     this.#me.retry.wakeTime = data.wakeTime
              //   } else {
              //     throw new Error('Invalid data.wakeTime')
              //   }
              // }
              if (typeof(data.sleepDuration) !== 'undefined') {
                let newWakeTime
                if (data.sleepDuration === null) {
                  // All good
                  newWakeTime = null
                } else if (typeof(data.sleepDuration) === 'number') {
                  // This value is used by the scheduler to restart steps. We'll add on fifteen
                  // seconds for the scheduler, so that if we have a setTimeut set to trigger
                  // a short timeout, it gets in before the scheduler.
                  newWakeTime = new Date(Date.now() + (data.sleepDuration * 1000) + (15 * 1000))
                } else {
                  throw new Error('Invalid data.sleepDuration')
                }
                if (this.#me.retry.wakeTime !== newWakeTime) {
                  if (VERBOSE) console.log(`Setting wakeTime to +${newWakeTime}`)
                  coreValuesChanged = true
                  this.#me.retry.wakeTime = newWakeTime
                  this.#me.retry.wakeNodeGroup = schedulerForThisNode.getNodeGroup()
                  this.#me.retry.wakeStepId = data.wakeStepId
                }
              }
              if (typeof(data.wakeSwitch) !== 'undefined' && this.#me.retry.wakeSwitch !== data.wakeSwitch) {
                if (VERBOSE) console.log(`Setting wakeSwitch to ${data.wakeSwitch}`)
                if (data.wakeSwitch === null || typeof(data.wakeSwitch) === 'string') {
                  coreValuesChanged = true
                  this.#me.retry.wakeSwitch = data.wakeSwitch
                  this.#me.retry.wakeNodeGroup = schedulerForThisNode.getNodeGroup()
                  this.#me.retry.wakeStepId = data.wakeStepId
                } else {
                  throw new Error('Invalid data.wakeSwitch')
                }
              }
              break

            default:
              throw new Error(`Invalid status [${data.status}]`)
          }
        }
        if (typeof(data['-progressReport']) !== 'undefined') {
          // Progress report being deleted
          coreValuesChanged = true
          this.#me.progressReport = null
        } else if (typeof(data.progressReport) !== 'undefined' && this.#me.progressReport !== data.progressReport) {
          if (VERBOSE) console.log(`Setting transaction progressReport to ${data.progressReport}`)
          if (data.progressReport !== null && typeof(data.progressReport) !== 'object') {
            throw new Error('data.progressReport must be an object')
          }
          coreValuesChanged = true
          this.#me.progressReport = data.progressReport
        }
        if (typeof(data.transactionOutput) !== 'undefined' && this.#me.transactionData.transactionOutput !== data.transactionOutput) {
          if (VERBOSE) console.log(`Setting transactionOutput to ${data.transactionOutput}`)
          if (typeof(data.transactionOutput) !== 'object') {
            throw new Error('data.transactionOutput must be an object')
          }
          coreValuesChanged = true
          this.#me.transactionData.transactionOutput = data.transactionOutput
        }
        if (typeof(data.completionTime) !== 'undefined' && this.#me.transactionData.completionTime !== data.completionTime) {
          if (VERBOSE) console.log(`Setting transaction completionTime to ${data.completionTime}`)
          if (data.completionTime !== null && !(data.completionTime instanceof Date)) {
            throw new Error('data.completionTime parameter must be of type Date')
          }
          coreValuesChanged = true
          this.#me.transactionData.completionTime = data.completionTime
        }

                    // if (this.#me.transactionData.status === STEP_SLEEPING) {
            //   if (this.)

            // } else if (this.#me.transactionData.status === STEP_ || this.#me.transactionData.status === STEP_SLEEPING)


        // If the core values were changed, update the database
        if (coreValuesChanged) {
          if (DEBUG_DB_ATP_TRANSACTION) console.log(`TX UPDATE ${countTxCoreUpdate++}`.cyan)
          this.#me.sequenceOfUpdate = this.#deltaCounter
          if (!replayingPastDeltas) {
            let sql = `UPDATE atp_transaction2 SET
              status=?,
              progress_report=?,
              transaction_output=?,
              completion_time=?,
              sequence_of_update=?,
              sleep_counter=?,
              sleeping_since=?,
              wake_time=?,
              wake_switch=?,
              wake_node_group=?,
              wake_step_id=?`

//ZZZZ Check that the transaction hasn't been updated by someone else.
            //  AND sequence_of_update=?   [ this.#me.sequenceOfUpdate ]
            const transactionOutputJSON = this.#me.transactionData.transactionOutput ? JSON.stringify(this.#me.transactionData.transactionOutput) : null
            const progressReportJSON = this.#me.progressReport ? JSON.stringify(this.#me.progressReport) : null
            const params = [
              this.#me.transactionData.status,
              progressReportJSON,
              transactionOutputJSON,
              this.#me.transactionData.completionTime,
              this.#deltaCounter,
              this.#me.retry.counter,
              this.#me.retry.sleepingSince,
              this.#me.retry.wakeTime,
              this.#me.retry.wakeSwitch,
              this.#me.retry.wakeNodeGroup,
              this.#me.retry.wakeStepId
            ]
            sql += ` WHERE transaction_id=? AND owner=?`
            params.push(this.#me.txId)
            params.push(this.#me.owner)
            // console.log(`sql=`, sql)
            // console.log(`params=`, params)
            const response = await dbupdate(sql, params)
            // console.log(`response=`, response)
            if (response.affectedRows !== 1) {
              // This should never happen.
              console.log(`sql=`, sql)
              console.log(`params=`, params)
              console.log(`response=`, response)
              throw new Error(`INTERNAL ERROR: Could not update transaction record`)
            }

            // Notify any event handler
            // console.log(`this.#me=`, this.#me)
            if (this.#me.onChange) {
              const queueName = Scheduler2.groupQueueName(this.#me.nodeGroup)
              if (VERBOSE) console.log(`Adding a TRANSACTION_CHANGE_EVENT to queue ${queueName}`)
              await schedulerForThisNode.enqueue_TransactionChange(queueName, {
                owner: this.#me.owner,
                txId: this.#me.txId
              })
            }
          }//- !replayingPastDeltas
        }//- coreValuesChanged
      }//- !stepId

      // Save this delta (i.e. like a journal entry)
      const obj = {
        sequence: this.#deltaCounter,
        stepId,
        data,
        time: new Date()
      }
      this.#deltas.push(obj)
      if (!replayingPastDeltas) {
        // Save the delta to the database
        await TransactionPersistance.persistDelta(this.#me.owner, this.#me.txId, obj)
      }

      // Update this in-memory transaction object
      if (stepId) {
        // We are updating a step
        let step = this.#me.steps[stepId]
        if (step === undefined) {
          step = { }
          this.#me.steps[stepId] = step
        }
        deepCopy(data, step)
      } else {
        // We are updating the transaction
// console.log(`*** Before deep copy:`)
// console.log(`data=`, data)
// console.log(`this.#me=`, this.#me)
        deepCopy(data, this.#me)
// console.log(`*** After deep copy:`)
// console.log(`this.#me=`, this.#me)
      }
      this.#processingDelta = false
    } catch (e) {
      this.#processingDelta = false
      throw e
    }
  }//- delta


  static async getSummary(owner, txId) {
    // console.log(`-----------------------------------------------------------------------------------`)
    // console.log(`getSummary(${owner}, ${txId})`)

    // Get the transaction state
    const txState = await TransactionCache.getTransactionState(txId)

    // console.log(`txState=`, txState)
    if (txState === null) {
      return null
    }

    return txState.summaryFromState()
  }

  summaryFromState() {
    // if (VERBOSE) console.log(`Transaction state: ${JSON.stringify(txStateObject, '', 2)}]`.yellow)

    let status = this.#me.transactionData.status
    if (status === TX_STATUS_QUEUED) {
      // Don't reveal the inner workings of DATP
      status = TX_STATUS_RUNNING
    }

    const summary2 = {
      "metadata": {
        "owner": this.#me.owner,
        "txId": this.#me.txId,
        "externalId": this.#me.externalId ? this.#me.externalId : null,
        "transactionType": this.#me.transactionData.transactionType,
        status,
        // "sequenceOfUpdate": this.#me.delta,
        "completionTime": this.#me.transactionData.completionTime,
        "lastUpdated": this.#me.transactionData.lastUpdated,
        "notifiedTime": this.#me.transactionData.notifiedTime
      },
      "progressReport": this.#me.progressReport,
      "data": this.#me.transactionData.transactionOutput
    }
    // console.log(`summary2=`, summary2)
    return summary2
  }




  static async getSummaryByExternalId(owner, externalId) {
    const sql = `SELECT
      owner,
      transaction_id AS txId,
      external_id AS externalId,
      transaction_type AS transactionType,
      status,
      sequence_of_update AS sequenceOfUpdate,
      progress_report AS progressReport,
      transaction_output AS transactionOutput,
      completion_time AS completionTime,
      last_updated AS lastUpdated
      FROM atp_transaction2
      WHERE owner=? AND external_id=?`
    const params = [ owner, externalId ]
    const rows = await query(sql, params)
    if (rows.length < 1) {
      return null
    }
    const row = rows[0]
    const transactionOutput = row.transactionOutput
    const progressReport = row.progressReport
    const summary = {
      metadata: row,
      progressReport
    }
    delete summary.metadata.transactionOutput
    delete summary.metadata.progressReport
    try { summary.progressReport = JSON.parse(progressReport) } catch (e) { /* We'll stick with the JSON string */ }
    if (
      row.status === STEP_SUCCESS
      || row.status === STEP_FAILED
      || row.status === STEP_ABORTED
      || row.status === STEP_INTERNAL_ERROR
      || row.status === STEP_TIMEOUT
    ) {
      // Step has completed
      try {
        if (!transactionOutput) transactionOutput = '{ }'
        // console.log(`transactionOutput=`, transactionOutput)
        summary.data = JSON.parse(transactionOutput)
        // console.log(`summary.data=`, summary.data)
      } catch (e) {
        summary.data = transactionOutput
      }
    } else {
      // Step is still in progress
      // try {
      //   if (!progressReport) progressReport = '{ }'
      //   summary.progressReport = JSON.parse(progressReport)
      // } catch (e) {
      //   summary.progressReport = progressReport
      // }
    }
    // console.log(`summary=`, summary)

    return summary
  }


  static async getSwitches(owner, txId) {
    const sql = `SELECT
      sequence_of_update,
      switches
      FROM atp_transaction2
      WHERE owner=? AND transaction_id=?`
    const params = [ owner, txId ]
    const rows = await query(sql, params)
    if (rows.length < 1) {
      return [ ]
    }

    let switches = { }
    try {
      if (rows[0].switches) {
        switches = JSON.parse(rows[0].switches)
      }
    } catch (e) {
      //ZZZZZ report alert
      const description = `Corrupt switches for transaction ${owner}, ${txId}`
      console.log(description)
      throw new Error(description)
    }
    return { sequenceOfUpdate: rows[0].sequence_of_update, switches }
  }

  static async getSwitch(owner, txId, name) {
    const { switches } = await TransactionState.getSwitches(owner, txId)
    if (typeof(switches[name]) === 'undefined') {
      return null
    }
    return switches[name]
  }

  /**
   * Set a transaction switch value
   * @param {String} owner
   * @param {string} txId
   * @param {string} name
   * @param {string|number|bookean} value If the value is null, the switch will be removed
   * @param {boolean} triggerNudgeEvent If true, a NUDGE_EVENT will be triggered for the transaction
   * if we successfully change the switch value. If the transaction is currently sleeping, this will
   * cause the current sleeping step to be immediately retried.
   */
  static async setSwitch(owner, txId, name, value, triggerNudgeEvent=false) {
    const { switches, sequenceOfUpdate } = await TransactionState.getSwitches(owner, txId)
    if (value === null || typeof(value) === 'undefined') {

      // Remove the switch
      delete switches[name]
    } else {

      // Add the switch
      switch (typeof(value)) {
        case 'string':
          if (value.length > 32) {
            throw new Error(`Switch exceeds maximum length (${name}: ${value})`)
          }
          switches[name] = value
          break

        case 'boolean':
        case 'number':
          switches[name] = value
          break

        default:
          throw new Error(`Switch can only be boolean, number, or string (< 32 chars)`)
      }
    }

    const json = JSON.stringify(switches, '', 0)

    // Update the transaction, ensuring nobody has changed the transaction before us
    if (DEBUG_DB_ATP_TRANSACTION) console.log(`TX UPDATE ${countTxCoreUpdate++} (switch)`)
    const sql = `UPDATE atp_transaction2 SET switches=?, sequence_of_update=? WHERE owner=? AND transaction_id=? AND sequence_of_update=?`
    const params = [ json, sequenceOfUpdate+1, owner, txId, sequenceOfUpdate ]
    const reply = await dbupdate(sql, params)
    // console.log(`reply=`, reply)
    if (reply.changedRows !== 1) {
      throw new Error('Concurrent attempts to set transaction switches - switch not set')
    }

    // Trigger an event for this change?
    if (triggerNudgeEvent) {
      console.log(`YARP Event not triggered`)
    }
  }

  /**
   *
   * @returns
   */
  getSleepingSince() {
    return this.#me.retry.sleepingSince
  }

  /**
   *
   * @returns
   */
  getRetryCounter() {
    return this.#me.retry.counter
  }

  /**
   *
   * @returns
   */
  getWakeTime() {
    return this.#me.retry.wakeTime
  }

  /**
   *
   * @returns
   */
  getWakeSwitch() {
    return this.#me.retry.wakeSwitch
  }

  getWakeNodeGroup() {
    return this.#me.retry.wakeNodeGroup
  }

  getWakeStepId() {
    return this.#me.retry.wakeStepId
  }

  getNodeGroupForStep(stepId) {
    for (let s = stepId; s; ) {
      const step = this.stepData(s)
      if (!step) {
        throw new Error(`Internal Error: Unknown step [${this.getTxId()}, ${s}]`)
      }
      if (step.nodeGroup) {
        return step.nodeGroup
      }
      s = step.parentStepId
    }
    throw new Error(`Internal Error: Could not find nodeGroup for step [${this.getTxId()}, ${stepId}]`)
  }

  /**
   *
   * @param {number} pagesize
   * @param {number} offset
   * @param {string} filter
   * @param {string[]} status
   * @returns
   */
  static async findTransactions(archived, pagesize=20, offset=0, filter='', statusList=[STEP_SUCCESS, STEP_FAILED, STEP_ABORTED]) {
    // console.log(`TransactionState.findTransactions()`, archived)

    archived = false
    if (!archived) {
      const redisLua = await RedisQueue.getRedisLua()
      if (offset == 0) {
        const result = await redisLua.findTransactions(filter, statusList)
        // const result = [ ]
        return result
      }
      return [ ]
    }

    let sql = `SELECT
      transaction_id AS txId,
      owner,
      external_id AS externalId,
      transaction_type AS transactionType,
      status,
      start_time AS startTime,
      completion_time AS completionTime,
      last_updated AS lastUpdated,
      response_acknowledge_time AS responseAcknowledgeTime,
      switches,
      sleep_counter AS sleepCounter,
      sleeping_since AS sleepingSince,
      wake_time AS wakeTime,
      wake_switch AS wakeSwitch
      FROM atp_transaction2`
    const params = [ ]

    let sep = `\nWHERE`
    // if (options.sleeping) {
    //   sql += ` ${sep} sleep_counter > 0`
    //   sep = `AND`
    // }
    // if (options.finished) {
    //   sql += ` ${sep} (status='${STEP_SUCCESS}' OR status='${STEP_FAILED}' OR status='${STEP_ABORTED}')`
    //   sep = `AND`
    // }

    if (statusList) {
      sql += `${sep} (`
      let sep2 = ''
      for (const s of statusList) {
        if (s) {
          sql += `${sep2} status=?`
          params.push(s)
          sep2 = ' OR '
        }
      }
      sql += `)`
      sep = `\nAND `
    }
    if (filter) {
      sql += `${sep} (
        transaction_id LIKE ?
        OR owner LIKE ?
        OR external_id LIKE ?
        OR transaction_type LIKE ?
        OR wake_switch =?
      )`
      params.push(`%${filter}%`)
      params.push(`%${filter}%`)
      params.push(`%${filter}%`)
      params.push(`%${filter}%`)
      params.push(`%${filter}%`)
    }

    // pagination
    // See https://www.w3schools.com/php/php_mysql_select_limit.asp
    const syntax1 = false
    if (syntax1) {
      sql += `\nORDER BY last_updated DESC LIMIT ? OFFSET ?`
      // 2022-03-15 Phil - need to pass parameters as strings due to MySQL bug.
      // See https://stackoverflow.com/questions/65543753/error-incorrect-arguments-to-mysqld-stmt-execute
      params.push('' + pagesize)
      params.push('' + offset)
    } else {
      sql += `\nORDER BY last_updated DESC LIMIT ?, ?`
      // 2022-03-15 Phil - need to pass parameters as strings due to MySQL bug.
      // See https://stackoverflow.com/questions/65543753/error-incorrect-arguments-to-mysqld-stmt-execute
      params.push(offset)
      params.push(pagesize) // reversed
    }

    console.log(`sql=`, sql)
    console.log(`params=`, params)
    const rows = await query(sql, params)
    // console.log(`${rows.length} transactions.`)
    return rows
  }

  stepIds() {
    // console.log(`stepIds()`, this.#me.steps)
    const stepIds = Object.keys(this.#me.steps)
    // console.log(`stepIds=`, stepIds)
    return stepIds
  }

  // Temporary debug hack for 16aug22.
  xoxYarp(msg='', highlightedStepId=null) {
    console.log(``)
    console.log(`${me()}: ${msg}: TX ===>>> ${this.#me.txId}`)
    let found = false
    for (const stepId in this.#me.steps) {
      const step = this.#me.steps[stepId]
      let arrow = ''
      if (stepId===highlightedStepId) {
        arrow = ' <====='
        found = true
      }
      console.log(`${me()}     -> step ${stepId} (fullSequence: ${step.fullSequence}) ${arrow}`)
    }
    if (!found) {
      console.log(``)
      console.log(``)
      console.log(``)
      console.log(``)
      console.log(``)
      console.log(``)
      console.log(``)
      console.log(``)
      console.log(``)
      console.log(``)
      console.log(`${me()} **********************************************`)
      console.log(`${me()} ***  STEP NOT FOUND IN TRANSACTION STATUS  ***`)
      console.log(`${me()} **********************************************`)
      console.log(``)
      console.log(``)
      console.log(``)
      console.log(``)
      console.log(``)
      console.log(``)
      console.log(``)
      console.log(``)
      console.log(``)
      console.log(``)
    }
    console.log(``)
  }

  async getDetails(withSteps=true, withDeltas=false) {
    const obj = {
      txId: this.#me.txId,
      owner: this.#me.owner,
      externalId: this.#me.externalId,
      status: this.#me.transactionData.status,
      sequenceOfUpdate: this.#me.sequenceOfUpdate,
      progressReport: this.#me.progressReport,
      transactionOutput: this.#me.transactionData.transactionOutput,
      completionTime: this.#me.transactionData.completionTime
    }
    if (withSteps) {
      obj.steps = [ ]
      for (const stepId in this.#me.steps) {
      // for (const step of this.#me.steps) {
        // console.log(`stepId=`, stepId)
        const step = this.#me.steps[stepId]
        // console.log(`step=`, step)
        const stepObj = {
          stepId,
          parentStepId: step.parentStepId,
          status: step.status,
          stepInput: step.stepInput,
          stepOutput: step.stepOutput,
          level: step.level,
          note: step.note,
        }
        stepObj.stepDefinition = step.stepDefinition
        if (typeof (step.stepDefinition) === 'string') {
          stepObj.isPipeline = true
          stepObj.pipelineName = step.stepDefinition
          stepObj.children = [ ]
          // console.log(`step.children.length=`, step.childStepIds.length)
          for (const childStepId of step.childStepIds) {
            // console.log(`childStepId=`, childStepId)
            stepObj.children.push(childStepId)
          }
        } else {
          stepObj.isPipeline = false
        }
        obj.steps.push(stepObj)
      }
    }
    return obj
  }


  getDeltas() {
    const deltas = this.#deltas
    this.#deltas = [ ]
    return deltas
  }

  // 
  pretty() {
    return JSON.stringify(this.asObject(), '', 2)
  }

  // 
  stringify() {
    return JSON.stringify(this.asObject())
  }

  dumpFlow2(f2i = -1) {
    for (let i = 0; i < this.#me.f2.length; i++) {
      let pointer = ''
      if (f2i >= 0) {
        pointer = (f2i === i) ? '-->' : '   '
      }
      const f2 = this.#me.f2[i]
      let num = `${i}`; while (num.length < 3) num = ` ${num}`
      let l = ''; for (let i = 0; i < f2[F2ATTR_LEVEL]; i++) l += '  ';
      let p = f2[F2ATTR_PARENT] ? ` p${f2[F2ATTR_PARENT]}` : ''
      let s = f2[F2ATTR_SIBLING] ? `s${f2[F2ATTR_SIBLING]}` : ''
      let by = f2._addedBy ? f2._addedBy : ''
      switch (f2[F2ATTR_TYPE]) {
        case F2_PIPELINE:
          console.log(`${pointer} ${num}:${l} ${f2[F2ATTR_TYPE]} (${f2[F2ATTR_PIPELINE]})` + `   [${p} ${s} ${by}]`.gray)
          break

        case F2_PIPELINE_CH:
          console.log(`${pointer} ${num}:${l} ${f2[F2ATTR_TYPE]} (${f2[F2ATTR_CALLBACK]})` + `   [${p} ${s} ${by}]`.gray)
          break

        case F2_STEP:
          const step = this.#me.steps[f2[F2ATTR_STEPID]]
          const stepType = (step && step.stepDefinition) ? step.stepDefinition.stepType : ''
          console.log(`${pointer} ${num}:${l} ${f2[F2ATTR_TYPE]} (${stepType})` + `   [${p} ${s} ${by}]`.gray)
          break
    
        default:
          console.log(`${pointer} ${num}:${l} ${f2[F2ATTR_TYPE]}` + `   [${p} ${s} ${by}]`.gray)
          // console.log(`f2=`, f2)
          break
      }
    }
  }

  // Returns a abbreviated description of the transaction
  toString() {
    // return JSON.stringify(this.asObject())
    const obj = {
      txId: this.#me.txId,
      owner: this.#me.owner,
      externalId: this.#me.externalId,
      transactionData: this.#me,
      steps: this.#me.steps
    }
    return JSON.stringify(obj)
  }
}
