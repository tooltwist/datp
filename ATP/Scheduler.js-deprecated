/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import GenerateHash from './GenerateHash'
import Step, { STEP_SLEEPING } from './Step'
import StepInstance from './StepInstance'
import StepIndexEntry from './StepIndexEntry'
import pad from '../lib/pad'
import statusString from '../lib/statusString'
import assert from 'assert'
import XData from './XData'
import dbStep from '../database/dbStep'

const VERBOSE = false
const DELETE_COMPLETED_STEPS_IMMEDIATELY = false

class Scheduler {

  constructor () {
    this.stepIndex = [ ] // stepId -> StepInstance
  }

  /**
   *
   * @param {*} parentContext
   * @param {String | Object} definition
   * @param {*} tx
   * @param {*} resultReceiver
   * @param {*} completionHandlerData
   * @returns
   */
  async invokeStep(transactionId, parentInstance, sequence, definition, tx, logbook, resultReceiver, completionHandlerData) {
    // console.log(``)
    assert(typeof(transactionId) === 'string')
    // console.log(`typeof(sequenceYARP)=`, typeof(sequenceYARP))
    assert(typeof(sequenceYARP) === 'string')
    // console.log(`${typeof resultReceiver}`)
    // console.log(`${typeof completionHandlerData}`)
    assert(tx instanceof XData)
    assert(typeof(resultReceiver) === 'string')
    assert(typeof(completionHandlerData) === 'object')


    // if (parentInstance) {
    //   parentInstance.console(`Scheduler.invokeStep(): ${JSON.stringify(definition, '', 0)}`.yellow.bgBlue)
    // } else {
    //   console.log(`Scheduler.invokeStep(): ${JSON.stringify(definition, '', 0)}`.yellow.bgBlue)
    // }

    let parentStepId = null
    let level = 0
    let fullSequence = `${sequence}`
    if (parentInstance) {
      if (!(parentInstance instanceof StepInstance)) {
        throw new Error(`context parameter must be an instance of StepInstance`)
      }
      parentStepId = parentInstance.getStepId()
      level = parentInstance.getLevel() + 1
      // console.log(`parentInstance.fullSequence=`, parentInstance.fullSequence)
      // console.log(`parentInstance.getSequence()=`, parentInstance.getSequence())
      // console.log(`sequence=`, sequence)
      fullSequence = `${parentInstance.getSequence()}.${sequence}`

      const parentTransactionId = await parentInstance.getTransactionId()
      if (transactionId !== parentTransactionId) {
        throw new Error(`Internal error 27811 - transactionId does not match that of parentInstance`)
      }
    }
    if (typeof(resultReceiver) !== 'string') {
      throw new Error(`resultReceiver parameter must be a string`)
    }
    if (typeof(completionHandlerData) !== 'object') {
      throw new Error(`completionHandlerData parameter must be an object`)
    }

    // We pass a "completion token" to the step. It get's passed back and is verified
    // by this scheduler. This prevents steps from faking the completion of other steps.
    const token = GenerateHash('token')


    /*
     *  Prepare the context for the step.
     */
    const instance = new StepInstance()
    await instance.materialize({
      transactionId,
      parentId,
      definition,
      data: tx,
      metadata: { },
      level,
      fullSequence,
      logbook,
      // resultReceiver,
      completionToken: token,
      // completionHandlerData
    })
    // console.log(`About to start step ${fullSequence}`)


    /*
     *  Register this step.
     */
    // const gateId = GenerateHash('gate')
    // console.log(`gateId is ${gateId}`)
    const schedulerSnapshotOfStepInstance = new StepIndexEntry(
    //   parentInstance.getStepId(), instance.getStepId())
    // stepIndex.setStepInstance(instance)

    // const schedulerSnapshotOfStepInstance =
    {
      // stepInstance: instance,
      transactionId,
      parentStepId: parentId,
      stepId: instance.getStepId(),
      fullSequence,
      stepType: instance.getStepType(),
      description: '',

      // How to handle completion of this step
      resultReceiver,
      completionToken: token, // step completion must quote this
      completionHandlerData,

      // name,

      //
      status: STEP_SLEEPING,

      // Information passed to the handler
      // handler,
      // payload,

      // ZZZZ Should contain a timeout, or several types of timeout
    })
    schedulerSnapshotOfStepInstance.setStepInstance(instance)
    this.stepIndex[instance.getStepId()] = schedulerSnapshotOfStepInstance

    // this.dumpSteps(`after register`)

    if (VERBOSE) {
      console.log(`adding step ${instance.getStepId()}`, step)
    }

    /*
     *  Prepare for how we are going to return from this step.
     */
    // instance.console(`>>>>>>>>>> >>>>>>>>>> >>>>>>>>>> START ${instance.getStepType()}: ${instance.getStepId()} ${token}`)
    instance.console(`>>>>>>>>>> >>>>>>>>>> >>>>>>>>>> START [${instance.getStepType()}] ${instance.getStepId()}`)


    /*
     *  Start the step, and don't wait for it to complete
     */
    const stepObject = instance.getStepObject()
    const reply = await stepObject.invoke_internal(instance)

    // NOTE: The reply will only be the step reply if the step is hard coded and synchronous.
    //ZZZZ
    // if (reply.xyz === SYNCHRONOUS)
    return reply

  }//- invokeStep

  async stepFinished(stepId, token, status, note, newTx) {
    assert(newTx instanceof XData)
    // console.log(`Scheduler.stepFinished(stepId=${stepId}, token=${token}, status=${status}, note=${note}) newTx=`, newTx.toString())
    // console.log(`Scheduler.stepFinished(stepId=${stepId}, token=${token}, status=${status}, note=${note})`)

    // Update the step status.
    // This will fail if the step has completed already.
    // await dbStep.saveExitStatus(stepId, status, data)



    // Find the step
    const schedulerSnapshotOfStepInstance = this.stepIndex[stepId]
    if (!schedulerSnapshotOfStepInstance) {
      // This is BAD, and should be reported. ZZZZZZ
      throw new Error(`Unknown step ${stepId} - possible hack attempt`)
    }
    const stepInstance = await schedulerSnapshotOfStepInstance.getStepInstance()
    // stepInstance.console(`<<<<<<<<<< <<<<<<<<<< <<<<<<<<<< END   ${stepInstance.getStepType()}: ${stepId} ${token}`)
    stepInstance.console(`<<<<<<<<<< <<<<<<<<<< <<<<<<<<<< END   [${stepInstance.getStepType()}] ${stepId} status=${status}`)

    // Check the token is correct
    await schedulerSnapshotOfStepInstance.validateToken(token)

    //ZZZZ Check the status is valid


    // Update the status in the database
    // await dbStep.updateStatus(stepId, status, 100, 100)

    // Update the persisted step details
    await schedulerSnapshotOfStepInstance.setStatus(token, status)
    await schedulerSnapshotOfStepInstance.setNote(token, note)
    await schedulerSnapshotOfStepInstance.setPostTx(token, newTx)


    //ZZZZ Persist the step result
    if (DELETE_COMPLETED_STEPS_IMMEDIATELY) {
      delete this.stepIndex[stepId]
    }


    // This is a valid completion.
    await schedulerSnapshotOfStepInstance.callCompletionHandler(token, status, note, newTx)
    // const completionHandlerObj = await ResultReceiverRegister.getHandler(resultReceiver)
    // // console.log(`completionHandlerObj=`, completionHandlerObj)
    // await completionHandlerObj.haveResult(completionHandlerData, status, note, newTx)

    // Tell the parent it has finished
    // if (schedulerSnapshotOfStepInstance.parentId) {
    //   //
    //   const parentPipeline = this.stepIndex[schedulerSnapshotOfStepInstance.parentId]
    //   if (parentPipeline) {
    //     await parentPipeline.childResponse(this.stepNo, response)
    //   } else {
    //     throw Error(`Internal Error 26667: Unknown parent pipeline ${schedulerSnapshotOfStepInstance.parentId}`)
    //   }
    // } else {
    //   // Remember the exist status, so the API client can pick up the result

    // }

    // ZZZZ Probably should not wait for the handler
    // ZZZZ Should check for timeout
  }

  async getStepEntry(stepId) {
    const schedulerSnapshotOfStepInstance = this.stepIndex[stepId]
    // if (!schedulerSnapshotOfStepInstance) {
    //   // This is BAD, and should be reported. ZZZZZZ
    //   throw new Error(`Unknown step ${stepId} - possible hack attempt`)
    // }

    return schedulerSnapshotOfStepInstance ? schedulerSnapshotOfStepInstance : null
  }

  async dumpSteps(msg, transactionId) {
    if (msg) {
      console.log(`${msg}:`)
    }
    // console.log(`registered steps:\n`, this.stepIndex)
    const steps = await this.stepList(true, transactionId)
    // console.log(`registered steps:\n`, steps)
    const ul = '----------------------------------------------------------------------'
    console.log(``)
    console.log(`  ${pad('Seq', 14)}  ${pad('Step', 45)}  ${pad('Status', 10)}  ${pad('Description', 25)}  ${pad('Type', 15)}  ${pad('Note', 10)}`)
    console.log(`  ${pad(ul, 14)}  ${pad(ul, 45)}  ${pad(ul, 10)}  ${pad(ul, 25)}  ${pad(ul, 15)}  ${pad(ul, 25)}`)
    for (const step of steps) {
      console.log(`  ${pad(step.fullSequence, 14)}  ${pad(step.stepId, 45)}  ${pad(step.status, 10)}  ${pad(step.description, 25)}  ${pad(step.stepType, 15)}  ${step.note}`)
    }
    console.log(``)
  }

  async stepList(includeCompleted, transactionId) {
    // console.log(`stepList(${includeCompleted}, ${transactionId})`)
    // console.log(`this.gate = `, this.gate)

    const list = [ ]
    for (const stepId in this.stepIndex) {
      const schedulerSnapshotOfStepInstance = this.stepIndex[stepId]
      // console.log(`-> ${schedulerSnapshotOfStepInstance.step.id}`)
      const status = await schedulerSnapshotOfStepInstance.getStatus()
      const txId = await schedulerSnapshotOfStepInstance.getTransactionId()
      // console.log(`-> ${status}, ${txId} vs ${STEP_COMPLETED}`)
      if (status === STEP_COMPLETED && !includeCompleted) {
        // console.log(`  dud status`)
        continue
      }
      // const stepInstance = schedulerSnapshotOfStepInstance.stepInstance
      // const fullSequence = stepInstance.getSequence()
      // const stepId = stepInstance.getStepId()
      // const stepType = stepInstance.getStepType()
      if (transactionId && txId !== transactionId) {
        // console.log(`  dud tx`)
        continue
      }

      const fullSequence = await schedulerSnapshotOfStepInstance.getFullSequence()
      // const stepId = await schedulerSnapshotOfStepInstance.getStepId()
      const stepType = await schedulerSnapshotOfStepInstance.getStepType()
      const description = await schedulerSnapshotOfStepInstance.getDescription() // Before running step
      const note = await schedulerSnapshotOfStepInstance.getNote() // After running step

      // let stepType = schedulerSnapshotOfStepInstance.step.constructor.name
      // if (typeof(schedulerSnapshotOfStepInstance.step.getNote) == 'function') {
      //   note = await schedulerSnapshotOfStepInstance.step.getNote()
      // }
      // console.log(`add to list`)
      list.push({
        // name: gate.name,
        fullSequence,
        stepId,
        description,
        status: statusString(status),
        stepType,
        note,
      })
    }
    list.sort((a, b) => {
      const expandedA = expandSequences(a.fullSequence)
      const expandedB = expandSequences(b.fullSequence)
      // console.log(`expandedA=`, expandedA)
      // console.log(`expandedB=`, expandedB)
      if (expandedA < expandedB) return -1
      if (expandedA > expandedB) return +1
      return 0
    })
    // console.log(`list=`, list)
    return list
  }
}//- class Scheduler

function expandSequences(seq) {
  // Split into the name + sequences
  // Make the sequences fixed length by appending zeros to the front
  // Join them back together again
  const arr = seq.split('.')
  for (let i = 1; i < arr.length; i++) {
    arr[i] = "00000".substring(0, 5 - arr[i].length) + arr[i]
  }
  return arr.join('.')
}


const scheduler = new Scheduler()
export default scheduler
