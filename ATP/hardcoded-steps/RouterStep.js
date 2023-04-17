/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import assert from 'assert'
import { schedulerForThisNode } from '../..'
import { deepCopy } from '../../lib/deepCopy'
import { CHILD_PIPELINE_COMPLETION_CALLBACK } from '../Scheduler2/ChildPipelineCompletionCallback'
import { flow2Msg } from '../Scheduler2/flowMsg'
import { FLOW_VERBOSE } from '../Scheduler2/queuing/redis-lua'
import { luaEnqueue_startStep } from '../Scheduler2/queuing/redis-startStep'
import Scheduler2 from '../Scheduler2/Scheduler2'
import { F2_PIPELINE, F2_PIPELINE_CH } from '../Scheduler2/TransactionState'
import { GO_BACK_AND_RELEASE_WORKER } from '../Scheduler2/Worker2'
import Step from '../Step'
import StepTypeRegister from '../StepTypeRegister'

export const ROUTERSTEP_VERBOSE = 0


export class RouterStep extends Step {
  #field
  #map
  #defaultPipeline

  constructor(definition) {
    super(definition)

    if (definition.field) {
      this.#field = definition.field
    } else {
      this.#field = null
    }
    if (definition.map) {
      this.#map = definition.map
    } else {
      this.#map = null
    }
    if (definition.defaultPipeline) {
      this.#defaultPipeline = definition.defaultPipeline
    } else {
      this.#defaultPipeline = null
    }
  }//- constructor


  /**
   * Start this step.
   * This function will use the input data to determine a pipeline to run,
   * and will then initiate that pipeline.
   * 
   * @param {StepInstance} instance 
   * @returns GO_BACK_AND_RELEASE_WORKER
   */
  async invoke(instance) {
    if (ROUTERSTEP_VERBOSE) {
      // instance.trace(`*****`)
      instance.trace(`RouterStep::invoke(${instance.getStepId()})`)
    }

    // See which child pipeline to call.
    const pipelineName = await this.choosePipeline(instance)
    if (ROUTERSTEP_VERBOSE) instance.trace(`pipelineName=`, pipelineName)
    if (!pipelineName) {
      return await instance.failed(`Unknown value for selection field`, { status: 'error', error: 'Invalid selector field'})
    }

    const txId = instance.getTransactionId()//ZZZZ rename
    const tx = instance._7agghtstrajj_37(txId) // This is a magical internal function that we don't want people to use.


    const data = await instance.getDataAsObject()
    if (ROUTERSTEP_VERBOSE) instance.trace(`RouterStep.invokeChildPipeline() input is `, data)
    const childData = deepCopy(data)
    if (ROUTERSTEP_VERBOSE) instance.trace(`RouterStep.invokeChildPipeline() data for child pipeline is `, childData)
    const metadata = await instance.getMetadata()

    // Start the child pipeline
    if (ROUTERSTEP_VERBOSE) instance.trace(`Start child pipeline - ${pipelineName}`)
    const parentStepId = await instance.getStepId()
    const parentNodeGroup = instance.getNodeGroup() // ZZZZ shouldn't this be the current node?
    const myNodeGroup = schedulerForThisNode.getNodeGroup()

    const childStepId = await tx.addPipelineStep(parentStepId, 0, pipelineName)
    if (ROUTERSTEP_VERBOSE) instance.trace(`Start child pipeline ${pipelineName}`)
    instance.syncLogs()


    // Add the pipeline step as a child to f2
    const f2i = instance.vog_getF2i()
    const { f2i:childF2i, f2:childF2} = tx.vf2_addF2child(f2i, F2_PIPELINE, 'RouterStep.invoke')
    tx.setF2pipeline(childF2i, pipelineName)
    tx.setF2stepId(childF2i, childStepId)// Remove this, it can be derived from f2i => f2 [=> sibling] => stepId
    childF2.input = childData
    childF2.ts1 = Date.now()
    childF2.ts2 = 0
    childF2.ts3 = 0
    // const { f2:completionHandlerF2 } = tx.vf2_addF2child(f2i, F2_PIPELINE_CH, 'RouterStep.invoke')
    const { f2:completionHandlerF2, f2i:completionHandlerF2i } = tx.vf2_addF2sibling(f2i, F2_PIPELINE_CH, 'RouterStep.invoke')
    tx.setF2callback(completionHandlerF2i, CHILD_PIPELINE_COMPLETION_CALLBACK)
    tx.setF2nodeGroup(completionHandlerF2i, parentNodeGroup)

    const workerForShortcut = instance.getWorker()
    //VOGGY
    if (FLOW_VERBOSE) {
      // console.log(`---------------------------`)
      flow2Msg(tx, `RouterStep.invoke`)
      // console.log(`---------------------------`)
    }


    //ZZZZZ Stuff to delete
    await tx.delta(childStepId, {
      stepDefinition: pipelineName, // VOGVOGVOG
    })
    const event = {
      eventType: Scheduler2.STEP_START_EVENT,
      txId,
      parentNodeGroup,
      metadata: metadata,
      data: childData,
      // level: instance.getLevel() + 1,
      f2i: childF2i,
    }
    const delayBeforeQueueing = 0
    const result = await luaEnqueue_startStep('start-pipeline', tx, event, delayBeforeQueueing)
    // console.log(`luaEnqueue_startStep() result=`, result)


    // We need to tell the instance that we are returning without calling succeeded(), failed(), etc.
    // After this step completes, CHILD_PIPELINE_COMPLETION_CALLBACK will be called, and it
    // will wither start aother step, or call one of the completion functions.
    instance.stepWillNotCallCompletionFunction()
    return GO_BACK_AND_RELEASE_WORKER
  }//- invoke

  /**
   * Use the definition to determine the mapping, if the following are present:
   * {
   *    field: 'method', // Field whose value we check
   *    map: [
   *      { value: 'aaa', pipeline: 'pipeline-a' },
   *      { value: 'bbb', pipeline: 'pipeline-b' },
   *    ],
   *    defaultPipeline: 'pipeline-c'
   * }
   *
   * @param {StepInstance} instance
   */
   async choosePipeline(instance) {
    if (ROUTERSTEP_VERBOSE) instance.trace(`choosePipeline()`)

    // Check the definition is not invalid
    if (!this.#field) {
      return await instance.badDefinition(`RouterStep.choosePipeline - definition does not specify field`)
    }
    if (!this.#map) {
      return await instance.badDefinition(`RouterStep.choosePipeline - definition does not specify map`)
    }
    if (!Array.isArray(this.#map)) {
      return await instance.badDefinition(`RouterStep.choosePipeline - definition map has wrong type`)
    }

    // See what value the selection field has
    if (this.#field && this.#field !== '-') {

      // It would be nice if this handled nested values
      const data = instance.getDataAsObject()
      const value = data[this.#field]
      if (typeof(value) === 'undefined') {

        // The value is not defined, proceed to use the default pipeline
        instance.trace(`Field missing [${this.#field}]`)
      } else {

        // Look for the mapping for this value
        for (const row of this.#map) {
          if (row.value === value) {
            return row.pipeline
          }
        }
        instance.trace(`Unexpected value for field [${this.#field}]: ${value}`)
      }
    }

    // No mapping found
    if (this.#defaultPipeline) {
      return this.#defaultPipeline
    }
    return null
  }
}//- class


async function register() {
  await StepTypeRegister.register(myDef, 'util/child-pipeline', 'Route to a child pipeline')
}//- register

async function defaultDefinition() {
  return {
    field: 'selection-field',
    map: [
      { value: 'aaa', pipeline: 'pipeline-a' },
      { value: 'bbb', pipeline: 'pipeline-b' },
    ],
    defaultPipeline: 'optional-parameter'
  }
}

async function factory(definition) {
  const obj = new RouterStep(definition)
  return obj
}//- factory


const myDef = {
  register,
  factory,
  defaultDefinition,
}
export default myDef
