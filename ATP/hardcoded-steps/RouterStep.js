/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import { assert } from 'joi'
import { schedulerForThisNode } from '../..'
import { getPipelineVersionInUse } from '../../database/dbPipelines'
import { deepCopy } from '../../lib/deepCopy'
import GenerateHash from '../GenerateHash'
import { CHILD_PIPELINE_COMPLETION_CALLBACK } from '../Scheduler2/ChildPipelineCompletionCallback'
import Scheduler2 from '../Scheduler2/Scheduler2'
import { GO_BACK_AND_RELEASE_WORKER } from '../Scheduler2/Worker2'
import Step from '../Step'
import StepTypeRegister from '../StepTypeRegister'
import XData from '../XData'

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

    // Start the child pipeline
    return await this.invokeChildPipeline(instance, pipelineName, null)
  }//- invoke


  /**
   * 
   * @param {StepInstance} instance
   * @param {string} pipelineName 
   * @param {*} data 
   * @returns 
   */
  async invokeChildPipeline(instance, pipelineName, data) {
    if (ROUTERSTEP_VERBOSE) {
      // instance.trace(`*****`)
      instance.trace(`RouterStep::invokeChildPipeline (${pipelineName})`)
    }

    const txId = instance.getTransactionId()//ZZZZ rename

    // assert(data)
    if (!data) {
      data = await instance.getDataAsObject()
    }
    if (ROUTERSTEP_VERBOSE) instance.trace(`RouterStep.invokeChildPipeline() input is `, data)
    const childData = deepCopy(data)
    if (ROUTERSTEP_VERBOSE) instance.trace(`RouterStep.invokeChildPipeline() data for child pipeline is `, childData)
    const metadata = await instance.getMetadata()

    // Start the child pipeline
    instance.trace(`Start child transaction pipeline - ${pipelineName}`)
    const parentStepId = await instance.getStepId()
    const parentNodeGroup = instance.getNodeGroup() // ZZZZ shouldn't this be the current node?
    const myNodeGroup = schedulerForThisNode.getNodeGroup()
    const myNodeId = schedulerForThisNode.getNodeId()
    const childStepId = GenerateHash('s')

    // Where does this pipeline run?
    const pipelineDetails = await getPipelineVersionInUse(pipelineName)
    if (ROUTERSTEP_VERBOSE) console.log(`RouterStep.invokeChildPipeline() - pipelineDetails:`, pipelineDetails)
    if (!pipelineDetails) {
      throw new Error(`Unknown transaction type ${metadata.transactionType}`)
    }
    const childNodeGroup = pipelineDetails.nodeGroup
    // const childNodeId = null

    // // If this pipeline runs in a different node group, we'll start it via the group
    // // queue for that nodeGroup. If the pipeline runs in the current node group, we'll
    // // run it in this current node, so it'll have access to the cached transaction.
    // let queueToNewPipeline
    // if (childNodeGroup === myNodeGroup) {
    //   // Run the new pipeline in this node - put the event in this node's pipeline.
    //   queueToNewPipeline = Scheduler2.nodeRegularQueueName(myNodeGroup, myNodeId)
    // } else {
    //   // The new pipeline will run in a different nodeGroup. Put the event in the group queue.
    //   queueToNewPipeline = Scheduler2.groupQueueName(childNodeGroup)
    // }

    // const queueToPipelineNode = Scheduler2.groupQueueName(parentNodeGroup)
    // console.log(`parentNodeGroup=`, parentNodeGroup)
    // console.log(`queueToPipelineNode=`, queueToPipelineNode)

    const childFullSequence = `${instance.getFullSequence()}.1` // Start sequence at 1


    instance.trace(`Start child pipeline ${pipelineName}`)
    instance.syncLogs()

    const workerForShortcut = instance.getWorker()
    await schedulerForThisNode.schedule_StepStart(childNodeGroup, null, workerForShortcut, {
      txId,
      nodeGroup: childNodeGroup,
      // nodeId: childNodeGroup,
      stepId: childStepId,
      // parentNodeId,
      parentStepId,
      fullSequence: childFullSequence,
      stepDefinition: pipelineName,
      metadata: metadata,
      data: childData,
      level: instance.getLevel() + 1,
      onComplete: {
        nodeGroup: myNodeGroup,
        nodeId: myNodeId,
        callback: CHILD_PIPELINE_COMPLETION_CALLBACK,
        context: { txId, parentNodeGroup, parentStepId, childStepId }
      }
    })

    // We need to tell the instance that we are returning without calling succeeded(), failed(), etc.
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

    // See what value the selection field hs
    const data = instance.getDataAsObject()
    // It would be nice if this handled nested values
    const value = data[this.#field]

    // Look for the mapping for this value
    for (const row of this.#map) {
      if (row.value === value) {
        return row.pipeline
      }
    }
    if (typeof(value) === 'undefined') {
      instance.trace(`Field missing [${this.#field}]`)
    } else {
      instance.trace(`Unknown value for field [${this.#field}]`)
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
