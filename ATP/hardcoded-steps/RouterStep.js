/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import assert from 'assert'
import { schedulerForThisNode } from '../..'
import { getPipelineVersionInUse } from '../../database/dbPipelines'
import { deepCopy } from '../../lib/deepCopy'
import GenerateHash from '../GenerateHash'
import { CHILD_PIPELINE_COMPLETION_CALLBACK } from '../Scheduler2/ChildPipelineCompletionCallback'
import { flow2Msg, flowMsg } from '../Scheduler2/flowMsg'
import { FLOW_VERBOSE } from '../Scheduler2/queuing/redis-lua'
import Scheduler2 from '../Scheduler2/Scheduler2'
import { F2_PIPELINE, F2_PIPELINE_CH, F2_STEP, F2_VERBOSE } from '../Scheduler2/TransactionState'
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
  //   return await this.invokeChildPipeline(instance, pipelineName, null)
  // }//- invoke


  // /**
  //  * 
  //  * @param {StepInstance} instance
  //  * @param {string} pipelineName 
  //  * @param {*} data 
  //  * @returns 
  //  */
  // async invokeChildPipeline(instance, pipelineName, data) {
  //   if (ROUTERSTEP_VERBOSE) {
  //     // instance.trace(`*****`)
  //     instance.trace(`RouterStep::invokeChildPipeline (${pipelineName})`)
  //   }

    const txId = instance.getTransactionId()//ZZZZ rename
    const tx = instance._7agghtstrajj_37(txId) // This is a magical internal function that we don't want people to use.


    // assert(data)
    // if (!data) {
    const data = await instance.getDataAsObject()
    // }
    if (ROUTERSTEP_VERBOSE) instance.trace(`RouterStep.invokeChildPipeline() input is `, data)
    const childData = deepCopy(data)
    if (ROUTERSTEP_VERBOSE) instance.trace(`RouterStep.invokeChildPipeline() data for child pipeline is `, childData)
    const metadata = await instance.getMetadata()

    // Start the child pipeline
    if (ROUTERSTEP_VERBOSE) instance.trace(`Start child pipeline - ${pipelineName}`)
    const parentStepId = await instance.getStepId()
    const parentNodeGroup = instance.getNodeGroup() // ZZZZ shouldn't this be the current node?
    const myNodeGroup = schedulerForThisNode.getNodeGroup()
    const myNodeId = schedulerForThisNode.getNodeId()
    // const childStepId = GenerateHash('s')

    // // Where does this pipeline run?
    // const pipelineDetails = await getPipelineVersionInUse(pipelineName)
    // if (ROUTERSTEP_VERBOSE) console.log(`RouterStep.invokeChildPipeline() - pipelineDetails:`, pipelineDetails)
    // if (!pipelineDetails) {
    //   throw new Error(`Unknown transaction type ${metadata.transactionType}`)
    // }
    // const childNodeGroup = pipelineDetails.nodeGroup
    // // const childNodeId = null

    const childStepId = await tx.addPipelineStep(parentStepId, 0, pipelineName)
    // await tx.delta(childStepId, { vogIsPipelineChild: true }, 'pipelineStep.invoke()')/// Temporary - remove this
    await tx.delta(childStepId, {
      vogAddedBy: 'RouterStep.invoke()'
    }, 'pipelineStep.invoke()')/// Temporary - remove this

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
    const childVogPath = `${instance.getVogPath()},1=R.${pipelineName}` // Start sequence at 1


    if (ROUTERSTEP_VERBOSE) instance.trace(`Start child pipeline ${pipelineName}`)
    instance.syncLogs()


    // Add the pipeline step as a child to f2
    const f2i = instance.vog_getF2i()
    const { f2i:childF2i, f2:childF2} = tx.vf2_addF2child(f2i, F2_PIPELINE, 'RouterStep.invoke')
    childF2._pipelineName = pipelineName
    childF2.stepId = childStepId
    childF2.input = childData
    childF2.ts1 = Date.now()
    childF2.ts2 = 0
    childF2.ts3 = 0
    // const { f2:completionHandlerF2 } = tx.vf2_addF2child(f2i, F2_PIPELINE_CH, 'RouterStep.invoke')
    const { f2:completionHandlerF2 } = tx.vf2_addF2sibling(f2i, F2_PIPELINE_CH, 'RouterStep.invoke')
    completionHandlerF2.callback = CHILD_PIPELINE_COMPLETION_CALLBACK
    completionHandlerF2.nodeGroup = parentNodeGroup


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
      // Need either a pipeline or a nodeGroup
      // yarpLuaPipeline: pipelineName,
      txId,
      // nodeGroup: childNodeGroup,
      // nodeId: childNodeGroup,
      // stepId: childStepId,
      parentNodeGroup,
      // parentNodeId,
      // parentStepId,
      // fullSequence: childFullSequence,
      // vogPath: childVogPath,
      // stepDefinition: pipelineName, // VOGVOGVOG
      metadata: metadata,
      data: childData,
      level: instance.getLevel() + 1,
      // onComplete: {
      //   nodeGroup: myNodeGroup,
      //   // nodeId: myNodeId,
      //   callback: CHILD_PIPELINE_COMPLETION_CALLBACK,
      //   context: { txId, parentNodeGroup, parentStepId, childStepId }
      // }
      f2i: childF2i,
    }
    const onComplete = {
      nodeGroup: myNodeGroup,
      // nodeId: myNodeId,
      callback: CHILD_PIPELINE_COMPLETION_CALLBACK,
//VOG777      context: { txId, parentNodeGroup, parentStepId, childStepId }
    }
    const parentFlowIndex = instance.vog_getFlowIndex()
    const parentF2i = instance.vog_getF2i()
    // console.log(`parentFlowIndex=`.bgMagenta, parentFlowIndex)
    // console.log(`parentF2i=`.bgMagenta, parentF2i)
    //VOG YARP THIS KILLS STUFF??? const parentF2i = instance.vf2_getF2i()
    const checkExternalIdIsUnique = false
    const rv = await schedulerForThisNode.enqueue_StartPipeline(tx, parentFlowIndex, childStepId, event, onComplete, checkExternalIdIsUnique, workerForShortcut)
    assert(rv === GO_BACK_AND_RELEASE_WORKER)

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
