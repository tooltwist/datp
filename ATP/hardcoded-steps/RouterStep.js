import { assert } from 'joi'
import { deepCopy } from '../../lib/deepCopy'
import GenerateHash from '../GenerateHash'
import { CHILD_PIPELINE_COMPLETION_CALLBACK } from '../Scheduler2/ChildPipelineCompletionCallback'
import Scheduler2, { DEFAULT_QUEUE } from '../Scheduler2/Scheduler2'
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
  }//- contructor


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

  async invokeChildPipeline(instance, pipelineName, data) {
    if (ROUTERSTEP_VERBOSE) {
      // instance.trace(`*****`)
      instance.trace(`RouterStep::invokeChildPipeline (${pipelineName})`)
    }

    const parentInstance = instance
    const txId = parentInstance.getTransactionId()//ZZZZ rename

    // assert(data)
    if (!data) {
      data = await parentInstance.getDataAsObject()
    }
    if (ROUTERSTEP_VERBOSE) instance.trace(`RouterStep.invokeChildPipeline() input is `, data)
    const childData = deepCopy(data)
    if (ROUTERSTEP_VERBOSE) instance.trace(`RouterStep.invokeChildPipeline() data for child pipeline is `, childData)
    const metadata = await parentInstance.getMetadata()

    // Start the child pipeline
    instance.trace(`Start child transaction pipeline - ${pipelineName}`)
    const parentStepId = await parentInstance.getStepId()
    const parentNodeGroup = parentInstance.getNodeGroup()
    const childStepId = GenerateHash('s')

    const childNodeGroup = parentNodeGroup

    //ZZZZZ We should check which node this pipeline runs on.
    const queueToPipelineNode = Scheduler2.standardQueueName(parentNodeGroup, DEFAULT_QUEUE)
    // console.log(`parentNodeGroup=`, parentNodeGroup)
    // console.log(`queueToPipelineNode=`, queueToPipelineNode)

    const childFullSequence = `${parentInstance.getFullSequence()}.1` // Start sequence at 1


    instance.trace(`Start child pipeline ${pipelineName}`)
    instance.syncLogs()


      // console.log(`metadata=`, metadata)
      // console.log(`txdata=`, txdata)
      // console.log(`parentNodeGroup=`, parentNodeGroup)
      await Scheduler2.enqueue_StepStart(queueToPipelineNode, {
        txId,
        nodeGroup: childNodeGroup,
        nodeId: childNodeGroup,
        stepId: childStepId,
        // parentNodeId,
        parentStepId,
        fullSequence: childFullSequence,
        stepDefinition: pipelineName,
        metadata: metadata,
        data: childData,
        level: parentInstance.getLevel() + 1,
        onComplete: {
          nodeGroup: parentNodeGroup,
          callback: CHILD_PIPELINE_COMPLETION_CALLBACK,
          context: { txId, parentNodeGroup, parentStepId, childStepId }
        }
      })
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
