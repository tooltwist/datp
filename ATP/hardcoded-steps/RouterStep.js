import Step from '../Step'
import StepTypeRegister from '../StepTypeRegister'
import Scheduler from '../Scheduler'
import XData from '../XData'
import ChildPipelineCompletionHandler from './ChildPipelineCompletionHandler'

const VERBOSE = true


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
    if (VERBOSE) {
      // instance.console(`*****`)
      instance.console(`RouterStep::invoke(${instance.getStepId()})`)
    }
    instance.log(``)
    instance.console()
    instance.console(`kycInitiateRouterStep initiating child pipeline`)
    instance.console()

    // See which child pipeline to call.
    const pipelineName = await this.choosePipeline(instance)
    console.log(`pipelineName=`, pipelineName)
    if (!pipelineName) {
      return await instance.failed(`Unknown value for selection field`, { status: 'error', error: 'Invalid selector field'})
    }

    // Start the child pipeline
    return await this.invokeChildPipeline(instance, pipelineName, null)
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
    console.log(`choosePipeline()`)

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
      instance.console(`Field missing [${this.#field}]`)
    } else {
      instance.console(`Unknown value for field [${this.#field}]`)
    }

    // No mapping found
    if (this.#defaultPipeline) {
      return this.#defaultPipeline
    }
    return null
  }

  async invokeChildPipeline(instance, pipelineName, data) {
    if (VERBOSE) {
      // instance.console(`*****`)
      instance.console(`RouterStep::invokeChildPipeline (${pipelineName})`)
    }
    instance.log(``)
    instance.console()
    instance.console(`RouterStep initiating child pipeline`)
    instance.console()

    if (!data) {
      data = await instance.getDataAsObject()
    }

    // Start the child pipeline
    instance.console(`Start child transaction pipeline ${pipelineName}`)
    const parentInstance = instance
    const parentStepId = await parentInstance.getStepId()
    const txId = await parentInstance.getTransactionId()
    const sequenceYARP = txId.substring(txId.length - 8)
    const definition = pipelineName
    const contextForCompletionHandler = {
      txId,
      parentStepId
    }
    const childData = new XData(data)
    const logbook = instance.getLogbook()
    const completionHandler = ChildPipelineCompletionHandler.CHILD_PIPELINE_COMPLETION_HANDLER_NAME
    return await Scheduler.invokeStep(txId, parentInstance, sequenceYARP, definition, childData, logbook, completionHandler, contextForCompletionHandler)
  }//- invoke
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
