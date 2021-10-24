/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import { ConversionHandler, FormsAndFields } from '../..'
import Step from '../Step'
import StepTypes from '../StepTypeRegister'

const FORM_TENANT = 'datp'

/**
 * This class represents a type of step, not an actual instance of a step
 * within a pipeline. In your application code you will need to call the
 * 'register' function below to register the step type. It can then be used
 * in pipelines.
 *
 * When a pipeline needs an actual step of this type, it will call the
 * 'factory' function. Note that the pipeline passes a definition to this
 * factory, so not all instances of this step type will be the same - you
 * can write this step to perform however you like, based upon the definition
 * it receives.
 *
 * When it is time to run the step, the pipeline will call the 'invoke'
 * function. When the step has completed running, it should call the
 * instance.finish function, including the completion status.
 *
 * For long running options, the invoke function may return before the
 * step has completed, but some other part of your server will need to later
 * tell the Scheduler that the step has completed. See ZZZZ for more information.
 */
class MapFieldsStep extends Step {
  #mappingId
  #targetView

  constructor(definition) {
    super(definition)
    // console.log(`definition=`, definition)
    this.#mappingId = definition.mappingId
    this.#targetView = definition.targetView ? definition.targetView : this.#mappingId
  }

  /**
   * This function is called to run this step. The step instance parameter
   * provides the context of the transaction and also convenience functions.
   * @param {StepInstance} instance
   */
  async invoke(instance) {
    instance.console(`MapFields (${instance.getStepId()})`)
    instance.console(`"${this.#mappingId}"`)

    const data = await instance.getDataAsObject()

    // Load the view definition
    // const provider = 'std'
    // const service = 'transfer'
    // const serviceDetails = await FormsAndFields.getServiceDetails(provider, service)
    // if (!serviceDetails) {
    //   // return next(new errors.NotImplementedError(`Provider ${provider} does not support ${service}`))
    //   // instance.fail
    //   //ZZZZ
    //   // instance.log(`Provider ${provider} does not support ${service}`)
    //   const note = `Provider ${provider} does not support ${service}`
    //   return instance.failed(note, instance.getDataAsObject())
    // }
    // console.log(`serviceDetails=`, serviceDetails)

    // Check the view exists
    const views = await FormsAndFields.getForms(FORM_TENANT, this.#targetView)
    // console.log(`views=`, views)
    if (views.length === 0) {
      return instance.failed(`Unknown view ${this.#targetView}`, { })
    }


    // const requestView = this.#form
    // console.log(`requestView=`, requestView)
    // // const version = serviceDetails.request_version
    // const version = "1.0"

    // Get details of the destination fields
    const targetFields = await FormsAndFields.getFields(FORM_TENANT, this.#targetView)
    const targetFieldIndex = [ ]
    for (const field of targetFields) {
      targetFieldIndex[field.name] = field
    }

    // Get the field mapping, from request to target
    const version = -1
    const mapping = await FormsAndFields.getMapping(FORM_TENANT, this.#mappingId, version)

    // Convert the objects
    const handler = new ConversionHandler()
    handler.addSource('request', null, data)
    const newData = handler.convert(mapping, targetFieldIndex)
    console.log(`newData=`, newData)


    if (data._mapFields) {
      newData._mapFields = data._mapFields
    } else {
      newData._mapFields = [ ]
    }
    newData._mapFields.push({
      mappingId: this.#mappingId,
      targetView: this.#targetView,
    })

    // Time to complete the step and send a result
    return instance.finish(Step.COMPLETED, '', newData)
  }
}

/**
 * This function is called to register this as an available step type.
 */
async function register() {
  await StepTypes.register(myDef, 'util/mapFields', 'Convert form using field mapping')
}//- register

/**
 * The data returned by this function will be the initial definition
 * when this step type is dragged into a pipeline.
 *
 * @returns Object
 */
 async function defaultDefinition() {
  return {
    "targetView": "domain_service_request",
    "mappingId": "domain_service_request",
  }
}

/**
 *
 * @param {Object} definition Object created from the JSON definition of the step in the pipeline.
 * @returns New step instance
 */
async function factory(definition) {
  const rec = new MapFieldsStep(definition)
  return rec
}//- factory


const myDef = {
  register,
  defaultDefinition,
  factory,
}
export default myDef
