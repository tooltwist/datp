import Step from "../Step"
import StepTypes from '../StepTypeRegister'
import ConversionHandler from '../../CONVERSION/lib/ConversionHandler'
import formsAndFields from "../../CONVERSION/lib/formsAndFields-dodgey"


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
    // const serviceDetails = await formsAndFields.getServiceDetails(provider, service)
    // if (!serviceDetails) {
    //   // return next(new errors.NotImplementedError(`Provider ${provider} does not support ${service}`))
    //   // instance.fail
    //   //ZZZZ
    //   // instance.log(`Provider ${provider} does not support ${service}`)
    //   const note = `Provider ${provider} does not support ${service}`
    //   return instance.finish(Step.FAIL, note, instance.getDataAsObject())
    // }
    // console.log(`serviceDetails=`, serviceDetails)

    // Check the view exists
    const views = await formsAndFields.getForms(FORM_TENANT, this.#targetView)
    // console.log(`views=`, views)
    if (views.length === 0) {
      return instance.finish(Step.FAIL, `Unknown view ${this.#targetView}`, { })
    }


    // const requestView = this.#form
    // console.log(`requestView=`, requestView)
    // // const version = serviceDetails.request_version
    // const version = "1.0"

    // const viewFields = await formsAndFields.getFields(FORM_TENANT, this.#form)
    // // console.log(`viewFields=`, viewFields)
    // if (viewFields.length === 0) {
    //   return instance.finish(Step.FAIL, note, data)
    // }


    const version = -1
    const mapping = await formsAndFields.getMapping(FORM_TENANT, this.#mappingId, version)
    // console.log(`mapping=`, mapping)


    const handler = new ConversionHandler()
    // console.log(`MY data=`, data)
    // console.log(`MY data=`, data.toString())
    // console.log(`MY data=`, typeof(data))
    handler.addSource('request', null, data)
    // handler.addSource('auth', null, {
    //   userId: 123,
    //   email: 'philcal@mac.com',
    // })
    // const newData = {
    //   yarp: 'whammo!',
    // }
    // handler.addSource('target', null, newData)
    const newData = handler.convert(mapping)
    console.log(`newData=`, newData)


    if (data._mapFields) {
      newData._mapFields = data._mapFields
    } else {
      newData._mapFields = [ ]
    }
    newData._mapFields.push({
      mappingId: this.#mappingId,
      targetView: this.#targetView,
      // input: data
    })




    // // Check all the mandatory fields exist
    // const errors = [ ]
    // for (const fld of viewFields) {
    //   // console.log(`fld=`, fld)
    //   if (fld.mandatory) {
    //     // console.log(`==> mandatory ${fld.name} of type ${fld.type}`)
    //     const value = handler.getSourceValue(`request:${fld.name}`)
    //     // console.log(`    value=`, value)
    //     if (value === null) {
    //       errors.push(`Expected request to contain ${fld.name}`)
    //     }
    //   }
    // }
    // // console.log(`errors=`, errors)
    // if (errors.length > 0) {
    //   // console.log(`YARP finishing now with errors`)
    //   return instance.finish(Step.FAIL, 'Invalid request', errors)
    // }

    // const requestMapping = await formsAndFields.getMapping('datp', requestView, version)
    // console.log(`requestMapping=`, requestMapping)
    // console.log(`+++++++++++++++++++^^^^^^^^^^^^^^^^^^^`)


    // ConversionHandler.

    // const newData = data
    // Time to complete the step and send a result
    return instance.finish(Step.COMPLETED, '', newData)
  }
}

/**
 * This function is called to register this as an available step type.
 */
async function register() {
  await StepTypes.register(myDef, 'MapFields', 'Convert form using field mapping')
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
