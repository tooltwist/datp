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
class MandatoryFieldsStep extends Step {
  #view

  constructor(definition) {
    super(definition)
    // console.log(`definition=`, definition)
    this.#view = definition.view
  }

  /**
   * This function is called to run this step. The step instance parameter
   * provides the context of the transaction and also convenience functions.
   * @param {StepInstance} instance
   */
  async invoke(instance) {
    instance.console(`MandatoryFieldsStep (${instance.getStepId()})`)
    instance.console(`"${this.#view}"`)

    const data = await instance.getDataAsObject()

    if (!this.#view) {
      return await instance.badDefinition(`Missing parameter [view]`)
    }

    // Check the view exists
    const views = await formsAndFields.getForms(FORM_TENANT, this.#view)
    // console.log(`views=`, views)
    if (views.length === 0) {
      return instance.fail(`Unknown view ${this.#view}`, { })
    }


    const viewFields = await formsAndFields.getFields(FORM_TENANT, this.#view)
    // console.log(`viewFields=`, viewFields)
    if (viewFields.length === 0) {
      return await instance.fail(note, data)
    }

    const handler = new ConversionHandler()
    handler.addSource('request', null, data)

    // Check all the mandatory fields exist
    const errors = [ ]
    for (const fld of viewFields) {
      // console.log(`fld=`, fld)
      if (fld.mandatory) {
        // console.log(`==> mandatory ${fld.name} of type ${fld.type}`)
        const value = handler.getSourceValue(`request:${fld.name}`)
        // console.log(`    value=`, value)
        if (value === null) {
          errors.push(`Expected request to contain ${fld.name}`)
        }
      }
    }

    if (errors.length > 0) {
      // console.log(`YARP finishing now with errors`)
      return await instance.finish(Step.FAIL, 'Invalid request', errors)
    }

    // Time to complete the step and send a result
    return await instance.finish(Step.COMPLETED, '', data)
  }
}

/**
 * This function is called to register this as an available step type.
 */
async function register() {
  await StepTypes.register(myDef, 'MandatoryFieldsStep', 'Verify data fields against form definition')
}//- register

/**
 * The data returned by this function will be the initial definition
 * when this step type is dragged into a pipeline.
 *
 * @returns Object
 */
 async function defaultDefinition() {
  return {
    "view": "std-SERVICE-request",
  }
}

/**
 *
 * @param {Object} definition Object created from the JSON definition of the step in the pipeline.
 * @returns New step instance
 */
async function factory(definition) {
  const rec = new MandatoryFieldsStep(definition)
  return rec
}//- factory


const myDef = {
  register,
  defaultDefinition,
  factory,
}
export default myDef
