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
class ConvertDatesStep extends Step {
  #definition

  constructor(definition) {
    super(definition)
    // console.log(`definition=`, definition)
    this.#definition = definition
  }

  /**
   * This function is called to run this step. The step instance parameter
   * provides the context of the transaction and also convenience functions.
   * @param {StepInstance} instance
   */
  async invoke(instance) {
    instance.console(`ConvertDatesStep (${instance.getStepId()})`)
    console.log(`this.#definition=`, this.#definition)

    const data = await instance.getDataAsObject()

    const handler = new ConversionHandler()
    const errors = [ ]
    if (this.#definition.isoToMDY) {
      for (const fieldName of this.#definition.isoToMDY) {
        console.log(` - ${fieldName}`)
        const oldValue = handler.getValue(data, fieldName)
        if (oldValue == null) {
          continue
        }
        console.log(`  ${fieldName} = ${oldValue}`)

        let dudDateValue = true
        const parts = (oldValue.indexOf('/') > 0) ? oldValue.split('/') : oldValue.split('-')
        if (parts.length === 3) {
          try {
            let yr = parseInt(parts[0])
            let mth = parseInt(parts[1])
            let day = parseInt(parts[2])
            console.log(`- ${yr} ${mth} ${day}`)
            if (
              (yr >= 1000 && yr < 5000)
              && (mth >= 1 && mth <= 12)
              && (day >= 1 && mth <= 31)
            ) {
              dudDateValue = false
              const newDate = `${twoDigits(mth)}-${twoDigits(day)}-${twoDigits(yr)}`
              handler.setValue(data, fieldName, newDate)
            }
          } catch (e) { /* do nothing */ }
        }
        if (dudDateValue) {
          errors.push({ error: `Invalid ISO date in ${fieldName} (${oldValue}). Please use YYYY-MM-DD.`})
        }
      }//- for
    }

    // Time to complete the step and send a result
    if (errors.length > 0) {
      // console.log(`YARP finishing now with errors`)
      const reply = {
        status: 'valid',
        errors
      }
      return await instance.failed('Invalid request', reply)
    }

    // Time to complete the step and send a result
    console.log(`data=`, data)
    return instance.finish(Step.COMPLETED, '', data)
  }
}

function twoDigits(num) {
  let s = `${num}`
  while (s.length < 2) {
    s = '0' + s
  }
  return s
}


/**
 * This function is called to register this as an available step type.
 */
async function register() {
  await StepTypes.register(myDef, 'convertDates', 'Convert dates from ISO to Philippine format')
}//- register

/**
 * The data returned by this function will be the initial definition
 * when this step type is dragged into a pipeline.
 *
 * @returns Object
 */
 async function defaultDefinition() {
  return {
    "isoToMDY": [
      "sender.birthDate"
    ]
  }
}

/**
 *
 * @param {Object} definition Object created from the JSON definition of the step in the pipeline.
 * @returns New step instance
 */
async function factory(definition) {
  const rec = new ConvertDatesStep(definition)
  return rec
}//- factory


const myDef = {
  register,
  defaultDefinition,
  factory,
}
export default myDef
