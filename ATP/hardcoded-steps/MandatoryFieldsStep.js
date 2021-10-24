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
class MandatoryFieldsStep extends Step {
  #view
  #validations
  #unknownFields
  #definition

  static UNKNOWN_FIELDS_IGNORE = 'ignore'
  static UNKNOWN_FIELDS_WARNING = 'warning'
  static UNKNOWN_FIELDS_ERROR = 'error'


  constructor(definition) {
    super(definition)
    // console.log(`MandatoryFieldsStep.constructor()`, definition)
    this.#definition = definition


    this.#view = definition.view
    this.#validations = definition.validations ? definition.validations : [ ]
    this.#unknownFields = definition.unknownFields ? definition.unknownFields : 'ignore'
  }

  async validateView(instance, handler, viewName, errors, viewFieldIndex) {
    if (typeof(viewName) != 'string') {
      await instance.badDefinition(`Parameter 'view' must be a string`)
      return { fatal: true }
    }

    // Check the view exists
    const views = await FormsAndFields.getForms(FORM_TENANT, this.#view)
    // console.log(`views=`, views)
    if (views.length === 0) {
      // await instance.badDefinition(`view parameter must be a string`)
      errors.push(`Unknown view [${this.#view}]`)
      // return true
    }

    // Check all the fields exist
    const viewFields = await FormsAndFields.getFields(FORM_TENANT, viewName)
    // console.log(`viewFields=`, viewFields)
    if (viewFields.length === 0) {
      // await instance.failed(`No fields for `, data)
      // No fields in theis view
      return { fatal: false }
    }

    // Check all the mandatory fields exist
    for (const fld of viewFields) {
      // Add to the index we are creating
      if (fld.type === 'amount3') {
        // console.log(`ADDING FIELDS FOR amount3 - ${fld.name}`)
        viewFieldIndex[`${fld.name}.currency`] = true
        viewFieldIndex[`${fld.name}.unscaledAmount`] = true
        viewFieldIndex[`${fld.name}.scale`] = true
      } else {
        viewFieldIndex[fld.name] = true
      }

      // If the field is mandatory, check it is in the input
      // console.log(`fld=`, fld)
      if (fld.mandatory) {
        if (fld.type === 'amount3') {
          // console.log(`\n\n----- IS amount3 ----\n`)

          // Special handling for amount3, which requires a three part object.
          const value1 = handler.getSourceValue(`request:${fld.name}.currency`)
          const value2 = handler.getSourceValue(`request:${fld.name}.unscaledAmount`)
          const value3 = handler.getSourceValue(`request:${fld.name}.scale`)
          if (value1 === null) {
            errors.push(`Expected request to contain field [${fld.name}.currency]`)
          }
          if (value2 === null) {
            errors.push(`Expected request to contain field [${fld.name}.unscaledAmount]`)
          }
          if (value3 === null) {
            errors.push(`Expected request to contain field [${fld.name}.scale]`)
          }

        } else {
          // Regular field
          // console.log(`==> mandatory ${fld.name} of type ${fld.type}`)
          const value = handler.getSourceValue(`request:${fld.name}`)
          // console.log(`    value=`, value)
          if (value === null) {
            errors.push(`Expected request to contain field [${fld.name}]`)
          }
        }

      }
    }
    return { fatal: false }
  }

  async validateFields(instance, handler, validations, errors) {
    if (typeof(validations) != 'object') {
      await instance.badDefinition(`Parameter 'validations' must be an object`)
      return { fatal: true }
    }

    // Check all the mandatory fields exist
    for (const fieldName in validations) {
      const values = validations[fieldName]
      // console.log(`fieldName=`, fieldName)
      // console.log(`values=`, values)

      const actualValue = handler.getSourceValue(`request:${fieldName}`)
      // console.log(` - ${fieldName}=`, actualValue)
      if (actualValue !== null) {
        let ok = false
        for (const value of values) {
          if (actualValue === value) {
            ok = true
            break
          }
        }
        if (!ok) {
          errors.push(`Invalid value for field [${fieldName}]`)
        }
      }
    }
    return { fatal: false }
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
    const handler = new ConversionHandler()
    handler.addSource('request', null, data)
    const errors = [ ]
    let viewName = null
    let viewFieldIndex = [ ]
    let checkForUnknownFields = true

    for (let def in this.#definition) {
      // console.log(`--------> `, def)
      switch (def) {
        case 'view':
          viewName = this.#definition.view
          let { fatal2 } = await this.validateView(instance, handler, viewName, errors, viewFieldIndex)
          if (fatal2) {
            return
          }
          break
        case 'unknownFields':
          const unknownFields = this.#definition.unknownFields
          if (!unknownFields) {
            checkForUnknownFields = false
          }
          break
        case 'validations':
          const validations = this.#definition.validations
          // console.log(`validations=`, validations)
          // console.log(`typeof(validations)=`, typeof(validations))
          let { fatal3 } = await this.validateFields(instance, handler, validations, errors)
          if (fatal3) {
            return
          }
          break
        case 'stepType':
        case 'description':
          // Ignore these
          break
        default:
          return await instance.badDefinition(`Unknown parameter in definition [${def}]`)
      }
    }

    // Check for unkown fields
    if (checkForUnknownFields) {
      // console.log(`Checking fields`, viewFieldIndex)
      if (!viewName) {
        return await instance.badDefinition(`Cannot use [unknownFields] parameter without specifying [view]`)
      }
      handler.recurseThroughAllFields('request', (fieldName, value) => {
        // console.log(`-> ${fieldName}, ${value}`)
        if (!viewFieldIndex[fieldName]) {
          errors.push(`Unknown field [${fieldName}]`)
        }
      })
    }

    // Time to complete the step and send a result
    instance.console(`${errors.length} errors.`)
    if (errors.length > 0) {
      // console.log(`YARP finishing now with errors`)
      instance.console(`Step failed`)
      return await instance.failed('Invalid request', errors)
    }
    instance.console(`Step success`)
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
