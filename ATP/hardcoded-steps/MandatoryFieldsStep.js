/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import DATP, { ConversionHandler, FormsAndFields } from '../..'
import { generateErrorByName } from '../../lib/errorCodes'
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
 * function. When the step has completed running, it should call
 * instance.succeeded, instance.failed, etc.
 *
 * For long running options, the invoke function may return before the
 * step has completed, but some other part of your server will need to later
 * tell the Scheduler that the step has completed. See ZZZZ for more information.
 */
class MandatoryFieldsStep extends Step {
  #view
  #unknownFields // Error if given unrecognized fields
  #validateFieldTypes // Check the type of each field
  #errorsAsOutput // Errors do not fail the transaction, but get passed in the output
  #definition

  /**
   * Validations are defined like this:
   * ```javascript
   * validations: {
   *   'field.name': [ value1, value2, value3 ]
   * }
   * ```
   */
  #validations

  static UNKNOWN_FIELDS_IGNORE = 'ignore'
  static UNKNOWN_FIELDS_WARNING = 'warning'
  static UNKNOWN_FIELDS_ERROR = 'error'


  constructor(definition) {
    super(definition)
    // console.log(`MandatoryFieldsStep.constructor()`, definition)
    this.#definition = definition

    this.#view = definition.view
    this.#validations = definition.validations ? definition.validations : [ ]
    this.#unknownFields = (typeof(definition.unknownFields)==='undefined') ? definition.unknownFields : 'ignore'
    this.#validateFieldTypes = (typeof(definition.validateFieldTypes)==='undefined') ? false : !!definition.unknownFields
    this.#errorsAsOutput = (typeof(definition.errorsAsOutput)==='undefined') ? false : !!definition.errorsAsOutput
  }

  async validateView(instance, handler, viewName, errors, viewFieldIndex, lang) {
    if (typeof(viewName) != 'string') {
      await instance.badDefinition(`Parameter 'view' must be a string`)
      return { fatal: true }
    }

    // Check the view exists
    const views = await FormsAndFields.getForms(FORM_TENANT, this.#view)
    // console.log(`views=`, views)
    if (views.length === 0) {
      await instance.badDefinition(`Unknown view [${this.#view}]`)
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
      if (fld.type === 'amount3') {

        /*
         *  Special handling for amount3, which requires a three part object.
         */

        // Check the currency
        const value1 = handler.getSourceValue(`request:${fld.name}.currency`)
        if (value1 != null) {
          // Field found - perhaps check the type
          if (this.#validateFieldTypes && typeof(value1) !== 'string') {
            const error = generateErrorByName('FIELD_IS_INVALID', { field: `${fld.name}.currency` }, lang)
            errors.push(error)
          }
        } else {
          // Field not found. Is it mandatory?
          if (fld.mandatory) {
            const error = generateErrorByName('FIELD_IS_REQUIRED', { field: `${fld.name}.currency` }, lang)
            errors.push(error)
          }
        }

        // Check the unscaled amount
        const value2 = handler.getSourceValue(`request:${fld.name}.unscaledAmount`)
        if (value2 != null) {
          // Found - perhaps check it is a number (can be float)
          if (this.#validateFieldTypes && typeof(value2) !== 'number') {
            const error = generateErrorByName('FIELD_IS_INVALID', { field: `${fld.name}.unscaledAmount` }, lang)
            errors.push(error)
          }
        } else {
          // Not found - was it mandatory?
          if (fld.mandatory) {
            const error = generateErrorByName('FIELD_IS_REQUIRED', { field: `${fld.name}.unscaledAmount` }, lang)
            errors.push(error)
          }
        }

        // Check the scale
        const value3 = handler.getSourceValue(`request:${fld.name}.scale`)
        if (value3 != null) {
          // Found - perhaps check it is an integer
          if (this.#validateFieldTypes && !Number.isInteger(value3)) {
            const error = generateErrorByName('FIELD_IS_INVALID', { field: `${fld.name}.scale` }, lang)
            errors.push(error)
          }
        } else {
          // Not found - is it mandatory?
          if (fld.mandatory) {
            const error = generateErrorByName('FIELD_IS_REQUIRED', { field: `${fld.name}.scale` }, lang)
            errors.push(error)
          }
        }

      }//- fld.type === 'amount3'
      else {

        /*
          *  This is a Regular single-value field (not amount3)
          */
        // console.log(`==> mandatory ${fld.name} of type ${fld.type}`)
        const value = handler.getSourceValue(`request:${fld.name}`)
        // console.log(`    value=`, value)
        if (value !== null) {
          // Field is provided - check the type of it's value
          if (this.#validateFieldTypes) {
            let valid
            switch (fld.type) {
              case 'string':
                valid = typeof(value) === 'string'
                break
              case 'integer':
                valid = typeof(value) === 'number' && Number.isInteger(value)
                break
              case 'float':
              case 'amount':
                valid = typeof(value) === 'number'
                break
              case 'boolean':
                valid = typeof(value) === 'boolean'
                break
              case 'date':
              case 'time':
              case 'timestamp':
                valid = typeof(value) === 'string'
                break

              default:
                console.log(`Internal error: unknown field type [${fld.type}]`)
            }
            if (!valid) {
              const error = generateErrorByName('FIELD_IS_INVALID', { field: fld.name }, lang)
              errors.push(error)
            }

          }//- this.#validateFieldTypes
        } else {

          // Field is not provided. Is it mandatory?
          if (fld.mandatory) {
            const error = generateErrorByName('FIELD_IS_REQUIRED', { field: fld.name }, lang, null)
            errors.push(error)
          }
        }
      }

    }
    return { fatal: false }
  }

  /**
   * Check validations defined in the step definition.
   * Errors are added to the `errors` array.
   * 
   * @param {StepInstance} instance 
   * @param {ConversionHandler} handler Used to get field values
   * @param {object} validations Object { 'field.name': [ value1, value2, ...], ... }
   * @param {Array} errors
   * @param {string} lang 
   * @returns object { fatal }
   */
  async validateFields(instance, handler, validations, errors, lang) {
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
          // Invalid value
          const error = generateErrorByName('FIELD_ENUM_IS_INVALID', { field: fieldName, values }, lang, `{{field}} is invalid. Acceptable values: ${values}`)
          errors.push(error)
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
    instance.trace(`MandatoryFieldsStep (${instance.getStepId()})`)
    instance.trace(`"${this.#view}"`)

    const data = await instance.getDataAsObject()
    const lang = await instance.getLangFromMetadata()

    const handler = new ConversionHandler()
    handler.addSource('request', null, data)
    const errors = [ ]
    let viewName = null
    let viewFieldIndex = [ ]
    // let validateFieldTypes = (typeof(this.#definition.validateFieldTypes) === 'undefined') ? true : this.#definition.validateFieldTypes
    // let errorsAsOutput = (typeof(this.#definition.errorsAsOutput) === 'undefined') ? true : this.#definition.errorsAsOutput
    

    for (let def in this.#definition) {
      // console.log(`--------> `, def)
      switch (def) {

        case 'view':
          viewName = this.#definition.view
          let { fatal2 } = await this.validateView(instance, handler, viewName, errors, viewFieldIndex, lang)
          if (fatal2) {
            return
          }
          break

        case 'validations':
          // const validations = this.#definition.validations
          // console.log(`validations=`, validations)
          // console.log(`typeof(validations)=`, typeof(validations))
          let { fatal3 } = await this.validateFields(instance, handler, this.#validations, errors, lang)
          if (fatal3) {
            return
          }
          break

        case 'unknownFields':
        case 'stepType':
        case 'description':
        case 'validateFieldTypes':
        case 'errorsAsOutput':
          // Ignore these
          break

        default:
          return await instance.badDefinition(`Unknown parameter in step definition [${def}]`)
      }
    }

    // Check for unknown fields
    if (!this.#unknownFields) {
      // console.log(`Checking fields`, viewFieldIndex)
      if (!viewName) {
        return await instance.badDefinition(`Cannot use [unknownFields] parameter without specifying [view]`)
      }
      handler.recurseThroughAllFields('request', (fieldName, value) => {
        // console.log(`-> ${fieldName}, ${value}`)
        if (!viewFieldIndex[fieldName]) {
          const error = generateErrorByName('FIELD_IS_UNKNOWN', { field: fieldName }, lang, null)
          errors.push(error)
        }
      })
    }

    // Time to complete the step and send a result
    if (errors.length > 0) {

      // Add errors to the transaction log.
      instance.trace(`${errors.length} errors:`)
      for (const error of errors) {
        instance.error(`    ${error}`)

        // Strip out stuff the API user does not need.
        delete error.httpStatus
        delete error.scope
        delete error.data
      }

      // console.log(`this.#errorsAsOutput=`, this.#errorsAsOutput)
      if (this.#errorsAsOutput) {

        // Pass the errors in the output of the step
        instance.error(`Errors passed to the next step.`)
        data.mandatoryFieldErrors = errors
        return await instance.succeeded('With errors', data)

      } else {

        // Fail the step
        instance.error(`Step failed due to invalid input.`)
        return await instance.failed('Invalid input', errors)
      }
    }

    // Successful
    instance.trace(`Step success`)
    return await instance.succeeded('No errors', data)
  }
}

/**
 * This function is called to register this as an available step type.
 */
async function register() {
  await StepTypes.register(myDef, 'util/mandatory-fields', 'Verify data fields against form definition')
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
    validateFieldTypes: true,
    errorsAsOutput: false,
    unknownFields: 'error',

    // Field definitions
    "_view": "standard-form:Fields to be validated",
    "_errorsAsOutput": "checkbox:Send errors to the next step",
    "_validateFieldTypes": "checkbox:Validate field types",
    "_unknownFields": "select:How to handle unknown fields:ignore=Ignore:warning=Warning message:error=Error, cannot proceed",

    _showJSON: true,
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
