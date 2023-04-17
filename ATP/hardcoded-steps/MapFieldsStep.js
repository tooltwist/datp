/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import { ConversionHandler, FormsAndFields } from '../..'
// import ConversionHandler from '../../CONVERSION/lib/ConversionHandler'
import Step from '../Step'
import StepInstance from '../StepInstance'
import StepTypes from '../StepTypeRegister'

const FORM_TENANT = 'datp'
const VERBOSE = 0

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
class MapFieldsStep extends Step {
  #mappingId
  #targetView

  // Additional conversion definitions
  #convertAmounts
  #convertDates
  #appendFields

  constructor(definition) {
    super(definition)
    // console.log(`definition=`, definition)
    this.#mappingId = definition.mappingId
    this.#targetView = definition.targetView ? definition.targetView : this.#mappingId

    this.#convertAmounts = definition.convertAmounts ? definition.convertAmounts : [ ]
    this.#convertDates = definition.convertDates ? definition.convertDates : [ ]
    this.#appendFields = definition.appendFields ? definition.appendFields : [ ]
  }

  /**
   * This function is called to run this step. The step instance parameter
   * provides the context of the transaction and also convenience functions.
   * @param {StepInstance} instance
   */
  async invoke(instance) {
    instance.trace(`MapFields (${instance.getStepId()})`)
    instance.trace(`"${this.#mappingId}"`)

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
    handler.convert(instance, mapping, targetFieldIndex)
    // instance.debug(`newData=`, newData)

    // Handle any conversion or append operations.
    if (
      await this.doConvertAmounts(instance, handler)
      ||
      await this.doConvertDates(instance, handler)
      ||
      await this.doAppendFields(instance, handler)
    ) {
      // Already finished the step with an error.
      return
    }

    // Prepare the output
    const newData = handler.getResult()
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
    return instance.succeeded('', newData)
  }

  async doConvertAmounts(instance, handler) {
    if (VERBOSE) console.log(`doConvertAmounts() have ${this.#convertAmounts.length} rules`)

    // Iterate through the rules
    for (const rule of this.#convertAmounts) {
      if (VERBOSE) console.log(`rule=`, rule)

      if (!Array.isArray(rule.from)) {
        await instance.badDefinition(`convertAmounts rule: 'from' must be provided as an array of { path, type }`)
        return true
      }
      if (!Array.isArray(rule.to)) {
        await instance.badDefinition(`convertAmounts rule: 'to' must be provided as an array of { path, type }`)
        return true
      }

      // Get the values from the source
      let currency = 'PHP'
      let unscaledAmount = 12345
      let scale = 2
      for (const fromField of rule.from) {
        if (typeof(fromField) !== 'object') {
          await instance.badDefinition(`convertAmounts rule: 'from' must be provided as an array of { path, type }`)
          return true
        }
        if (typeof(fromField.path) !== 'string') {
          await instance.badDefinition(`convertAmounts rule: 'from' must be provided as an array of { path, type }`)
          return true
        }
        if (typeof(fromField.type) !== 'string') {
          await instance.badDefinition(`convertAmounts rule: 'from' must be provided as an array of { path, type }`)
          return true
        }
        const value = handler.getSourceValue(`request:${fromField.path}`)
        console.log(`Got value ${value} from ${fromField.path}`)
        switch (fromField.type.toLowerCase()) {
          case 'iso':
          case 'amount3':
            currency = value.currency
            unscaledAmount = value.unscaledAmount
            scale = value.scale
            if (VERBOSE) console.log(`From amount3 (${currency}, ${unscaledAmount}, ${scale})`)
            break
          case 'currency':
            currency = value
            if (VERBOSE) console.log(`From currency (${currency})`)
            break
          case 'unscaledAmount':
            unscaledAmount = value
            if (VERBOSE) console.log(`From unscaledAmount (${unscaledAmount})`)
            break
          case 'scale':
            scale = value
            if (VERBOSE) console.log(`From scale (${scale})`)
            break
          default:
            await instance.badDefinition(`convertAmounts rule: 'type' must be one of iso|amount3|currency|unscaledAmpount|scale`)
            return true
        }//- switch
      }//- fromField

      // Save value(s)
      for (const toField of rule.to) {
        if (typeof(toField) !== 'object') {
          await instance.badDefinition(`convertAmounts rule: 'to' must be provided as an array of { path, type }`)
          return true
        }
        if (typeof(toField.path) !== 'string') {
          await instance.badDefinition(`convertAmounts rule: 'to' must be provided as an array of { path, type }`)
          return true
        }
        if (typeof(toField.type) !== 'string') {
          await instance.badDefinition(`convertAmounts rule: 'to' must be provided as an array of { path, type }`)
          return true
        }
        switch (toField.type.toLowerCase()) {
          case 'iso':
          case 'amount3':
            handler.setTargetValue(`${toField.path}.currency`, currency)
            handler.setTargetValue(`${toField.path}.unscaledAmount`, unscaledAmount)
            handler.setTargetValue(`${toField.path}.scale`, scale)
            console.log(`To amount3 (${currency}, ${unscaledAmount}, ${scale})`)
            break
          case 'currency':
            handler.setTargetValue(toField.path, currency)
            console.log(`To currency (${currency})`)
            break
          case 'unscaledAmount':
            handler.setTargetValue(toField.path, unscaledAmount)
            console.log(`To unscaledAmount (${unscaledAmount})`)
            break
          case 'scale':
            handler.setTargetValue(toField.path, scale)
            console.log(`To scale (${scale})`)
            break
          case 'amount':
            let amount = unscaledAmount
            // for (let i = 0; i < scale; i++) { amount /= 10.0 }
            // for (let i = 0; i > scale; i--) { amount *= 10.0 }
            // amount = Math.round(amount)
            handler.setTargetValue(toField.path, amount)
            if (VERBOSE) console.log(`To amount (${amount})`)
            break
          default:
            await instance.badDefinition(`convertAmounts rule: 'type' must be one of iso|amount3|currency|unscaledAmpount|scale`)
            return true
          }//- switch
      }//- toField
    }//- next rule
    return false
  }

  async doConvertDates(instance, handler) {
    if (VERBOSE) console.log(`doConvertDates() have ${this.#convertDates.length} rules`)

    //ZZZZZ Not yet
    return false
  }

  /**
   * Handle the field appending rule. For example:
   *```
   *  "appendFields": [
   *    {
   *        "from": [
   *            "beneficiary.name.firstName",
   *            "beneficiary.name.middleName",
   *            "beneficiary.name.lastName"
   *        ],
   *        "to": "beneficiary.name"
   *    }
   *  ]
   *```
   * @param {StepInstance} instance
   * @param {ConversionHandler} handler
   */
  async doAppendFields(instance, handler) {
    if (VERBOSE) console.log(`doAppendFields() have ${this.#appendFields.length} rules`)

    // Iterate through the rules
    for (const rule of this.#appendFields) {
      // console.log(`rule=`, rule)
      if (!Array.isArray(rule.from)) {
        await instance.badDefinition(`appendField rule: 'from' must be provided as an array of field paths`)
        return true
      }
      if (typeof(rule.to) !== 'string') {
        await instance.badDefinition(`appendField rule: 'to' must be provided as a field path`)
        return true
      }

      // Concatenate the fields
      let s = ''
      let sep = ''
      for (const fromField of rule.from) {
        const value = handler.getSourceValue(`request:${fromField}`)
        s += `${sep}${value}`
        sep = ' '
      }

      // Save the result
      handler.setTargetValue(rule.to, s)
    }//- next rule
    return false
  }//- doAppendFields

}

/**
 * This function is called to register this as an available step type.
 */
async function register() {
  await StepTypes.register(myDef, 'util/map-fields', 'Convert form using field mapping')
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

    _mappingId: 'string:Mapping Id',
    mappingId: "domain_service_request",

    retainInput: '_original',
    _retainInput: 'string:Retain the input as',

    _convertAmountsZ: 'text:Convert amount fields',
    _convertDatesZ: 'text:Convert dates',
    _appendFieldsZ: 'text:AppendFields',

    _showJSON: true
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
