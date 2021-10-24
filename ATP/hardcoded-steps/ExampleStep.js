/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import Step from "../Step"
import StepTypes from '../StepTypeRegister'

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
class ExampleStep extends Step {
  #delta

  constructor(definition) {
    console.log(`ExampleStep.constructor()`, definition)
    super(definition)
    console.log(`zzzzz=`, typeof(definition.delta))
    switch (typeof(definition.delta)) {
      case 'string':
        this.#delta = parseFloat(definition.delta)
        break

      case 'number':
        this.#delta = definition.delta
        break

      default:
        console.log(`Invalid or missing 'definition.delta'.`)
        this.#delta = 0.0
    }
    // console.log(`this.#delta=`, this.#delta)
  }

  /**
   * This function is called to run this step. The step instance parameter
   * provides the context of the transaction and also convenience functions.
   * @param {StepInstance} instance
   */
  async invoke(instance) {
    instance.console(`ExampleStep(hardcoded) (${instance.getStepId()})`)
    instance.console(`this.#delta is ${this.#delta} (${typeof(this.#delta)})`)

    const meta = await instance.getMetadata()
    instance.console(`meta=`, meta)

    // Do something here
    //...
    const data = instance.getDataAsObject()
    instance.console(`data before =`, data)
    instance.console(`data before =`, typeof data)
    instance.console(`data before =`, data.toString())
    if (typeof(data.amt) !== 'number') {
      instance.console('Initializing data.amt')
      data.amt = 0.0
    }
    data.amt += this.#delta
    instance.console(`data after =`, data)


    // Time to complete the step and send a result
    // const note = 'whatever'
    // instance.finish(Step.COMPLETED, note, tx)
    const note = ''
    instance.succeeded(note, data)
  }
}

/**
 * This function is called to register this as an available step type.
 */
async function register() {
  await StepTypes.register(myDef, 'exampleStep', 'Example')
}//- register

/**
 * The data returned by this function will be the initial definition
 * when this step type is dragged into a pipeline.
 *
 * @returns Object
 */
 async function defaultDefinition() {
  return {
    delta: 1.5
  }
}

/**
 *
 * @param {Object} definition Object created from the JSON definition of the step in the pipeline.
 * @returns New step instance
 */
async function factory(definition) {
  const rec = new ExampleStep(definition)
  return rec
}//- factory


const myDef = {
  register,
  defaultDefinition,
  factory,
}
export default myDef
