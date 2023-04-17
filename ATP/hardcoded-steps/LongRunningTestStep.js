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
 * function. When the step has completed running, it should call
 * instance.succeeded, instance.failed, etc.
 *
 * For long running options, the invoke function may return before the
 * step has completed, but some other part of your server will need to later
 * tell the Scheduler that the step has completed. See ZZZZ for more information.
 */
class LongRunningTestStep extends Step {
  #duration

  constructor(definition) {
    super(definition)
    this.#duration = parseInt(definition.duration)
    if (isNaN(this.#duration)) {
      this.#duration = 120 // default
    }
  }

  /**
   * This function is called to run this step. The step instance parameter
   * provides the context of the transaction and also convenience functions.
   * @param {StepInstance} instance
   */
  async invoke(instance) {
    // instance.trace(`LongRunningTestStep (${instance.getStepId()})`)
    // const data = instance.getDataAsObject()

    // Do something here
    const input = instance.getDataAsObject()

    const duration = 2 * 60
    await pause(duration)

    const note = `Paused for ${duration} seconds`
    const output = input
    return await instance.succeeded(note, output)
  }//- invoke
}

/**
 * This function is called to register this as an available step type.
 */
async function register() {
  await StepTypes.register(myDef, 'test/longRunning', 'Long running step - use for testing only')
}//- register

/**
 * The data returned by this function will be the initial definition
 * when this step type is dragged into a pipeline.
 *
 * @returns Object
 */
 async function defaultDefinition() {
  return {
    "duration": "seconds"
  }
}

/**
 *
 * @param {Object} definition Object created from the JSON definition of the step in the pipeline.
 * @returns New step instance
 */
async function factory(definition) {
  const rec = new LongRunningTestStep(definition)
  return rec
}//- factory

async function pause(seconds) {
  return new Promise((resolve, reject) => {
    setTimeout(() => {
      resolve(null)
    }, seconds * 1000)
  })
}

const myDef = {
  register,
  defaultDefinition,
  factory,
}
export default myDef
