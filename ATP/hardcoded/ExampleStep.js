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
  #msg = null
  #delta = 0.0

  constructor(definition) {
    // console.log(`ExampleStep.constructor()`, definition)
    super(definition)
    if (typeof(definition.msg) === 'string') {
      this.#delta = definition.amt
    }
    if (typeof(definition.amt) === 'number') {
      this.#delta = definition.amt
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
    instance.console(`this.#msg is ${this.#msg} (${typeof(this.#msg)})`)
    instance.console(`this.#delta is ${this.#delta} (${typeof(this.#delta)})`)

    const meta = await instance.getMetadata()
    instance.console(`meta=`, meta)

    // Do something here
    //...
    if (this.#msg) {
      console.log(`${this.#msg}`)
    }
    const data = instance.getData()
    instance.console(`data before =`, data)
    if (typeof(data.amt) !== 'number') {
      instance.console('Initializing data.amt')
      data.amt = 0.0
    }
    data.amt += this.#delta
    instance.console(`data after =`, data)


    // Time to complete the step and send a result
    // const note = 'whatever'
    // instance.finish(Step.COMPLETED, note, tx)
    instance.succeeded(data)
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
    msg: 'Running example step!!!!!',
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
