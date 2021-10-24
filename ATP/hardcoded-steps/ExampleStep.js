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

  constructor(definition) {
    super(definition)
    this.someValue = definition.someValue
  }

  /**
   * This function is called to run this step. The step instance parameter
   * provides the context of the transaction and also convenience functions.
   * @param {StepInstance} instance
   */
  async invoke(instance) {
    instance.console(`ExampleStep (${instance.getStepId()})`)
    instance.console(`"${this.someValue}"`)
    const data = instance.getDataAsObject()

    // Do something here
    //...

    // Time to complete the step and send a result
    const note = ''
    instance.succeeded(note, data)
  }
}

/**
 * This function is called to register this as an available step type.
 */
async function register() {
  await StepTypes.register(myDef, 'example/exampleStep', 'Example step')
}//- register


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
  factory,
}
export default myDef
