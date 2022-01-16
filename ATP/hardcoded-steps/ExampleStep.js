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
    instance.trace(`ExampleStep (${instance.getStepId()})`)
    // const data = instance.getDataAsObject()

    // Do something here
    const input = instance.getDataAsObject()
    const instruction = input.instruction
    instance.trace(`input.instruction is [${instruction}]`)

    switch (instruction) {
      case 'fail':
        const note = 'Hello World'
        const output = { ...input, foo: 'bar', info: 'I failed!' }
        return await instance.failed(note, output)

      case 'abort':
        const note2 = 'Golly Damn Gosh'
        const output2 = { ...input, foo: 'bar', alert: 'Catastrophic failure!' }
        return await instance.aborted(note2, output2)
    }

    // Time to complete the step and send a result
    const note = 'Hello World'
    const output = { ...input, foo: 'bar' }
    await instance.succeeded(note, output)
  }

  /**
   * This function is called to roll back anything done when invoke was run.
   * The step instance parameter provides the context of the transaction
   * and also convenience functions.
   * @param {StepInstance} instance
   */
   async rollback(instance) {
    instance.trace(`ExampleStep rolling back (${instance.getStepId()})`)
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
