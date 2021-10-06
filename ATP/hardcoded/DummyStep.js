import Step from "../Step"
import StepTypes from '../StepTypeRegister'

class DummyStep extends Step {

  constructor(definition) {
    super(definition)
    this.msg = definition.msg
  }//- contructor

  async invoke(instance) {
    // instance.console(`*****`)
    instance.console(`DummyStep (${instance.getStepId()})`)
    instance.console(`"${this.msg}"`)
    // instance.console(`*****`)
    // console.log(`this=`, this)
    // console.log(`instance=`, instance)

    const data = instance.getDataAsObject()


    setTimeout(() => {
      data.said = 'nothing'
      const note = this.msg
      instance.finish(Step.COMPLETED, note, data)
    }, 1000)
  }//- invoke

  // async getNote() {
  //   return 'NoNoye'
  // }
}//- class Dummy

async function register() {
  await StepTypes.register(myDef, 'dummy', 'Dummy step')
}//- register

async function factory(definition) {
  const obj = new DummyStep(definition)
  // console.log(`obj=`, obj)
  return obj
}//- factory

async function defaultDefinition() {
  return {

  }
}

const myDef = {
  register,
  factory,
}
export default myDef