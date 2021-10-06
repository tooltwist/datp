import Step from "../Step"
import StepTypes from '../StepTypeRegister'

class SaySomething extends Step {

  constructor(definition) {
    super(definition)
    this.msg = definition.msg
  }

  async invoke(instance) {
    // instance.console(`*****`)
    instance.console(`SaySomething (${instance.getStepId()})`)
    instance.console(`"${this.msg}"`)
    const data = instance.getDataAsObject()

    setTimeout(() => {
      data.said = this.msg
      const note = `Said "${this.msg}"`
      instance.finish(Step.COMPLETED, note, data)
    }, 1000)
  }

  // async getNote() {
  //   return `I'm happy! ${this.msg}`
  // }
}

async function register() {
  await StepTypes.register(myDef, 'saySomething', 'Display a message to the console')
}//- register

async function defaultDefinition() {
  return {
    msg: 'Hello World',
  }
}

async function factory(definition) {
  // return new SaySomething(definition)
  const rec = new SaySomething(definition)
  console.log(`rec=`, rec)
  return rec
}//- factory

const myDef = {
  register,
  factory,
  defaultDefinition,
}
export default myDef
