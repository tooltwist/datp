/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import Step from "../Step"
import StepTypes from '../StepTypeRegister'

class SaySomething extends Step {

  constructor(definition) {
    super(definition)
    this.msg = definition.msg
  }

  async invoke(instance) {
    // instance.trace(`*****`)
    instance.trace(`SaySomething (${instance.getStepId()})`)
    instance.trace(`"${this.msg}"`)
    const data = instance.getDataAsObject()

    setTimeout(() => {
      data.said = this.msg
      const note = `Said "${this.msg}"`
      instance.succeedeed(note, data)
    }, 1000)
  }

  // async getNote() {
  //   return `I'm happy! ${this.msg}`
  // }
}

async function register() {
  await StepTypes.register(myDef, 'example/saySomething', 'Display a message to the console')
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
