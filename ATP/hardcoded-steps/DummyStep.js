/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import Step from "../Step"
import StepTypes from '../StepTypeRegister'

class DummyStep extends Step {

  constructor(definition) {
    super(definition)
    this.msg = definition.msg
  }//- constructor

  async invoke(instance) {
    // instance.trace(`*****`)
    instance.trace(`DummyStep (${instance.getStepId()})`)
    instance.trace(`"${this.msg}"`)
    // instance.trace(`*****`)
    // console.log(`this=`, this)
    // console.log(`instance=`, instance)

    const data = instance.getDataAsObject()


    setTimeout(() => {
      data.said = 'nothing'
      const note = this.msg
      instance.succeedeed(note, data)
    }, 1000)
  }//- invoke

  // async getNote() {
  //   return 'NoNoye'
  // }
}//- class Dummy

async function register() {
  await StepTypes.register(myDef, 'example/dummy', 'Dummy step')
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
  defaultDefinition,
}
export default myDef
