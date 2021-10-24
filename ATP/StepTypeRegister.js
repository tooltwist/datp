/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
/**
 * This module maintains a list of step types.
 */
class StepTypeRegister {
  constructor() {
    this.index = [ ]
  }

  async register(stepModule, type, description = '') {
    console.log(`- Registering step type ${type}`)
    if (!stepModule) {
      const msg = `StepTypeRegister.register(${type}) - stepModule is null`
      console.log(msg)
      throw new Error(msg)
    }
    if (typeof(type) !== 'string') {
      const msg = `StepTypeRegister.register(${type}) - stepModule is null`
      console.log(msg)
      throw new Error(msg)
    }
    if (!stepModule.factory || typeof(stepModule.factory) !== 'function') {
      console.log(stepModule)
      throw new Error(`StepTypeRegister.register(${type}) - invalid stepModule.factory`)
    }
    this.index[type] = {
      stepModule,
      description
    }

  }

  async getStepType(type) {
    const record = this.index[type]
    return record ? record.stepModule : null
  }

  async getDescription(type) {
    const record = this.index[type]
    return record ? record.description : ''
  }

  async factory(type, definition) {
    // console.error('factory ${type}')
    const stepModule = this.index[type]
    if (!stepModule) {
      throw new Error(`Unknown step type (${type})`)
    }
    const step = await stepModule.stepModule.factory(definition)
    // console.log(`StepTypes.factory: step=`, step)
    return step
  }

  // async getNote(stepType, schedulerSnapshotOfStepInstance) {
  //   const cls = this.index[stepType]
  //   return cls ? cls : null
  // }

  async names() {
    const list = [ ]
    for (const name in this.index) {
      // console.log(`-> ${name}`)
      list.push(name)
    }
    list.sort((a, b) => {
      if (a < b) return -1
      if (a > b) return +1
      return 0
    })
    return list
  }

  async myStepTypes() {
    const list = [ ]
    for (const name in this.index) {
      // console.log(`-> ${name}`)
      const stepType = this.index[name]
// console.log(`myStepTypes: step=`, stepType)
      const record = {
        name,
        description: stepType.description,
      }
      if (stepType.stepModule.defaultDefinition) {
        record.defaultDefinition = await stepType.stepModule.defaultDefinition()
// console.log(`**** USING DEFAULT DEFINTION`, record)
      } else {
        record.defaultDefinition = { }
      }
      if (!record.defaultDefinition.description) {
        record.defaultDefinition.description = stepType.description
      }
      list.push(record)
    }
    list.sort((a, b) => {
      if (a.name < b.name) return -1
      if (a.name > b.name) return +1
      return 0
    })
    return list
  }
}

const defaultRegister = new StepTypeRegister()
export default defaultRegister
