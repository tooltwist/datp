import StepTypeRegister from './StepTypeRegister'
import fs from 'fs'
import query from '../database/query'
import dbStep from '../database/dbStep'

const VERBOSE = false

export default class Step {

  // Statuses
  static RUNNING = 'running'
  static WAITING = 'waiting'
  // static ERROR = 'error'
  static TIMEOUT = 'timeout'

  static COMPLETED = 'completed'
  // static TERMINATED,
  // static ERROR,
  // static OFFLINE,


  constructor(definition) {
    // this.id = GenerateHash('step')
    if (VERBOSE) {
      this.definition = definition
    }

  }//- constructor

  async invoke_internal(instance) {
    let reply = this.invoke(instance) // Provided by the step implementation
    // if (reply) {
    //   return reply
    // }

    return {
      stepId: instance.getStepId(),
      status: Step.WAITING,
    }
  }

  static async resolveDefinition(definition) {
    /*
      *  Load the definition of the step (which is probably a pipeline)
      */
    // console.log(`typeof(options.definition)=`, typeof(options.definition))
    switch (typeof(definition)) {
      case 'string':
        // console.log(`Loading definition for pipeline ${definition}`)
        // const rawdata = fs.readFileSync(`./pipeline-definitions/transaction-${definition}.json`)
        const pipelineName = definition
        const sql = `SELECT node_name, name, version, description, steps_json FROM atp_pipeline WHERE name=?`
        const params = [ pipelineName ]
        const result = await query(sql, params)

        const row = result[0]
        // console.log(`row=`, row)
        // console.log(`row.steps_json=`, row.steps_json)
        const steps = JSON.parse(row.steps_json)
        definition = {
          stepType: 'pipeline',
          name: row.name,
          description: row.description,
          steps,
        }
        // console.log(`PIPELINE definition=`, definition)

    case 'object':
      // console.log(`already have definition`)
      break

    default:
      throw new Error(`Invalid value for parameter definition (${typeof(definition)})`)
    }
    return definition
  }


  static async describe(definition) {
    console.log(`stepDescription()`)
    try {
      definition = await Step.resolveDefinition(definition)
    } catch (e) {
      console.log(`Error resolving pipeline definition (${definition}):`, e)
    }
    console.log(`definition=`, definition)
    const stepTypeObject = await StepTypeRegister.getStepType(definition.stepType)
    console.log(`stepTypeObject=`, stepTypeObject)


    if (stepTypeObject.describe) {
      // This step type wants to describe itself
      const description = await stepTypeObject.describe(definition)
      console.log(`description=`, description)
      return description
    } else {
      // Default description
      console.log(`default step description`)
      return {
        stepType: definition.stepType
      }
    }
  }//- stepDescription

}