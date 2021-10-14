import Step from "../ATP/Step"
import StepTypes from '../ATP/StepTypeRegister'
import axios from 'axios'
import { authenticate } from './i2i-misc'

const VERBOSE = true

class i2iBackend_WalletTopupStatusStep extends Step {

  constructor(definition) {
    super(definition)
  }//- contructor

  async invoke(instance) {
    if (VERBOSE) {
      instance.console(`i2iBackend_WalletTopupStatusStep (${instance.getStepId()})`)
    }

    // Check the input parameters
    const data = await instance.getDataAsObject()
    if (!data.amount) {
      return await instance.badDefinition(`Missing parameter [amount]`)
    }
    console.log(`data is `, data)
    const amount = data.amount
    const callback = 'https://yoursite.domain.ph/callback'

    let authenticationToken
    try {
      authenticationToken = await authenticate(instance, true)
      console.log(`authenticationToken=`, authenticationToken)
    } catch (e) {
      console.error(e)
      instance.finish(Step.FAIL, 'Authentication error', { })
    }

    try {
      // See https://i2i.readme.io/reference/getfeesremittance
      //   curl --request GET \
      //  --url 'https://api.stg.i2i.ph/api-apic/remittance/fees?amount=123' \
      //  --header 'Accept: application/json' \
      //  --header 'Authorization: aaaa' \
      //  --header 'Content-Type: application/json'
      const url = `https://api.stg.i2i.ph/api-apic/remittance/fees?amount=${amount}`
      const reply = await axios.get(url, {
        headers: {
          Authorization: authenticationToken
        }
      })
      console.log(`reply=`, reply)
      if (reply.status !== 200) {
        console.log(`\n\n\n ********** ERROR RETURN\n\n`)
        console.log(`Getting topup status failed with status ${reply.response.status}`)
        console.log(`\n\n\n ********** ERROR RETURN\n\n`)
        return instance.finish(Step.FAIL, 'Requesting top up status failed', reply.data)
      }

      // All good
      const note = 'Success'
      instance.finish(Step.COMPLETED, note, reply.data)
    } catch (e) {
      console.log(`\n\n\n ********** EXCEPTION RETURN\n\n`)
      console.error(e)
      console.log(`\n\n\n ********** EXCEPTION RETURN\n\n`)
      console.log(`Response is`, e.response)
      console.log(`Data is`, e.response.data)
      console.log(`\n\n\n ********** EXCEPTION RETURN\n\n`)
      instance.finish(Step.FAIL, 'Requesting top up status failed', e.response.data)
    }
  }//- invoke
}//- class

async function register() {
  await StepTypes.register(myDef, 'i2iBackend_WalletTopupStatus', 'Get transaction fees')
}//- register

async function defaultDefinition() {
  return {
  }
}
async function factory(definition) {
  const obj = new i2iBackend_WalletTopupStatusStep(definition)
  return obj
}//- factory

async function describe(definition) {
  return {
    stepType: definition.stepType,
    description: 'Get transaction fees'
  }
}

const myDef = {
  register,
  factory,
  describe,
  defaultDefinition,
}
export default myDef
