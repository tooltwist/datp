import Step from "../ATP/Step"
import StepTypes from '../ATP/StepTypeRegister'
import axios from 'axios'
import { authenticate } from './i2i-misc'

const VERBOSE = true

class i2iBackend_WalletWithdrawalStatusStep extends Step {

  constructor(definition) {
    super(definition)
  }//- contructor

  async invoke(instance) {
    if (VERBOSE) {
      instance.console(`i2iBackend_WalletWithdrawalStatusStep (${instance.getStepId()})`)
    }

    // Check the input parameters
    const data = await instance.getDataAsObject()
    if (!data.senderReference) {
      return await instance.badDefinition(`Missing parameter [senderReference]`)
    }
    console.log(`data is `, data)

    // Validate the input fields
    const senderReference = data.senderReference
    const callback = 'https://yoursite.domain.ph/callback'

    let authenticationToken
    try {
      authenticationToken = await authenticate(instance)
      console.log(`authenticationToken=`, authenticationToken)
    } catch (e) {
      console.error(e)
      instance.finish(Step.FAIL, 'Authentication error', { })
    }

    try {
      // See https://i2i.readme.io/reference/getwithdrawstatus
      // curl --request GET \
      // --url 'https://api.stg.i2i.ph/api-apic/wallet/withdraw?senderReference=UB123456' \
      // --header 'Accept: application/json' \
      // --header 'Authorization: aaaaa'
      const url = `https://api.stg.i2i.ph/api-apic/wallet/withdraw?senderReference=${senderReference}`
      const reply = await axios.get(url, {
        headers: {
          Authorization: authenticationToken
        }
      })
      console.log(`reply=`, reply)
      if (reply.status !== 200) {
        console.log(`\n\n\n ********** ERROR RETURN\n\n`)
        console.log(`Getting withdrawal status failed with status ${reply.response.status}`)
        console.log(`\n\n\n ********** ERROR RETURN\n\n`)
        return instance.finish(Step.FAIL, 'Getting withdrawal status failed', reply.data)
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
      instance.finish(Step.FAIL, 'Getting withdrawal status failed', e.response.data)
    }
  }//- invoke
}//- class

async function register() {
  await StepTypes.register(myDef, 'i2iBackend_WalletWithdrawalStatus', 'Get wallet withdrawal status')
}//- register

async function defaultDefinition() {
  return {
  }
}
async function factory(definition) {
  const obj = new i2iBackend_WalletWithdrawalStatusStep(definition)
  // console.log(`obj=`, obj)
  return obj
}//- factory

async function describe(definition) {
  return {
    stepType: definition.stepType,
    description: 'Get wallet withdrawal status'
  }
}

const myDef = {
  register,
  factory,
  describe,
  defaultDefinition,
}
export default myDef
