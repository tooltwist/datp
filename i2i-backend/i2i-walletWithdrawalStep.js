import Step from "../ATP/Step"
import StepTypes from '../ATP/StepTypeRegister'
import axios from 'axios'
import { authenticate } from './i2i-misc'

const VERBOSE = true

class i2iBackend_WalletWithdrawalStep extends Step {

  constructor(definition) {
    super(definition)
  }//- contructor

  async invoke(instance) {
    if (VERBOSE) {
      instance.console(`i2iBackend_WalletWithdrawalStep (${instance.getStepId()})`)
    }

    // Check the input parameters
    const data = await instance.getDataAsObject()
    if (!data.senderReference) {
      return await instance.badDefinition(`Missing parameter [senderReference]`)
    }
    if (!data.amount) {
      return await instance.badDefinition(`Missing parameter [amount]`)
    }
    console.log(`data is `, data)
    const senderReference = data.senderReference
    const amount = data.amount
    const remarks = data.remarks ? data.remarks : ''
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
      // See https://i2i.readme.io/reference/withdraw
      //       curl --request POST \
      //       --url https://api.stg.i2i.ph/api-apic/wallet/withdraw/process \
      //       --header 'Accept: application/json' \
      //       --header 'Authorization: asasasasas' \
      //       --header 'Content-Type: application/json' \
      //       --data '
      //  {
      //       "senderReference": "UB123456",
      //       "amount": 100,
      //       "remarks": "i2i"
      //  }
      //  '
      const url = `https://api.stg.i2i.ph/api-apic/wallet/withdraw/process`
      const reply = await axios.post(url, {
        senderReference,
        amount,
        remarks
      }, {
        headers: {
          Authorization: authenticationToken
        }
      })
      console.log(`reply=`, reply)
      if (reply.status !== 200) {
        console.log(`\n\n\n ********** ERROR RETURN\n\n`)
        console.log(`Withdrawal request failed with status ${reply.response.status}`)
        console.log(`\n\n\n ********** ERROR RETURN\n\n`)
        return instance.finish(Step.FAIL, 'Withdrawal request failed', reply.data)
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
      instance.finish(Step.FAIL, 'Withdrawal request failed', e.response.data)
    }
  }//- invoke
}//- class

async function register() {
  await StepTypes.register(myDef, 'i2iBackend_WalletWithdrawal', 'Wallet withdrawal request')
}//- register

async function defaultDefinition() {
  return {
  }
}
async function factory(definition) {
  const obj = new i2iBackend_WalletWithdrawalStep(definition)
  // console.log(`obj=`, obj)
  return obj
}//- factory

async function describe(definition) {
  return {
    stepType: definition.stepType,
    description: 'Wallet withdrawal request'
  }
}

const myDef = {
  register,
  factory,
  describe,
  defaultDefinition,
}
export default myDef
