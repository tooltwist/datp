import Step from "../ATP/Step"
import StepTypes from '../ATP/StepTypeRegister'
import { authenticate, getWalletBalance, TEST_TREASURER_EMAIL, TEST_TREASURER_PASSWORD, TEST_TXMAKER_EMAIL, TEST_TXMAKER_PASSWORD } from './i2i-misc'

const VERBOSE = true

class i2iBackend_WalletBalanceStep extends Step {

  constructor(definition) {
    super(definition)
  }//- contructor

  async invoke(instance) {
    if (VERBOSE) {
      // instance.console(`*****`)
      instance.console(`i2iBackend_WalletBalanceStep (${instance.getStepId()})`)
    }

    let authenticationToken
    try {
      authenticationToken = await authenticate(instance)
      console.log(`authenticationToken=`, authenticationToken)
    } catch (e) {
      console.error(e)
      instance.finish(Step.FAIL, 'Authentication error', { })
    }

    let balance
    try {
      balance = await getWalletBalance(authenticationToken)
      // console.log(`balance=`, balance)
    } catch (e) {
      console.error(e)
      instance.finish(Step.FAIL, 'Getting balance failed', { })
    }

    // All good
    const note = 'Success'
    instance.finish(Step.COMPLETED, note, { balance })
  }//- invoke
}//- class

async function register() {
  await StepTypes.register(myDef, 'i2iBackend_WalletBalanceStep', 'Get wallet balance')
}//- register

async function defaultDefinition() {
  return {
  }
}
async function factory(definition) {
  const obj = new i2iBackend_WalletBalanceStep(definition)
  // console.log(`obj=`, obj)
  return obj
}//- factory

async function describe(definition) {
  return {
    stepType: definition.stepType,
    description: 'Get wallet balance'
  }
}

const myDef = {
  register,
  factory,
  describe,
  defaultDefinition,
}
export default myDef
