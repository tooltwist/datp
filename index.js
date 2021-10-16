import masterServer from './restify/masterServer'
import slaveServer from './restify/slaveServer'
import step from './ATP/Step'
import stepTypeRegister from './ATP/StepTypeRegister'
import conversionHandler from './CONVERSION/lib/ConversionHandler'
import formsAndFields from './CONVERSION/lib/formsAndFields-dodgey'

export const Step = step
export const StepTypes = stepTypeRegister
export const ConversionHandler = conversionHandler
export const FormsAndFields = formsAndFields

async function restifySlaveServer(options) {
  return slaveServer.startSlaveServer(options)
}

async function restifyMasterServer(options) {
  return masterServer.startMasterServer(options)
}

export default {
  restifyMasterServer,
  restifySlaveServer,
  Step,
  StepTypes,
  ConversionHandler,
  FormsAndFields,
}