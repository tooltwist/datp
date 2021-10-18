import masterServer from './restify/masterServer'
import slaveServer from './restify/slaveServer'
import step from './ATP/Step'
import stepTypeRegister from './ATP/StepTypeRegister'
import conversionHandler from './CONVERSION/lib/ConversionHandler'
import formsAndFields from './CONVERSION/lib/formsAndFields-dodgey'
import dbQuery from './database/query'
import { LOGIN_IGNORED, defineRoute } from './ATP/lib/apiVersions'
import { initiateTransaction, getTransactionResult } from './DATP/datp'
import resultReceiver from './ATP/ResultReceiver'
import resultReceiverRegister from './ATP/ResultReceiverRegister'
import scheduler from './ATP/Scheduler'


export const Step = step
export const StepTypes = stepTypeRegister
export const ConversionHandler = conversionHandler
export const FormsAndFields = formsAndFields
export const ResultReceiver = resultReceiver
export const ResultReceiverRegister = resultReceiverRegister
export const Scheduler = scheduler

export const query = dbQuery

async function restifySlaveServer(options) {
  return slaveServer.startSlaveServer(options)
}

async function restifyMasterServer(options) {
  return masterServer.startMasterServer(options)
}

export function addRoute(server, operation, urlPrefix, path, versionFunctionMapping) {
  // console.log(`addRoute(server, ${operation}, urlPrefix=${urlPrefix}, path="${path}, versionFunctionMapping)`, versionFunctionMapping)
  const mapping = [ ]
  for (const row of versionFunctionMapping) {
    mapping.push({
      versions: row.versions,
      handler: row.handler,
      auth: LOGIN_IGNORED,
      noTenant: true
    })
  }
  const tenantInUrl = false
  defineRoute(server, operation, tenantInUrl, urlPrefix, path, mapping)

}//- addRoute

export default {
  restifyMasterServer,
  restifySlaveServer,

  // These are here for convenience for external applications.
  Step,
  StepTypes,
  ConversionHandler,
  FormsAndFields,
  ResultReceiver,
  ResultReceiverRegister,
  query: dbQuery,
  addRoute,
  initiateTransaction,
  getTransactionResult,
}