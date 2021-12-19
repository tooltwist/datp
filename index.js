/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import masterServer from './restify/masterServer'
import slaveServer from './restify/slaveServer'
import step from './ATP/Step'
import stepTypeRegister from './ATP/StepTypeRegister'
import conversionHandler from './CONVERSION/lib/ConversionHandler'
import formsAndFields from './CONVERSION/lib/formsAndFields'
import dbQuery from './database/query'
import { LOGIN_IGNORED, defineRoute } from './lib/apiVersions'
import { initiateTransaction, getTransactionResult } from './DATP/datp'
import resultReceiver from './ATP/ResultReceiver'
import resultReceiverRegister from './ATP/ResultReceiverRegister'
import healthcheck from './restify/healthcheck'
import juice from '@tooltwist/juice-client'
import { RouterStep as RouterStepInternal } from './ATP/hardcoded-steps/RouterStep'
import pause from './lib/pause'
import Scheduler2 from './ATP/Scheduler2/Scheduler2'


export const Step = step
export const StepTypes = stepTypeRegister
export const ConversionHandler = conversionHandler
export const FormsAndFields = formsAndFields
export const ResultReceiver = resultReceiver
export const ResultReceiverRegister = resultReceiverRegister
export const RouterStep = RouterStepInternal

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

export async function goLive(server) {
  // Registering the healthcheck will allow the Load Balancer to recognise the server is active.
  healthcheck.registerRoutes(server)

  // Perhaps serve up MONDAT
  const serveMondat = await juice.boolean('datp.serveMondat', false)
  if (serveMondat) {
    await masterServer.serveMondat(server)
  }

  // Start the master Scheduler
  const MASTER_NODE_GROUP = 'master'
  const scheduler = new Scheduler2(MASTER_NODE_GROUP, null)
  // await scheduler.drainQueue()
  await scheduler.start()
}


export default {
  restifyMasterServer,
  restifySlaveServer,
  goLive,

  // These are here for convenience for external applications.
  Step,
  StepTypes,
  RouterStep,
  ConversionHandler,
  FormsAndFields,
  ResultReceiver,
  ResultReceiverRegister,
  query: dbQuery,
  addRoute,
  initiateTransaction,
  getTransactionResult,
  RouterStep,
  pause,
}