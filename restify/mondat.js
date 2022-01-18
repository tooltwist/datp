/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import { defineRoute, LOGIN_IGNORED } from '../extras/apiVersions'
// import { getMidiValuesV1 } from '../mondat/midi';
import { listNodesV1 } from '../mondat/nodes'
import { listPipelinesV1, pipelineDefinitionV1, pipelineDescriptionV1, savePipelineDraftV1 } from '../mondat/pipelines'
import { getRecentPerformanceV1 } from '../mondat/recentPerformance';
import { getStepInstanceDetailsV1 } from '../mondat/stepInstances';
import { deleteTestCasesV1, getTestCasesV1, saveTestCasesV1 } from '../mondat/testCases';
import { deleteTransactionMappingsV1, getTransactionMappingsV1, saveTransactionMappingsV1 } from '../mondat/transactionMapping';
import { mondatTransactionsV1, transactionStatusV1 } from '../mondat/transactions';


// server.get(`${ROUTE_PREFIX}/${ROUTE_VERSION}/healthcheck`, async function (req, res, next) {
async function healthcheckHandler(req, res, next) {
  // console.log("Running health check...");
  var status = {
    subsystem: 'mondat',
    status: 'ok'
  }
  return res.send(status);
}


async function registerRoutes(server) {

  /*
  *	Healthcheck page.
  */
  const MONITOR_PREFIX = '/mondat'

  defineRoute(server, 'get', false, MONITOR_PREFIX, '/healthcheck', [
    { versions: '1.0 - 1.0', handler: healthcheckHandler, auth: LOGIN_IGNORED, noTenant: true }
  ])

  defineRoute(server, 'get', false, MONITOR_PREFIX, '/transactions', [
    { versions: '1.0 - 1.0', handler: mondatTransactionsV1, auth: LOGIN_IGNORED, noTenant: true }
  ])

  defineRoute(server, 'get', false, MONITOR_PREFIX, '/stepInstance/:stepId', [
    { versions: '1.0 - 1.0', handler: getStepInstanceDetailsV1, auth: LOGIN_IGNORED, noTenant: true }
  ])


  defineRoute(server, 'get', false, MONITOR_PREFIX, '/transaction/:txId', [
    { versions: '1.0 - 1.0', handler: transactionStatusV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'get', false, MONITOR_PREFIX, '/pipelines', [
    { versions: '1.0 - 1.0', handler: listPipelinesV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'post', false, MONITOR_PREFIX, '/pipeline/draft', [
    { versions: '1.0 - 1.0', handler: savePipelineDraftV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'get', false, MONITOR_PREFIX, '/pipeline/:pipeline/description', [
    { versions: '1.0 - 1.0', handler: pipelineDescriptionV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'get', false, MONITOR_PREFIX, '/pipeline/:pipeline/definition', [
    { versions: '1.0 - 1.0', handler: pipelineDefinitionV1, auth: LOGIN_IGNORED, noTenant: true }
  ])

  defineRoute(server, 'get', false, MONITOR_PREFIX, '/nodes', [
    { versions: '1.0 - 1.0', handler: listNodesV1, auth: LOGIN_IGNORED, noTenant: true }
  ])

  /*
   *  Transaction -> Pipeline mapping
   */
  defineRoute(server, 'get', false, MONITOR_PREFIX, '/transactionMapping', [
    { versions: '1.0 - 1.0', handler: getTransactionMappingsV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'post', false, MONITOR_PREFIX, '/transactionMapping', [
    { versions: '1.0 - 1.0', handler: saveTransactionMappingsV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'delete', false, MONITOR_PREFIX, '/transactionMapping/:transactionType', [
    { versions: '1.0 - 1.0', handler: deleteTransactionMappingsV1, auth: LOGIN_IGNORED, noTenant: true }
  ])

  /*
   *  Test cases
   */
  defineRoute(server, 'get', false, MONITOR_PREFIX, '/testCases', [
    { versions: '1.0 - 1.0', handler: getTestCasesV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'post', false, MONITOR_PREFIX, '/testCase', [
    { versions: '1.0 - 1.0', handler: saveTestCasesV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'delete', false, MONITOR_PREFIX, '/testCase/:name', [
    { versions: '1.0 - 1.0', handler: deleteTestCasesV1, auth: LOGIN_IGNORED, noTenant: true }
  ])

  // /*
  //  *  Midi values
  //  */
  // defineRoute(server, 'get', false, MONITOR_PREFIX, '/midiValues', [
  //   { versions: '1.0 - 1.0', handler: getMidiValuesV1, auth: LOGIN_IGNORED, noTenant: true }
  // ])

  /*
   *  Performance
   */
  defineRoute(server, 'get', false, MONITOR_PREFIX, '/:nodeId/recentPerformance', [
    { versions: '1.0 - 1.0', handler: getRecentPerformanceV1, auth: LOGIN_IGNORED, noTenant: true }
  ])


}

export default {
  registerRoutes,
}
