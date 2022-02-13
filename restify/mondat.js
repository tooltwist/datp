/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import { MONITOR_URL_PREFIX } from '../CONVERSION/lib/constants';
import { defineRoute, LOGIN_IGNORED } from '../extras/apiVersions'
// import { getMidiValuesV1 } from '../mondat/midi';
import { routeListNodesV1 } from '../mondat/nodes'
import { getNodeStatsV1 } from '../mondat/nodeStats';
import { listPipelinesV1, pipelineDefinitionV1, pipelineDescriptionV1, pipelineVersionsV1, savePipelineDraftV1 } from '../mondat/pipelines'
import { getRecentPerformanceV1 } from '../mondat/recentPerformance';
import { getStepInstanceDetailsV1 } from '../mondat/stepInstances';
import { deleteTestCasesV1, getTestCasesV1, saveTestCasesV1 } from '../mondat/testCases';
import { deleteTransactionMappingsV1, getTransactionMappingsV1, saveTransactionMappingsV1 } from '../mondat/transactionMapping';
import { mondatTransactionsV1, transactionStatusV1 } from '../mondat/transactions';




async function registerRoutes(server) {


  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/transactions', [
    { versions: '1.0 - 1.0', handler: mondatTransactionsV1, auth: LOGIN_IGNORED, noTenant: true }
  ])

  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/stepInstance/:stepId', [
    { versions: '1.0 - 1.0', handler: getStepInstanceDetailsV1, auth: LOGIN_IGNORED, noTenant: true }
  ])

  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/transaction/:txId', [
    { versions: '1.0 - 1.0', handler: transactionStatusV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/pipelines', [
    { versions: '1.0 - 1.0', handler: listPipelinesV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'post', false, MONITOR_URL_PREFIX, '/pipeline/draft', [
    { versions: '1.0 - 1.0', handler: savePipelineDraftV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/pipeline/:pipeline', [
    { versions: '1.0 - 1.0', handler: pipelineVersionsV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/pipeline/:pipeline/description', [
    { versions: '1.0 - 1.0', handler: pipelineDescriptionV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/pipeline/:pipeline/definition', [
    { versions: '1.0 - 1.0', handler: pipelineDefinitionV1, auth: LOGIN_IGNORED, noTenant: true }
  ])

  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/nodes', [
    { versions: '1.0 - 1.0', handler: routeListNodesV1, auth: LOGIN_IGNORED, noTenant: true }
  ])

  /*
   *  Transaction -> Pipeline mapping
   */
  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/transactionMapping', [
    { versions: '1.0 - 1.0', handler: getTransactionMappingsV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'post', false, MONITOR_URL_PREFIX, '/transactionMapping', [
    { versions: '1.0 - 1.0', handler: saveTransactionMappingsV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'delete', false, MONITOR_URL_PREFIX, '/transactionMapping/:transactionType', [
    { versions: '1.0 - 1.0', handler: deleteTransactionMappingsV1, auth: LOGIN_IGNORED, noTenant: true }
  ])

  /*
   *  Test cases
   */
  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/testCases', [
    { versions: '1.0 - 1.0', handler: getTestCasesV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'post', false, MONITOR_URL_PREFIX, '/testCase', [
    { versions: '1.0 - 1.0', handler: saveTestCasesV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'delete', false, MONITOR_URL_PREFIX, '/testCase/:name', [
    { versions: '1.0 - 1.0', handler: deleteTestCasesV1, auth: LOGIN_IGNORED, noTenant: true }
  ])

  // /*
  //  *  Midi values
  //  */
  // defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/midiValues', [
  //   { versions: '1.0 - 1.0', handler: getMidiValuesV1, auth: LOGIN_IGNORED, noTenant: true }
  // ])

  /*
   *  Performance
   */
  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/:nodeId/recentPerformance', [
    { versions: '1.0 - 1.0', handler: getRecentPerformanceV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/nodeStats', [
    { versions: '1.0 - 1.0', handler: getNodeStatsV1, auth: LOGIN_IGNORED, noTenant: true }
  ])


}

export default {
  registerRoutes,
}
