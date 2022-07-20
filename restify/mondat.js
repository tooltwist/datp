/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import { MONITOR_URL_PREFIX } from '../CONVERSION/lib/constants';
import { defineRoute, LOGIN_IGNORED } from '../extras/apiVersions'
// import { getMidiValuesV1 } from '../mondat/midi';
import { route_activeNodesV1 } from '../mondat/nodes'
import { route_nodeGroupsV1 } from '../mondat/nodeGroups'
import { getQueueStatsV1 } from '../mondat/queueStats';
import { clonePipelineV1, commitPipelineV1, route_deletePipelineVersionV1, listPipelinesV1, pipelineDefinitionV1, pipelineDescriptionV1, route_getPipelineV1, savePipelineDraftV1, route_updatePipelineTypeV1, route_exportPipelineVersionV1, route_importPipelineVersionV1, route_getPipelinesTypesV1, route_newPipelineTypeV1 } from '../mondat/pipelines'
import { getRecentPerformanceV1 } from '../mondat/recentPerformance';
import { getStepInstanceDetailsV1 } from '../mondat/stepInstances';
import { deleteTestCasesV1, getTestCasesV1, saveTestCasesV1 } from '../mondat/testCases';
import { deleteTransactionMappingsV1, getTransactionMappingsV1, saveTransactionMappingsV1 } from '../mondat/transactionMapping';
import { mondatTransactionsV1, route_transactionStatusV1 } from '../mondat/transactions';
import { handleOrphanQueuesV1 } from '../mondat/handleOrphanQueues';
import { routeCacheStatsV1 } from '../mondat/cacheStats';
import { setNumWorkersV1 } from '../mondat/setNumWorkers';


async function registerRoutes(server) {

  // Get a list of pipeline types (previously called transaction types)
  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/pipelineTypes', [
    { versions: '1.0 - 1.0', handler: route_getPipelinesTypesV1, auth: LOGIN_IGNORED, noTenant: true }
  ])


  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/transactions', [
    { versions: '1.0 - 1.0', handler: mondatTransactionsV1, auth: LOGIN_IGNORED, noTenant: true }
  ])

  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/stepInstance/:stepId', [
    { versions: '1.0 - 1.0', handler: getStepInstanceDetailsV1, auth: LOGIN_IGNORED, noTenant: true }
  ])

  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/transaction/:txId', [
    { versions: '1.0 - 1.0', handler: route_transactionStatusV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/pipelines', [
    { versions: '1.0 - 1.0', handler: listPipelinesV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  // Create draft pipeline from an existing version
  defineRoute(server, 'put', false, MONITOR_URL_PREFIX, '/pipeline/clone/:pipeline/:version', [
    { versions: '1.0 - 1.0', handler: clonePipelineV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  // Save the draft version of a pipeline
  defineRoute(server, 'post', false, MONITOR_URL_PREFIX, '/pipeline/:pipeline', [
    { versions: '1.0 - 1.0', handler: savePipelineDraftV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  // Commit the draft version of a pipeline
  defineRoute(server, 'post', false, MONITOR_URL_PREFIX, '/pipeline/:pipeline/commit', [
    { versions: '1.0 - 1.0', handler: commitPipelineV1, auth: LOGIN_IGNORED, noTenant: true }
  ])

  // Get the pipelineType and the pipeline records
  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/pipeline/:pipeline', [
    { versions: '1.0 - 1.0', handler: route_getPipelineV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'del', false, MONITOR_URL_PREFIX, '/pipeline/:pipeline/:version', [
    { versions: '1.0 - 1.0', handler: route_deletePipelineVersionV1, auth: LOGIN_IGNORED, noTenant: true }
  ])

  // Import a pipeline version
  defineRoute(server, 'put', false, MONITOR_URL_PREFIX, '/pipeline/import/:pipelineName', [
    { versions: '1.0 - 1.0', handler: route_importPipelineVersionV1, auth: LOGIN_IGNORED, noTenant: true }
  ])

  // Return a file to export
  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/pipeline/:pipelineName/:version/export', [
    { versions: '1.0 - 1.0', handler: route_exportPipelineVersionV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/pipeline/:pipeline/description', [
    { versions: '1.0 - 1.0', handler: pipelineDescriptionV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/pipeline/:pipeline/definition', [
    { versions: '1.0 - 1.0', handler: pipelineDefinitionV1, auth: LOGIN_IGNORED, noTenant: true }
  ])

  // Update the pipelineType
  defineRoute(server, 'post', false, MONITOR_URL_PREFIX, '/pipelineType/:pipelineName', [
    { versions: '1.0 - 1.0', handler: route_updatePipelineTypeV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'put', false, MONITOR_URL_PREFIX, '/pipelineType/:pipelineName/new', [
    { versions: '1.0 - 1.0', handler: route_newPipelineTypeV1, auth: LOGIN_IGNORED, noTenant: true }
  ])

  // Return a list of active node groups and nodes. If the stepTypes parameter is set,
  // the group records will contain a list of available step types in the group.
  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/activeNodes', [
    { versions: '1.0 - 1.0', handler: route_activeNodesV1, auth: LOGIN_IGNORED, noTenant: true }
  ])

  // List all defined node groups, with number of running nodes for each.
  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/nodeGroups', [
    { versions: '1.0 - 1.0', handler: route_nodeGroupsV1, auth: LOGIN_IGNORED, noTenant: true }
  ])

  /*
   *  Transaction -> Pipeline mapping
   *  ZZZZZ These chould be replace with /pipelineType routes.
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
  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/queueStats', [
    { versions: '1.0 - 1.0', handler: getQueueStatsV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/cacheStats', [
    { versions: '1.0 - 1.0', handler: routeCacheStatsV1, auth: LOGIN_IGNORED, noTenant: true }
  ])

  /*
   *  Admin functions
   */
  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/handleOrphanQueues/:nodeGroup/:nodeId', [
    { versions: '1.0 - 1.0', handler: handleOrphanQueuesV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'put', false, MONITOR_URL_PREFIX, '/nodeGroup/:nodeGroup/setNumWorkers', [
    { versions: '1.0 - 1.0', handler: setNumWorkersV1, auth: LOGIN_IGNORED, noTenant: true }
  ])

}

export default {
  registerRoutes,
}
