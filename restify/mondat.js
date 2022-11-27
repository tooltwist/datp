/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import { MONITOR_URL_PREFIX } from '../CONVERSION/lib/constants';
import { defineRoute, LOGIN_IGNORED } from '../extras/apiVersions'
// import { getMidiValuesV1 } from '../mondat/midi';
import { mondatRoute_activeNodesV1 } from '../mondat/nodes'
import { mondatRoute_nodeGroupsV1 } from '../mondat/nodeGroups'
import { mondatRoute_getQueueStatsV1 } from '../mondat/queueStats';
import { mondatRoute_clonePipelineV1 } from '../mondat/pipelines'
import { mondatRoute_commitPipelineV1 } from '../mondat/pipelines'
import { mondatRoute_deletePipelineVersionV1 } from '../mondat/pipelines'
import { mondatRoute_listPipelinesV1 } from '../mondat/pipelines'
import { mondatRoute_pipelineDefinitionV1 } from '../mondat/pipelines'
import { mondatRoute_pipelineDescriptionV1 } from '../mondat/pipelines'
import { mondatRoute_getPipelineV1 } from '../mondat/pipelines'
import { mondatRoute_savePipelineDraftV1 } from '../mondat/pipelines'
import { mondatRoute_updatePipelineTypeV1 } from '../mondat/pipelines'
import { mondatRoute_exportPipelineVersionV1 } from '../mondat/pipelines'
import { mondatRoute_importPipelineVersionV1 } from '../mondat/pipelines'
import { mondatRoute_getPipelinesTypesV1 } from '../mondat/pipelines'
import { mondatRoute_newPipelineTypeV1 } from '../mondat/pipelines'
import { mondatRoute_getRecentPerformanceV1 } from '../mondat/recentPerformance';
import { mondatRoute_getStepInstanceDetailsV1 } from '../mondat/stepInstances';
import { mondatRoute_deleteTestCasesV1 } from '../mondat/testCases';
import { mondatRoute_getTestCasesV1 } from '../mondat/testCases';
import { mondatRoute_saveTestCasesV1 } from '../mondat/testCases';
import { mondatRoute_deleteTransactionMappingsV1 } from '../mondat/transactionMapping';
import { mondatRoute_getTransactionMappingsV1 } from '../mondat/transactionMapping';
import { mondatRoute_saveTransactionMappingsV1 } from '../mondat/transactionMapping';
import { mondatRoute_transactionStatusV1, mondatRoute_transactionStateV1 } from '../mondat/transactions';
import { mondatRoute_transactionsV1 } from '../mondat/transactions';
import { mondatRoute_handleOrphanQueuesV1 } from '../mondat/handleOrphanQueues';
import { mondatRoute_cacheStatsV1 } from '../mondat/cacheStats';
import { mondatRoute_setEventloopWorkersV1 } from '../mondat/setNumWorkers';
import { mondatRoute_getMetricsV1 } from '../mondat/metrics';


async function registerRoutes(server) {

  // Get a list of pipeline types (previously called transaction types)
  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/pipelineTypes', [
    { versions: '1.0 - 1.0', handler: mondatRoute_getPipelinesTypesV1, auth: LOGIN_IGNORED, noTenant: true }
  ])


  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/transactions', [
    { versions: '1.0 - 1.0', handler: mondatRoute_transactionsV1, auth: LOGIN_IGNORED, noTenant: true }
  ])

  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/stepInstance/:stepId', [
    { versions: '1.0 - 1.0', handler: mondatRoute_getStepInstanceDetailsV1, auth: LOGIN_IGNORED, noTenant: true }
  ])

  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/transaction/:txId', [
    { versions: '1.0 - 1.0', handler: mondatRoute_transactionStatusV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/transaction/:txId/state', [
    { versions: '1.0 - 1.0', handler: mondatRoute_transactionStateV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/pipelines', [
    { versions: '1.0 - 1.0', handler: mondatRoute_listPipelinesV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  // Create draft pipeline from an existing version
  defineRoute(server, 'put', false, MONITOR_URL_PREFIX, '/pipeline/clone/:pipeline/:version', [
    { versions: '1.0 - 1.0', handler: mondatRoute_clonePipelineV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  // Save the draft version of a pipeline
  defineRoute(server, 'post', false, MONITOR_URL_PREFIX, '/pipeline/:pipeline', [
    { versions: '1.0 - 1.0', handler: mondatRoute_savePipelineDraftV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  // Commit the draft version of a pipeline
  defineRoute(server, 'post', false, MONITOR_URL_PREFIX, '/pipeline/:pipeline/commit', [
    { versions: '1.0 - 1.0', handler: mondatRoute_commitPipelineV1, auth: LOGIN_IGNORED, noTenant: true }
  ])

  // Get the pipelineType and the pipeline records
  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/pipeline/:pipeline', [
    { versions: '1.0 - 1.0', handler: mondatRoute_getPipelineV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'del', false, MONITOR_URL_PREFIX, '/pipeline/:pipeline/:version', [
    { versions: '1.0 - 1.0', handler: mondatRoute_deletePipelineVersionV1, auth: LOGIN_IGNORED, noTenant: true }
  ])

  // Import a pipeline version
  defineRoute(server, 'put', false, MONITOR_URL_PREFIX, '/pipeline/import/:pipelineName', [
    { versions: '1.0 - 1.0', handler: mondatRoute_importPipelineVersionV1, auth: LOGIN_IGNORED, noTenant: true }
  ])

  // Return a file to export
  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/pipeline/:pipelineName/:version/export', [
    { versions: '1.0 - 1.0', handler: mondatRoute_exportPipelineVersionV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/pipeline/:pipeline/description', [
    { versions: '1.0 - 1.0', handler: mondatRoute_pipelineDescriptionV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/pipeline/:pipeline/definition', [
    { versions: '1.0 - 1.0', handler: mondatRoute_pipelineDefinitionV1, auth: LOGIN_IGNORED, noTenant: true }
  ])

  // Update the pipelineType
  defineRoute(server, 'post', false, MONITOR_URL_PREFIX, '/pipelineType/:pipelineName', [
    { versions: '1.0 - 1.0', handler: mondatRoute_updatePipelineTypeV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'put', false, MONITOR_URL_PREFIX, '/pipelineType/:pipelineName/new', [
    { versions: '1.0 - 1.0', handler: mondatRoute_newPipelineTypeV1, auth: LOGIN_IGNORED, noTenant: true }
  ])

  // Return a list of active node groups and nodes. If the stepTypes parameter is set,
  // the group records will contain a list of available step types in the group.
  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/activeNodes', [
    { versions: '1.0 - 1.0', handler: mondatRoute_activeNodesV1, auth: LOGIN_IGNORED, noTenant: true }
  ])

  // List all defined node groups, with number of running nodes for each.
  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/nodeGroups', [
    { versions: '1.0 - 1.0', handler: mondatRoute_nodeGroupsV1, auth: LOGIN_IGNORED, noTenant: true }
  ])

  /*
   *  Transaction -> Pipeline mapping
   *  ZZZZZ These chould be replace with /pipelineType routes.
   */
  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/transactionMapping', [
    { versions: '1.0 - 1.0', handler: mondatRoute_getTransactionMappingsV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'post', false, MONITOR_URL_PREFIX, '/transactionMapping', [
    { versions: '1.0 - 1.0', handler: mondatRoute_saveTransactionMappingsV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'delete', false, MONITOR_URL_PREFIX, '/transactionMapping/:transactionType', [
    { versions: '1.0 - 1.0', handler: mondatRoute_deleteTransactionMappingsV1, auth: LOGIN_IGNORED, noTenant: true }
  ])

  /*
   *  Test cases
   */
  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/testCases', [
    { versions: '1.0 - 1.0', handler: mondatRoute_getTestCasesV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'post', false, MONITOR_URL_PREFIX, '/testCase', [
    { versions: '1.0 - 1.0', handler: mondatRoute_saveTestCasesV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'delete', false, MONITOR_URL_PREFIX, '/testCase/:name', [
    { versions: '1.0 - 1.0', handler: mondatRoute_deleteTestCasesV1, auth: LOGIN_IGNORED, noTenant: true }
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
    { versions: '1.0 - 1.0', handler: mondatRoute_getRecentPerformanceV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/queueStats', [
    { versions: '1.0 - 1.0', handler: mondatRoute_getQueueStatsV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/cacheStats', [
    { versions: '1.0 - 1.0', handler: mondatRoute_cacheStatsV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/metrics', [
    { versions: '1.0 - 1.0', handler: mondatRoute_getMetricsV1, auth: LOGIN_IGNORED, noTenant: true }
  ])

  /*
   *  Admin functions
   */
  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/handleOrphanQueues/:nodeGroup/:nodeId', [
    { versions: '1.0 - 1.0', handler: mondatRoute_handleOrphanQueuesV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'put', false, MONITOR_URL_PREFIX, '/nodeGroup/:nodeGroup/seteventloopWorkers', [
    { versions: '1.0 - 1.0', handler: mondatRoute_setEventloopWorkersV1, auth: LOGIN_IGNORED, noTenant: true }
  ])

}

export default {
  registerRoutes,
}
