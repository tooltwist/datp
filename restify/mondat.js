import { defineRoute, LOGIN_IGNORED } from '../ATP/lib/apiVersions'
import { getMidiValuesV1 } from '../mondat/midi';
import { listNodesV1 } from '../mondat/nodes'
import { listPipelinesV1, pipelineDefinitionV1, pipelineDescriptionV1, savePipelineDraftV1 } from '../mondat/pipelines'
import { getRecentPerformanceV1 } from '../mondat/recentPerformance';
import { deleteTestCasesV1, getTestCasesV1, saveTestCasesV1 } from '../mondat/testCases';
import { deleteTransactionMappingsV1, getTransactionMappingsV1, saveTransactionMappingsV1 } from '../mondat/transactionMapping';
import { listAllTransactionsV1, transactionStatusV1 } from '../mondat/transactions';


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
  const URL_PREFIX = '/mondat'

  defineRoute(server, 'get', false, URL_PREFIX, '/healthcheck', [
    { versions: '1.0 - 1.0', handler: healthcheckHandler, auth: LOGIN_IGNORED, noTenant: true }
  ])


  // server.get('/dump', restify.plugins.conditionalHandler([
  //   { version: '1.1.3', handler: dumpAllTransactionsV1 },
  // ]));

  // server.get('/dump/:txId', restify.plugins.conditionalHandler([
  //   { version: '1.1.3', handler: dumpTransactionV1 },
  // ]));

  // server.get('/transactions', restify.plugins.conditionalHandler([
  //   { version: '1.1.3', handler: listAllTransactionsV1 },
  // ]));
  defineRoute(server, 'get', false, URL_PREFIX, '/transactions', [
    { versions: '1.0 - 1.0', handler: listAllTransactionsV1, auth: LOGIN_IGNORED, noTenant: true }
  ])


  // server.get('/transaction/:txId', restify.plugins.conditionalHandler([
  //   { version: '1.1.3', handler: transactionStatusV1 },
  // ]));
  defineRoute(server, 'get', false, URL_PREFIX, '/transaction/:txId', [
    { versions: '1.0 - 1.0', handler: transactionStatusV1, auth: LOGIN_IGNORED, noTenant: true }
  ])

  // server.get('/pipelines', restify.plugins.conditionalHandler([
  //   { version: '1.1.3', handler: listPipelinesV1 },
  // ]));
  defineRoute(server, 'get', false, URL_PREFIX, '/pipelines', [
    { versions: '1.0 - 1.0', handler: listPipelinesV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'post', false, URL_PREFIX, '/pipeline/draft', [
    { versions: '1.0 - 1.0', handler: savePipelineDraftV1, auth: LOGIN_IGNORED, noTenant: true }
  ])

  // server.get('/pipeline/:pipeline/description', restify.plugins.conditionalHandler([
  //   { version: '1.1.3', handler: pipelineDescriptionV1 },
  // ]));
  defineRoute(server, 'get', false, URL_PREFIX, '/pipeline/:pipeline/description', [
    { versions: '1.0 - 1.0', handler: pipelineDescriptionV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'get', false, URL_PREFIX, '/pipeline/:pipeline/definition', [
    { versions: '1.0 - 1.0', handler: pipelineDefinitionV1, auth: LOGIN_IGNORED, noTenant: true }
  ])


  // server.get(`${MONDAT_PREFIX}/nodes`, restify.plugins.conditionalHandler([
  //   { version: '1.1.3', handler: listNodesV1 },
  // ]));
  defineRoute(server, 'get', false, URL_PREFIX, '/nodes', [
    { versions: '1.0 - 1.0', handler: listNodesV1, auth: LOGIN_IGNORED, noTenant: true }
  ])

  /*
   *  Transaction -> Pipeline mapping
   */
  defineRoute(server, 'get', false, URL_PREFIX, '/transactionMapping', [
    { versions: '1.0 - 1.0', handler: getTransactionMappingsV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'post', false, URL_PREFIX, '/transactionMapping', [
    { versions: '1.0 - 1.0', handler: saveTransactionMappingsV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'delete', false, URL_PREFIX, '/transactionMapping/:transactionType', [
    { versions: '1.0 - 1.0', handler: deleteTransactionMappingsV1, auth: LOGIN_IGNORED, noTenant: true }
  ])

  /*
   *  Test cases
   */
  defineRoute(server, 'get', false, URL_PREFIX, '/testCases', [
    { versions: '1.0 - 1.0', handler: getTestCasesV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'post', false, URL_PREFIX, '/testCase', [
    { versions: '1.0 - 1.0', handler: saveTestCasesV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'delete', false, URL_PREFIX, '/testCase/:name', [
    { versions: '1.0 - 1.0', handler: deleteTestCasesV1, auth: LOGIN_IGNORED, noTenant: true }
  ])

  /*
   *  Midi values
   */
  defineRoute(server, 'get', false, URL_PREFIX, '/midiValues', [
    { versions: '1.0 - 1.0', handler: getMidiValuesV1, auth: LOGIN_IGNORED, noTenant: true }
  ])

  /*
   *  Performance
   */
  defineRoute(server, 'get', false, URL_PREFIX, '/:nodeId/recentPerformance', [
    { versions: '1.0 - 1.0', handler: getRecentPerformanceV1, auth: LOGIN_IGNORED, noTenant: true }
  ])


}

export default {
  registerRoutes,
}