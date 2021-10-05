import { defineRoute, LOGIN_IGNORED } from '../ATP/lib/apiVersions'

async function registerRoutes(server) {

  /*
  *	Healthcheck page.
  */
  const URL_PREFIX = '/datp'

  defineRoute(server, 'get', false, URL_PREFIX, '/healthcheck', [
    { versions: '1.0 - 1.0', handler: healthcheckHandler, auth: LOGIN_IGNORED, noTenant: true }
  ])

  // server.get(`${ROUTE_PREFIX}/${ROUTE_VERSION}/healthcheck`, async function (req, res, next) {
  async function healthcheckHandler(req, res, next) {
    // console.log("Running health check...");
    var status = {
      subsystem: 'datp',
      status: 'ok'
    }
    return res.send(status);
  }

}

export default {
  registerRoutes,
}