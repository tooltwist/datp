import { defineRoute, LOGIN_IGNORED } from '../extras/apiVersions'
import { DATP_URL_PREFIX } from '../CONVERSION/lib/constants'
import juice from '@tooltwist/juice-client'

let logHealthcheck = null

async function registerRoutes(server) {

  /*
  *	Healthcheck page.
  */
  defineRoute(server, 'get', false, DATP_URL_PREFIX, '/healthcheck', [
    { versions: '1.0 - 1.0', handler: healthcheckHandler, auth: LOGIN_IGNORED, noTenant: true }
  ])

  // server.get(`${ROUTE_PREFIX}/${ROUTE_VERSION}/healthcheck`, async function (req, res, next) {
  async function healthcheckHandler(req, res, next) {

    if (logHealthcheck === null) {
      logHealthcheck = await juice.boolean('datp.logHealthcheck', false)
    }

    if (logHealthcheck) {
      console.log("Running health check...");
    }
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