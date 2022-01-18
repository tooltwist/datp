/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
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