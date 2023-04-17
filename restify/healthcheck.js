/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import { defineRoute, LOGIN_IGNORED } from '../extras/apiVersions'
import { DATP_URL_PREFIX, MONITOR_URL_PREFIX } from '../CONVERSION/lib/constants'
import juice from '@tooltwist/juice-client'
import { appVersion, datpVersion, buildTime } from '../build-version'

let logHealthcheck = null

async function registerRoutes(server) {

  /*
  *	Healthcheck pages.
  */
  defineRoute(server, 'get', false, DATP_URL_PREFIX, '/healthcheck', [
    { versions: '1.0 - 1.0', handler: healthcheckHandler, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/healthcheck', [
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
      status: 'ok',

      // Information inserted during Docker build.
      appVersion,
      datpVersion,
      buildTime
    }
    return res.send(status);
  }

  /*
  *	Healthcheck page.
  */
  defineRoute(server, 'get', false, DATP_URL_PREFIX, '/ping', [
    { versions: '1.0 - 1.0', handler: pingHandler, auth: LOGIN_IGNORED, noTenant: true }
  ])

  async function pingHandler(req, res, next) {
    var status = { status: 'ok' }
    return res.send(status);
  }
}

export default {
  registerRoutes,
}