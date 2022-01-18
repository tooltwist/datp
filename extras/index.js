/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import { defineRoute, LOGIN_IGNORED } from "./apiVersions"

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
