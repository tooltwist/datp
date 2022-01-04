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
