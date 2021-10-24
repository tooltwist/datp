/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import semver from 'semver'
import errors from 'restify-errors';


// Define the versions this server supports.
// The version number in URLs will be matched against this list.
// Once a version is decided, the handler wrapper will find a
// route handler that matches that chosen version.
export const apiVersions = [
  // Descending order.
  // { version: '2.0', desc: 'Version 2.0' },
  { version: '1.0', desc: 'Initial version' },
]

export const LOGIN_OPTIONAL = false
export const LOGIN_MANDATORY = true
export const LOGIN_IGNORED = 'ignore'

const VERBOSE = false


export function showVersions() {
  console.log(``)
  console.log(`API versions supported by this software:`)
  for (const v of apiVersions) {
    const version = semver.coerce(v.version)
    console.log(`  ${version} ${v.desc}`)
  }
  console.log(``)
}

export function defineRoute(server, operation, tenantInUrl, prefix, path, handlers) {
  const tenantBit = tenantInUrl ? ':tenantId/' : ''
  if (!prefix.startsWith('/')) {
    prefix = `/${prefix}`
  }

  // Create a wrapper that will do authentication and handle handler versions.
  const wrapper = async function wrapper (req, res, next) {
    if (VERBOSE) {
    console.log(`+++++++++++++++++++++++++++++++++++++++++++++++++++++++++`)
    console.log(`route=${prefix}/:version/${tenantBit}${path}`)
    console.log(`path=${req.getPath()}`)
      const query = req.getQuery()
      if (query) {
        console.log(`query=${query}`)
      }
    }
    // console.log(`YARP KADF 0`)

    if (VERBOSE) console.log(`req.params=`, req.params)

    // Find the handler to use, based on the API version
    try {

      // Find the version that this server supports, that matches the request.
      let urlVersion = req.params['version']
      if (VERBOSE) console.log(`urlVersion=`, urlVersion)

      // Check that the version in the URL is valid as a version number.
      const tmpVersion = semver.coerce(urlVersion)
      //console.log(`${version} -> ${tmpVersion}`)
      if (!tmpVersion) {
        console.log(`${urlVersion} - invalid version number`)
        throw new errors.InvalidArgumentError(`Invalid version number`)
      }

      // Find the first version supported by this server that matches
      // this requested version. Example matches:
      //    5 - matches server version 5.0, 5.0.1, 5.1, 5.1.1
      //    5.1 - matches 5.1, 5.1.1
      //    5.1.0 - matches 5.1, 5.1.0
      //    5.1.1 - matches 5.1.1 only
      let selectedVersionNumber = null
      for (let v of apiVersions) {
        const ourVersion = semver.coerce(v.version)
        // console.log(`${ourVersion} vs ${urlVersion} - ${urlVersion}`)
        const match = semver.satisfies(ourVersion, `${urlVersion} - ${urlVersion}`)
        //console.log(`     ${match}`)
        if (match) {
          if (VERBOSE) console.log(`${urlVersion} - will use version ${v.version} (${v.desc})`)
          selectedVersionNumber = semver.coerce(v.version)
          break
        }
      }
      if (!selectedVersionNumber) {
        console.log(`${urlVersion} - not supported`)
        // return null
        throw new errors.NotImplementedError(`Unsupported version number`)
      }

      // console.log(`aaa`)
      // console.log(`5.0 - `, semver.gte(version, '5.0.0', true))
      // console.log(`bbb`)
      // console.log(`4.1 - `, semver.gte(version, '4.1.*', true))
      // console.log(`4.0 - `, semver.gte(version, '4.0.*', true))
      // console.log(`3.9 - `, semver.gte(version, '3.9.*', true))

// console.log(`YARP KADF 1`)
// console.log(`req.params=`, req.params)
// We've decided which API version we are using. Now find a
      // hander for this route that supports that version number.
      let chosenHandler = null
      for (const h of handlers) {
        if (VERBOSE) console.log(`-> ${selectedVersionNumber} VS ${h.versions}`)
        const match = semver.satisfies(selectedVersionNumber, h.versions)
        if (VERBOSE) console.log(`     ${match}`)
        if (match) {
          chosenHandler = h
          break
        }
      }
      // console.log(`YARP KADF 2`)
      if (chosenHandler === null) {
        console.log(`No handler for version ${urlVersion} (resolved to ${selectedVersionNumber})`)
        throw new errors.NotImplementedError(`Route not available in this version`)
      }

      // console.log(`YARP KADF 3`)
      // If this route can be called without credentials, call the handler now.
      if (chosenHandler.auth === LOGIN_IGNORED) {
        return await chosenHandler.handler(req, res, next)
      }

      // console.log(`YARP KADF 4`)
      // console.log(`req.params=`, req.params)
      // Check the credentials, then call the handler.
      await set$authFromCredentials(chosenHandler.auth, req)//, res, async (err) => {
      // authLoginMandatory(req, res, async (err) => {
        // console.log(`YARP KADF 5a`)
        // if (err) throw err
        // if (err) {
        //   return next(err)
        // }
        // console.log(`YARP KADF 5b`)
        return await chosenHandler.handler(req, res, next)
      // })
    } catch (err) {
      // console.log(`YARP KADF 6`, err)
      console.log(err)
      return next(err)
    }
  }// End of wrapper

  // Display a nice message
  if (path.startsWith('/')) {
    path = path.substring(1)
  }
  let op4 = operation.toUpperCase()
  while (op4.length < 6) {
    // op4 += ' '
    op4 = ' ' + op4
  }
  // console.log(`Registering ${op4} ${prefix}/:version/${tenantBit}${path}`)
  console.log(`  ${op4} ${prefix}/:version/${tenantBit}${path}`)

  // Now register the route
  switch (operation) {
    case 'get':
      server.get(`${prefix}/:version/${tenantBit}${path}`, wrapper)
      break

    case 'put':
      server.put(`${prefix}/:version/${tenantBit}${path}`, wrapper)
      break

    case 'post':
      server.post(`${prefix}/:version/${tenantBit}${path}`, wrapper)
      break

    case 'delete':
    case 'del':
      server.del(`${prefix}/:version/${tenantBit}${path}`, wrapper)
      break

    default:
      console.log(`Unknown operation ${operation} for route ${prefix}/:version/${tenantBit}${path}`)
  }
}


export default {
  apiVersions,
  showVersions,
  defineRoute,
  LOGIN_OPTIONAL,
  LOGIN_MANDATORY,
  LOGIN_IGNORED,
}
