/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import providers from './providers-needToRemove/providers'
import errors from 'restify-errors'
import query from '../database/query'
import constants from './lib/constants'
import apiVersions from '../extras/apiVersions'
const { defineRoute, LOGIN_IGNORED } = apiVersions

import { MONITOR_URL_PREFIX } from './lib/constants'


export default {
  init,
}

async function init(server) {
  // console.log(`proviserAndServiceRoutes.init()`)

  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/metadata/domains', [
    { versions: '1.0 - 1.0', auth: LOGIN_IGNORED, noTenant: true, handler: async (req, res, next) => {
      console.log(`---------------------------------`)
      console.log(`/metadata/domains`)
      const list = await providers.all()
      res.send(list)
      next()
    } }
  ])//- /metadata/domains


  // Get a specific provider
  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/domain/:code', [
    { versions: '1.0 - 1.0', auth: LOGIN_IGNORED, noTenant: true, handler: async (req, res, next) => {
      const code = req.params.code

      console.log(`---------------------------------`)
      console.log(`/metadata/domain/${code}`)

      const provider = await providers.get(code)
      // console.log(`provider=`, provider)
      if (!provider) {
        res.send(new errors.NotFoundError(`Unknown domain ${code}`))
        return next()
      }
      res.send(provider)
      next()
    }}
  ])//- /domain/:code


  // Return a list of services
  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/metadata/services', [
    { versions: '1.0 - 1.0', auth: LOGIN_IGNORED, noTenant: true, handler: async (req, res, next) => {

      console.log(`---------------------------------`)
      console.log(`/metadata/services`)

      const sql = `
        SELECT S.service, S.description, C.category, C.description AS category_description
        FROM map_service S
        LEFT JOIN map_service_category C ON C.category = S.category
        ORDER BY C.sequence
        `
      // console.log(`sql=`, sql)
      const result = await query(sql)
      // console.log(`result=`, result)
      // const list = await providers.all()
      res.send(result)
      next()
    }}//
  ])//- /metadata/services


  // Get services for a provider
  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/services/:providerCode', [
    { versions: '1.0 - 1.0', auth: LOGIN_IGNORED, noTenant: true, handler: async (req, res, next) => {
      const providerCode = req.params.providerCode

      console.log(`---------------------------------`)
      console.log(`/metadata/services/${providerCode}`)

      const provider = await providers.get(providerCode)
      // console.log(`provider=`, provider)
      if (!provider) {
        res.send(new errors.NotFoundError(`Unknown provider ${providerCode}`))
        return next()
      }

      const sql = `
        SELECT ps.provider, ps.service, s.description
        FROM provider_service ps
        LEFT JOIN map_service s ON s.service = ps.service
        WHERE provider=?`
      const params = [ providerCode ]
      const result = await query(sql, params)
      // console.log(`result=`, result)
      res.send(result)
      next()
    }}//
  ])//- /services/:providerCode


  // Get services for a provider
  defineRoute(server, 'get', false, MONITOR_URL_PREFIX, '/views/:providerCode/:serviceCode', [
    { versions: '1.0 - 1.0', auth: LOGIN_IGNORED, noTenant: true, handler: async (req, res, next) => {
      const providerCode = req.params.providerCode
      const serviceCode = req.params.serviceCode
      const withFields = req.params.fields
      const withMappings = req.params.mappings
      // console.log(`req.params=`, req.params)
      // console.log(`withFields=`, withFields)

      console.log(`---------------------------------`)
      console.log(`/metadata/views/${providerCode}/${serviceCode}`)

      // Check the provider exists
      const provider = await providers.get(providerCode)
      // console.log(`provider=`, provider)
      if (!provider) {
        res.send(new errors.NotFoundError(`Unknown provider ${providerCode}`))
        return next()
      }

      // Find the views for this provider / service
      const viewPattern = `${providerCode}-${serviceCode}-%`
      const views = await parameters.getForms(constants.DEFAULT_TENANT, viewPattern)
      // console.log(`views=`, views)
      const reply = [ ]
      for (const v of views) {
        const viewSuffix = v.view.substring(viewPattern.length - 1)
        const record = {
          provider: providerCode,
          service: serviceCode,
          message: viewSuffix,
          view: v.view,
          description: v.description,
        }
        const version = -1 //ZZZZZZ Version should be specified
        if (withFields === 'true') {
          //ZZZZ Should use schema
          record.fields = await parameters.getFields(constants.DEFAULT_TENANT, v.view, version)
          // console.log(`record.fields=`, record.fields)
        }
        if (withMappings === 'true') {
          //ZZZZ Should use schema
          record.mapping = await parameters.getMapping(constants.DEFAULT_TENANT, v.view, version)
          console.log(`record.mapping=`, record.mapping)
        }
        reply.push(record)
      }
      // console.log(`reply=`, reply)
      res.send(reply)
      next()
    }}
  ])//- /views/:providerCode/:serviceCode

  // // Get services for a provider
  // server.get(`${constants.URL_PREFIX}/metadata/views/:providerCode/:serviceCode`, async function(req, res, next) {
  //   const providerCode = req.params.providerCode
  //   const serviceCode = req.params.serviceCode
  //   const withFields = req.params.fields
  //   const withMappings = req.params.mappings
  //   // console.log(`req.params=`, req.params)
  //   // console.log(`withFields=`, withFields)

  //   console.log(`---------------------------------`)
  //   console.log(`/metadata/views/${providerCode}/${serviceCode}`)

  //   // Check the provider exists
  //   const provider = await providers.get(providerCode)
  //   // console.log(`provider=`, provider)
  //   if (!provider) {
  //     res.send(new errors.NotFoundError(`Unknown provider ${providerCode}`))
  //     return next()
  //   }

  //   // Find the views for this provider / service
  //   const viewPattern = `${providerCode}-${serviceCode}-%`
  //   const views = await parameters.getForms(constants.DEFAULT_TENANT, viewPattern)
  //   console.log(`views=`, views)
  //   const reply = [ ]
  //   for (const v of views) {
  //     const viewSuffix = v.view.substring(viewPattern.length - 1)
  //     const record = {
  //       provider: providerCode,
  //       service: serviceCode,
  //       message: viewSuffix,
  //       view: v.view,
  //       description: v.description,
  //     }
  //     const version = -1 //ZZZZZZ Version should be specified
  //     if (withFields === 'true') {
  //       //ZZZZ Should use schema
  //       record.fields = await parameters.getFields(constants.DEFAULT_TENANT, v.view, version)
  //       // console.log(`record.fields=`, record.fields)
  //     }
  //     if (withMappings === 'true') {
  //       //ZZZZ Should use schema
  //       record.mapping = await parameters.getMapping(constants.DEFAULT_TENANT, v.view, version)
  //       console.log(`record.mapping=`, record.mapping)
  //     }
  //     reply.push(record)
  //   }
  //   console.log(`reply=`, reply)
  //   res.send(reply)
  //   next()
  // })//

}
