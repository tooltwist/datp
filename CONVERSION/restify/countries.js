/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import errors from 'restify-errors'
import constants from '../lib/constants'
import countries from '../lookup/countries'
import { LOOKUP_URL_PREFIX } from '../lib/constants'
import apiVersions from '../../extras/apiVersions'
const { defineRoute, LOGIN_IGNORED } = apiVersions



export default {
  init,
}

async function init(server) {
  // console.log(`routes/countries:init()`)

  // Return all currencies
  defineRoute(server, 'get', false, LOOKUP_URL_PREFIX, '/countries', [
    { versions: '1.0 - 1.0', auth: LOGIN_IGNORED, noTenant: true, handler: async (req, res, next) => {
      // server.get(`${constants.URL_PREFIX}/countries`, async function (req, res, next) {
      console.log(`-------------------------------------`)
      console.log(`${constants.URL_PREFIX}/countries`)
      res.send(countries)
      next()
    }}
  ])//- /gateway/countries

  // Return a specific country
  defineRoute(server, 'get', false, LOOKUP_URL_PREFIX, '/country/:countryCode', [
    { versions: '1.0 - 1.0', auth: LOGIN_IGNORED, noTenant: true, handler: async (req, res, next) => {
      // server.get(`${constants.URL_PREFIX}/country/:countryCode`, async function (req, res, next) {
      console.log(`-------------------------------------`)
      console.log(`${constants.URL_PREFIX}/country/:countryCode`)
      const countryCode = req.params.countryCode
      for (const c of countries) {
        if (c.code === countryCode) {
          res.send(c.name)
          return next()
        }
      }
      // const country = countries.get(countryCode)
      return next(new errors.NotFoundError(`Unknown country code ${countryCode}`))
    }}
  ])//- /gateway/country/:countryCode

  // // Return countries for a specific provider
  // server.get(`${constants.URL_PREFIX}/countries/:providerCode`, async function (req, res, next) {
  //   console.log(`-------------------------------------`)
  //   console.log(`${constants.URL_PREFIX}/countries/:providerCode`)
  //   const providerCode = req.params.providerCode

  //   const provider = await providers.get(providerCode)
  //   console.log(`provider=`, provider)

  //   const countries = provider.plugin.getCountries()
  //   res.send(countries)
  //   next()
  // })//- /gateway/countries/:providerCode

}