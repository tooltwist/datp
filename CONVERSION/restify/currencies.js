/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import errors from 'restify-errors'
import currencies from '../lookup/currencies'
import providers from '../providers-needToRemove/providers'
import { LOOKUP_URL_PREFIX } from '../lib/constants'
import apiVersions from '../../lib/apiVersions'
const { defineRoute, LOGIN_IGNORED } = apiVersions



export default {
  init,
}

async function init(server) {
  // console.log(`countries/lookups:init()`)

  // Return all currencies
  defineRoute(server, 'get', false, LOOKUP_URL_PREFIX, '/currencies', [
    { versions: '1.0 - 1.0', auth: LOGIN_IGNORED, noTenant: true, handler: async (req, res, next) => {
      // server.get(`${LOOKUP_URL_PREFIX}/currencies`, async function (req, res, next) {
      console.log(`-------------------------------------`)
      console.log(`/gateway/currencies`)
      res.send(currencies)
      next()
    }}
  ])//- /gateway/currencies

  // Return a specific currency
  defineRoute(server, 'get', false, LOOKUP_URL_PREFIX, '/currency/:currencyCode', [
    { versions: '1.0 - 1.0', auth: LOGIN_IGNORED, noTenant: true, handler: async (req, res, next) => {
      // server.get(`${LOOKUP_URL_PREFIX}/currency/:currencyCode`, async function (req, res, next) {
      console.log(`-------------------------------------`)
      console.log(`/gateway/currency/:currencyCode`)
      const currencyCode = req.params.currencyCode

      const currency = currencies[currencyCode]
      if (currency) {
        res.send(currency)
        return next()
      } else {
        return next(new errors.NotFoundError(`Unknown currency ${currencyCode}`))
      }
    }}
  ])//- /gateway/currency/:currencyCode

  // Return currencies for a specific provider
  defineRoute(server, 'get', false, LOOKUP_URL_PREFIX, '/currencies/:providerCode', [
    { versions: '1.0 - 1.0', auth: LOGIN_IGNORED, noTenant: true, handler: async (req, res, next) => {
      // server.get(`${LOOKUP_URL_PREFIX}/currencies/:providerCode`, async function (req, res, next) {
      console.log(`-------------------------------------`)
      console.log(`/gateway/currencies/:providerCode`)
      const providerCode = req.params.providerCode

      const provider = await providers.get(providerCode)
      console.log(`provider=`, provider)

      const currencies = provider.plugin.getCurrencies()
      res.send(currencies)
      next()
    }}
  ])//- /gateway/currency/:currencyCode

}