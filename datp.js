/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import ATP from './ATP/ATP'
import apiVersions from './extras/apiVersions'
import assert from 'assert'
import providerAndServiceRoutes from './CONVERSION/providerAndServiceRoutes'
import currencies_routes from './CONVERSION/restify/currencies'
import countries_routes from './CONVERSION/restify/countries'
import formserviceYarp from './restify/formservice'
import { DATP_URL_PREFIX } from './CONVERSION/lib/constants'
import { startTransactionRoute } from '.';
import { addRoute } from './extras';
import { transactionStatusByTxIdV1 } from './restify/transactionStatus';
import { transactionStatusByExternalIdV1 } from './restify/transactionStatusByExternalId';
import { isDevelopmentMode, TEST_TENANT } from './datp-constants'
import errors, { UnauthorizedError } from 'restify-errors'

const { showVersions } = apiVersions


/**
 *
 * @param {Request} req
 * @param {Response} res
 * @param {function} next
 */
async function route_tx_start_$transactionType(req, res, next) {
  // console.log(`>>>>    route_tx_start_$transactionType ${req.params.transactionType}  `.white.bgRed.bold)
  // console.log(`req.params=`, req.params)
  // console.log(`req.body=`, req.body)
  // console.log(`req.query=`, req.query)
  // if (await isDevelopmentMode()) {

    const transactionType = req.params.transactionType
    if (typeof(req.params.transactionType) !== 'string') {
      throw new Error(`Invalid req.params.transactionType`)
    }
    if (typeof(req.body) !== 'object') {
      throw new Error(`Invalid req.body`)
    }

    let metadata = req.body.metadata ? req.body.metadata : { }
    let data = req.body.data ? req.body.data : { }
    assert(typeof(metadata) === 'object')
    assert(typeof(data) === 'object')

    // This will reply, although maybe not till it times out.
    await startTransactionRoute(req, res, next, TEST_TENANT, transactionType, data, metadata)
  // } else {
  //   res.send(new errors.UnauthorizedError('Can only start transactions like this in development environment.'))
  //   return next()
  // }
}


async function routesForRestify(server, isMaster = false) {

  /*
   *  Special routes for the master node only.
   */
  showVersions()
  addRoute(server, 'put', DATP_URL_PREFIX, '/tx/start/:transactionType', [ { versions: '1.0 - 1.0', handler: route_tx_start_$transactionType } ])
  addRoute(server, 'get', DATP_URL_PREFIX, '/tx/status/:txId', [ { versions: '1.0 - 1.0', handler: transactionStatusByTxIdV1 } ])
  addRoute(server, 'get', DATP_URL_PREFIX, '/tx/statusByExternalId/:externalId', [ { versions: '1.0 - 1.0', handler: transactionStatusByExternalIdV1 } ])

  // providers.init()
  currencies_routes.init(server)
  countries_routes.init(server)

  providerAndServiceRoutes.init(server)
  formserviceYarp.init(server)
}//- routesForRestify

async function run() {
  console.log(`Run DATP`)
  await ATP.initialize()
}

export default {
  routesForRestify,
  run,
}
