import ATP from '../ATP/ATP'
import ResultReceiver from '../ATP/ResultReceiver'
import master from '../restify/master'
// import slave from './restify/registerAsSlave'
import apiVersions from '../ATP/lib/apiVersions'
import masterServer from '../restify/masterServer'
import slaveServer from '../restify/slaveServer'
// import { monitorMidi } from '../mondat/midi'
import dbTransactionInstance from '../database/dbTransactionInstance'
import colors from 'colors' // Yep, it is used
import errors from 'restify-errors';
import assert from 'assert'
import TxData from '../ATP/TxData'
import providerAndServiceRoutes from '../CONVERSION/providerAndServiceRoutes'
// import providers from '../CONVERSION/providers-needToRemove/providers'
import currencies_routes from '../CONVERSION/restify/currencies'
import countries_routes from '../CONVERSION/restify/countries'
import formserviceYarp from '../restify/formservice_yarp'


const { defineRoute, showVersions, LOGIN_IGNORED } = apiVersions

// console.log(`apiVersions=`, apiVersions)

// Index of response objects
const responsesForSynchronousReturn = [ ] // txId -> { res, next, timestamp, timeoutHandle }


const API_TRANSACTION_COMPLETION_HANDLER_NAME = 'api-transaction-completion-handler'

class ApiTransactionCompletionHandler extends ResultReceiver {
  constructor() {
    super()
  }
  async haveResult(contextForCompletionHandler, status, note, response) {
    assert(response instanceof TxData)
    console.log(`<<<<    ApiTransactionCompletionHandler.haveResult()  `.white.bgRed.bold)
    // console.log(`  contextForCompletionHandler=`, JSON.stringify(contextForCompletionHandler, '', 0))
    // console.log(`  status=`, status)
    // console.log(`  response=`, response.toString())
    // // await Scheduler.dumpSteps(`\nAfter Completion`)

    console.log(`TRANSACTION ${contextForCompletionHandler.txId} HAS FINISHED [${status}].`)
    console.log(`responsesForSynchronousReturn is holding ${Object.keys(responsesForSynchronousReturn).length} responses`.dim)

    // See if we can respond immediately
    const immediateResponse = responsesForSynchronousReturn[contextForCompletionHandler.txId]
    if (immediateResponse) {
      // We can respond using the original API call
      console.log(`  HAVE TRANSACTION RESULT - RETURNING IN ORIGINAL API RESPONSE  `.blue.bgYellow.bold)
      const res = immediateResponse.res
      const next = immediateResponse.next

      // Remove this response from the index
      if (immediateResponse.timeoutHandle) {
        clearTimeout(immediateResponse.timeoutHandle)
        immediateResponse.timeoutHandle = null
      }
      delete responsesForSynchronousReturn[contextForCompletionHandler.txId]

      // Send the reply
      res.send({
        metadata: {
          transactionId: contextForCompletionHandler.txId,
          responseType: 'synchronous',
          status,
          note,
        },
        data: response.getData(),
      })
      // console.log(`responsesForSynchronousReturn=`, responsesForSynchronousReturn)
      console.log(`responsesForSynchronousReturn is holding ${Object.keys(responsesForSynchronousReturn).length} responses`.dim)
      return next(null)
    } else {
      // The response object has already been used by the timeout.
      console.log(`  HAVE TRANSACTION RESULT, BUT API RESPONSE IS ALREADY USED - WILL NEED TO REPLY BY POLLING OR WEBHOOK`.blue.bgYellow.bold)
    }
  }
}




async function initiateTransactionRouteV1(req, res, next) {
  console.log(`>>>>    Initiate transaction ${req.params.transactionType}  `.white.bgRed.bold)
  // console.log(`req.params=`, req.params)
  // console.log(`req.body=`, req.body)
  // console.log(`req.query=`, req.query)

  const transactionType = req.params.transactionType
  // // const transactionType = 'remittance-init'
  // const color = req.query.color
  // // console.log(`color=`, color)
  // // const initialData = { amount: 123.45, hello: 'there' }
  // const initiatedBy = 'nobody' //ZZZZ
  const initialData = req.body

  const options = {
    color: req.query.color,
    initiatedBy: 'nobody' //ZZZZ
  }

  // This will reply, although maybe not till it times out.
  await initiateTransaction(req, res, next, transactionType, initialData, options)
}


export async function initiateTransaction(req, res, next, transactionType, initialData, options) {

  try {//ZZZZZ
    // const transactionType = req.params.transactionType
    // const transactionType = 'remittance-init'
    const color = options.color ? options.color : null
    // console.log(`color=`, color)
    // const initialData = { amount: 123.45, hello: 'there' }
    const initiatedBy = options.initiatedBy ? options.initiatedBy : 'unknown'
    // const initialData = req.body

    // Remember this transaction and it's 'req' object, so we can send an API response, either:
    //  1. When we get a response from the pipeline.
    //  2. If we don't get a pipeline result before the timeout.
    const transactionId = await ATP.allocateTransactionId()
    const syncResponse = {
      res,
      next,
      timestamp: Date.now(),
      timeoutHandle: null
    }
    responsesForSynchronousReturn[transactionId] = syncResponse


    // Now initiate the transaction
    // const color = 'red'
    const { inquiryToken } = await ATP.initiateTransaction(transactionId, transactionType, initiatedBy, initialData, API_TRANSACTION_COMPLETION_HANDLER_NAME, color)

    /*
     * Let's set a timeout. Hopefully we get a result from the pipeline before the
     * timeout occurs, in which case the timeout will be cancelled and the 'res' for
     * this transaction be used to send the pipline result synchronously. i.e. As the
     * response of the original API call.
     *
     * If the pipeline doesn't return in time, this timeout will use 'res' to send a response
     * to the original API call, telling the API client they will need to get the transaction
     * result using one of the asynchronous methods (i.e. Polling or via webhook).
     */
    const MAX_SYNC_REPLY_WAIT_TIME = 2500 // milliseconds
    syncResponse.timeoutHandle = setTimeout(() => {
      // Check that the transaction hasn't completed, and already used 'res' and 'next'
      try {
        if (responsesForSynchronousReturn[transactionId]) {
          // Don't hold onto the reponse any longer
          delete responsesForSynchronousReturn[transactionId]
          console.log(`  TIMEOUT IN MAIN REQUEST - WILL NOT REPLY SYNCHRONOUSLY  `.blue.bgYellow.bold)
          console.log(`responsesForSynchronousReturn is holding ${Object.keys(responsesForSynchronousReturn).length} responses`.dim)

          // Send the reply
          res.send({
            metadata: {
              transactionId,
              responseType: 'poll-for-result',
              inquiryToken,
            },
            data: null
          })
          return next(null)
        } else {
          // The response record is missing - the transaction must have completed and used it already.
          console.log(`responsesForSynchronousReturn already removed`.blue.bgYellow.bold)
        }
      } catch (e) {
        console.error(`Exception in response timeout handler.`, e)
      }
    }, MAX_SYNC_REPLY_WAIT_TIME)
    console.log(`RETURNING FROM API FUNCTION (NO res.send() YET, WE'LL LEAVE THAT FOR A TIMEOUT OR FAST PIPELINE RESULT)`.dim)

    // res.send({ transactionId })
    // return next();
  } catch (e) {
    console.log(`Exception while initiating transaction:`, e)
    throw e
  }
}

async function getTransactionResultRouteV1(req, res, next) {
  // console.log(`>>>>    get transaction result ${req.params.transactionId}  `.white.bgRed.bold)
  // console.log(`req.params=`, req.params)
  // console.log(`req.body=`, req.body)
  // console.log(`req.query=`, req.query)

  // try {
    const transactionId = req.params.transactionId
    const fetchToken = 'xyz'//ZZZZZ
    // console.log(`transactionId=`, transactionId)
    await getTransactionResult(req, res, next, transactionId, fetchToken)
  }


export async function getTransactionResult(req, res, next, transactionId, fetchToken) {
  console.log(`>>>>    get transaction result ${transactionId}  `.white.bgRed.bold)
  console.log(`req.params=`, req.params)
  console.log(`req.body=`, req.body)
  console.log(`req.query=`, req.query)

  try {

    // console.log(`transactionId=`, transactionId)

    const tx = await ATP.getTransactionResult(transactionId)
    // console.log(`tx.response=`, tx.response)

    if (!tx) {
      res.send(new errors.NotFoundError(`Unknown transaction`))
      return next()
    }

    // Check that the fetch token is correct
    //ZZZZZZ Check that the user is allowed to access this response
    // 1. Is admin?
    // 2.
    // if (fetchToken && fetchToken != result[0].inquiryToken) {
    //   throw new Error()
    // }

    delete tx.inquiryToken
    delete tx.responseMethod
    delete tx.inquiryToken

    // console.log(`tx.response=`, tx.response)
    // console.log(`typeof(tx.response)=`, typeof(tx.response))
    let result
    try {
      result = JSON.parse(tx.response)
    } catch (e) {
      result = tx.response.toString()
    }
    // switch (typeof(tx.response)) {
    //   case 'undefined':
    //     result = 'undefined'
    //     break
    //   case 'string':
    //     result = tx.response
    //     break
    //   case 'object':
    //     result = tx.response
    //     break
    //   default:
    //     if (tx.result) {
    //       result = tx.result.toString()
    //     } else {
    //       result = 'null'
    //     }
    // }
    const reply = {
      status: tx.status,
      result,
    }
    // console.log(`reply=`, reply)
    res.send(reply)
    return next()
  } catch (e) {
    console.log(`Exception while getting transaction response:`, e)
    throw e
  }
}



async function routesForRestify(server, isMaster = false) {

/*
  *  Special routes for the master node only.
  */
// if (isMaster) {
//   await restifyRoutes.defineHealthcheckRoutes(server)
//   await restifyRoutes.defineMondatRoutes(server)
//   await restifyRoutes.defineMasterRoutes(server)
// } else {
//   await restifyRoutes.defineSlaveRoutes(server)
// }

  /*
  *	Healthcheck page.
  */
  const MONDAT_PREFIX = '/mondat'

  // defineRoute(server, 'get', false, MONDAT_PREFIX, '/healthcheck', [
  //   { versions: '1.0 - 1.0', handler: healthcheckHandler, auth: LOGIN_IGNORED, noTenant: true }
  // ])

  // // server.get(`${ROUTE_PREFIX}/${ROUTE_VERSION}/healthcheck`, async function (req, res, next) {
  // async function healthcheckHandler(req, res, next) {
  //   // console.log("Running health check...");
  //   var status = {"status": "ok"};
  //   return res.send(status);
  // }

  await showVersions()


  //ZZZZ Change to a PUT
  // server.get('/initiate/:transactionType', restify.plugins.conditionalHandler([
  //   { version: '1.1.3', handler: initiateTransactionRouteV1 },
  //   // { version: '2.0.0', handler: sendV2 }
  // ]));
  const URL_PREFIX = 'datp'
  defineRoute(server, 'put', false, URL_PREFIX, '/initiate/:transactionType', [
    { versions: '1.0 - 1.0', handler: initiateTransactionRouteV1, auth: LOGIN_IGNORED, noTenant: true }
  ])
  defineRoute(server, 'get', false, URL_PREFIX, '/result/:transactionId', [
    { versions: '1.0 - 1.0', handler: getTransactionResultRouteV1, auth: LOGIN_IGNORED, noTenant: true }
  ])


  // providers.init()
  currencies_routes.init(server)
  countries_routes.init(server)

  providerAndServiceRoutes.init(server)
  formserviceYarp.init(server)
}//- routesForRestify

async function run() {
  console.log(`Run DATP`)
  await ATP.initialize()
  await ResultReceiver.register(API_TRANSACTION_COMPLETION_HANDLER_NAME, new ApiTransactionCompletionHandler())
}

export default {
  routesForRestify,
  registerAsMaster: master.registerAsMaster,
  // monitorMidi,
  run,
  initiateTransaction,
}
