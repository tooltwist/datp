/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
const { BarrageTester } = require('./datp-barrage')

const URL = 'http://localhost:4000'
// const NUM_TRANSACTIONS = 1000
// const NUM_COLUMNS = 50
const NUM_TRANSACTIONS = 10000
const NUM_COLUMNS = 200


const tester = new BarrageTester({ columns: NUM_COLUMNS })

const autoCannonDefinition = {
  url: URL,
  connections: 100,
  amount: NUM_TRANSACTIONS,
  requests: [

    /*
     *  Start the transaction
     */
    {
      method: 'PUT',
      path: '/datp/1.0/tx/start/example',
      // method: 'GET',
      setupRequest: (req, context) => {
        // req.path = `/datp/1.0/ping`
        context.externalId = tester.giveMeATest()
        req.body = JSON.stringify({
          metadata: {
            reply: 'shortpoll',
            externalId: context.externalId,
            webhook: 'http://localhost:3030/webhook',
            progressReports: false
          }
        })//- stringify
        return req
      },
      headers: {
        'Content-Type': 'application/json',
        'X-Client-Id': 'acme',
      },
      onResponse: (status, body, context) => tester.handleDatpResponse(status, body, context)
    },

    // /*
    //  *  Get the status of the transaction with a long poll
    //  */
    // {
    //   method: 'GET',
    //   // path: `/datp/1.0/tx/status/TXID`,
    //   // path: `/datp/1.0/tx/status/${context.txId}?reply=longpoll`,
    //   setupRequest: (req, context) => {
    //     // console.log(`req=`, req)
    //     req.path = `/datp/1.0/tx/status/${context.txId}?reply=longpoll&tenantId=acme`
    //     // req.body = JSON.stringify({
    //     //   productId: context.product.id
    //     // })
    //     return req
    //   },
    //   headers: {
    //     // 'Content-Type': 'application/json',
    //     'X-Client-Id': 'acme',
    //     // 'X-Client-Id': 'd79e30f36b6cfda74bfc0a75eb4dfc86',
    //     // 'x-Client-Secret': 'ef7e7357ab06190ddec0375981908136',
    //     // 'x-partner-id': '1e65fe68-c777-40a7-892a-a3a153de2406',
    //   },
    //   onResponse: (status, body, context) => tester.handleDatpResponse(status, body, context)
    // }
  ]
}

tester.run(autoCannonDefinition)
