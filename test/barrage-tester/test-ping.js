/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
const { BarrageTester } = require('./datp-barrage')

const URL = 'http://localhost:4000'
const NUM_TRANSACTIONS = 1000
const NUM_COLUMNS = 50

const tester = new BarrageTester({ columns: NUM_COLUMNS })

// console.log(`process.env=`, process.env)
const autoCannonDefinition = {
  url: URL,
  connections: 50,
  amount: NUM_TRANSACTIONS,
  requests: [
    {
      method: 'GET',
      setupRequest: (req, context) => {
        req.path = `/datp/1.0/ping`
        context.externalId = tester.giveMeATest()
        return req
      },
      headers: {
        // 'Content-Type': 'application/json',
        'X-Client-Id': 'acme',
      },
      onResponse: (status, body, context) => {
        // This is a simple ping reply, not a DATP transaction status.
        // console.log(`status=`, status)
        // console.log(`body=`, body)
        if (status === 200) {
          const reply = JSON.parse(body)
          // console.log(`reply 2 =`, reply)
          const status = reply.status
          // console.log(`status=`, status)
          if (status === 'ok') {
            // badStatusCounter++
            tester.setStatusByExternalId(context.externalId, tester.status.STATUS_COMPLETE)
          } else {
            tester.incrementBadStatusCounter()
            tester.setStatusByExternalId(context.externalId, tester.status.STATUS_ERROR)
          }
        } else {
          console.log(`ERROR GETTING STATUS:`)
          console.log(`status=`, status)
          console.log(`body=`, body)
          console.log(`context=`, context)
          tester.incrementBadStatusCounter()
          tester.setStatusByExternalId(context.externalId, tester.status.STATUS_ERROR)
      }
      },
    }
  ]
}

tester.run(autoCannonDefinition)
