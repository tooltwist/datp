/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import DATP from '../DATP/datp'
import corsMiddleware from "restify-cors-middleware";
import figlet from 'figlet'
import juice from '@tooltwist/juice-client'
import me from '../ATP/me'
import { registerAsSlave } from './registerAsSlave'


const restify = require('restify');

async function startSlaveServer(options) {
  console.log(`slaveServer()`)
  const server = restify.createServer({
    handleUncaughtExceptions: false,
  })

  // CORS setup
  // See https://codepunk.io/using-cors-with-restify-in-nodejs/
  // const cors = corsMiddleware({
  //   preflightMaxAge: 5, //Optional
  //   origins: ['http://localhost.com'],
  //   allowHeaders: ['API-Token'],
  //   exposeHeaders: ['API-Token-Expiry']
  // })
  const cors = corsMiddleware({
    origins: ["*"],
    allowHeaders: ["Authorization"],
    exposeHeaders: ["Authorization"]
  });
  server.pre(cors.preflight);
  server.use(cors.actual);

  // See http://restify.com/docs/plugins-api/#queryparser
  server.use(restify.plugins.queryParser({
    mapParams: false
  }));

  // See http://restify.com/docs/plugins-api/#bodyparser
  server.use(restify.plugins.bodyParser({
    mapParams: false,
    maxBodySize: 10 * 1024 * 1024, // 10 MB
    mapFiles: true,
    keepExtensions: true,
  }));



  // Start DATP (Distributed Asynchronous Transaction Engine)
  await DATP.run()
  await DATP.routesForRestify(server)
  await registerAsSlave(server)

  /*
  *  Display a nice message.
  */
  const JUICE_CONFIG = process.env['JUICE_CONFIG']
  console.log(`JUICE_CONFIG=`, JUICE_CONFIG)
  console.log();
  const name = await me.getName()
  console.log(figlet.textSync(name, {
    horizontalLayout: 'fitted'
  }));
  console.log();

  /*
  *  Start the server.
  */
  const port = await juice.int('datp.port', 8081)
  console.log(`Starting server on port ${port}`)
  server.listen(port, '0.0.0.0');
}

export default {
  startSlaveServer
}