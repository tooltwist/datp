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
import me from '../../DATP/ATP/me'

const restify = require('restify');


async function startMasterServer(options) {
  // console.log(`startMasterServer()`)

  var server = restify.createServer({
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
  server.use(restify.plugins.queryParser({ mapParams: false }));
  server.use(restify.plugins.bodyParser({ mapParams: false }));

  await DATP.run()
  await DATP.routesForRestify(server)
  await DATP.registerAsMaster(server)
  // await DATP.monitorMidi()

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
  const port = await juice.int('datp.port', 8080)
  console.log(`Starting server on port ${port}`)
  server.listen(port, '0.0.0.0');
  return server
}

async function serveMondat(server) {
  const port = await juice.int('datp.port', 8080)
  console.log(`Hosting Mondat application at http://0.0.0.0:${port}/mondat`)
  const staticFilesDir = `${__dirname}/../../MONDAT/dist`
  console.log(`staticFilesDir=`, staticFilesDir)
  server.get('/*', restify.plugins.serveStaticFiles(staticFilesDir))
}

export default {
  startMasterServer,
  serveMondat,
}
