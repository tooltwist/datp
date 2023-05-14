/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import DATP from '../datp'
import corsMiddleware from "restify-cors-middleware2";
import figlet from 'figlet'
import juice from '@tooltwist/juice-client'
import mondat from './mondat'
import dbLogbook from '../database/dbLogbook';
import DatabaseLogbook from '../database/DatabaseLogbook';
import NullLogbook from '../database/NullLogbook';
import DatadogLogbook from '../database/DatadogLogbook';
import ConsoleLogbook from '../database/ConsoleLogbook';
import { getNodeGroup } from '../database/dbNodeGroup';
import pause from '../lib/pause';

const restify = require('restify');


export async function startDatpServer(options) {
  // console.log(`startServer()`)

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

  // X-Client-Id is used by IBM APIC (API Connect)
  const cors = corsMiddleware({
    origins: ["*"],
    allowHeaders: ["Authorization", "X-Client-Id"],
    exposeHeaders: ["Authorization", "X-Client-Id"]
  });
  server.pre(cors.preflight);
  server.use(cors.actual);

  // See http://restify.com/docs/plugins-api/#queryparser
  server.use(restify.plugins.queryParser({
    mapParams: false,
  }));

  // See http://restify.com/docs/plugins-api/#bodyparser
  server.use(restify.plugins.bodyParser({
    mapParams: false,
    maxBodySize: 10 * 1024 * 1024, // 10 MB
    mapFiles: true,
    keepExtensions: true,
  }));

  // Set up our default options for logging
  dbLogbook.registerHandler('db', new DatabaseLogbook())
  dbLogbook.registerHandler('none', new NullLogbook())
  dbLogbook.registerHandler('datadog', new DatadogLogbook())
  dbLogbook.registerHandler('console', new ConsoleLogbook())

  /*
  *  Display a nice message.
  */
  const JUICE_CONFIG = process.env['JUICE_CONFIG']
  const nodeGroup = await juice.string('datp.nodeGroup', "master") // Checked elsewhere
  console.log(`JUICE_CONFIG=`, JUICE_CONFIG)
  console.log();
  console.log(figlet.textSync(nodeGroup, {
    horizontalLayout: 'fitted'
  }));
  console.log();

  await DATP.run()
  await DATP.routesForRestify(server)
  // await DATP.registerAsMaster(server)


  /*
  *  Start the server.
  */
  const port = await juice.int('datp.port', 8080)
  console.log(`Starting server on port ${port}`)
  server.listen(port, '0.0.0.0');
  server.keepAliveTimeout = await juice.integer('datp.keepAliveTimeout', 65000)
  server.headersTimeout = await juice.integer('datp.headersTimeout', 66000)

  // If required, provide routes for MONDAT to call
  const group = await getNodeGroup(nodeGroup)
  // console.log(`group=`, group)
  if (group.serveMondatApi) {
    mondat.registerRoutes(server)
    // await DATP.monitorMidi()
    console.log(` ✔ `.brightGreen + `hosting the API for MONDAT`)
  } else {
    console.log(` ✖ `.red + `not hosting the API for MONDAT`)
  }
  return server
}

export async function hostMondatWebapp(server) {
  const port = await juice.int('datp.port', 8080)
  console.log(`Hosting Mondat application at http://0.0.0.0:${port}/mondat`)
  const staticFilesDir = `${__dirname}/../../MONDAT/dist`
  console.log(`staticFilesDir=`, staticFilesDir)
  server.get('/*', restify.plugins.serveStaticFiles(staticFilesDir))
}
