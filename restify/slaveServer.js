import DATP from '../DATP/datp'
// import dateRunner from './date-runner'
// import * as corsMiddleware from "restify-cors-middleware";
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
  // server.pre(cors.preflight)
  // server.use(cors.actual)

  // See http://restify.com/docs/plugins-api/#queryparser
  server.use(restify.plugins.queryParser({ mapParams: false }));
  server.use(restify.plugins.bodyParser({ mapParams: false }));


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
  server.listen(port);
}

export default {
  startSlaveServer
}