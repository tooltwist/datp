// import DATP from '../DATP/datp'
import DATP from '../DATP/datp'
import corsMiddleware from "restify-cors-middleware";
import figlet from 'figlet'
import juice from '@tooltwist/juice-client'
import me from '../../DATP/ATP/me'

const restify = require('restify');

async function startMasterServer(options) {
  console.log(`startMasterServer()`)

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
  // server.pre(cors.preflight)
  // server.use(cors.actual)

  // See http://restify.com/docs/plugins-api/#queryparser
  server.use(restify.plugins.queryParser({ mapParams: false }));
  server.use(restify.plugins.bodyParser({ mapParams: false }));


  // A few routes
  // function sendV1(req, res, next) {
  //   res.send('hello: ' + req.params.name);
  //   return next();
  // }

  // function sendV2(req, res, next) {
  //   res.send({ hello: req.params.name });
  //   return next();
  // }

  // server.get('/hello/:name', restify.plugins.conditionalHandler([
  //   { version: '1.1.3', handler: sendV1 },
  //   { version: '2.0.0', handler: sendV2 }
  // ]));
  // const server = await restifyMasterServer()
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
   *  See if we are also serving up Mondate static files.
   */
  const port = await juice.int('datp.port', 8080)
  const serveMondat = await juice.boolean('datp.serveMondat', false)
  if (serveMondat) {
    console.log(`Hosting Mondate application at http://0.0.0.0:${port}//mondate`)
    const staticFilesDir = `${__dirname}/../../MONDAT/dist`
    console.log(`staticFilesDir=`, staticFilesDir)
    server.get('/mondat/*', restify.plugins.serveStaticFiles(staticFilesDir))
  }

  /*
  *  Start the server.
  */
  console.log(`Starting server on port ${port}`)
  server.listen(port, '0.0.0.0');
// })().catch(e => {
//   console.error(e)
// })

  return server
}

export default {
  startMasterServer
}
