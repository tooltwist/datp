import masterServer from './restify/masterServer'
import slaveServer from './restify/slaveServer'

async function restifySlaveServer(options) {
  return slaveServer.startSlaveServer(options)
}

async function restifyMasterServer(options) {
  return masterServer.startMasterServer(options)
}

export default {
  restifyMasterServer,
  restifySlaveServer,
}