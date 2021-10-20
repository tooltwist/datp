/*
 *  Master server.
 */
import restify from 'restify'
import NodeRegister from '../DATP/NodeRegister'
import me from '../ATP/me'
import dbNode from '../DATP/database/dbNode'
// import healthcheck from './healthcheck'
import mondat from './mondat'

async function registerSlaveV1(req, res) {
  console.log(`registerSlaveV1()`)
  // console.log(`req.params=`, req.params)
  // console.log(`req.body=`, req.body)

  const nodeStatus = req.body

  const slaveEndpoint = nodeStatus.slaveEndpoint
  console.log(`slaveEndpoint=`, slaveEndpoint)

  //ZZZZZ Should authenticate request.
  await NodeRegister.register(slaveEndpoint, nodeStatus)

  res.send({ status: 'ok' })
}


async function registerAsMaster(server) {
  console.log(`registerAsMaster()`)
  // const datpMasterEndpoint = await juice.string('datp.masterEndpoint', juice.MANDATORY)
  // console.log(`datpMasterEndpoint=`, datpMasterEndpoint)

  // healthcheck.registerRoutes(server)
  mondat.registerRoutes(server)

  // Initialize the name, etc
  const nodeId = await me.getNodeId()
  const name = await me.getName()
  const description = await me.getDescription()
  // console.log(`Y nodeId=`, nodeId)
  // console.log(`Y name=`, name)

  // Register this master node
  const masterNode = {
    nodeId,
    name,
    description,
    type: 'master',
    status: 'online'
  }
  await dbNode.save(masterNode)

  server.post('/master/registerSlave', restify.plugins.conditionalHandler([
    { version: '1.1.3', handler: registerSlaveV1 },
  ]));
}


export default {
  registerAsMaster,
  // registerSlaveV1,
}
