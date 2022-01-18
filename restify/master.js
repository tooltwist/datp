/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
/*
 *  Master server.
 */
import restify from 'restify'
import NodeRegister from '../DATP/NodeRegister'
import me from '../ATP/me'
import dbNode from '../database/dbNode'
import mondat from './mondat'

async function registerSlaveV1(req, res) {
  // console.log(`registerSlaveV1()`)

  const nodeStatus = req.body

  const slaveEndpoint = nodeStatus.slaveEndpoint
  console.log(`slaveEndpoint=`, slaveEndpoint)

  //ZZZZZ Should authenticate request.
  await NodeRegister.register(slaveEndpoint, nodeStatus)

  res.send({ status: 'ok' })
}


async function registerAsMaster(server) {
  // console.log(`registerAsMaster()`)

  mondat.registerRoutes(server)

  // Initialize the name, etc
  const nodeId = await me.getNodeId()
  const name = await me.getName()
  const description = await me.getDescription()

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
