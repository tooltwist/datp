import NodeRegister from '../DATP/NodeRegister'

export async function listNodesV1(req, res, next) {
  // console.log(`listNodesV1()`)
  try {
    const includeMe = true
    const nodeList = await NodeRegister.getNodes(includeMe)
    // console.log(`nodeList=`, nodeList)
    res.send(nodeList)
    return next();
  } catch (e) {
    console.log(`Error in listNodesV1():`, e)
  }
}
