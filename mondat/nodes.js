/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
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
