/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import GenerateHash from "./GenerateHash"
import juice from '@tooltwist/juice-client'

let myId = null
let name = null
let description = null

async function getNodeId() {
  if (!myId) {
    myId = await GenerateHash('node')
    // console.log(`Z myId=`, myId)
  }
  return myId
}

async function getName() {
  if (!name) {
    name = await juice.string('datp.name')
    // console.log(`Z name=`, name)
  }
  return name
}

async function getDescription() {
  if (!description) {
    description = await juice.string('datp.description')
    // console.log(`Z description=`, description)
  }
  return description
}

async function getNodeStatus () {
  const status = {
    nodeId: await getNodeId(),
    name: await getName(),
    description: await getDescription(),
  }
  return status
}


export default {
  getNodeStatus,
  getNodeId,
  getName,
  getDescription,
}
