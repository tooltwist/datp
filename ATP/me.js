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
