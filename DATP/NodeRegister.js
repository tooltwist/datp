/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import me from '../ATP/me'
import dbNode from '../database/dbNode'
import dbPipelines from '../database/dbPipelines'
import StepTypeRegister from  '../ATP/StepTypeRegister'

/**
 * This module maintains a list of step types.
 */
class NodeRegister {
  constructor() {
    this.index = [ ]
  }

  async register(endpoint, status) {
    console.log(`NodeRegister.register(${endpoint})`)
    // console.log(`YARP status.pipelines:`, status.pipelines)
    // console.log(`YARP status.stepTypes:`, status.stepTypes)
    let node = this.index[endpoint]
    if (!node) {
      console.log(`    (new)`)
      node = status
      node.registrationTime = new Date()
      this.index[endpoint] = node
    } else {
      console.log(`    (exists)`)
      node.nodeId = status.nodeId
      node.name = status.name
      node.description = status.description
    }
    node.lastReportedIn = new Date()
    node.status = 'online'

    // Update the database
    dbNode.save(node)
  }

  async getNode(endpoint) {
    console.log(`NodeRegister.getNode(${endpoint})`)
    const node = this.index[endpoint]
    return node ? node : null
  }

  async getNodes(includeMe) {
    // console.log(`NodeRegister.getNodes()`)
    const list = [ ]
    for (const endpoint in this.index) {
      console.log(`-> ${endpoint}`)
      const node = this.index[endpoint]
      list.push(node)
    }

    if (includeMe) {
      const myStatus = await me.getNodeStatus()
      myStatus.type = 'master'
      myStatus.pipelines = await dbPipelines.myPipelines()
      myStatus.stepTypes = await StepTypeRegister.myStepTypes()
      list.push(myStatus)
    }

    list.sort((a, b) => {
      if (a.name < b.name) return -1
      if (a.name > b.name) return +1
      return 0
    })
    return list
  }

  // async endpoints() {
  //   console.log(`NodeRegister.endpoints()`)
  //   const list = [ ]
  //   for (const endpoint in this.index) {
  //     console.log(`-> ${endpoint}`)
  //     list.push(endpoint)
  //   }
  //   list.sort((a, b) => {
  //     if (a.endpoint < b.endpoint) return -1
  //     if (a.endpoint > b.endpoint) return +1
  //     return 0
  //   })
  //   return list
  // }//- endpoints
}

const defaultRegister = new NodeRegister()
export default defaultRegister
