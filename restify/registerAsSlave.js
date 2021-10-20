import juice from '@tooltwist/juice-client'
import axios from 'axios'
import me from '../ATP/me'
import dbPipelines from '../database/dbPipelines'
// import healthcheck from './healthcheck'
import StepTypeRegister from  '../ATP/StepTypeRegister'


const REPORT_IN_INTERVAL = 15 * 1000

export async function registerAsSlave(server) {
  console.log(`registerAsSlave()`)

  // healthcheck.registerRoutes(server)

  const protocol = await juice.string('datp.protocol')
  const host = await juice.string('datp.host')
  const port = await juice.int('datp.port')
  const slaveEndpoint = `${protocol}://${host}:${port}`


  // Initialize the name, etc
  const nodeId = await me.getNodeId()
  const name = await me.getName()
  const masterEndpoint = await juice.string('datp.master.endpoint', juice.MANDATORY)
  console.log(`masterEndpoint=`, masterEndpoint)
  console.log(`nodeId=`, nodeId)
  console.log(`name=`, name)

  const registerMe = async () => {
    console.log(`Registering with master`)
    const url = `${masterEndpoint}/master/registerSlave`
    const status = await me.getNodeStatus()
    status.type = 'slave'
    status.slaveEndpoint = slaveEndpoint
    status.pipelines = await dbPipelines.myPipelines()
    status.stepTypes = await StepTypeRegister.myStepTypes()
    // console.log(`status=`, status)
    const result = await axios.post(url, status)
    // console.log(`result.data=`, result.data)
  }

  // Initial registration
  registerMe()

  // Periodically send our details to the master node.
  setInterval(registerMe, REPORT_IN_INTERVAL)
}

export default {
  registerAsSlave,
}
