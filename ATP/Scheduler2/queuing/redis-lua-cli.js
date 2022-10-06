import {RedisLua} from './redis-lua'

function needArgs(num) {
  if (process.argv.length < num) {
    console.log(`Need ${num} arguments`)
    process.exit(1)
  }
}

async function main() {
  // console.log(`process.argv=`, process.argv)
  const lua = new RedisLua()
  await RedisLua._checkLoaded()

  needArgs(3)
  const cmd = process.argv[2]
  // console.log(`cmd=`, cmd)
  switch (cmd) {
    case 'in':
    case 'out':
    case 'admin':
      const queueName = cmd
      needArgs(5)
      const nodeGroup = process.argv[3]
      const txId = process.argv[4]
      await lua.enqueue(queueName, 'test-pipeline', {
        eventType: 'start-pipeline',
        nodeGroup,
        state: {
          txId,
          status: 'running',
          metadata: { webhook: 'http://localhost:3000/webhook' },
          data: { some: 'stuff' }
        }
      })
      break

    case 'pull':
      let num = 1
      needArgs(4)
      const nodeGroup2 = process.argv[3]
      if (process.argv.length >= 5) {
        const tmp = parseInt(process.argv[4])
        if (!isNaN(tmp)) {
          num = tmp
        }
      }
      const events = await lua.getEvents(nodeGroup2, num)
      console.log(events)
      break

    case 'state':
      needArgs(4)
      const txId2 = process.argv[3]
      const state = await lua.getState(txId2)
      console.log(`state=`, state)
      break
      
    case 'complete':
      needArgs(5)
      const txId3 = process.argv[3]
      const status3 = process.argv[4]
      const result3 = await lua.transactionCompleted(txId3, status3)
      console.log(`result3=`, result3)
      break

    case 'keys':
      const keys = await lua.keys('*')
      console.log(`keys=`, keys)
      break

    case 'delete':
      needArgs(4)
      const txId4 = process.argv[3]
      const result4 = await lua.deleteZZZ(txId4)
      console.log(`result4=`, result4)
      break

    case 'subscribe':
      await lua.listenToNotifications((type, txId, status) => {
        console.log(`Transaction ${txId} has ${type}. Status=${status}`)
      })
      return 'keep-running'

    case 'notify':
      needArgs(4)
      const txId5 = process.argv[3]
      await lua.notify(txId5)
      break
  
    default:
      console.log(`Unknown command [${cmd}]`)
      return 1
  }
  return 0
}

main().then(status => {
  console.log(`Finished`, status)
  if (status !== 'keep-running') {
    process.exit(status)
  }
}).catch(e => {
  console.log(`e=`, e)
  process.exit(1)
})
