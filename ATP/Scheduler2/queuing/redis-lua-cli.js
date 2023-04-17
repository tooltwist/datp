/* Copyright Twist Innovations Limited - All Rights Reserved
 * This file is part of the DATP software and subject to license restrictions.
 * See the LICENSE file distributed with this software for details.
 * All rights reserved. No warranty, explicit or implicit, provided. In no
 * event shall the author or owner be liable for any claim or damages.
 */
import { archiveTransactionState } from '../archiving/ArchiveProcessor'
import { luaGetCachedState } from './redis-cachedState'
import {RedisLua} from './redis-lua'
import { luaListenToNotifications, luaNotify } from './redis-notifications'
import { luaTransactionsToArchive } from './redis-transactions'
import { luaTransactionCompleted } from './redis-txCompleted'

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
      const txState = { }
      const event = {
        eventType: 'start-pipeline',
        nodeGroup,
        state: {
          txId,
          status: 'running',
          metadata: { webhook: 'http://localhost:3000/webhook' },
          data: { some: 'stuff' }
        }
      }
      const checkExternalIdIsUnique = false
      await lua.luaEnqueue(queueName, txState, event, 'test-pipeline', checkExternalIdIsUnique, null)
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
      const events = await lua.luaDequeue(nodeGroup2, num)
      console.log(events)
      break

    case 'state':
      needArgs(4)
      const txId2 = process.argv[3]
      const withMondatDetails = false
      const state = await luaGetCachedState(txId2, withMondatDetails)
      // console.log(`state=`, state)
      break
      
    // case 'complete':
    //   needArgs(5)
    //   const txId3 = process.argv[3]
    //   const status3 = process.argv[4]
    //   const result3 = await luaTransactionCompleted(txId3, status3)
    //   console.log(`result3=`, result3)
    //   break

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
      await luaListenToNotifications((type, txId, status) => {
        console.log(`Transaction ${txId} has ${type}. Status=${status}`)
      })
      return 'keep-running'

    case 'notify':
      needArgs(4)
      const txId5 = process.argv[3]
      await luaNotify(txId5)
      break
      
    case 'archive':
      needArgs(3)
      // const txId3 = process.argv[3]
      // const status3 = process.argv[4]
      const persisted = [ ]
      const nodeId = 'me'
      const numRequired = 50
      const result5 = await luaTransactionsToArchive(persisted, nodeId, numRequired)

      for (const item of result5) {
        if (item[0] === 'transaction') {
          const txId = item[1]
          const json = item[2]
          const state = JSON.parse(json)
          console.log(`PERSIST ${txId}:`, JSON.stringify(state, '', 2))

          try {
            await archiveTransactionState(txId, json)
            persisted.push(txId)
          } catch (e) {
            console.log(`Could not save state of transaction ${txId}`, e)
          }
        }
      }
      // Remove the states from REDIS
      const result6 = await luaTransactionsToArchive(persisted, nodeId, 0)
      console.log(`result6=`, result6)

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
