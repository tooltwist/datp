import { RedisQueue } from "./RedisQueue"

// let manager = null

// async function _check() {
//   if (manager) {
//     return
//   }
//   const type = 'redis'
//   switch (type) {
//     case 'redis':
//       manager = new RedisQueue()
//       break

//     default:
//       console.log(`Fatal error: Unknown queue manager type [${type}]. Please check config.`)
//       process.exit(1)
//   }
// }

// export async function checkRunning() {
//   await _check()
// }

// export async function enqueue(nodeGroup, queueName, item) {
//   await _check()
//   return manager.enqueue(nodeGroup, queueName, item)
// }

// export async function dequeue(nodeGroup, queueName, wait=true) {
//   await _check()
//   return manager.dequeue(nodeGroup, queueName, wait)
// }

// export async function queueLength(nodeGroup, queueName=null) {
//   // console.log(`Queue2.queueLength(${nodeGroup}, ${queueName})`)

//   await _check()
//   return manager.queueLength(nodeGroup, queueName)
// }


export async function getQueueConnection() {
    const type = 'redis'

  switch (type) {
    case 'redis':
      return new RedisQueue()
      break

    default:
      console.log(`Fatal error: Unknown queue manager type [${type}]. Please check config.`)
      process.exit(1)
  }

}