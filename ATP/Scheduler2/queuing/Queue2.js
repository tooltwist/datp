/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
import { RedisQueue } from "./RedisQueue-ioredis"
// import { RedisQueue } from "./RedisQueue-node-redis"

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