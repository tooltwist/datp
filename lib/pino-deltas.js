/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */
const pino = require('pino');

export const logger = pino({
    level: 'trace',
  },
  // pino.destination(`${__dirname}/../,my.log`)
  pino.destination(`/tmp/,my.deltas`)
)
