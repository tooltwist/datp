/* Copyright Tooltwist Innovations Limited - All Rights Reserved
 * This file is part of DATP and as such is proprietary and confidential software.
 * Unauthorized copying of this file, via any medium is strictly prohibited. All
 * rights reserved. No warranty, explicit or implicit, provided. In no event shall
 * the author or owner be liable for any claim or damages.
 */

const COMPLETE = 'complete'
const HOLD = 'hold'
const ROLLBACK = 'rollback'
const ERROR = 'error'


class StepResponse {
  constructor(status, description, data) {
    this.status = status
    this.description = description
    this.data = data
  }

  getStatus () {
    return this.status
  }

  getDescription () {
    return this.description
  }

  getData () {
    return this.data
  }
}

export default {
  cls: StepResponse,
  COMPLETE,
  HOLD,
  ROLLBACK,
  ERROR,
}
