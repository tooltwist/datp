
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
