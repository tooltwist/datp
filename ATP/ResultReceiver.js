import ResultReceiverRegister from './ResultReceiverRegister'

export default class ResultReceiver {
  constructor() {

  }

  static async register(name, resultReceiver) {
    return ResultReceiverRegister.register(name, resultReceiver)
  }
}