import walletBalanceStep from './i2i-walletBalanceStep'
import walletTopupStep from './i2i-walletTopupStep'
import walletTopupStatusStep from './i2i-walletTopupStatusStep'
import walletWithdrawalStep from './i2i-walletWithdrawalStep'
import walletWithdrawalStatusStep from './i2i-walletWithdrawalStatusStep'
import transferFeesStep from './i2i-transferFeesStep'

export async function register() {
  walletBalanceStep.register()
  walletTopupStep.register()
  walletTopupStatusStep.register()
  walletWithdrawalStep.register()
  walletWithdrawalStatusStep.register()
  transferFeesStep.register()
}

export default {
  register,
}
