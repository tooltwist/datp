// const sdk = require('api')('@i2i/v1.0#5cj6w2sksefu54g');
import axios from 'axios'


// Please note that for all WALLET related endpoints, you must use the Treasurer role credentials.
// For i2i Transfer, Instapay, Pesonet, Domestic Transfer and Report related endpoints, you must use the TxnMaker role credentials
export const TEST_TREASURER_EMAIL = 'i2iacct+tooltwisttreasurer'
export const TEST_TREASURER_PASSWORD = 'P@ssw0rd123!'
export const TEST_TXMAKER_EMAIL = 'i2iacct+tooltwisttxnmaker'
export const TEST_TXMAKER_PASSWORD = 'P@ssw0rd123!'


// For Instapay and Pesonet, you may use the following receiving bank information for testing:
// Bank   Account Number
// Chinabank Savings (CBS)       3590333624
export const TEST_CHINABANK_ACCOUNT = '3590333624'
// UNIONBANK                              00010083420
export const TEST_UNIONBANK_ACCOUNT = '00010083420'

export async function authenticate(instance) {

  //ZZZZZ Get credentials for the current user the database
  // const username = TEST_TXMAKER_EMAIL
  // const password = TEST_TXMAKER_PASSWORD
  const username = TEST_TREASURER_EMAIL
  const password = TEST_TREASURER_PASSWORD

  console.log(`authenticate(${username}, ${password})`)

  try {
    // See https://i2i.readme.io/reference/gettingtheaccesstokentoauthenticaterequest

    // const reply = await sdk.Gettingtheaccesstokentoauthenticaterequest()
    // curl --request POST \
    // --url https://api.stg.i2i.ph/api-apic/auth/token \
    // --header 'Accept: application/json' \
    // --header 'Content-Type: application/json'

    const url = `https://api.stg.i2i.ph/api-apic/auth/token`


    const reply = await axios.post(url, { username, password }, {
      // headers: {
      //   'Authorization': `Basic ${token}`
      // }
    })

    // console.log(reply)
    return reply.data.accessToken
  } catch (e) {
    console.error(err)
  }
}

export async function getWalletBalance(authenticationToken) {
  console.log(`getWalletBalance()`)
  // See https://i2i.readme.io/reference/getwalletbalance
  //curl --request GET \
  // --url https://api.stg.i2i.ph/api-apic/wallet/balance \
  // --header 'Accept: application/json'

  const url = 'https://api.stg.i2i.ph/api-apic/wallet/balance'
  const reply = await axios.get(url, {
    headers: {
      Authorization: authenticationToken
    }
  })
  // console.log(`reply=`, reply)
  return reply.data.walletBalance
}


// export async function topupWallet(authenticationToken, senderReference, amount, remarks) {
//   console.log(`getWalletBalance(${amount})`)
//   // // See https://i2i.readme.io/reference/topup
//   //   curl --request POST \
//   //   --url https://api.stg.i2i.ph/api-apic/wallet/topup/process \
//   //   --header 'Accept: application/json' \
//   //   --header 'Content-Type: application/json' \
//   //   --data '
//   // {
//   //   "senderReference": "UB123456",
//   //   "amount": 123.45,
//   //   "remarks": "i2i"
//   // }
//   // '
//   return reply
// }