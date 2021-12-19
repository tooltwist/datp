export const DO_NOTHING_CALLBACK = `doNothingCallback`

export async function doNothingCallback (callbackContext, data) {
  console.log(`==> doNothingCallback()`, callbackContext, data)
}
