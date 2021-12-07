
import Scheduler, { DUMP, EVENT, STEP_START, TICK, TX_START } from './Scheduler2'

const TRANSACTION_COMPLETION_HANDLER_NAME = 'tx_end_CH'

async function main() {

  // Add a transactions to the scheduler
  const txId = 't-90872645906258976'
  await EVENT(TX_START, {
    metadata: {
      txId,
      transactionType: 'example',
      tenant: {
        tenantId: 'fred',
      },
      // pipeline: 'example',
      completionHandlerName: TRANSACTION_COMPLETION_HANDLER_NAME,
      contextForCompletionHandler: {
        txId,
        userCompletionHandlerName: 'sendReplyCH',
        // options
      }
    },
    data: {
      amount: 123.456
    }
  })

  await TICK()
  // await pause(6)

  // Add a task to the scheduler and see what happens
  // await EVENT(STEP_START, {
  //   metadata: {
  //     txId: 't-123',
  //     stepId: 's-456',
  //     definition: {
  //       stepType: 'example/demo',
  //     }
  //     // tenant: {
  //     //   tenantId: 'fred',
  //     // },
  //     // completionHandlerName: 'call me some time'
  //   },
  //   data: {
  //   }
  // })

  // DUMP()



  // await EVENT(TX_START, {
  //   tx: '2'
  //   // metadata: {
  //   //   mdYarp: 'abc'
  //   // },
  //   // data: {
  //   //   txId: '123'
  //   // }
  // })




  // const status = await Scheduler.getStatus()
  // console.log(`status=`, status)

  // DUMP()

  // await TICK()
  // DUMP()


  // await pause(8)
  // DUMP()



  // TICK()
  // DUMP()

}

async function pause(seconds) {
  return new Promise((resolve, reject) => {
    setTimeout(() => {
      return resolve()
    }, seconds * 1000)
  })
}

main().catch(e => {
  console.log(`e=`, e)
})