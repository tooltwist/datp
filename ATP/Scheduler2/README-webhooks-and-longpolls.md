# Webhooks and Longpolls

ZZZ This needs to be revised - Phil 12th April 2022


The API client can request to receive updates on transaction status by providing a
webhook URL in the request. They can also use long polling to get the transaction
result, provided the transaction completes within the configured timeout.

```json
{
  "webhook": "http..........",
  "progressReports": true,
  "poll": "long"
}
```

If progress reports is also true, the webhook will also be called whenever the _progress report_  value for the transaction changes.

## Completed Transactions
Transaction completion, whether success or failure, is reported via the completion handler for the transaction pipeline. The name is `RETURN_TX_STATUS_CALLBACK_ZZZ` and it is handled in `returnTxStatusCallbackZZZ.js`. The completion handler runs on the same node as the transaction pipeline.

First it tries to send the ttransaction status to the API client via long poll,
and will succeed if there is currently an API long polling to get the transaction
status. This could be the initial transaction initiation request, or a subsequent
call to get the transaction status, in either case having long polling set.

The completion handler also tries to reply via webhook, if the transaction
initiation request specified a webhook, by calling `sendStatusByWebhook()`.

It is possible to have the transaction status returned by both long polling _and_
the webhook. There is a special case however...

If the API call that initiates the transaction specified long polling, and the
transaction completes before that long poll timeouts, then the transaction
status will be returned by the longpoll, but not by the webook.

Once the initial API call's long poll expires however, this webhook cancelling
will not occur.

## Progress reports
Progress reports are triggered within `StepInstance.progressReport()` when the progress report value is changed, by also calling `sendStatusByWebhook()`.

## Audit and retries
An atp_webhook record is created for each required webhook reply, before the first attempt. In this record there is a retry counter, and also a retry time. If the webhook call to the client is successful we clear the retry time. If the call is not successful, we log the problem to the transaction's log, and our `cron` process will call `resendStatusByWebhook()` at the alloted time.

Retries initially occur fairly quickly, but the interval between retries increases exponentially with subsequent attempts, up to a maximum retry interval.

## Webhooks that fail to complete
We don't want webhooks that fail to succeed causing retries forever and clogging the system, so we have the ability to view outstanding webhooks in Mondat. A webhook that is continually failing indicates a transaction where the client is not being notified of the status of the transaction. While an administrator can cancel the webhook, they shoud probably also contact the client to inform them that their webhook URL is not responding correctly.

## Dedicated webhook processing
At the moment that webhook call will occur from whatever node is running the trasaction pipeline, or in the case of progress reports, the current step. In the future we may wish to centralize the dispatching of webhooks by running them through a queue.
