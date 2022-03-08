# Webhooks

The API client has request to receive updates on transaction status by providing a URL in the request.

```json
{
  "reply": "http://..........",
  "progressReports": true
}
```

If progress reports is also true, the webhook will also be called whenever the _progress report_  value for the transaction changes.

## Completed Transactions
Transaction completion, whether success or failure, is reported by the webhook via the completion handler for the transaction pipeline. The name is `RETURN_TX_STATUS_WITH_WEBHOOK_CALLBACK` and it is handled in `returnTxStatusWithWebhookCallback.js`. The completion handler runs on the same node as the transaction pipeline, and calls `sendStatusByWebhook()`

## Progress reports
Progress reports are triggered within `StepInstance.progressReport()` when the progress report value is changed, by also calling `sendStatusByWebhook()`.

## Audit and retries
An atp_webhook record is created for each required webhook reply, before the first attempt. In this record there is a retry counter, and also a retry time. If the webhook call to the client is successful we clear the retry time. If the call is not successful, we log the problem to the transaction's log, and our `cron` process will call `resendStatusByWebhook()` at the alloted time.

Retries initially occur fairly quickly, but the interval between retries increases exponentially with subsequent attempts, up to a maximum retry interval.

## Webhooks that fail to complete
We don't want webhooks that fail to succeed causing retries forever an clogging the system, so we have the ability to view utstanding webhooks in Mondat. A webhook that is continually failing indicates a transaction where the client is not being notified of the status of the transaction. While an administrator can cancel the webhook, they shoud probably also contact the client to inform them that their webhook URL is not responding correctly.

## Dedicated webhook processing
At the moment that webhook call will occur from whatever node is running the trasaction pipeline, or in the case of progress reports, the current step. In the future we may wish to centralize the dispatching of webhooks by running them through a queue.
