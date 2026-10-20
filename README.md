# smartlead

A Python job that cleans up [SmartLead](https://www.smartlead.ai/) campaigns. For every **completed** campaign it checks each **completed** lead's message history. If the lead never replied and the last message is more than 7 days old, it deletes the lead from the campaign.

## Features

- **Producer/consumer threading.** One thread (`addToLeadQueue`) fetches the leads for each campaign and queues them. A second thread (`processLeadQueue`) checks each lead's message history and deletes it if needed. Campaigns and leads sit on separate `queue.Queue`s (`campaignQueue` and `leadQueue`). The campaign thread exits when it reads a `None` sentinel. The lead thread does not break on `None` (that `break` is not in the current code); both loops stop when `stop_event`, a `threading.Event`, is set, including on Ctrl-C.
- **Client-side rate limiting.** Every API call goes through one `request()` method wrapped in `ratelimit` (`@sleep_and_retry` above `@limits(calls=110, period=60)`), which caps the job at 110 calls per 60 seconds.
- **Retries.** An HTTP error response is logged and retried after 2 seconds (a recursive `request()` call, no retry cap). If the call fails before a response exists, the handler reads `response.status_code` and the retry does not run.
- **Polars for CSV.** The campaign's lead export (`/leads-export`, CSV) is parsed with Polars and filtered to `status == "COMPLETED"`.
- **GitHub Actions.** `.github/workflows/main.yml` installs dependencies and runs `python main.py` with `API_KEY` set from the `API_KEY` repository secret. It currently runs manually (`workflow_dispatch`). A daily cron at 18:30 UTC (midnight IST) is in the file but commented out, so the `schedule` trigger has no active cron.

## SmartLead endpoints used

Requests go to `https://server.smartlead.ai`. The API key is sent as the `api_key` query parameter.

| Purpose | Endpoint |
|---|---|
| List campaigns | `GET /api/v1/campaigns` |
| Export a campaign's leads (CSV) | `GET /api/v1/campaigns/{campaign_id}/leads-export` |
| Lead message history | `GET /api/v1/campaigns/{campaign_id}/leads/{lead_id}/message-history` |
| Remove lead from campaign | `DELETE /api/v1/campaigns/{campaign_id}/leads/{lead_id}` |

## Setup

```bash
git clone https://github.com/swayam0322/smartlead.git
cd smartlead
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
```

## Configuration

The only setting is your SmartLead API key, read from the environment variable `API_KEY` (a `.env` file is loaded with `python-dotenv`):

```bash
# .env  (gitignored; never commit it)
API_KEY=your-smartlead-api-key
```

The current source does not hardcode this key. In GitHub Actions, add `API_KEY` under Settings → Secrets and variables → Actions.

## Run

```bash
python3 main.py
```

To schedule it, uncomment the `cron` line in `.github/workflows/main.yml`.

## Caution

This job **deletes** leads. Try it on a test campaign first. To make it a dry run, comment out the `self._delete_leads(...)` call in `processLeadQueue`.

## Limitations / next steps

- HTTP-error retries have no upper limit and no backoff (fixed 2 second delay). A persistent HTTP error will retry forever. A good next step is capped exponential backoff.
- The "7 days" threshold and the "COMPLETED" filters are hard-coded. Making them CLI flags or env vars would help.
- Only one worker thread checks lead histories. A small thread pool would speed it up within the rate limit.
- A lead with an empty message history raises an error (`emails[-1]`), which is skipped silently.
