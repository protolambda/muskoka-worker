# muskoka-worker

Wrapper around Eth2 consensus state machines, to run state transitions on demand,
 and upload results to a shared service.
This can then be triggered, diffed between clients, and stay tracked on a website.

For local testing, just use an user-account to authenticate the pubsub subscription:
`gcloud auth login` and `export GCP_PROJECT=my_project_id` to run.

To run with a service account: 
- `GOOGLE_APPLICATION_CREDENTIALS=muskoka-testing.key.json`: path to a service key for testing (`.key.json` is git-ignored).
    Required permissions: Pub/Sub subscriber, to the specified subscription id (`sub-id`).

Options:

| type   | option name      | default                          | description |
|--------|------------------|----------------------------------|-------------|
| `str`  | `bucket-name`    | `muskoka-transitions`            | the name of the storage bucket to download input data from |
| `str`  | `results-api`    | `https://example.com/foobar`     | the API endpoint to notify of the transition results, and retrieve signed urls from for output uploading |
| `str`  | `cli-cmd`        | `zcli transition blocks`         | change the cli cmd to run transitions with |
| `str`  | `gcp-project-id` | `muskoka`                        | change the google cloud project to connect with pubsub to |
| `str`  | `sub-id`         | `poc-v0.8.3`                     | the pubsub subscription to listen for tasks from |
| `str`  | `client-name`  | `eth2team`                       | the client name; 'zrnt', 'lighthouse', etc. |
| `str`  | `client-version` | `v0.1.2_1a2b3c4`                 | the client version, and git commit hash start. In this order, separated by an underscore. |
| `bool` | `cleanup-tmp`    | `true`                           | if the temporary files should be removed after uploading the results of a transition |


Also see [`muskoka-server`](https://github.com/protolambda/muskoka-server).
