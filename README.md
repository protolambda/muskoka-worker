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
| `str`  | `inputs-bucket`  | `muskoka-transitions`            | the name of the storage bucket to download input data from |
| `str`  | `spec-version`   | `v0.8.3`                         | the spec-version to target |
| `str`  | `spec-config`    | `minimal`                        | the config name to target |
| `str`  | `cli-cmd`        | `zcli transition blocks`         | change the cli cmd to run transitions with |
| `str`  | `gcp-project-id` | `muskoka`                        | change the google cloud project to connect with pubsub to |
| `str`  | `worker-id`      | `poc`                            | the name of the worker. Pubsub subscription id is formatted as: `<spec version>~<spec config>~<client name>~<worker id>` to get a unique subscription name |
| `str`  | `client-name`    | `eth2team`                       | the client name; 'zrnt', 'lighthouse', etc. |
| `str`  | `results-bucket` | `results-eth2team`               | the name of the bucket to upload the results to |
| `str`  | `client-version` | `v0.1.2_1a2b3c4`                 | the client version, and git commit hash start. In this order, separated by an underscore. |
| `bool` | `cleanup-tmp`    | `true`                           | if the temporary files should be removed after uploading the results of a transition |


Also see [`muskoka-server`](https://github.com/protolambda/muskoka-server).

## Dockerfile

This code is build in a docker image, for other docker images to extend or extract the executable (`muskoka_worker`) from.
Dockerhub: [`protolambda/muskoka_worker`](https://hub.docker.com/repository/docker/protolambda/muskoka_worker)

## License

MIT, see [LICENSE](./LICENSE) file.
