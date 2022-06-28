# Overview

Go app to download logs from SumoLogic, export to a CSV file and optionally upload to S3. If large date ranges of logs are required they will be split into a CSV file per day. 

## Usage

The app can be run using the following command `go run cmd/main.go` or built via `make build` and executed via binary `./app`.

### Config.yaml

A config file exists at the root with the following options:

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| concurrency | int | 5 | Number of goroutines to use when querying, e.g. if 5, 5 days worth of logs will be retrieved concurrently |
| sumo.apiUrl | string | `https://api.eu.sumologic.com/api/v1/search/jobs` | Base sumologic api endpoint to use, differs per region  |
| sumo.query.statement | string | `nil` | The query to execute |
| sumo.query.statement | string | `nil` | The query to execute |
| sumo.query.statement | string | `nil` | The query to execute |
| sumo.query.timeZone | string | `GMT` | The query to execute |
| filename | string | `nil` | The base filename for the CSV |
| s3.enabled | bool | `false` | Specifies whether files should be uploaded to s3 |
| s3.bucket | string | `nil` | The name of the bucket to upload to |
| s3.deleteOnUpload | `false` | Specifies whether files should be deleted locally after upload |

The following can be set in `config.yaml` or as environment variables:

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| ACCESS_ID | string | `nil` | SumoLogic Access ID |
| ACCESS_KEY | string | `nil`| SumoLogic Access Key  |
| AWS_REGION | string | `nil` | AWS region the bucket resides in |
| AWS_ACCESS_KEY_ID | string | `nil` | AWS Access Key ID |
| AWS_SECRET_ACCESS_KEY | string | `nil` | AWS Secret Access Key |