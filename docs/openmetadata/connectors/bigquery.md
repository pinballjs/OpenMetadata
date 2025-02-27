---
description: This guide will help install BigQuery connector and run manually
---

# BigQuery

{% hint style="info" %}
**Prerequisites**

OpenMetadata is built using Java, DropWizard, Jetty, and MySQL.

1. Python 3.7 or above
{% endhint %}

### Install from PyPI

{% tabs %}
{% tab title="Install Using PyPI" %}
```bash
pip install 'openmetadata-ingestion[bigquery]'
```
{% endtab %}
{% endtabs %}

## Run Manually

```bash
metadata ingest -c ./examples/workflows/bigquery.json
```

### Configuration

{% code title="bigquery-creds.json (boilerplate)" %}
```javascript
{
  "type": "service_account",
  "project_id": "project_id",
  "private_key_id": "private_key_id",
  "private_key": "",
  "client_email": "gcpuser@project_id.iam.gserviceaccount.com",
  "client_id": "",
  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
  "token_uri": "https://oauth2.googleapis.com/token",
  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
  "client_x509_cert_url": ""
}
```
{% endcode %}

{% code title="bigquery.json" %}
```javascript
{
  "source": {
    "type": "bigquery",
    "config": {
      "project_id": "project_id",
      "host_port": "bigquery.googleapis.com",
      "username": "username",
      "service_name": "gcp_bigquery",
      "data_profiler_enabled": "true",
      "data_profiler_offset": "0",
      "data_profiler_limit": "50000",
      "options": {
        "credentials_path": "examples/creds/bigquery-cred.json"
      },
      "table_filter_pattern": {
        "excludes": ["demo.*","orders.*"]
      },
      "schema_filter_pattern": {
        "excludes": [
          "[\\w]*cloudaudit.*",
          "[\\w]*logging_googleapis_com.*",
          "[\\w]*clouderrorreporting.*"
        ]
      }
    }
  },
```
{% endcode %}

1. **username** - pass the Bigquery username.
2. **password** - the password for the Bigquery username.
3. **service\_name** - Service Name for this Bigquery cluster. If you added the Bigquery cluster through OpenMetadata UI, make sure the service name matches the same.
4. **schema\_filter\_pattern** - It contains includes, excludes options to choose which pattern of schemas you want to ingest into OpenMetadata.
5. **table\_filter\_pattern** - It contains includes, excludes options to choose which pattern of tables you want to ingest into OpenMetadata.
6. **database -** Database name from where data is to be fetched.
7. **data\_profiler\_enabled** - Enable data-profiling (Optional). It will provide you the newly ingested data.
8. **data\_profiler\_offset** - Specify offset.
9. **data\_profiler\_limit** - Specify limit.

### Publish to OpenMetadata

Below is the configuration to publish Bigquery data into the OpenMetadata service.

Add `metadata-rest` sink along with `metadata-server` config

{% code title="bigquery.json" %}
```javascript
{
  "source": {
    "type": "bigquery",
    "config": {
      "project_id": "project_id",
      "host_port": "bigquery.googleapis.com",
      "username": "username",
      "service_name": "gcp_bigquery",
      "data_profiler_enabled": "true",
      "data_profiler_offset": "0",
      "data_profiler_limit": "50000",
      "options": {
        "credentials_path": "examples/creds/bigquery-cred.json"
      },
      "table_filter_pattern": {
        "excludes": ["demo.*","orders.*"]
      },
      "schema_filter_pattern": {
        "excludes": [
          "[\\w]*cloudaudit.*",
          "[\\w]*logging_googleapis_com.*",
          "[\\w]*clouderrorreporting.*"
        ]
      }
    }
  },
  "sink": {
    "type": "metadata-rest",
    "config": {
      "api_endpoint": "http://localhost:8585/api"
    }
  },
  "metadata_server": {
    "type": "metadata-server",
    "config": {
      "api_endpoint": "http://localhost:8585/api",
      "auth_provider_type": "no-auth"
    }
  }
}
```
{% endcode %}
