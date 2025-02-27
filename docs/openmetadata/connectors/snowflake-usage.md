---
description: This guide will help install Snowflake Usage connector and run manually
---

# Snowflake Usage

{% hint style="info" %}
**Prerequisites**

OpenMetadata is built using Java, DropWizard, Jetty, and MySQL.

1. Python 3.7 or above
{% endhint %}

### Install from PyPI

{% tabs %}
{% tab title="Install Using PyPI" %}
```bash
pip install 'openmetadata-ingestion[snowflake-usage]'
```
{% endtab %}
{% endtabs %}

## Run Manually

```bash
metadata ingest -c ./examples/workflows/snowflake_usage.json
```

### Configuration

{% code title="snowflake_usage.json" %}
```javascript
{
  "source": {
    "type": "snowflake-usage",
    "config": {
      "host_port": "account.region.service.snowflakecomputing.com",
      "username": "username",
      "password": "strong_password",
      "database": "SNOWFLAKE_SAMPLE_DATA",
      "account": "account_name",
      "service_name": "snowflake",
      "duration": 2
    }
  },
```
{% endcode %}

1. **username** - pass the Snowflake username.
2. **password** - the password for the Snowflake username.
3. **service\_name** - Service Name for this Snowflake cluster. If you added the Snowflake cluster through OpenMetadata UI, make sure the service name matches the same.
4. **schema\_filter\_pattern** - It contains includes, excludes options to choose which pattern of schemas you want to ingest into OpenMetadata.
5. **table\_filter\_pattern** - It contains includes, excludes options to choose which pattern of tables you want to ingest into OpenMetadata.
6. **database -** Database name from where data is to be fetched.

### Publish to OpenMetadata

Below is the configuration to publish Snowflake Usage data into the OpenMetadata service.

Add Optionally `query-parser` processor, `table-usage` stage and`metadata-usage` bulk\_sink along with `metadata-server` config

{% code title="snowflake_usage.json" %}
```javascript
{
  "source": {
    "type": "snowflake-usage",
    "config": {
      "host_port": "account.region.service.snowflakecomputing.com",
      "username": "username",
      "password": "strong_password",
      "database": "SNOWFLAKE_SAMPLE_DATA",
      "account": "account_name",
      "service_name": "snowflake",
      "duration": 2
    }
  },
  "processor": {
    "type": "query-parser",
    "config": {
      "filter": ""
    }
  },
  "stage": {
    "type": "table-usage",
    "config": {
      "filename": "/tmp/snowflake_usage"
    }
  },
  "bulk_sink": {
    "type": "metadata-usage",
    "config": {
      "filename": "/tmp/snowflake_usage"
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
