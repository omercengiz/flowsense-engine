# Airflow authentication

FlowSense keeps Airflow authentication behind the `AirflowAuthProvider` port.
The collector does not need to know whether credentials come from Airflow,
a reverse proxy, a secret manager, or an external identity system.

## Built-in modes

### Airflow login token

This remains the default for Airflow 3 deployments that expose `/auth/token`:

```env
AIRFLOW_AUTH_MODE=token
AIRFLOW_USERNAME=airflow
AIRFLOW_PASSWORD=secret
```

FlowSense exchanges the credentials once and caches the returned bearer token
for the lifetime of the client.

### Basic authentication

Common Airflow 2 Stable REST API deployments can use:

```env
AIRFLOW_API_VERSION=v1
AIRFLOW_AUTH_MODE=basic
AIRFLOW_USERNAME=airflow
AIRFLOW_PASSWORD=secret
```

### Static bearer token

For deployments that issue a token outside Airflow:

```env
AIRFLOW_AUTH_MODE=bearer
AIRFLOW_BEARER_TOKEN=secret-token
```

Username and password are not required in bearer mode. Secrets are excluded
from the `AirflowConfig` representation, but applications must still keep them
out of logs, shell history, source control, and command-line arguments.

## Custom provider

Enterprise integrations can inject an `AirflowAuthProvider` directly. This is
appropriate for rotating tokens, identity-aware proxies, workload identity, or
deployment-specific headers:

```python
from flowsense.infrastructure.airflow import (
    AirflowClient,
    AirflowConfig,
    HeaderAuthProvider,
)

provider = HeaderAuthProvider({"X-Forwarded-User": "flowsense"})
client = AirflowClient(
    AirflowConfig(base_url="https://airflow.example.com"),
    auth_provider=provider,
)
```

For rotating bearer credentials, pass a callable. It is evaluated for every
Airflow request:

```python
from flowsense.infrastructure.airflow import BearerTokenAuthProvider

provider = BearerTokenAuthProvider(load_current_token)
```

Custom providers should return only authentication material and leave request
execution, retry, timeout, payload validation, and error translation to
`AirflowClient`.
