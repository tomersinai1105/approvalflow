# Dapr client for communication between services using Dapr sidecar APIs.





import os

import urllib.request
import urllib.parse
import urllib.error

import json

from typing import Any




DAPR_HTTP_PORT = os.getenv("DAPR_HTTP_PORT", "3500")
DAPR_BASE_URL = os.getenv("DAPR_BASE_URL", f"http://127.0.0.1:{DAPR_HTTP_PORT}")
DAPR_PUBSUB_NAME = os.getenv("DAPR_PUBSUB_NAME", "pubsub")
DAPR_STATE_STORE_NAME = os.getenv("DAPR_STATE_STORE_NAME", "statestore")
DAPR_SECRET_STORE_NAME = os.getenv("DAPR_SECRET_STORE_NAME", "local-secret-store")




# Error raised when the local Dapr sidecar cannot complete a request.
class DaprClientError(RuntimeError):
    """Error raised when the local Dapr sidecar cannot complete a request."""




# Convert a given payload to its JSON version. 
def convert_to_json(payload: dict | list | None) -> bytes | None:
    if payload is None:
        return None
    return json.dumps(payload, ensure_ascii = False).encode("utf-8")




# Send an HTTP request based on given details to the local Dapr sidecar. 
# If the local Dapr sidecar cannot complete a request, raise the error accordingly.
# Else return the response of the local Dapr sidecar. 
def dapr_request(
    method: str,
    path: str,
    payload: dict | list | None = None,
    expected_statuses: tuple[int, ...] = (200, 204),
) -> Any:
    
    
    url = f"{DAPR_BASE_URL}{path}"
    headers = {}
    payload_json = convert_to_json(payload)

    if payload_json is not None:
        headers["Content-Type"] = "application/json"

    request = urllib.request.Request(
        url = url,
        data = payload_json,
        headers = headers,
        method = method.upper(),
    )


    try:
        
        with urllib.request.urlopen(request, timeout = 10) as response:
            
            status = response.status
            response_body = response.read().decode("utf-8")

            if status not in expected_statuses:
                raise DaprClientError(
                    f"Dapr call failed. method = {method}, path = {path}, "
                    f"status = {status}, body = {response_body}"
                )

            if response_body.strip() == "":
                return None

            return json.loads(response_body)


    except urllib.error.HTTPError as error:
        
        error_body = error.read().decode("utf-8")
        
        raise DaprClientError(
            f"Dapr HTTP error. method = {method}, path = {path}, "
            f"status = {error.code}, body = {error_body}"
        ) from error


    except urllib.error.URLError as error:
        
        raise DaprClientError(
            f"Could not connect to Dapr sidecar at {DAPR_BASE_URL}. "
            "Make sure the service is running with its Dapr sidecar."
        ) from error




# Publish an event based on a given payload through Dapr Pub/Sub to services listening to a given topic.
def dapr_publish_event(topic: str, data: dict) -> None:
    dapr_request(
        method = "POST",
        path = f"/v1.0/publish/{DAPR_PUBSUB_NAME}/{topic}",
        payload = data,
        expected_statuses = (200, 204),
    )




# Invoke another service through Dapr service invocation based on given details.
def dapr_invoke_service(
    app_id: str,
    method_name: str,
    payload: dict | None = None,
    http_method: str = "POST",
):

    clean_method_name = method_name.lstrip("/")

    return dapr_request(
        method = http_method,
        path = f"/v1.0/invoke/{app_id}/method/{clean_method_name}",
        payload = payload,
        expected_statuses = (200, 204),
    )




# Save a JSON based on given key and value in the Dapr state store.
def dapr_save_state(key: str, value: Any) -> None:

    dapr_request(
        method = "POST",
        path = f"/v1.0/state/{DAPR_STATE_STORE_NAME}",
        payload = [{"key": key, "value": value}],
        expected_statuses = (200, 204),
    )




# Read the value of a given key from the Dapr state store.
# If the value exists, return the value, else return a given default. 
def dapr_get_state(key: str, default = None):

    encoded_key = urllib.parse.quote(key, safe = "")

    result = dapr_request(
        method = "GET",
        path = f"/v1.0/state/{DAPR_STATE_STORE_NAME}/{encoded_key}",
        payload = None,
        expected_statuses = (200, 204),
    )

    if result is None:
        return default

    return result




# Delete the value of a given key from the Dapr state store.
def dapr_delete_state(key: str) -> None:

    encoded_key = urllib.parse.quote(key, safe = "")

    dapr_request(
        method = "DELETE",
        path = f"/v1.0/state/{DAPR_STATE_STORE_NAME}/{encoded_key}",
        payload = None,
        expected_statuses = (200, 204),
    )




# Read a secret with a given name from the from the configured Dapr secret store.
# If the secret exists, return it, else return a given default. 
def dapr_get_secret(secret_name: str, default=None):

    encoded_secret_name = urllib.parse.quote(secret_name, safe="")

    result = dapr_request(
        method = "GET",
        path = f"/v1.0/secrets/{DAPR_SECRET_STORE_NAME}/{encoded_secret_name}",
        payload = None,
        expected_statuses = (200,),
    )

    if isinstance(result, dict):
        
        if secret_name in result:
            return result[secret_name]

        if len(result) == 1:
            return next(iter(result.values()))

    if result is None:
        return default

    return result




# Read a secret with a given name from the from the configured Dapr secret store.
# If the local Dapr sidecar cannot complete a request, raise the error accordingly.
# Else if the secret exists in the configured Dapr secret store, return it.
# Else read the secret from the .env file.
# If the secret exists in the .env file, return it.
# Else return a given default. 
def dapr_get_secret_or_env(secret_name: str, default = None):
    
    try:
        secret = dapr_get_secret(secret_name, default = None)
        
    except DaprClientError:
        secret = None

    if secret in (None, ""):
        secret = os.getenv(secret_name, default)

    return secret