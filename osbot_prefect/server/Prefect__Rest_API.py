import requests
from osbot_utils.decorators.methods.cache_on_self   import cache_on_self
from osbot_utils.utils.Http                         import url_join_safe, is_url_online
from osbot_utils.utils.Env                          import get_env
from osbot_utils.base_classes.Type_Safe             import Type_Safe
from osbot_utils.utils.Objects                      import dict_to_obj
from osbot_utils.utils.Status                       import status_ok, status_error

ENV_NAME__PREFECT_CLOUD__API_KEY      = 'PREFECT_CLOUD__API_KEY'
ENV_NAME__PREFECT_CLOUD__ACCOUNT_ID   = 'PREFECT_CLOUD__ACCOUNT_ID'
ENV_NAME__PREFECT_CLOUD__WORKSPACE_ID = 'PREFECT_CLOUD__WORKSPACE_ID'
ENV_NAME__PREFECT_TARGET_SERVER       = 'PREFECT_TARGET_SERVER'

DEFAULT_URL__PREFECT_TARGET_SERVER    = "http://localhost:4200/api"

class Prefect__Rest_API(Type_Safe):

    # raw request methods
    def prefect_cloud__api_key(self):
        return get_env(ENV_NAME__PREFECT_CLOUD__API_KEY)

    def prefect_cloud__api_url(self):
        api_key      = self.prefect_cloud__api_key()
        if api_key:
            account_id   = self.prefect_cloud__account_id()
            workspace_id =  self.prefect_cloud__workspace_id()
            if account_id and workspace_id:
                return f"https://api.prefect.cloud/api/accounts/{account_id}/workspaces/{workspace_id}/"


    def prefect_cloud__account_id(self):
        return get_env(ENV_NAME__PREFECT_CLOUD__ACCOUNT_ID)

    def prefect_cloud__workspace_id(self):
        return get_env(ENV_NAME__PREFECT_CLOUD__WORKSPACE_ID)

    @cache_on_self
    def prefect_api_url(self):                                  # todo: add back the support for Prefect API Cloud (to be configured by en
        prefect_cloud_url = self.prefect_cloud__api_url()
        if prefect_cloud_url:
            return prefect_cloud_url
        return DEFAULT_URL__PREFECT_TARGET_SERVER

    def prefect_is_using_local_server(self):
        return self.prefect_api_url() == DEFAULT_URL__PREFECT_TARGET_SERVER

    @cache_on_self
    def prefect_is_server_online(self):
        if self.prefect_is_using_local_server():
            return is_url_online(self.prefect_api_url())
        return self.requests__get('').status == 'ok'

    def get_headers(self):
        return {"Authorization": f"Bearer {self.prefect_cloud__api_key()}"}                            # Create headers dictionary including authorization token

    def requests__for_method(self, method, path, data=None):
        headers  = self.get_headers()
        endpoint = url_join_safe(self.prefect_api_url(), path)                          # Construct the full endpoint URL by joining the base URL with the path

        if method == requests.delete:                                                   # For DELETE requests, pass data as query parameters
            response = method(endpoint, headers=headers, params=data)
        elif method == requests.get:                                                    # For GET requests, pass data as query parameters
            response = method(endpoint, headers=headers, params=data)
        elif method == requests.head:                                                   # For HEAD requests, no payload or parameters are needed
            response = method(endpoint, headers=headers)
        elif method == requests.post:                                                   # For POST requests, pass data as JSON in the request body
            response = method(endpoint, headers=headers, json=data)
        elif method == requests.patch:                                                  # For PATCH requests, pass data as JSON in the request body
            response = method(endpoint, headers=headers, json=data)
        else:
            return status_error("Unsupported request method")                           # Return an error if the method is not supported

        status_code  = response.status_code                                             # Handle the response and return an appropriate result
        content_type = response.headers.get('Content-Type', '')
        if 200 <= status_code < 300:
            if method == requests.head:                                                 # For HEAD requests, return the headers as the response data
                result = status_ok(data=response.headers)
            elif content_type == 'application/json':                                      # For successful JSON responses, return the JSON data
                json_data  = response.json()
                result = status_ok(data=json_data)
            else:
                result = status_ok(data=response.text)                                      # For other successful requests, return the JSON data
        else:
            result = status_error(message=f"{method.__name__.upper()} request to {path}, failed with status {status_code}", error=response.text) # For failed requests, return an error message with status and response text
        return dict_to_obj(result)


    def requests__delete(self, path, params=None):                                      # Wrapper for executing DELETE requests
        return self.requests__for_method(requests.delete, path, data=params)

    def requests__get(self, path, params=None):                                         # Wrapper for executing GET requests
        return self.requests__for_method(requests.get, path, data=params)

    def requests__post(self, path, data):                                               # Wrapper for executing POST requests
        return self.requests__for_method(requests.post, path, data=data)

    def requests__head(self, path):                                                     # Wrapper for executing HEAD requests
        return self.requests__for_method(requests.head, path)

    def requests__update(self, path, data):                                               # Wrapper for executing PATCH requests
        return self.requests__for_method(requests.patch, path, data=data)

    # request helpers

    def create(self, target, data):
        path = f'/{target}'
        return self.requests__post(path, data)

    def delete(self, target, target_id):
        path = f'/{target}/{target_id}'
        return self.requests__delete(path)

    def read(self, target, target_id):
        path = f'/{target}/{target_id}'
        return self.requests__get(path)

    def filter(self, target, filter_data):          # todo: add support for fetching all items
        path = f'/{target}/filter'
        return self.requests__post(path, filter_data)

    def update(self, target, target_id, target_data):
        path = f'/{target}/{target_id}'
        return self.requests__update(path, target_data)

    def update_action(self, target, target_id, target_action, target_data):
        path = f'/{target}/{target_id}/{target_action}'
        return self.requests__post(path, target_data)