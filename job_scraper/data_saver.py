import requests


class Saver:

    def __init__(self, api_endpoint: str, verbosity: int = 0):
        self.verbosity = verbosity
        self.api_endpoint = api_endpoint

    def upload(self, job):
        response = requests.post(url=self.api_endpoint, json=job, verify=False)
        if self.verbosity > 0:
            print('uploaded data.')

        if self.verbosity > 1:
            print('status: ' + str(response.status_code))

        if response.status_code != 201:
            print(response.text)
            raise RuntimeError('mapping error')

        return response.status_code
