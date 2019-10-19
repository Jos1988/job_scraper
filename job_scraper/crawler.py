import requests


class Crawler:

    def __init__(self, url_format: str):
        self.url_format = url_format

    def crawl_id(self, id: str):
        url = self.url_format.format(id=id)

        return requests.get(url).json()
