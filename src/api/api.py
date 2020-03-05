from sys import setrecursionlimit, getrecursionlimit
import json

from typing import Text, Generic, Union, Dict, List
import requests as r
import pandas


class API(object):
    def __init__(self, api_key: Text) -> None:
        self.api_key = api_key
        self._api_url_base = "https://api.apitalks.store/czso.cz/"
        self._api_auth_header = {"x-api-key": self.api_key}

    def get_all_lide_domy_byty(self, skip=0, data=None) -> Union[None, List]:
        cur_rec_lim = getrecursionlimit()
        setrecursionlimit(10000)

        filter = '?filter={"skip": %d}' % (skip)
        reurl = f"{self._api_url_base}lide-domy-byty{filter}"
        response = r.get(reurl, headers=self._api_auth_header)
        print(reurl, response.status_code)

        while response.status_code == 200:
            if skip == 0:
                data = pandas.read_json(
                    json.dumps(response.json()["data"], ensure_ascii=False)
                )
            else:
                data = data.append(
                    pandas.read_json(
                        json.dumps(response.json()["data"], ensure_ascii=False)
                    )
                )
            print(data.head)
            skip += 30
            self.get_all_lide_domy_byty(skip=skip, data=data)

        setrecursionlimit(cur_rec_lim)
        return data

    def save_data(self, data: object, filename: Text) -> None:
        data.to_excel(f"{filename}.xlsx")
