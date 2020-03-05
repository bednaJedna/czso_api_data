import json
from sys import setrecursionlimit
from typing import Any, Text, Union

import pandas
import requests as r
from pandas import ExcelWriter


class API:
    def __init__(self, api_key: Text) -> None:
        self.api_key = api_key
        self._api_url_base = "https://api.apitalks.store/czso.cz/"
        self._api_auth_header = {"x-api-key": self.api_key}

    def get_all_lide_domy_byty(
        self, skip_start=0, skip_step=30, data=None
    ) -> Union[None, Any]:

        setrecursionlimit(100000)

        filter_ = '?filter={"skip": %d}' % (skip_start)
        reurl = f"{self._api_url_base}lide-domy-byty{filter_}"
        response = r.get(reurl, headers=self._api_auth_header)
        dj = response.json()

        print(reurl, response.status_code)

        if response.status_code == 200:
            if len(dj["data"]) > 0:
                if skip_start == 0:
                    data = pandas.read_json(json.dumps(dj["data"], ensure_ascii=False))
                else:
                    data = data.append(
                        pandas.read_json(json.dumps(dj["data"], ensure_ascii=False))
                    )
                skip_start += skip_step
                return self.get_all_lide_domy_byty(
                    skip_start=skip_start, skip_step=skip_step, data=data
                )

            else:
                print("Finished.")
                return data

        else:
            print(
                f"Connection error, response status code {response.status_code} \
                Salvaging all data fetched up to this point."
            )
            return data

    def save_data(self, data: Any, filename: Text) -> None:
        with ExcelWriter(f"{filename}.xlsx", engine="xlsxwriter") as w:
            data.to_excel(w, sheet_name="{filename}")
