import json
from typing import Any, Dict, Text

import pandas
import requests as r
from pandas import ExcelWriter


API_SUFFIXES: Dict = {"L_D_B": "lide-domy-byty"}


class API:
    def __init__(self, api_key: Text) -> None:
        self.api_key: Text = api_key
        self._api_url_base: Text = "https://api.apitalks.store/czso.cz/"
        self._api_auth_header: Dict = {"x-api-key": self.api_key}
        self.data: Any = None

    def _get_all(self, api_suffix: Text, skip_start=0, skip_step=30, data=None) -> Any:

        while True:
            filter_ = '?filter={"skip": %d}' % (skip_start)
            reurl = f"{self._api_url_base}{api_suffix}{filter_}"
            response = r.get(reurl, headers=self._api_auth_header)
            dj = response.json()

            print(reurl, response.status_code)

            if response.status_code == 200:
                if len(dj["data"]) > 0:
                    if skip_start == 0:
                        data = pandas.read_json(
                            json.dumps(dj["data"], ensure_ascii=False)
                        )
                    else:
                        data = data.append(
                            pandas.read_json(json.dumps(dj["data"], ensure_ascii=False))
                        )
                    skip_start += skip_step
                else:
                    print("Finished.")
                    self.data = data
                    break

            else:
                print(
                    f"Connection error.\nResponse status code: {response.status_code} \
                    \nResponse message: {response.json()}\n \
                    Salvaging all data fetched up to this point."
                )
                self.data = data
                break

        return self

    def get_all_lide_domy_byty(self) -> Any:
        self._get_all(API_SUFFIXES["L_D_B"])
        return self

    def save_data(self, filename: Text) -> None:
        print("Saving data...")
        with ExcelWriter(f"{filename}.xlsx", engine="xlsxwriter") as w:
            self.data.to_excel(w, sheet_name=f"{filename}")
        print(f"Data saved to {filename}")
