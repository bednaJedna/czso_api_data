import json
from typing import Any, Dict, Text

import pandas
import requests as r
from pandas import ExcelWriter

from src.api.schemas import LABELS_LIDE_DOMY_BYTY, LABELS_VYJIZDKY_ZAMESTNANI

API_SUFFIXES: Dict = {"L_D_B": "lide-domy-byty", "V_Z": "vyjizdky-zamestnani"}


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

    def replace_labels(self, labels: Dict) -> Any:
        try:
            self.data = self.data.rename(columns=labels)
            return self
        except Exception:
            return self

    def get_all_lide_domy_byty(self, human_labels=False) -> Any:
        self._get_all(API_SUFFIXES["L_D_B"])
        if human_labels:
            self.replace_labels(LABELS_LIDE_DOMY_BYTY)
        return self

    def get_all_vyjizdky_do_zamestnani(self, human_labels=False) -> Any:
        self._get_all(API_SUFFIXES["V_Z"])
        if human_labels:
            self.replace_labels(LABELS_VYJIZDKY_ZAMESTNANI)
        return self

    def save_data(self, filename: Text) -> None:
        print("Saving data...")
        with ExcelWriter(f"{filename}.xlsx", engine="xlsxwriter") as w:
            self.data.to_excel(w, sheet_name=f"{filename}")
        print(f"Data saved to {filename}")
