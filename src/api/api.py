import json
from typing import Any, Dict, Text
from multiprocessing import Pool
from os.path import isdir, join
from os import makedirs, getcwd
from time import sleep

import pandas
import requests as r
from pandas import ExcelWriter

from src.api.schemas import LABELS_LIDE_DOMY_BYTY, LABELS_VYJIZDKY_ZAMESTNANI

DATA_OUTPUT: Text = join(getcwd(), "data")
API_SUFFIXES: Dict = {
    "L_D_B": "lide-domy-byty",
    "V_Z": "vyjizdky-zamestnani",
    "O_S_J": "obyvatele-sidelni-jednotky",
    "O_D": "obyvatelstvo-domy",
    "P_H_M": "prumerne-mzdy-odvetvi",
    "C_O_V_P": "cizinci-podle-statniho-obcanstvi-veku-a-pohlavi",
    "P_O": "pohyb-obyvatel-za-cr-kraje-okresy-so-orp-a-obce",
    "N_D_O": "nadeje-doziti-v-okresech-a-spravnich-obvodech-orp",
    "P_C_P_V": "prumerne-spotrebitelske-ceny-vybranych-vyrobku-potravinarske-vyrobky",
    "H_P_Z": "hoste-a-prenocovani-v-hromadnych-ubytovacich-zarizenich-podle-zemi",
}


class API:
    def __init__(self, api_key: Text) -> None:
        self.api_key: Text = api_key
        self._api_url_base: Text = "https://api.apitalks.store/czso.cz/"
        self._api_auth_header: Dict = {"x-api-key": self.api_key}
        self.data: Any = None

    def _get_all(
        self, api_suffix: Text, skip_start=0, skip_step=30, data=None, sleep_=False
    ) -> Any:

        while True:
            filter_ = '?filter={"skip": %d}' % (skip_start)
            reurl = f"{self._api_url_base}{api_suffix}{filter_}"
            response = r.get(reurl, headers=self._api_auth_header)
            dj = response.json()

            print(reurl, response.status_code, dj)

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
                    if sleep_:
                        sleep(0.5)
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

    def worker(self, api_suffix: Text, sleep_=True) -> Any:
        self._get_all(api_suffix, sleep_=sleep_).save_data(f"{api_suffix}"[:30])

    def get_all_data(self) -> None:
        with Pool() as p:
            p.map(self.worker, list(API_SUFFIXES.values()))

    def _get_single(self, api_suffix: Text, id_: Text) -> Any:
        reurl = f"{self._api_url_base}{api_suffix}/{id_}"
        response = r.get(reurl, headers=self._api_auth_header)
        dj = response.json()

        print(reurl, response.status_code, dj)

        if response.status_code == 200:
            self.data = pandas.read_json(
                json.dumps(dj, ensure_ascii=False), typ="frame", orient="index"
            ).T
        else:
            print(
                f"Connection error.\nResponse status code: {response.status_code} \
                \nResponse message: {response.json()}"
            )

        return self

    def _create_dirs_if_not_exist(self, dirpath: Text) -> None:
        if not isdir(dirpath):
            makedirs(dirpath, exist_ok=True)

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

    def get_single_lide_domy_byty(self, id_: Text, human_labels=False) -> Any:
        self._get_single(API_SUFFIXES["L_D_B"], id_)
        if human_labels:
            self.replace_labels(LABELS_LIDE_DOMY_BYTY)
        return self

    def get_all_vyjizdky_do_zamestnani(self, human_labels=False) -> Any:
        self._get_all(API_SUFFIXES["V_Z"])
        if human_labels:
            self.replace_labels(LABELS_VYJIZDKY_ZAMESTNANI)
        return self

    def get_all_ceny_potravin(self, human_labels=False) -> Any:
        self._get_all(API_SUFFIXES["P_C_P_V"])
        if human_labels:
            # TODO
            pass
        return self

    def save_data(self, filename: Text, dirpath=DATA_OUTPUT) -> None:
        print("Saving data...")
        self._create_dirs_if_not_exist(dirpath)
        filepath = join(dirpath, f"{filename}.xlsx")
        with ExcelWriter(filepath, engine="xlsxwriter") as w:
            self.data.to_excel(w, sheet_name=f"{filename}")
        print(f"Data saved to {filepath}")
