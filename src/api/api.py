# pyre-strict
import json
from typing import Any, Dict, Text
from multiprocessing import Pool
from os.path import isdir, join
from os import makedirs, getcwd
from time import sleep

# pyre-fixme[21]: Could not find `pandas`.
import pandas
import requests as r
from pandas import ExcelWriter

from src.api.schemas import LABELS_LIDE_DOMY_BYTY, LABELS_VYJIZDKY_ZAMESTNANI

DATA_OUTPUT: Text = join(getcwd(), "data")
# pyre-fixme[24]: Generic type `dict` expects 2 type parameters, use `typing.Dict`
#  to avoid runtime subscripting errors.
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
    """Class for API interactions with "api.apitalks.store/czso.cz/" api.
    Provides all methods to access, process and save data.

    Methods are constructed to be used as chained, e.g:

    api = API("yourkey")
    api.get_all_lide_domy_byty().save_data("filename_string")
    
    Returns:
        object -- API class instance
    """

    def __init__(self, api_key: Text) -> None:
        """Initializer for API class.
        
        Arguments:
            api_key {Text} -- private API key provided by Apitalks
        """
        self.api_key: Text = api_key
        self._api_url_base: Text = "https://api.apitalks.store/czso.cz/"
        # pyre-fixme[24]: Generic type `dict` expects 2 type parameters, use
        #  `typing.Dict` to avoid runtime subscripting errors.
        self._api_auth_header: Dict = {"x-api-key": self.api_key}
        # pyre-fixme[4]: Attribute annotation cannot be `Any`.
        self.data: Any = None

    # pyre-fixme[3]: Return annotation cannot be `Any`.
    def _get_all(
        # pyre-fixme[2]: Parameter must be annotated.
        # pyre-fixme[2]: Parameter must be annotated.
        # pyre-fixme[2]: Parameter must be annotated.
        self, api_suffix: Text, skip_start=0, skip_step=30, sleep_=False
    ) -> Any:
        """Private method.
        Gets all data from given resource of the api, as specified via api_suffix
        argument.
        Data are stored as pandas dataframe in public self.data attribute of the API class instance.
        Api data are provided as 30 records per "page", i.e.
        list of 30 dicts.
        
        Arguments:
            api_suffix {Text} -- resource for specific data which is added to base api url
        
        Keyword Arguments:
            skip_start {int} -- offset, where to start with data retrieval  (default: {0})
            skip_step {int} -- offset for data paging, range between 0 - 30 (default: {30})
            sleep_ {bool} -- whether to pause for 0.5 secs between each request
            (used only, when method get_all_data() is called, since it is using multiprocessing) (default: {False})
        
        Returns:
            Any -- pandas DataFrame in self.data attribute
        """

        data = None

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
                        # pyre-fixme[16]: Optional type has no attribute `append`.
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

    # pyre-fixme[3]: Return annotation cannot be `Any`.
    def _get_single(self, api_suffix: Text, id_: Text) -> Any:
        """Private method.
        Gets data for provided id_ parameter from given resource of the api, as specified via api_suffix
        argument.
        Data are stored as pandas dataframe in public self.data attribute of the API class instance.
        
        Arguments:
            api_suffix {Text} -- resource for specific data which is added to base api url
            id_ {Text} -- unique id of the dataset which is accessed via api resource using api_suffix argument
        
        Returns:
            Any -- pandas DataFrames stored id self.data attribute
        """
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
        """Private method.
        Checks, if directory in given directory path exists.
        If not, creates all directories recursively on provided path.
        
        Arguments:
            dirpath {Text} -- absolute path to the directory which is to be checked
        """
        if not isdir(dirpath):
            makedirs(dirpath, exist_ok=True)

    # pyre-fixme[3]: Return annotation cannot be `Any`.
    # pyre-fixme[24]: Generic type `dict` expects 2 type parameters, use
    #  `typing.Dict` to avoid runtime subscripting errors.
    def replace_labels(self, labels: Dict) -> Any:
        """Replaces labels in pandas DataFrame object.
        
        Arguments:
            labels {Dict} -- labels which are to be used as replacement
        
        Returns:
            Any -- pandas DataFrame object with replaced labels
        """
        try:
            self.data = self.data.rename(columns=labels)
            return self
        except Exception:
            return self

    # pyre-fixme[3]: Return annotation cannot be `Any`.
    # pyre-fixme[2]: Parameter must be annotated.
    def _worker(self, api_suffix: Text, sleep_=True) -> Any:
        """Private method.
        Worker for multiprocessed method "get_all_data".
        Gets data from given api resource into the pandas DataFrame object.
        
        Arguments:
            api_suffix {Text} -- resource for specific data which is added to base api url
        
        Keyword Arguments:
            sleep_ {bool} -- whether to pause for 0.5 secs between each request (default: {True})
        
        Returns:
            Any -- pandas DataFrame object stored in self.data attribute
        """
        self._get_all(api_suffix, sleep_=sleep_).save_data(f"{api_suffix}"[:30])

    def get_all_data(self) -> None:
        """Gets data from all resources of the API.
        Resources are specified in API_SUFFIXES dict.
        """
        with Pool(maxtasksperchild=1) as p:
            p.map(self._worker, list(API_SUFFIXES.values()))

    # pyre-fixme[3]: Return annotation cannot be `Any`.
    # pyre-fixme[2]: Parameter must be annotated.
    def get_all_lide_domy_byty(self, human_labels=False) -> Any:
        """Gets all data from api resource "lide-domy-byty".
        
        Keyword Arguments:
            human_labels {bool} -- whether to change default "code" labels to human readeble ones (default: {False})
        
        Returns:
            Any -- pandas DataFrame object stored in self.data attribute
        """
        self._get_all(API_SUFFIXES["L_D_B"])
        if human_labels:
            self.replace_labels(LABELS_LIDE_DOMY_BYTY)
        return self

    # pyre-fixme[3]: Return annotation cannot be `Any`.
    # pyre-fixme[2]: Parameter must be annotated.
    def get_single_lide_domy_byty(self, id_: Text, human_labels=False) -> Any:
        """Gets data for given id from specific api resource "lide-domy-byty".
        
        Arguments:
            id_ {Text} -- unique id of the data of the api resource
        
        Keyword Arguments:
            human_labels {bool} -- whether to change default "code" labels to human readeble ones (default: {False})
        
        Returns:
            Any -- pandas DataFrame object stored in self.data attribute
        """
        self._get_single(API_SUFFIXES["L_D_B"], id_)
        if human_labels:
            self.replace_labels(LABELS_LIDE_DOMY_BYTY)
        return self

    # pyre-fixme[3]: Return annotation cannot be `Any`.
    # pyre-fixme[2]: Parameter must be annotated.
    def get_all_vyjizdky_do_zamestnani(self, human_labels=False) -> Any:
        """Gets all data from api resource "vyjizdky-zamestnani"
        
        Keyword Arguments:
            human_labels {bool} -- whether to change default "code" labels to human readeble ones (default: {False})
        
        Returns:
            Any -- pandas DataFrame object stored in self.data attribute
        """
        self._get_all(API_SUFFIXES["V_Z"])
        if human_labels:
            self.replace_labels(LABELS_VYJIZDKY_ZAMESTNANI)
        return self

    # pyre-fixme[3]: Return annotation cannot be `Any`.
    # pyre-fixme[2]: Parameter must be annotated.
    def get_all_ceny_potravin(self, human_labels=False) -> Any:
        """Gets all data from api resource "prumerne-spotrebitelske-ceny-vybranych-vyrobku-potravinarske-vyrobky"
        
        Keyword Arguments:
            human_labels {bool} -- whether to change default "code" labels to human readeble ones (default: {False})
        
        Returns:
            Any -- pandas DataFrame object stored in self.data attribute
        """
        self._get_all(API_SUFFIXES["P_C_P_V"])
        if human_labels:
            # TODO
            pass
        return self

    # pyre-fixme[2]: Parameter must be annotated.
    def save_data(self, filename: Text, dirpath=DATA_OUTPUT) -> None:
        """Saves data in pandas DataFrame object into xlsx file.
        Filename and sheetname are specified by filename argument.
        !!!Sheetname can be at max 31 chars long!!!
        
        Arguments:
            filename {Text} -- string used to name file and data sheet
        
        Keyword Arguments:
            dirpath {[type]} -- absolute path to directory, where file is saved (default: {DATA_OUTPUT})
        """
        print("Saving data...")
        self._create_dirs_if_not_exist(dirpath)
        filepath = join(dirpath, f"{filename}.xlsx")
        with ExcelWriter(filepath, engine="xlsxwriter") as w:
            self.data.to_excel(w, sheet_name=f"{filename}")
        print(f"Data saved to {filepath}")
