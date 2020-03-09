from typing import Dict


LABELS_LIDE_DOMY_BYTY: Dict[str, str] = {
    "id": "Unique_ID",
    "typuz_naz": "Typ_území",
    "nazev": "Název_územní_jednotky",
    "uzcis": "Kód_územního_číselníku",
    "uzkod": "Kód_položky_územního_číselníku",
    "u01": "Obyvatelstvo_celkem",
    "u02": "Obyvatelstvo_muži",
    "u03": "Obyvatelstvo_ženy",
    "u04": "Obyvatelstvo_ve_věku_0-14_let",
    "u05": "Obyvatelstvo_ve_věku_15-64_let",
    "u06": "Obyvatelstvo_ve_věku_64_let_a_více",
    "u07": "Ekonomicky_aktivní",
    "u08": "Ekonomicky_aktivní-zaměstnaní",
    "u09": "Obydlené_domy",
    "u10": "Obydlené_byty",
    "u11": "Hospodařící_domácnosti",
}

LABELS_VYJIZDKY_ZAMESTNANI: Dict[str, str] = {
    "idhod": "Unique_ID",
    "hodnota": "Hodnota",
    "stapro_kod": "Kód_statistické_proměnné_datové_sady",
    "ekonaktiv_kod": "Kód_ekonomické_aktivity_v_datové_sadě",
    "uzemiz_cis": "Kód_pro_území_odkud_se_vyjíždí",
    "uzemiz_kod": "Kód_položky_pro_území_odkud_se_vyjíždí",
    "uzemido_cis": "Kód_pro_území_kam_se_dojíždí",
    "uzemido_kod": "Kód_položky_pro_území_kam_se_dojíždí",
    "datum": "Referenční_období",
    "uzemiz_txt": "Název_území_odkud_se_vyjíždí",
    "uzemido_txt": "Název_území_kam_se_dojíždí",
}
