from fastapi import Form, UploadFile
from pydantic import BaseModel
from typing import Annotated


class AnlzPrjInfo(BaseModel):
    internal_id: Annotated[str, Form()]
    name: Annotated[str, Form()]
    categorical: Annotated[str, Form()]
    type: Annotated[str, Form()]
    status: Annotated[str, Form()]
    owner: Annotated[str, Form()]


class AnlzUploadRawdataFiles(BaseModel):
    is_md: Annotated[bool, Form()]
    is_qme: Annotated[bool, Form()]
    lst_file_rawdata: Annotated[list[UploadFile], Form()]


class AnlzRawdata(BaseModel):
    is_md: bool = False
    data: str = ''
    info: str = ''


class AnlzLogging(BaseModel):
    is_running: bool = False
    lst_logging: list = [list]


class AnlzPyScript(BaseModel):
    txt_pre_processing_script: str = """
import pandas as pd
import numpy as np
# :param df_data: dataframe from uploaded rawdata
# :param df_info: dataframe from uploaded rawdata
# :return: update df_data and df_info to UPLOADED RAWDATA
# WARNING!!! DO NOT RENAME df_data and df_info
# ---------PRE-PROCESSING---------
self.logger.info('Pre processing')
# Example: 
# df_data['Age_Group'] = 2023 - df_data['Age']
# df_info = pd.concat([df_info, pd.DataFrame(columns=['var_name', 'var_lbl', 'var_type', 'val_lbl'], data=[['Age_Group', 'Độ tuổi', 'SA', {'1': '18-25', '2': '26-35'}]])], axis=0)

# Input your python code from here


# -------END PRE-PROCESSING-------"""

    txt_define_structure_script: str = """
# ---------DEFINE STRUCTURE---------
self.logger.info('Define structure')

# Add mean & group(T2B, B2B,...) to exist scaling questions (if any)
dict_qre_group_mean = {
    'Main_1_OL_Noi_chung': {
        'range': [],
        'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
        'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
    },
}

# Net code for exist questions (if any)
dict_qre_net_info = {
    'Main_MA_vi_ngot_Q12_[0-9]+': {
        '2': 'Đường mía',
        '3': 'Đường cát/ đường trắng',
        '4': 'Đường phèn', '5': 'Đường bột',
        'net_code': {
            '90001|TRÁI CÂY (NET)': {
                '1': 'Trái cây',
                '13': 'Trái cây, vui lòng ghi rõ loại trái cây',
                '201': 'Chanh dây', '202': 'Cam/ chanh/ quýt',
            },
        }
    }
}

# -------END DEFINE STRUCTURE-------"""

    txt_main_processing_script: str = """
# -----Re-Format uploaded OE files-----
dict_qre_OE_info_org = dict()
if codelist_file.filename:
    dict_qre_OE_info_org = eval(codelist_file.file.read())

dict_qre_OE_info = dict()
for k, v in dict_qre_OE_info_org.items():
    oe_name, oe_num_col = k.rsplit('|', 1)
    oe_num_col = oe_num_col.split('-')
    oe_num_col = range(int(oe_num_col[0]), int(oe_num_col[1]) + 1)

    for i in oe_num_col:
        dict_qre_OE_info.update({
            f'{oe_name}_{i}': [f'{oe_name}_{i}'] + v
        })

lst_addin_OE_value = list()
if coding_file.filename:
    lst_addin_OE_value = eval(coding_file.file.read())
# ---END Re-Format uploaded OE files---

# -----Export data tables-----
if tables_format_file.filename:

    # -----ADD MEAN & GROUP-----
    self.logger.info('ADD MEAN & GROUP')

    lst_qre_mean = list()
    lst_qre_group = list()

    if dict_qre_group_mean:
        for key, val in dict_qre_group_mean.items():

            if val['range']:
                for i in val['range']:
                    lst_qre_mean.append([f'{key}_{i}', val['mean']])
                    lst_qre_group.append([f'{key}_{i}', val['group']])
            else:
                lst_qre_mean.append([key, val['mean']])
                lst_qre_group.append([key, val['group']])

    # -----End ADD MEAN & GROUP-----

    str_topline_file_name = self.str_file_name.replace('.xlsx', '_Topline.xlsx')
    DataTableGenerator.__init__(self, df_data=df_data_stack, df_info=df_info_stack,
                                xlsx_name=str_topline_file_name, logger=self.logger,
                                lst_qre_group=lst_qre_group, lst_qre_mean=lst_qre_mean, is_md=False)

    lst_func_to_run = eval(tables_format_file.file.read())
    self.run_tables_by_js_files(lst_func_to_run)
    self.format_sig_table()
# -----End Export data tables-----

# -----Generate SAV files-----
self.logger.info('Generate SAV files')

# -----Remove net_code to export sav-----
df_info_stack_without_net = df_info_stack.copy()

for idx in df_info_stack_without_net.index:
    val_lbl = df_info_stack_without_net.at[idx, 'val_lbl']

    if 'net_code' in val_lbl.keys():
        df_info_stack_without_net.at[idx, 'val_lbl'] = self.unnetted_qre_val(val_lbl)
# -----END Remove net_code to export sav-----

self.generate_sav_sps(df_data=df_data_stack, df_qres_info=df_info_stack_without_net,
                      df_data_2=df_data_unstack, df_qres_info_2=df_info_unstack,
                      is_md=False, is_export_xlsx=True)
# -----END Generate SAV files-----"""



class AnlzDTables(BaseModel):
    lst_dtables: list = [
        {
            "func_name": "run_standard_table_sig",
            "tables_to_run": ["SCREENER", "MAIN", "KPI"],
            "tables_format": {

                "SCREENER": {
                    "tbl_name": "SCREENER",
                    "tbl_filter": "Ma_SP == 1",
                    "is_count": 0,
                    "is_pct_sign": 1,
                    "is_hide_oe_zero_cats": 1,
                    "sig_test_info": {
                        "sig_type": "",
                        "sig_cols": [],
                        "lst_sig_lvl": []
                    },
                    "lst_side_qres": [
                        {"qre_name": "Recruit_S0_Thanh_pho"},
                        {"qre_name": "$Recruit_S1_Nganh_cam"},
                        {"qre_name": "Recruit_S2_Gioi_tinh"}
                    ],
                    "lst_header_qres": [
                        [
                            {
                                "qre_name": "Recruit_S2_Gioi_tinh",
                                "qre_lbl": "Gender",
                                "cats": {
                                    "TOTAL": "TOTAL",
                                    "1": "Nam",
                                    "2": "Nữ"
                                }
                            },
                            {
                                "qre_name": "@S5",
                                "qre_lbl": "HHI",
                                "cats": {
                                    "Recruit_S5_Thu_nhap_HGD.isin([1, 2, 3])": "Dưới 7.5M",
                                    "Recruit_S5_Thu_nhap_HGD == 4": "7.5M - dưới 13.5M",
                                    "Recruit_S5_Thu_nhap_HGD == 5": "13.5M - dưới 22.5M",
                                    "Recruit_S5_Thu_nhap_HGD == 6": "Trên 22.5M",
                                    "Recruit_S5_Thu_nhap_HGD.isin([7, 8])": "Refuse/DK/NA"
                                }
                            },
                            {
                                "qre_name": "$Qre_Name",
                                "qre_lbl": "MA Qre",
                                "cats": {
                                    "1": "Hảo hảo",
                                    "2": "3 Miền",
                                    "3": "Gấu Đỏ",
                                    "4": "Omachi",
                                    "5": "Kokomi 65",
                                    "6": "Cung Đình",
                                    "7": "3 Miền Gold",
                                    "8": "Khác"
                                }
                            }
                        ]
                    ]
                },

                "MAIN": {
                    "tbl_name": "MAIN",
                    "tbl_filter": "Ma_SP > 0",
                    "is_count": 0,
                    "is_pct_sign": 1,
                    "is_hide_oe_zero_cats": 1,
                    "sig_test_info": {
                        "sig_type": "",
                        "sig_cols": [],
                        "lst_sig_lvl": []
                    },
                    "lst_side_qres": [

                        {"qre_name": "Main_1_OL_Noi_chung", "qre_lbl": "{lbl} - Mã 473", "qre_filter": "Ma_SP == 1"},
                        {"qre_name": "Main_1_OL_Noi_chung_Group", "qre_lbl": "{lbl} - Mã 473", "qre_filter": "Ma_SP == 1"},
                        {"qre_name": "Main_1_OL_Noi_chung_Mean", "qre_lbl": "{lbl} - Mã 473", "qre_filter": "Ma_SP == 1"},

                        {"qre_name": "Main_1_OL_Noi_chung", "qre_lbl": "{lbl} - Mã 843", "qre_filter": "Ma_SP == 2"},
                        {"qre_name": "Main_1_OL_Noi_chung_Group", "qre_lbl": "{lbl} - Mã 843", "qre_filter": "Ma_SP == 2"},
                        {"qre_name": "Main_1_OL_Noi_chung_Mean", "qre_lbl": "{lbl} - Mã 843", "qre_filter": "Ma_SP == 2"},

                        {"qre_name": "Main_1_OL_Noi_chung", "qre_lbl": "{lbl} - Mã 691", "qre_filter": "Ma_SP == 3"},
                        {"qre_name": "Main_1_OL_Noi_chung_Group", "qre_lbl": "{lbl} - Mã 691", "qre_filter": "Ma_SP == 3"},
                        {"qre_name": "Main_1_OL_Noi_chung_Mean", "qre_lbl": "{lbl} - Mã 691", "qre_filter": "Ma_SP == 3"},

                        {"qre_name": "Main_1_OL_Noi_chung", "qre_lbl": "{lbl} - Mã 350", "qre_filter": "Ma_SP == 4"},
                        {"qre_name": "Main_1_OL_Noi_chung_Group", "qre_lbl": "{lbl} - Mã 350", "qre_filter": "Ma_SP == 4"},
                        {"qre_name": "Main_1_OL_Noi_chung_Mean", "qre_lbl": "{lbl} - Mã 350", "qre_filter": "Ma_SP == 4"},
                        {"qre_name": "Main_2_OL_Soi_mi", "qre_lbl": "{lbl} - Mã 350", "qre_filter": "Ma_SP == 4"},

                        {"qre_name": "Main_P100a_Thich_nhat_by_sp", "qre_lbl": "{lbl}", "qre_filter": "Ma_SP == 1"},
                        {"qre_name": "Main_P100b_Thich_nhi_by_sp", "qre_lbl": "{lbl}", "qre_filter": "Ma_SP == 1"}

                    ],
                    "lst_header_qres": [
                        [
                            {
                                "qre_name": "Recruit_S2_Gioi_tinh",
                                "qre_lbl": "Gender",
                                "cats": {
                                    "TOTAL": "TOTAL",
                                    "1": "Nam",
                                    "2": "Nữ"
                                }
                            }
                        ]
                    ]
                },

                "KPI": {
                    "tbl_name": "KPI",
                    "tbl_filter": "Ma_SP > 0",
                    "is_count": 0,
                    "is_pct_sign": 1,
                    "is_hide_oe_zero_cats": 1,
                    "sig_test_info": {
                        "sig_type": "rel",
                        "sig_cols": [],
                        "lst_sig_lvl": [90, 95]
                    },
                    "lst_side_qres": [

                        {"qre_name": "Main_1_OL_Noi_chung"},
                        {"qre_name": "Main_1_OL_Noi_chung_Group"},
                        {"qre_name": "Main_1_OL_Noi_chung_Mean"},

                        {"qre_name": "Main_P100a_Thich_nhat_YN"},
                        {"qre_name": "Main_P100b_Thich_nhi_YN"}
                    ],
                    "lst_header_qres": [
                        [
                            {
                                "qre_name": "Recruit_S2_Gioi_tinh",
                                "qre_lbl": "Gender",
                                "cats": {
                                    "TOTAL": "TOTAL",
                                    "1": "Nam",
                                    "2": "Nữ"
                                }
                            }
                        ],
                        [
                            {
                                "qre_name": "Ma_SP",
                                "qre_lbl": "Mã SP",
                                "cats": {
                                    "1": "473",
                                    "2": "843",
                                    "3": "691",
                                    "4": "350"
                                }
                            }
                        ]
                    ]
                }
            }
        }
    ]





