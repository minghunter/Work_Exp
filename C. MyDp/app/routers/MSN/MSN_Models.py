from fastapi import Form, UploadFile
from pydantic import BaseModel, Field
from typing import Annotated, Any
from datetime import datetime


class ModelMsnPrjInfo(BaseModel):
    internal_id: Annotated[str, Form()] = None
    name: Annotated[str, Form()] = None
    categorical: Annotated[str, Form()] = None
    type: Annotated[str, Form()] = None
    status: Annotated[str, Form()] = None
    create_date: datetime = None
    detail_prj_info: Annotated[dict, Form()] = None


    def set_prj_info(self, msn_prj_info: dict):
        self.internal_id = msn_prj_info['internal_id']
        self.name = msn_prj_info['name']
        self.categorical = msn_prj_info['categorical']
        self.type = msn_prj_info['type']
        self.status = msn_prj_info['status']
        self.create_date = msn_prj_info['create_date']
        self.detail_prj_info = msn_prj_info['detail']['prj_info']


class ModelMsnPrjSections(BaseModel):
    join_col: Annotated[str, Form()] = None
    order_col: Annotated[str, Form()] = None
    sections: Annotated[dict, Form()] = None


    def set_prj_sections(self, msn_prj_sections: dict):
        self.join_col = msn_prj_sections['detail']['join_col']
        self.order_col = msn_prj_sections['detail']['order_col']
        self.sections = msn_prj_sections['detail']['sections']





new_user_template = {
    "_id": {},
    "email": "",
    "password": "",
    "name": "",
    "role": "user",
    "create_at": "",
    "login_at": "",
    "legal": False
}

new_prj_template = {
    "_id": {},
    "internal_id": "",
    "name": "",
    "categorical": "",
    "type": "",
    "status": "",
    "create_date": "",
    "detail": {
        "prj_info": {
            "1": {"name": "1. Mục tiêu nghiên cứu", "val": ""},
            "2_1": {"name": "2.1. Đối tượng nghiên cứu", "val": "Đối tượng"},
            "2_2": {"name": "2.2. Khu vực nghiên cứu", "val": "Khu vực"},
            "2_3": {"name": "2.3. Phương pháp nghiên cứu", "val": "Phương pháp"},
            "2_4": {"name": "2.4. Số mẫu nghiên cứu", "val": "Số mẫu"},
            "3": {"name": "3. Thông tin nghiên cứu (OL, JAR, Like/Dislikes, v.v…)", "val": ""},
            "4": {"name": "4. Action Standard", "val": ""},
            "5": {"name": "5. Thời gian thực hiện", "val": ""},
            "6_1": {"name": "6.1. Chú thích mã sản phẩm", "val": ""},
            "6_2": {"name": "6.2. Chú thích mã sản phẩm", "val": ""},
            "6_3": {"name": "6.3. Chú thích mã sản phẩm", "val": ""},
            "6_4": {"name": "6.4. Chú thích mã sản phẩm", "val": ""}
        },
        "join_col": "Q0a_RespondentID",
        "order_col": "",
        "sections": {
            f"{i}": {
                "name": "",
                "filter": "",

                "rotation": {
                    "name": "ROTATION",
                    "lbl": "Xoay vòng",
                    "cats": {
                        "1": ["Thử [ma sp1] trước", "1|3"],
                        "2": ["Thử [ma sp2] trước", "2|4"]
                    },
                    "qres": ["Rotation_Question_In_Rawdata"]
                },
                "product": {
                    "name": "Ma_SP",
                    "lbl": "Mã sản phẩm",
                    "cats": {
                        "1": ["[ma sp1]"],
                        "2": ["[ma sp2]"]
                    },
                    "qres": ["1st_Product_Questions_In_Rawdata", "2nd_Product_Questions_In_Rawdata"]
                },
                "force_choice": {
                    "qres": ["Force_Choice_Questions_In_Rawdata"]
                },

            } for i in range(1, 4)},
        "oe_combine_cols": {},
        "scr_cols": {},
        "plm_to_scr_cols": {},
        "plm_to_prod_cols": {},
        "product_cols": {},
        "fc_cols": {},
        "addin_vars": {
            "1": {
                "name": "FC",
                "lbl": "FC",
                "cats": {
                    "1": {
                        "val": "1",
                        "lbl": "Thích XXX",
                        "condition": "P100 = 1"
                    },
                    "2": {
                        "val": "2",
                        "lbl": "Thích YYY",
                        "condition": "P100 = 2"
                    }
                }
            },
            "2": {
                "name": "Rotation",
                "lbl": "Rotation",
                "cats": {
                    "1": {
                        "val": "1",
                        "lbl": "Thử XXX trước",
                        "condition": "ROTATION = 1 OR ROTATION = 3"
                    },
                    "2": {
                        "val": "2",
                        "lbl": "Thử YYY trước",
                        "condition": "ROTATION = 2 OR ROTATION = 4"
                    }
                }
            }
        },
        "topline_design": {
            "is_display_pct_sign": False,
            "is_jar_scale_3": True,
            "header": {
                "1": {
                    "name": "Total",
                    "lbl": "Total",
                    "hidden_cats": "",
                    "filter": "",
                    "run_secs": ""
                }
            },
            "side": {
                "1": {
                  "group_lbl": "I. ĐÁNH GIÁ SẢN PHẨM TRƯỚC KHI ĂN",
                  "name": "Qre_XXX",
                  "lbl": "PXXX. YYY",
                  "type": "OL",
                  "t2b": True,
                  "b2b": True,
                  "mean": True,
                  "ma_cats": "",
                  "hidden_cats": "",
                  "is_count": False,
                  "is_corr": True,
                  "is_ua": False
                },
                "2": {
                  "group_lbl": "I. ĐÁNH GIÁ SẢN PHẨM TRƯỚC KHI ĂN",
                  "name": "Qre_XXX",
                  "lbl": "PXXX. YYY",
                  "type": "OL",
                  "t2b": True,
                  "b2b": True,
                  "mean": True,
                  "ma_cats": "",
                  "hidden_cats": "",
                  "is_count": False,
                  "is_corr": True,
                  "is_ua": False
                },
            }
        },
        "split_callback": {
          "is_split_callback": False,
          "split_with_cols": ""
        }
    },
    "screener": 0,
    "placement": 0,
    "main": 0,
    "has_oe": 0,
    "topline_exporter": {},
    "rawdata_exporter": {}
}

new_rawdata_template = {
    "_id": {},
    "_ref_id": {},
    "screener": {},
    "placement": {},
    "main": {},
    "topline_structure": {},
    "unstack_data": {},
}

new_openend_template = {
    "_id": {},
    "_ref_id": {},
    # "codeframes": {
    #     "1": {
    #         "name": "",
    #         "qres": "",
    #     },
    #     "2": {
    #         "name": "",
    #         "qres": "",
    #     }
    # },
}