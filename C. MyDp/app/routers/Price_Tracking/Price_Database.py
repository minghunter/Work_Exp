from .Price_Processing import PriceProcess
from .Price_Models import *
from bson.objectid import ObjectId
from datetime import datetime
from dotenv import load_dotenv
import motor.motor_asyncio
import pandas as pd
import numpy as np
import traceback
import os
import io
import json



class PriceDb(PriceProcess):

    def __init__(self, logger):
        PriceProcess.__init__(self)

        load_dotenv()
        MONGO_DETAILS = os.environ.get("MONGO_DETAILS")
        PRICE_PRJ_CRE = os.environ.get("PRICE_PRJ_CRE")

        if not MONGO_DETAILS:
            with open("mongodb_cre.txt", 'r') as txt_mongodb_cre:
                MONGO_DETAILS = txt_mongodb_cre.readline()
                MONGO_DETAILS = MONGO_DETAILS[:-1]

                txt_mongodb_cre.readline()

                PRICE_PRJ_CRE = txt_mongodb_cre.readline()

        client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_DETAILS)
        price_client = motor.motor_asyncio.AsyncIOMotorClient(PRICE_PRJ_CRE)

        db_msn = client.msn
        self.collection_user = db_msn.get_collection('users')
        del client
        del db_msn

        db_anlz = price_client.price_tracking
        self.price_colls: dict = {
            'prj_info': db_anlz.get_collection('prj_info'),
            'sku_info': db_anlz.get_collection('sku_info'),
            'sku_data': db_anlz.get_collection('sku_data'),
        }


    async def get_prj_result(self, prj: dict = None, str_id: str = "", tab_name: str = 'INFO') -> dict:

        tab_name_upper = tab_name.upper()

        if prj is None:
            obj_id = ObjectId(str_id)
            prj = await self.price_colls.get('prj_info').find_one({'_id': obj_id}, {
                '_id': 1,
                'internal_id': 1,
                'name': 1,
                'categorical': 1,
                'type': 1,
                'status': 1,
                'owner': 1,
                'NumOfRes': 1,
                'current_week': 1
            })
        else:
            obj_id = ObjectId(prj['_id'])

        prj_result = {
            'id': str(prj['_id']),
            'internal_id': prj['internal_id'],
            'name': prj['name'],
            'categorical': prj['categorical'],
            'type': prj['type'],
            'status': prj['status'],
            'owner': prj['owner'],
            'NumOfRes': prj['NumOfRes'],
            'current_week': prj['current_week']
        }

        if tab_name_upper in ['INFO', 'SKU_INFO', 'SKU_DATA', 'PROCESSING']:
            pass

        # elif tab_name_upper in ['RAWDATA']:
        #     prj_doc = await self.anlz_colls.get('rawdata').find_one({'_id_ref': obj_id}, {'is_md': 1, 'date_upload': 1, 'data': 1})
        #
        #     prj_result.update({
        #         'is_md': None,
        #         'date_upload': None,
        #         'num_of_row': 0,
        #         'num_of_col': 0
        #     })
        #
        #     if prj_doc['data']:
        #         df_data = pd.DataFrame.from_dict(json.loads(prj_doc['data']))
        #         prj_result.update({
        #             'is_md': prj_doc['is_md'],
        #             'date_upload': prj_doc['date_upload'].strftime("%d/%m/%Y, %H:%M:%S"),
        #             'num_of_row': df_data.shape[0],
        #             'num_of_col': df_data.shape[1]
        #         })
        #
        # elif tab_name_upper in ['DOWNLOAD_RAWDATA']:
        #     prj_doc = await self.anlz_colls.get('rawdata').find_one({'_id_ref': obj_id}, {'data': 1, 'info': 1})
        #     prj_result.update({
        #         'data': pd.DataFrame.from_dict(json.loads(prj_doc['data'])),
        #         'info': pd.DataFrame.from_dict(json.loads(prj_doc['info'])),
        #     })
        #
        # elif tab_name_upper in ['PYSCRIPT_PRE_PROCESSING']:
        #     prj_doc = await self.anlz_colls.get('py_script').find_one({'_id_ref': obj_id}, {'txt_pre_processing_script': 1})
        #     prj_result['txt_pre_processing_script'] = prj_doc['txt_pre_processing_script']
        #
        # elif tab_name_upper in ['PYSCRIPT_DEFINE_STRUCTURE']:
        #     prj_doc = await self.anlz_colls.get('py_script').find_one({'_id_ref': obj_id}, {'txt_define_structure_script': 1})
        #     prj_result['txt_define_structure_script'] = prj_doc['txt_define_structure_script']
        #
        # elif tab_name_upper in ['PYSCRIPT_MAIN_PROCESSING']:
        #     prj_doc = await self.anlz_colls.get('py_script').find_one({'_id_ref': obj_id}, {'txt_main_processing_script': 1})
        #     prj_result['txt_main_processing_script'] = prj_doc['txt_main_processing_script']
        #
        # elif tab_name_upper in ['DTABLES']:
        #     prj_doc = await self.anlz_colls.get('dtables').find_one({'_id_ref': obj_id}, {'lst_dtables': 1})
        #     json_str_dtables = json.dumps(prj_doc['lst_dtables'], indent=4, ensure_ascii=False)
        #     prj_result['lst_dtables'] = json_str_dtables

        return prj_result


    @staticmethod
    def count_page(lst_prj, step) -> int:
        tuple_divMod = divmod(len(lst_prj), step)

        if tuple_divMod[1] != 0:
            page_count = tuple_divMod[0] + 1
        else:
            page_count = tuple_divMod[0]

        return page_count


    async def check_permission(self, _id, user_info, is_admin: bool = False) -> bool:
        prj_info_cur = await self.price_colls.get('prj_info').find_one({'_id': ObjectId(_id)}, {'_id': 1, 'owner': 1})

        if is_admin:
            if user_info.get('role') == 'admin':
                return True
        else:
            if user_info.get('role') == 'admin' or user_info.get('name') == prj_info_cur.get('owner'):
                return True

        return False


    async def retrieve_prjs(self, page: int) -> dict:
        try:
            self.logger.info('Retrieve all analyze projects')

            lst_prj = list()
            async for prj in self.price_colls.get('prj_info').find({}, {
                "_id": 1,
                "internal_id": 1,
                "name": 1,
                "categorical": 1,
                "type": 1,
                "status": 1,
                "owner": 1,
                "NumOfRes": 1,
                "create_date": 1,
                "current_week": 1
            }).sort('create_date', -1):
                lst_prj.append(await self.get_prj_result(prj=prj))

            step = 5
            page_count = self.count_page(lst_prj, step)
            lst_prj = lst_prj[((page - 1) * step):(page * step)]

            return {
                'isSuccess': True,
                'strErr': None,
                'htmlErr': None,
                'lst_prj': lst_prj,
                'page_count': page_count
            }

        except Exception:
            self.logger.error(f'Retrieve all analyze projects:\n {traceback.format_exc()}')
            return {
                'isSuccess': False,
                'strErr': traceback.format_exc(),
                'htmlErr': None,
                'lst_prj': None,
                'page_count': None
            }


    async def retrieve_prj_by_id(self, _id: str, tab_name: str) -> dict:
        try:
            self.logger.info(f'Retrieve price tracking project ID {_id}')

            prj_result = await self.get_prj_result(str_id=_id, tab_name=tab_name)

            lst_users = list()
            if tab_name.upper() == 'INFO':
                async for user in self.collection_user.find({}, {"name": 1}):
                    lst_users.append(user.get('name'))

            return {
                'isSuccess': True,
                'strErr': None,
                'htmlErr': None,
                'lst_users': lst_users,
                'prj': prj_result
            }

        except Exception:
            self.logger.error(f'Retrieve price tracking project ID {_id}:\n {traceback.format_exc()}')
            return {
                'isSuccess': False,
                'strErr': traceback.format_exc(),
                'htmlErr': None,
                'prj': None
            }


    async def update_prj_info(self, _id, prj_info: PricePrjInfo, user_info: dict) -> dict:

        try:
            self.logger.info(f'Update analyze project ID {_id} info')

            is_has_permission = await self.check_permission(_id=_id, user_info=user_info, is_admin=True)

            if not is_has_permission:
                return {
                    'isSuccess': False,
                    'strErr': f"You don't have permission to update price tracking project ID {_id}",
                    'htmlErr': None,
                }

            prj_updated = await self.price_colls.get('prj_info').update_one({'_id': ObjectId(_id)}, {'$set': prj_info.dict()})

            if prj_updated:
                return {
                    'isSuccess': True,
                    'strErr': None,
                    'htmlErr': None,
                }

            return {
                'isSuccess': False,
                'strErr': f'Update price tracking project ID {_id} info fail',
                'htmlErr': None,
            }

        except Exception:
            self.logger.error(f'Update price tracking project ID {_id} info:\n {traceback.format_exc()}')
            return {
                'isSuccess': False,
                'strErr': traceback.format_exc(),
                'htmlErr': None,
            }


    async def upload_prj_sku_info(self, _id, user_info: dict, file_sku_info: PriceUploadSkuInfo) -> dict:
        try:
            self.logger.info(f'Upload SKU info to price tracking project ID {_id}')

            is_has_permission = await self.check_permission(_id=_id, user_info=user_info)

            if not is_has_permission:
                return {
                    'isSuccess': False,
                    'strErr': f"You don't have permission to upload SKU info to price tracking project ID {_id}",
                    'htmlErr': None,
                }

            xlsx = io.BytesIO(file_sku_info.file_sku_info.file.read())
            df_sku_info = pd.read_excel(xlsx)

            prj_updated = await self.price_colls.get('sku_info').update_one(
                {'_id_ref': ObjectId(_id)},
                {
                    '$set': {
                        'sku_info': df_sku_info.to_json()
                    }
                }
            )

            if not prj_updated:
                return {
                    'isSuccess': False,
                    'strErr': f'Upload SKU info to price tracking project ID {_id} fail',
                    'htmlErr': None,
                }

            prj_updated = await self.price_colls.get('prj_info').update_one(
                {'_id': ObjectId(_id)},
                {
                    '$set': {
                        'NumOfRes': int(df_sku_info.shape[0])
                    }
                }
            )

            if not prj_updated:
                return {
                    'isSuccess': False,
                    'strErr': f'Update NumOfRes to price tracking project ID {_id} fail',
                    'htmlErr': None,
                }

            return {
                'isSuccess': True,
                'strErr': None,
                'htmlErr': None,
            }

        except Exception:
            self.logger.error(f'Upload SKU info to price tracking project ID {_id} fail:\n {traceback.format_exc()}')
            return {
                'isSuccess': False,
                'strErr': traceback.format_exc(),
                'htmlErr': None,
            }


    async def get_sku_info(self, _id, user_info: dict, is_export_df: bool, is_check_permission: bool = True) -> str:
        self.logger.info(f'Get SKU info from price tracking project ID {_id}')

        if is_check_permission:
            is_has_permission = await self.check_permission(_id=_id, user_info=user_info)
            if not is_has_permission:
                return f"You don't have permission to get SKU info from price tracking project ID {_id}"

        prj_sku_info = await self.price_colls.get('sku_info').find_one({'_id_ref': ObjectId(_id)}, {'sku_info': 1})

        if is_export_df:
            cols = ['STT', 'Product Description', 'Sheet', 'Category', 'Brand']
        else:
            cols = ['STT', 'Aricle Number (banner)', 'BARCODE', 'Product Description', 'Sheet', 'Category', 'Brand']

        df_sku_data = pd.DataFrame.from_dict(json.loads(prj_sku_info['sku_info'])).loc[:, cols]

        if is_export_df:
            return df_sku_data

        return df_sku_data.to_html(index=False)



    async def get_sku_info_to_dict(self, _id, user_info: dict, is_export_df: bool, is_check_permission: bool) -> dict:
        self.logger.info(f'Get SKU info from price tracking project ID {_id} to dict')

        df_sku_data = await self.get_sku_info(_id, user_info, is_export_df, is_check_permission)
        df_sku_data = pd.DataFrame(df_sku_data)

        dict_sku_info = dict()

        for idx in df_sku_data.index:
            store = df_sku_data.at[idx, 'Sheet']
            cate = df_sku_data.at[idx, 'Category']
            brand = df_sku_data.at[idx, 'Brand']
            stt = df_sku_data.at[idx, 'STT']
            prod_desc = df_sku_data.at[idx, 'Product Description']

            sku_key = f"{store}_{stt}"
            sku_lbl = f"{stt}. {store} - {prod_desc}"



            if store not in dict_sku_info.keys():
                dict_sku_info.update({store: {}})

            if cate not in dict_sku_info[store].keys():
                dict_sku_info[store].update({cate: {}})

            if brand not in dict_sku_info[store][cate].keys():
                dict_sku_info[store][cate].update({brand: {sku_key: sku_lbl}})
            else:
                dict_sku_info[store][cate][brand].update({sku_key: sku_lbl})


        return dict_sku_info



    async def get_week(self, _id, user_info: dict, is_check_permission: bool = True) -> dict:
        self.logger.info(f'Get SKU info from price tracking project ID {_id}')

        if is_check_permission:
            is_has_permission = await self.check_permission(_id=_id, user_info=user_info)
            if not is_has_permission:
                return {'error': f"You don't have permission to get Week from price tracking project ID {_id}"}

        dict_week = dict()
        async for prj in self.price_colls.get('sku_data').find(
                {
                    '_id_ref': ObjectId(_id),
                }, {'week': 1}).sort('week', -1):

            dict_week.update({int(prj['week']): f"W{prj['week']}"})

        return dict_week



    async def get_sku_data_dashboard(self, _id, user_info: dict, price_dashboard_input: PriceDashboardInput) -> dict:
        self.logger.info(f'Get SKU Data from price tracking project ID {_id} for dashboard')

        # is_has_permission = await self.check_permission(_id=_id, user_info=user_info)
        # if not is_has_permission:
        #     return {'error': f"You don't have permission to get Week from price tracking project ID {_id}"}


        df_merge = pd.DataFrame()
        dict_replace = dict()

        async for prj in self.price_colls.get('sku_data').find(
                {
                    '_id_ref': ObjectId(_id),
                    'week': {'$in': price_dashboard_input.lst_week}
                }, {}).sort('week', 1):

            if prj.get('sku_data'):
                df_temp = pd.DataFrame.from_dict(json.loads(prj.get('sku_data')))
            else:
                df_temp = pd.DataFrame.from_dict(json.loads(prj.get('sku_data_watson')))

                if prj.get('sku_data_bhx'):
                    df_temp = pd.concat([df_temp, pd.DataFrame.from_dict(json.loads(prj.get('sku_data_bhx')))], axis=0, ignore_index=True)

            df_temp['ID'] = df_temp['Competitor'] + '_' + df_temp['STT'].astype(str)
            df_temp.set_index('ID', inplace=True)

            if 'STT' not in df_merge.columns:
                df_temp = df_temp.loc[:, ['STT', 'Competitor', 'Competitor Price']]

            else:
                df_temp = df_temp.loc[:, ['Competitor Price']]

            df_temp.rename(columns={'Competitor Price': f"W{prj.get('week')}"}, inplace=True)
            dict_replace.update({f"W{prj.get('week')}": {np.nan: 0, None: 0, 99: 0, 'OOS': 0, 'NA': 0}})

            df_merge = pd.concat([df_merge, df_temp], axis=1)

        df_merge = df_merge.loc[price_dashboard_input.lst_sku, :].copy()

        df_merge.replace(dict_replace, inplace=True)

        prj_sku_info = await self.price_colls.get('sku_info').find_one({'_id_ref': ObjectId(_id)}, {'sku_info': 1})
        df_sku_info = pd.DataFrame.from_dict(json.loads(prj_sku_info.get('sku_info')))
        df_sku_info = df_sku_info.loc[:, ['STT', 'Sheet', 'Product Description']]
        df_sku_info.rename(columns={'Sheet': 'Competitor'}, inplace=True)

        df_merge = df_sku_info.merge(df_merge, how='inner', on=['STT', 'Competitor'])

        lst_data_week = df_merge.loc[:, dict_replace.keys()].to_dict(orient="records")
        df_merge = pd.concat([df_merge, pd.DataFrame(columns=['Week_data'], data=[])])
        for i, v in enumerate(lst_data_week):
            df_merge.loc[i, ['Week_data']] = [v]

        df_merge['ID'] = df_merge['Competitor'] + '_' + df_merge['STT'].astype(str)
        df_merge.set_index('ID', inplace=True)

        dict_data_merge = df_merge.loc[:, ['STT', 'Competitor', 'Product Description', 'Week_data']].to_dict(orient="index")



        return dict_data_merge



    async def upload_prj_sku_data(self, _id, user_info: dict, sku_data_upload: PriceUploadSkuDataExt) -> dict:
        try:
            self.logger.info(f'Upload SKU data to price tracking project ID {_id}')

            is_has_permission = await self.check_permission(_id=_id, user_info=user_info)

            if not is_has_permission:
                return {
                    'isSuccess': False,
                    'strErr': f"You don't have permission to upload SKU data to price tracking project ID {_id}",
                    'htmlErr': None,
                }

            is_qme = sku_data_upload.is_qme
            is_bhx = sku_data_upload.is_bhx
            int_week = sku_data_upload.week

            if int_week < 1:
                return {
                    'isSuccess': False,
                    'strErr': f'Insert SKU data to price tracking project ID {_id} fail: week = {int_week}',
                    'htmlErr': None,
                }

            xlsx = io.BytesIO(sku_data_upload.upload_file.file.read())

            if is_qme:
                df_sku_data = self.convert_qme_file_to_df(qme_xlsx=xlsx, int_week=int_week, is_bhx=is_bhx)

            else:
                df_sku_data = pd.read_excel(xlsx)

            prj_updated = await self.price_colls.get('sku_data').find_one({'_id_ref': ObjectId(_id), 'week': int_week}, {})

            doc_name = 'sku_data_bhx' if is_bhx else 'sku_data_watson'
            doc_name_remaining = 'sku_data_watson' if is_bhx else 'sku_data_bhx'

            if prj_updated:

                prj_updated = await self.price_colls.get('sku_data').update_one(
                    {'_id_ref': ObjectId(_id), 'week': int_week},
                    {
                        '$set': {
                            doc_name: df_sku_data.to_json(date_format='iso')
                        }
                    }
                )

                if not prj_updated:
                    return {
                        'isSuccess': False,
                        'strErr': f'Upload SKU data to price tracking project ID {_id} fail',
                        'htmlErr': None,
                    }

            else:
                prj_updated = await self.price_colls.get('sku_data').insert_one({
                    '_id': ObjectId(),
                    '_id_ref': ObjectId(_id),
                    'week': int_week,
                    doc_name: df_sku_data.to_json(date_format='iso'),
                    doc_name_remaining: '',
                })

                if not prj_updated:
                    return {
                        'isSuccess': False,
                        'strErr': f'Insert SKU data to price tracking project ID {_id} fail',
                        'htmlErr': None,
                    }

            # VALIDATION AFTER UPDATE-----------------------------------------------------------------------------------
            if is_qme:
                prj_sku_info = await self.price_colls.get('sku_info').find_one({'_id_ref': ObjectId(_id)}, {'sku_info': 1})

                prj_sku_data = await self.price_colls.get('sku_data').find_one(
                    {'_id_ref': ObjectId(_id), 'week': int_week},
                    {'sku_data_bhx': 1, 'sku_data_watson': 1}
                )

                df_sku_data_watson, df_sku_data_bhx = pd.DataFrame(), pd.DataFrame()

                if prj_sku_data.get('sku_data_watson'):
                    df_sku_data_watson = pd.DataFrame.from_dict(json.loads(prj_sku_data.get('sku_data_watson')))

                if prj_sku_data.get('sku_data_bhx'):
                    df_sku_data_bhx = pd.DataFrame.from_dict(json.loads(prj_sku_data.get('sku_data_bhx')))

                df_sku_data = pd.concat([df_sku_data_watson, df_sku_data_bhx], axis=0)
                df_sku_data.reset_index(drop=True, inplace=True)

                lst_sku_data_err = self.validate_df_sku_data(prj_sku_info=prj_sku_info, df_sku_data=df_sku_data)


                del prj_sku_info

                await self.price_colls.get('sku_data').update_one(
                    {'_id_ref': ObjectId(_id), 'week': int_week},
                    {'$set': {'lst_err': lst_sku_data_err}}
                )
            # END VALIDATION AFTER UPDATE-------------------------------------------------------------------------------

            return {
                'isSuccess': True,
                'strErr': None,
                'htmlErr': None,
            }

        except Exception:
            self.logger.error(f'Upload SKU data to price tracking project ID {_id} fail:\n {traceback.format_exc()}')
            return {
                'isSuccess': False,
                'strErr': traceback.format_exc(),
                'htmlErr': None,
            }


    async def get_sku_data_errors(self, _id, user_info: dict, week: int) -> str:

        html_danger = """
        <div class="alert alert-danger alert-dismissible fade show" role="alert">
            <i class="fa fa-exclamation-circle me-2"></i>Errors detected
            <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
        </div>"""

        html_success = """
        <div class="alert alert-success alert-dismissible fade show" role="alert">
            <i class="fa fa-exclamation-circle me-2"></i>No error detected
            <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
        </div>
        """

        self.logger.info(f'Get SKU data errors from price tracking project ID {_id}')

        is_has_permission = await self.check_permission(_id=_id, user_info=user_info)
        if not is_has_permission:
            return f"You don't have permission to get SKU data errors from price tracking project ID {_id}"

        prj_sku_data = await self.price_colls.get('sku_data').find_one({'_id_ref': ObjectId(_id), 'week': week}, {
            'sku_data_bhx': 1,
            'sku_data_watson': 1,
            'lst_err': 1
        })

        if not prj_sku_data:
            return '<h5 class="text-warning text-center">No data detected</h5>'

        df_sku_data_watson, df_sku_data_bhx = pd.DataFrame(), pd.DataFrame()

        if prj_sku_data.get('sku_data_watson'):
            df_sku_data_watson = pd.DataFrame.from_dict(json.loads(prj_sku_data.get('sku_data_watson')))

        if prj_sku_data.get('sku_data_bhx'):
            df_sku_data_bhx = pd.DataFrame.from_dict(json.loads(prj_sku_data.get('sku_data_bhx')))

        df_sku_data = pd.concat([df_sku_data_watson, df_sku_data_bhx], axis=0)
        df_sku_data.reset_index(drop=True, inplace=True)

        if df_sku_data.empty:
            return '<h5 class="text-warning text-center">No data detected</h5>'

        df_sku_data = df_sku_data.loc[df_sku_data['ERRORS_NOTE'] != '', ['STT', 'Week Number', 'Competitor', 'ERRORS_NOTE']]

        html_response = str()
        if not df_sku_data.empty:
            html_response += f"""
            <h5 class="text-danger text-center">UPLOADED DATA ERRORS</h5>
            {df_sku_data.to_html(index=False)}
            """

        for err in prj_sku_data['lst_err']:
            html_response += f"""
            <h5 class="text-danger text-center">{err[0].replace("|", "<br/>")}</h5>
            {err[1]}
            """

        return html_danger + html_response if html_response else html_success


    async def export_prj_data(self, _id, user_info: dict, price_export_data: PriceExportData) -> dict:
        try:
            self.logger.info(f'Export data from price tracking project ID {_id}')

            is_has_permission = await self.check_permission(_id=_id, user_info=user_info)

            if not is_has_permission:
                return {
                    'isSuccess': False,
                    'strErr': f"You don't have permission to export data from price tracking project ID {_id}",
                    'htmlErr': None,
                }

            is_to_client = price_export_data.is_to_client
            is_to_ggdrive = price_export_data.is_to_ggdrive
            int_week = price_export_data.week

            prj_info = await self.price_colls.get('prj_info').find_one({'_id': ObjectId(_id)}, {'categorical': 1})

            if is_to_client:
                if len(str(int_week)) <= 2:
                    deliver_date = datetime.fromisocalendar(datetime.now().year, int_week, 2)
                    int_week_temp = int_week
                else:
                    int_week_temp = int(str(int_week)[-2:])
                    int_year_temp = int(str(int_week)[:-2])
                    deliver_date = datetime.fromisocalendar(int_year_temp, int_week_temp, 2)

            else:
                if len(str(int_week)) == 2:
                    int_week_temp = int_week
                else:
                    int_week_temp = int(str(int_week)[-2:])

                deliver_date = datetime.now()


            file_name = f"{prj_info['categorical']}_Week{int_week_temp}{'' if is_to_client else '_Internal Checking'}_{deliver_date.strftime('%y%d%m')}"
            

            prj_sku_info = await self.price_colls.get('sku_info').find_one({'_id_ref': ObjectId(_id)}, {'sku_info': 1})
            df_sku_info = pd.DataFrame.from_dict(json.loads(prj_sku_info['sku_info']))

            df_sku_info['sheet_stt'] = df_sku_info['Sheet'] + '_' + df_sku_info['STT'].astype(str)
            df_sku_info.set_index('sheet_stt', inplace=True)

            dict_dfs_prj_sku_data = dict()
            async for prj in self.price_colls.get('sku_data').find(
                    {
                        '_id_ref': ObjectId(_id),
                        # 'week': {'$lte': int_week, '$gte': 23}  # for 2023
                        'week': {'$lte': int_week, '$gte': 4}  # for 2024
                    }, {'week': 1, 'sku_data': 1, 'sku_data_bhx': 1, 'sku_data_watson': 1}).sort('week', -1):

                # Update 22/09/2023-------------------------------------------------------------------------------------
                if prj.get('sku_data'):
                    df_sku_data = pd.DataFrame.from_dict(json.loads(prj['sku_data']))
                else:

                    df_sku_data_watson, df_sku_data_bhx = pd.DataFrame(), pd.DataFrame()

                    if prj.get('sku_data_watson'):
                        df_sku_data_watson = pd.DataFrame.from_dict(json.loads(prj.get('sku_data_watson')))

                    if prj.get('sku_data_bhx'):
                        df_sku_data_bhx = pd.DataFrame.from_dict(json.loads(prj.get('sku_data_bhx')))

                    df_sku_data = pd.concat([df_sku_data_watson, df_sku_data_bhx], axis=0)
                    df_sku_data.reset_index(drop=True, inplace=True)
                # Update 22/09/2023-------------------------------------------------------------------------------------

                dict_dfs_prj_sku_data.update({prj['week']: df_sku_data})

            if len(list(dict_dfs_prj_sku_data.keys())) > 1:
                int_pre_week = list(dict_dfs_prj_sku_data.keys())[1]
            else:
                int_pre_week = -1

            if not is_to_client:
                df_sku_info['Product Description'] = df_sku_info['Product Label'].values
                df_sku_info.drop(columns=['Product Label'], inplace=True)

            # Export & format *.xlsx file
            file_name = self.export_xlsx_data(df_data=df_sku_info.copy(), dict_dfs_prj_sku_data=dict_dfs_prj_sku_data,
                                              int_week=int_week, int_pre_week=int_pre_week, is_to_client=is_to_client,
                                              file_name=file_name, is_to_ggdrive=is_to_ggdrive)

            del prj_info
            del df_sku_info
            del prj_sku_info

            return {
                'isSuccess': True,
                'strErr': None,
                'htmlErr': None,
                'file_name': file_name,
            }

        except Exception:
            self.logger.error(f'Export data from price tracking project ID {_id} fail:\n {traceback.format_exc()}')
            return {
                'isSuccess': False,
                'strErr': traceback.format_exc(),
                'htmlErr': None,
            }




