from app.classes.AP_DataConverter import APDataConverter
from .ANLZ_Models import *
from .ANLZ_Processing import AnlzProcessing
from bson.objectid import ObjectId
from datetime import datetime
from dotenv import load_dotenv
import motor.motor_asyncio
import pandas as pd
import numpy as np
import traceback
import os
import json
import time


class AnlzDb:

    def __init__(self, logger):

        self.logger = logger
        self.lst_logging = list()

        # DataExporter.__init__(self)

        # --------------------------------------------------------------------------------------------------------------

        load_dotenv()
        MONGO_DETAILS = os.environ.get("MONGO_DETAILS")
        ANLZ_PRJ_CRE = os.environ.get("ANLZ_PRJ_CRE")

        if not MONGO_DETAILS:
            with open("mongodb_cre.txt", 'r') as txt_mongodb_cre:
                MONGO_DETAILS = txt_mongodb_cre.readline()
                MONGO_DETAILS = MONGO_DETAILS[:-1]

                ANLZ_PRJ_CRE = txt_mongodb_cre.readline()
                ANLZ_PRJ_CRE = ANLZ_PRJ_CRE[:-1]

        client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_DETAILS)
        anlz_client = motor.motor_asyncio.AsyncIOMotorClient(ANLZ_PRJ_CRE)

        db_msn = client.msn
        self.collection_user = db_msn.get_collection('users')
        del client
        del db_msn

        db_anlz = anlz_client.anlz_projects
        self.anlz_colls: dict = {
            'prj_info': db_anlz.get_collection('prj_info'),
            'py_script': db_anlz.get_collection('py_script'),
            'dtables': db_anlz.get_collection('dtables'),
            'rawdata': db_anlz.get_collection('rawdata'),
            'logging': db_anlz.get_collection('logging'),
        }



    async def get_prj_result(self, prj: dict = None, str_id: str = "", tab_name: str = 'INFO') -> dict:

        tab_name_upper = tab_name.upper()

        if prj is None:
            obj_id = ObjectId(str_id)
            prj = await self.anlz_colls.get('prj_info').find_one({'_id': obj_id}, {
                '_id': 1,
                'internal_id': 1,
                'name': 1,
                'categorical': 1,
                'type': 1,
                'status': 1,
                'owner': 1,
                'NumOfRes': 1
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
            'NumOfRes': prj['NumOfRes']
        }

        if tab_name_upper in ['INFO', 'LOGGING']:
            pass

        elif tab_name_upper in ['RAWDATA']:
            prj_doc = await self.anlz_colls.get('rawdata').find_one({'_id_ref': obj_id}, {'is_md': 1, 'date_upload': 1, 'data': 1})

            prj_result.update({
                'is_md': None,
                'date_upload': None,
                'num_of_row': 0,
                'num_of_col': 0
            })

            if prj_doc['data']:
                df_data = pd.DataFrame.from_dict(json.loads(prj_doc['data']))
                prj_result.update({
                    'is_md': prj_doc['is_md'],
                    'date_upload': prj_doc['date_upload'].strftime("%d/%m/%Y, %H:%M:%S"),
                    'num_of_row': df_data.shape[0],
                    'num_of_col': df_data.shape[1]
                })

        elif tab_name_upper in ['DOWNLOAD_RAWDATA']:
            prj_doc = await self.anlz_colls.get('rawdata').find_one({'_id_ref': obj_id}, {'data': 1, 'info': 1})
            prj_result.update({
                'data': pd.DataFrame.from_dict(json.loads(prj_doc['data'])),
                'info': pd.DataFrame.from_dict(json.loads(prj_doc['info'])),
            })

        elif tab_name_upper in ['PYSCRIPT_PRE_PROCESSING']:
            prj_doc = await self.anlz_colls.get('py_script').find_one({'_id_ref': obj_id}, {'txt_pre_processing_script': 1})
            prj_result['txt_pre_processing_script'] = prj_doc['txt_pre_processing_script']

        elif tab_name_upper in ['PYSCRIPT_DEFINE_STRUCTURE']:
            prj_doc = await self.anlz_colls.get('py_script').find_one({'_id_ref': obj_id}, {'txt_define_structure_script': 1})
            prj_result['txt_define_structure_script'] = prj_doc['txt_define_structure_script']

        elif tab_name_upper in ['PYSCRIPT_MAIN_PROCESSING']:
            prj_doc = await self.anlz_colls.get('py_script').find_one({'_id_ref': obj_id}, {'txt_main_processing_script': 1})
            prj_result['txt_main_processing_script'] = prj_doc['txt_main_processing_script']

        elif tab_name_upper in ['DTABLES']:
            prj_doc = await self.anlz_colls.get('dtables').find_one({'_id_ref': obj_id}, {'lst_dtables': 1})
            json_str_dtables = json.dumps(prj_doc['lst_dtables'], indent=4, ensure_ascii=False)
            prj_result['lst_dtables'] = json_str_dtables

        return prj_result


    @staticmethod
    def count_page(lst_prj, step) -> int:
        tuple_divMod = divmod(len(lst_prj), step)

        if tuple_divMod[1] != 0:
            page_count = tuple_divMod[0] + 1
        else:
            page_count = tuple_divMod[0]

        return page_count


    async def check_permission(self, _id, user_info) -> bool:
        prj_info_cur = await self.anlz_colls.get('prj_info').find_one({'_id': ObjectId(_id)}, {'_id': 1, 'owner': 1})
        if user_info.get('role') == 'admin' or user_info.get('name') == prj_info_cur.get('owner'):
            return True

        return False


    async def retrieve_prjs(self, page: int) -> dict:
        try:
            self.logger.info('Retrieve all analyze projects')

            lst_prj = list()
            async for prj in self.anlz_colls.get('prj_info').find({}, {
                "_id": 1,
                "internal_id": 1,
                "name": 1,
                "categorical": 1,
                "type": 1,
                "status": 1,
                "owner": 1,
                "NumOfRes": 1,
                "create_date": 1,
            }).sort('create_date', -1):
                lst_prj.append(await self.get_prj_result(prj=prj))

            step = 5
            page_count = self.count_page(lst_prj, step)
            lst_prj = lst_prj[((page - 1) * step):(page * step)]

            return {
                'isSuccess': True,
                'strErr': None,
                'lst_prj': lst_prj,
                'page_count': page_count
            }

        except Exception:
            self.logger.error(f'Retrieve all analyze projects:\n {traceback.format_exc()}')
            return {
                'isSuccess': False,
                'strErr': traceback.format_exc(),
                'lst_prj': None,
                'page_count': None
            }



    async def search_prjs(self, str_key_search: str = '') -> dict:
        try:
            self.logger.info(f'Search analyze projects - search_key = {str_key_search}')

            if str_key_search == 'has data':
                lst_key_search = [
                    {'NumOfRes': {'$gt': 0}},
                ]
            elif 'type:' in str_key_search:
                lst_key_search = [
                    {'type': {'$regex': f'.*{str_key_search.split(":")[1]}.*', '$options': 'i'}},
                ]
            else:
                lst_key_search = [
                    {'name': {'$regex': f'.*{str_key_search}.*', '$options': 'i'}},
                    {'internal_id': {'$regex': f'.*{str_key_search}.*', '$options': 'i'}},
                    {'status': {'$regex': f'.*{str_key_search}.*', '$options': 'i'}},
                ]

            lst_prj = list()
            async for prj in self.anlz_colls.get('prj_info').find(
                    {
                        '$or': lst_key_search
                    },
                    {
                        "_id": 1,
                        "internal_id": 1,
                        "name": 1,
                        "categorical": 1,
                        "type": 1,
                        "status": 1,
                        "owner": 1,
                        "NumOfRes": 1,
                        "create_date": 1,
                    }
            ).sort('create_date', -1):
                lst_prj.append(await self.get_prj_result(prj=prj))

            step = 10
            page_count = self.count_page(lst_prj, step)

            return {
                'isSuccess': True,
                'strErr': None,
                'lst_prj': lst_prj,
                'page_count': page_count
            }

        except Exception:
            self.logger.error(f'Search analyze projects - search_key = {str_key_search}:\n {traceback.format_exc()}')
            return {
                'isSuccess': False,
                'strErr': traceback.format_exc(),
                'lst_prj': None,
                'page_count': None
            }


    async def add_prj(self, prj_info: AnlzPrjInfo, user_info: dict) -> dict:
        try:
            self.logger.info(f'Add analyze project - {prj_info}')

            if prj_info.owner != user_info.get('name') or user_info.get('role') not in ['admin', 'dp', 'cs']:
                return {
                    'isSuccess': False,
                    'strErr': f"You don't have permission to add project"
                }

            new_prj_id = ObjectId()

            dict_add_new_prj: dict = {
                'prj_info': {
                    '_id': new_prj_id,
                    'internal_id': prj_info.internal_id,
                    'name': prj_info.name,
                    'categorical': prj_info.categorical,
                    'type': prj_info.type,
                    'status': prj_info.status,
                    'owner': prj_info.owner,
                    'NumOfRes': 0,
                    'create_date': datetime.now(),
                },
                'py_script': {
                    '_id': ObjectId(),
                    '_id_ref': new_prj_id,
                    'txt_pre_processing_script': AnlzPyScript().txt_pre_processing_script,
                    'txt_define_structure_script': AnlzPyScript().txt_define_structure_script,
                    'txt_main_processing_script': AnlzPyScript().txt_main_processing_script
                },
                'dtables': {
                    '_id': ObjectId(),
                    '_id_ref': new_prj_id,
                    'lst_dtables': AnlzDTables().lst_dtables
                },
                'rawdata': {
                    '_id': ObjectId(),
                    '_id_ref': new_prj_id,
                    'is_md': AnlzRawdata().is_md,
                    'date_upload': None,
                    'data': AnlzRawdata().data,
                    'info': AnlzRawdata().info,
                },
                'logging': {
                    '_id': ObjectId(),
                    '_id_ref': new_prj_id,
                    'is_running': AnlzLogging().is_running,
                    'lst_logging': AnlzLogging().lst_logging,
                },

            }

            for key, val in dict_add_new_prj.items():
                insert_doc = await self.anlz_colls.get(key).insert_one(val)

                if not insert_doc:
                    return {
                        'isSuccess': False,
                        'strErr': f'Add projects error at {key}: {val}'
                    }

            return {
                'isSuccess': True,
                'strErr': None
            }

        except Exception:
            self.logger.error(f'Add analyze project - {prj_info}:\n {traceback.format_exc()}')
            return {
                'isSuccess': False,
                'strErr': traceback.format_exc()
            }


    async def delete_prj(self, _id: str, user_info: dict) -> dict:
        try:
            self.logger.info(f'Delete analyze project ID {_id}')

            is_has_permission = await self.check_permission(_id=_id, user_info=user_info)

            if not is_has_permission:
                return {
                    'isSuccess': False,
                    'strErr': f"You don't have permission to delete project ID {_id}"
                }

            # !!!! REMEMBER DELETE ALL REFERENCE DOCUMENTS IN ALL COLLECTIONS !!!!
            for key, val in self.anlz_colls.items():
                if key == 'prj_info':
                    del_doc = await val.delete_one({'_id': ObjectId(_id)})
                else:
                    del_doc = await val.delete_one({'_id_ref': ObjectId(_id)})

                if not del_doc:
                    return {
                        'isSuccess': False,
                        'strErr': f'Delete project ID {_id} fail at {key}'
                    }

            return {
                'isSuccess': True,
                'strErr': None
            }

        except Exception:
            self.logger.error(f'Delete analyze project ID {_id}:\n {traceback.format_exc()}')
            return {
                'isSuccess': False,
                'strErr': traceback.format_exc()
            }


    async def copy_prj(self, _id, user_info: dict) -> dict:
        try:
            self.logger.info(f'Copy analyze project ID {_id}')

            new_prj_id = ObjectId()

            for key, val in self.anlz_colls.items():
                if key == 'prj_info':
                    copy_doc = await self.anlz_colls.get(key).find_one({'_id': ObjectId(_id)})

                    copy_doc['_id'] = new_prj_id
                    copy_doc['name'] = f"{copy_doc['name']} - copy at {datetime.now().strftime('%d/%m/%Y, %H:%M:%S')}"
                    copy_doc['owner'] = user_info.get('name')
                    copy_doc['NumOfRes'] = 0
                    copy_doc['create_date'] = datetime.now()

                else:
                    copy_doc = await self.anlz_colls.get(key).find_one({'_id_ref': ObjectId(_id)})

                    copy_doc['_id'] = ObjectId()
                    copy_doc['_id_ref'] = new_prj_id

                    if key == 'rawdata':
                        copy_doc['data'] = ""
                        copy_doc['info'] = ""

                    elif key == 'logging':
                        copy_doc['is_running'] = False
                        copy_doc['lst_logging'] = []

                if not copy_doc:
                    return {
                        'isSuccess': False,
                        'strErr': f'Copy project ID {_id} fail at {key}'
                    }

                new_copy_doc = await self.anlz_colls.get(key).insert_one(copy_doc)

                if not new_copy_doc:
                    return {
                        'isSuccess': False,
                        'strErr': f'Copy project ID {_id} to {new_prj_id} fail at {key}'
                    }

            return {
                'isSuccess': True,
                'strErr': None
            }

        except Exception:
            self.logger.error(f'Copy analyze project ID {_id}:\n {traceback.format_exc()}')
            return {
                'isSuccess': False,
                'strErr': traceback.format_exc()
            }


    async def retrieve_prj_by_id(self, _id: str, tab_name: str) -> dict:
        try:
            self.logger.info(f'Retrieve analyze project ID {_id}')

            prj_result = await self.get_prj_result(str_id=_id, tab_name=tab_name)

            lst_users = list()
            if tab_name.upper() == 'INFO':
                async for user in self.collection_user.find({}, {"name": 1}):
                    lst_users.append(user.get('name'))

            return {
                'isSuccess': True,
                'strErr': None,
                'lst_users': lst_users,
                'prj': prj_result
            }

        except Exception:
            self.logger.error(f'Retrieve analyze project ID {_id}:\n {traceback.format_exc()}')
            return {
                'isSuccess': False,
                'strErr': traceback.format_exc(),
                'prj': None
            }


    async def update_prj_info(self, _id, prj_info: AnlzPrjInfo, user_info: dict) -> dict:

        try:
            self.logger.info(f'Update analyze project ID {_id} info')

            is_has_permission = await self.check_permission(_id=_id, user_info=user_info)

            if not is_has_permission:
                return {
                    'isSuccess': False,
                    'strErr': f"You don't have permission to update project ID {_id}"
                }

            prj_updated = await self.anlz_colls.get('prj_info').update_one({'_id': ObjectId(_id)}, {'$set': prj_info.dict()})

            if prj_updated:
                return {
                    'isSuccess': True,
                    'strErr': None
                }

            return {
                'isSuccess': False,
                'strErr': f'Update analyze project ID {_id} info fail'
            }

        except Exception:
            self.logger.error(f'Update analyze project ID {_id} info:\n {traceback.format_exc()}')
            return {
                'isSuccess': False,
                'strErr': traceback.format_exc()
            }


    async def update_prj_pyscript(self, _id, tab_name: str, txt_py_script: str, user_info: dict) -> dict:

        try:
            self.logger.info(f'Update analyze project ID {_id} pyscript {tab_name}')

            is_has_permission = await self.check_permission(_id=_id, user_info=user_info)

            if not is_has_permission:
                return {
                    'isSuccess': False,
                    'strErr': f"You don't have permission to update project ID {_id}"
                }

            dict_update = dict()
            if tab_name.upper() in ['PRE_PROCESSING']:
                dict_update = {'txt_pre_processing_script': txt_py_script}
            elif tab_name.upper() in ['DEFINE_STRUCTURE']:
                dict_update = {'txt_define_structure_script': txt_py_script}
            elif tab_name.upper() in ['MAIN_PROCESSING']:
                dict_update = {'txt_main_processing_script': txt_py_script}

            prj_updated = await self.anlz_colls.get('py_script').update_one({'_id_ref': ObjectId(_id)}, {'$set': dict_update})

            if prj_updated:
                return {
                    'isSuccess': True,
                    'strErr': None
                }

            return {
                'isSuccess': False,
                'strErr': f'Update analyze project ID {_id} pyscript {tab_name} fail'
            }

        except Exception:
            self.logger.error(f'Update analyze project ID {_id} pyscript {tab_name}:\n {traceback.format_exc()}')
            return {
                'isSuccess': False,
                'strErr': traceback.format_exc()
            }


    async def update_prj_dtables(self, _id, lst_dtables: str, user_info: dict) -> dict:

        try:
            self.logger.info(f'Update analyze project ID {_id} dtables')

            is_has_permission = await self.check_permission(_id=_id, user_info=user_info)

            if not is_has_permission:
                return {
                    'isSuccess': False,
                    'strErr': f"You don't have permission to update project ID {_id}"
                }

            lst_dtables = json.loads(lst_dtables)

            prj_updated = await self.anlz_colls.get('dtables').update_one({'_id_ref': ObjectId(_id)}, {'$set': {'lst_dtables': lst_dtables}})

            if prj_updated:
                return {
                    'isSuccess': True,
                    'strErr': None
                }

            return {
                'isSuccess': False,
                'strErr': f'Update analyze project ID {_id} dtables fail'
            }

        except Exception:
            self.logger.error(f'Update analyze project ID {_id} dtables:\n {traceback.format_exc()}')
            return {
                'isSuccess': False,
                'strErr': traceback.format_exc()
            }


    async def upload_prj_rawdata(self, _id, user_info: dict, upload_files: AnlzUploadRawdataFiles) -> dict:
        try:
            self.logger.info(f'Upload rawdata to project ID {_id}')

            is_has_permission = await self.check_permission(_id=_id, user_info=user_info)

            if not is_has_permission:
                return {
                    'isSuccess': False,
                    'strErr': f"You don't have permission to upload rawdata to project ID {_id}"
                }

            data_converter = APDataConverter(files=upload_files.lst_file_rawdata, logger=self.logger, is_qme=upload_files.is_qme)

            if upload_files.is_qme:
                df_data, df_info = data_converter.convert_df_md() if upload_files.is_md else data_converter.convert_df_mc()
            else:
                df_data, df_info = data_converter.df_data_input, data_converter.df_qres_info_input

            prj_updated = await self.anlz_colls.get('rawdata').update_one(
                {'_id_ref': ObjectId(_id)},
                {
                    '$set': {
                        'is_md': upload_files.is_md,
                        'date_upload': datetime.now(),
                        'data': df_data.to_json(),
                        'info': df_info.to_json()
                    }
                }
            )

            if not prj_updated:
                return {
                    'isSuccess': False,
                    'strErr': f'Upload rawdata to project ID {_id} fail'
                }

            prj_updated = await self.anlz_colls.get('prj_info').update_one(
                {'_id': ObjectId(_id)},
                {
                    '$set': {
                        'NumOfRes': int(df_data.shape[0])
                    }
                }
            )

            if not prj_updated:
                return {
                    'isSuccess': False,
                    'strErr': f'Update NumOfRes to project ID {_id} fail'
                }

            return {
                'isSuccess': True,
                'strErr': None
            }

        except Exception:
            self.logger.error(f'Upload rawdata to project ID {_id} fail:\n {traceback.format_exc()}')
            return {
                'isSuccess': False,
                'strErr': traceback.format_exc()
            }


    async def delete_prj_rawdata(self, _id, user_info: dict) -> dict:
        try:
            self.logger.info(f'Delete rawdata of project ID {_id}')

            is_has_permission = await self.check_permission(_id=_id, user_info=user_info)

            if not is_has_permission:
                return {
                    'isSuccess': False,
                    'strErr': f"You don't have permission to delete rawdata of project ID {_id}"
                }

            prj_updated = await self.anlz_colls.get('rawdata').update_one(
                {'_id_ref': ObjectId(_id)},
                {
                    '$set': {
                        'date_upload': None,
                        'data': "",
                        'info': ""
                    }
                }
            )

            if not prj_updated:
                return {
                    'isSuccess': False,
                    'strErr': f'Delete rawdata of project ID {_id} fail'
                }

            prj_updated = await self.anlz_colls.get('prj_info').update_one({'_id': ObjectId(_id)}, {'$set': {'NumOfRes': 0}})

            if not prj_updated:
                return {
                    'isSuccess': False,
                    'strErr': f'Delete rawdata of project ID {_id} fail'
                }

            return {
                'isSuccess': True,
                'strErr': None
            }

        except Exception:
            self.logger.error(f'Delete rawdata of project ID {_id} fail:\n {traceback.format_exc()}')
            return {
                'isSuccess': False,
                'strErr': traceback.format_exc()
            }


    async def download_prj_rawdata(self, _id, user_info: dict) -> dict:
        try:
            self.logger.info(f'Download rawdata of project ID {_id}')

            is_has_permission = await self.check_permission(_id=_id, user_info=user_info)

            if not is_has_permission:
                return {
                    'isSuccess': False,
                    'strErr': f"You don't have permission to download rawdata of project ID {_id}"
                }

            prj_rawdata = await self.get_prj_result(str_id=_id, tab_name='download_rawdata')
            file_name = f"{prj_rawdata['internal_id']}_{prj_rawdata['name']}_Rawdata - {datetime.now().strftime('%d%m%Y')}.xlsx"

            with pd.ExcelWriter(file_name, engine="openpyxl") as writer:
                prj_rawdata['data'].to_excel(writer, sheet_name='Rawdata', index=False)
                prj_rawdata['info'].to_excel(writer, sheet_name='Datamap', index=False)

            return {
                'isSuccess': True,
                'strErr': None,
                'file_name': file_name,
            }

        except Exception:
            self.logger.error(f'Download rawdata of project ID {_id} fail:\n {traceback.format_exc()}')
            return {
                'isSuccess': False,
                'strErr': traceback.format_exc()
            }


    async def get_prj_log(self, _id):
        log = await self.anlz_colls.get('logging').find_one({'_id_ref': ObjectId(_id)}, {'is_running': 1, 'lst_logging': 1})
        return log


    async def set_logging(self, _id, is_running: bool, str_logging: str, is_err: bool = False):

        str_timenow = datetime.now().strftime("%d/%m/%Y, %H:%M:%S")

        if is_err:
            self.logger.error(str_logging)
            str_class = 'Error'  # 'text-danger'
        else:
            self.logger.info(str_logging)
            str_class = 'Info'  # 'text-success'

        # self.lst_logging.append(f"<div class='{str_class}'>{str_timenow} - <b>{str_logging}</b></div>")
        self.lst_logging.append([str_class, str_timenow, str_logging])

        await self.anlz_colls.get('logging').update_one({'_id_ref': ObjectId(_id)}, {
            '$set': {
                'is_running': is_running,
                'lst_logging': self.lst_logging
            }
        })


    async def run_prj_pyscript(self, _id, tab_name: str, user_info: dict):

        self.lst_logging: list = []

        try:
            is_running = await self.anlz_colls.get('logging').find_one({'_id_ref': ObjectId(_id)}, {'is_running': 1})
            if is_running['is_running']:
                await self.set_logging(_id, is_running=False, str_logging=f"Script is already running", is_err=True)
                return False

            await self.set_logging(_id, str_logging=f'Analyze project ID {_id} run pyscript {tab_name}', is_running=True)

            is_has_permission = await self.check_permission(_id=_id, user_info=user_info)
            if not is_has_permission:
                await self.set_logging(_id, is_running=False, str_logging=f"You don't have permission to run pyscript {tab_name} of project ID {_id}", is_err=True)
                return False

            prj_rawdata = await self.anlz_colls.get('rawdata').find_one({'_id_ref': ObjectId(_id)}, {'data': 1, 'info': 1, 'is_md': 1})
            if not prj_rawdata:
                await self.set_logging(_id, is_running=False, str_logging='Loading rawdata fail', is_err=True)
                return False

            await self.set_logging(_id, is_running=True, str_logging='Loading rawdata successful')

            if tab_name.upper() in ['PRE_PROCESSING']:  # ['DEFINE_STRUCTURE'] ['MAIN_PROCESSING']
                prj_pyscript = await self.anlz_colls.get('py_script').find_one({'_id_ref': ObjectId(_id)}, {'txt_pre_processing_script': 1})
            else:
                prj_pyscript = None

            if not prj_pyscript:
                await self.set_logging(_id, is_running=False, str_logging='Loading pyscript fail', is_err=True)
                return False

            df_data = pd.read_json(prj_rawdata['data'])
            df_info = pd.read_json(prj_rawdata['info'])

            local_vars = locals()
            exec(prj_pyscript['txt_pre_processing_script'], globals(), local_vars)

            df_data: pd.DataFrame = local_vars['df_data']
            df_info: pd.DataFrame = local_vars['df_info']

            df_data.reset_index(drop=True, inplace=True)
            df_info.drop_duplicates(subset=['var_name'], keep='last', inplace=True)
            df_info.reset_index(drop=True, inplace=True)

            set_df_data_col = set(list(df_data.columns))
            set_info_var_name = set(list(df_info['var_name']))

            missing1 = set_df_data_col.difference(set_info_var_name)
            missing2 = set_info_var_name.difference(set_df_data_col)

            if missing1 or missing2:
                str_err = "df_data.columns are not the same as df_info.var_name."
                if missing1:
                    str_err += f"\n- Missing {missing1} in df_info.var_name"
                if missing2:
                    str_err += f"\n- Missing {missing2} in df_data.columns"

                await self.set_logging(_id, is_running=False, str_logging=str_err, is_err=True)
                return False

            await self.set_logging(_id, is_running=True, str_logging='Running pyscript successful')

            await self.anlz_colls.get('rawdata').update_one({'_id_ref': ObjectId(_id)}, {
                '$set': {
                    'date_upload': datetime.now(),
                    'data': df_data.to_json(),
                    'info': df_info.to_json(),
                }
            })

            await self.set_logging(_id, is_running=False, str_logging='Updating rawdata successful\n-----COMPLETED-----')
            return True

        except Exception:
            await self.set_logging(_id, is_running=False, str_logging=traceback.format_exc(), is_err=True)
            return False

































    # async def get_proc_time(self, _id):
    #
    #     prj = await self.prj_collection.find_one({'_id': ObjectId(_id)}, {
    #         '_id': 1,
    #         'internal_id': 1,
    #         'detail': 1,
    #         'main': 1
    #     })
    #
    #     lenHeader = len(prj['detail']['topline_design']['header'].keys())
    #     lenSide = len(prj['detail']['topline_design']['side'].keys())
    #     secs = lenHeader * lenSide * prj['main'] * 3.5E-05
    #     secs = secs if secs >= 30 else 30
    #
    #     return secs
    #
    #
    # async def get_overView(self):
    #
    #     try:
    #         lst_prj = list()
    #         async for prj in self.prj_collection.find({}, {
    #             "_id": 1,
    #             "internal_id": 1,
    #             "name": 1,
    #             "categorical": 1,
    #             "type": 1,
    #             "status": 1,
    #             "create_date": 1,
    #             "screener": 1,
    #             "placement": 1,
    #             "main": 1
    #
    #         }):
    #             lst_prj.append(self.prj_info(prj, True))
    #
    #         overView = {
    #             'total': len(lst_prj),
    #             'completed': 0,
    #             'on_going': 0,
    #             'pending': 0,
    #             'cancel': 0
    #         }
    #
    #         for item in lst_prj:
    #             if item['status'] in ['Completed']:
    #                 overView['completed'] += 1
    #             elif item['status'] in ['On Going']:
    #                 overView['on_going'] += 1
    #             elif item['status'] in ['Pending']:
    #                 overView['pending'] += 1
    #             elif item['status'] in ['Cancel']:
    #                 overView['cancel'] += 1
    #
    #         return {
    #             'isSuccess': True,
    #             'strErr': None,
    #             'overView': overView
    #         }
    #
    #     except Exception:
    #         print(traceback.format_exc())
    #         return {
    #             'isSuccess': False,
    #             'strErr': traceback.format_exc(),
    #             'overView': None
    #         }
    #
    #



    # async def update_prj(self, _id: str, strBody: str):
    #
    #     try:
    #         prj = await self.prj_collection.find_one({'_id': ObjectId(_id)})
    #
    #         lstSec = [v['name'] for v in prj['detail']['sections'].values()]
    #
    #         data = self.body_to_json(strBody, lstSec)
    #
    #         if len(data) < 1:
    #             return {
    #                 'isSuccess': False,
    #                 'strErr': 'Data is null'
    #             }
    #
    #         if prj:
    #
    #             prj_updated = await self.prj_collection.update_one({'_id': ObjectId(_id)}, {'$set': data})
    #
    #             if prj_updated:
    #                 return {
    #                     'isSuccess': True,
    #                     'strErr': None
    #                 }
    #
    #         return {
    #             'isSuccess': False,
    #             'strErr': 'Cannot update'
    #         }
    #
    #     except Exception:
    #         print(traceback.format_exc())
    #         return {
    #             'isSuccess': False,
    #             'strErr': traceback.format_exc()
    #         }
    #
    #
    # @staticmethod
    # def body_to_json(strBody: str, lstSec: list):
    #     re_json = re.compile(r'\{"output".+}')
    #     str_json = re_json.search(strBody).group()
    #     dictBody = dict(json.loads(str_json))
    #     dictBody.pop('output')
    #
    #     updateData = dict()
    #     for key, val in dictBody.items():
    #         if val is not None:
    #
    #             if str(val).lower() in ['true', 'false']:
    #                 upVal = True if str(val).lower() == 'true' else False
    #             else:
    #                 upVal = val
    #
    #             if '.' in key:
    #                 parentKey = str(key).rsplit('.', 1)[0]
    #                 childKey = str(key).rsplit('.', 1)[1]
    #
    #                 if 'detail.oe_combine_cols' in parentKey or 'detail.scr_cols' in parentKey \
    #                         or 'detail.product_cols' in parentKey or 'detail.fc_cols' in parentKey \
    #                         or 'detail.plm_to_scr_cols' in parentKey or 'detail.plm_to_prod_cols' in parentKey:
    #
    #                     if isinstance(upVal, list):
    #
    #                         if parentKey in updateData.keys():
    #                             updateData[parentKey].update({childKey: upVal})
    #                         else:
    #                             updateData = {parentKey: {childKey: upVal}}
    #
    #                     else:
    #
    #                         lstKey = str(key).split('.')
    #                         parentKey2 = '.'.join(lstKey[:2])
    #                         childKey2 = lstKey[-2]
    #
    #                         if updateData:
    #
    #                             if parentKey2 in updateData.keys():
    #                                 if childKey2 in updateData[parentKey2].keys():
    #                                     updateData[parentKey2][childKey2].append(upVal)
    #                                 else:
    #                                     updateData[parentKey2].update({childKey2: [upVal]})
    #                             else:
    #                                 updateData[parentKey2] = {
    #                                     childKey2: [upVal]
    #                                 }
    #
    #                         else:
    #                             updateData = {
    #                                 parentKey2:
    #                                     {
    #                                         childKey2: [upVal]
    #                                     }
    #                             }
    #
    #                 elif 'detail.addin_vars' in parentKey:
    #
    #                     lstKey = str(key).rsplit('.')
    #
    #                     parentKey2 = '.'.join(lstKey[:2])
    #                     varIdx = lstKey[2]
    #                     varAtt = lstKey[3]
    #
    #                     catIdx, catAtt = None, None
    #                     if len(lstKey) > 4:
    #                         catIdx = lstKey[4]
    #                         catAtt = lstKey[5]
    #
    #                     if parentKey2 not in updateData.keys():
    #                         updateData[parentKey2] = dict()
    #
    #                     if varIdx not in updateData[parentKey2].keys():
    #                         updateData[parentKey2][varIdx] = dict()
    #
    #                     if varAtt not in updateData[parentKey2][varIdx].keys():
    #                         updateData[parentKey2][varIdx][varAtt] = dict()
    #
    #                     if varAtt in ['name', 'lbl']:
    #                         updateData[parentKey2][varIdx][varAtt] = upVal
    #                     else:
    #                         if catIdx not in updateData[parentKey2][varIdx][varAtt].keys():
    #                             updateData[parentKey2][varIdx][varAtt][catIdx] = dict()
    #
    #                         updateData[parentKey2][varIdx][varAtt][catIdx][catAtt] = upVal
    #
    #                 elif 'topline_design.header' in parentKey:
    #                     lstKey = str(key).rsplit('.')
    #                     parentKey2 = '.'.join(lstKey[:3])
    #                     varIdx = lstKey[3]
    #
    #                     if parentKey2 not in updateData.keys():
    #                         updateData[parentKey2] = dict()
    #
    #                     if varIdx not in updateData[parentKey2].keys():
    #                         updateData[parentKey2][varIdx] = dict()
    #
    #                     if isinstance(upVal, list):
    #                         updateData[parentKey2][varIdx] = {
    #                             'name': upVal[0],
    #                             'lbl': upVal[1],
    #                             'hidden_cats': upVal[2],
    #                             "filter": upVal[3],
    #                             'run_secs': upVal[4] if len(upVal) == 5 else ','.join(lstSec)
    #                         }
    #
    #                     else:
    #                         varAtt = lstKey[4]
    #                         updateData[parentKey2][varIdx][varAtt] = upVal
    #
    #                 elif 'topline_design.side' in parentKey:
    #
    #                     lstKey = str(key).rsplit('.')
    #                     parentKey2 = '.'.join(lstKey[:3])
    #                     varIdx = lstKey[3]
    #
    #                     if parentKey2 not in updateData.keys():
    #                         updateData[parentKey2] = dict()
    #
    #                     if varIdx not in updateData[parentKey2].keys():
    #                         updateData[parentKey2][varIdx] = {
    #                             'group_lbl': '',
    #                             'name': '',
    #                             'lbl': '',
    #                             'type': '',
    #                             't2b': False,
    #                             'b2b': False,
    #                             'mean': False,
    #                             'ma_cats': '',
    #                             'hidden_cats': '',
    #                             'is_count': False,
    #                             'is_corr': False,
    #                             'is_ua': False
    #                         }
    #
    #                     if isinstance(upVal, list):
    #
    #                         lstAttKey = updateData[parentKey2][varIdx].keys()
    #                         for idx_AttKey, val_AttKey in enumerate(lstAttKey):
    #
    #                             if str(upVal[idx_AttKey]).lower() in ['true', 'false']:
    #                                 newUpVal = True if str(upVal[idx_AttKey]).lower() == 'true' else False
    #                             else:
    #                                 newUpVal = upVal[idx_AttKey]
    #
    #                             updateData[parentKey2][varIdx][val_AttKey] = newUpVal
    #
    #                     else:
    #                         varAtt = lstKey[4]
    #                         updateData[parentKey2][varIdx][varAtt] = upVal
    #
    #                 elif 'detail.prj_info' in parentKey:
    #                     parentKey2 = str(key).replace('prj.detail.prj_info', 'detail.prj_info')
    #                     updateData[parentKey2] = upVal
    #
    #                 else:
    #                     updateData[key] = upVal
    #
    #             else:
    #                 updateData[key] = upVal
    #
    #     return updateData
    #
    #
    # async def upload_prj_data(self, _id: str, file_scr, file_plm, file_main, file_coding, file_codelist) -> dict:
    #
    #     try:
    #         # ----- MA by column ----- NEW FORMAT
    #         scr_data, scr_qreInfo = self.convert_by_mc(file_scr)
    #
    #         plm_data, plm_qreInfo = {}, {}
    #         if file_plm is not None:
    #             if file_plm.filename:
    #                 plm_data, plm_qreInfo = self.convert_by_mc(file_plm)
    #
    #         main_data, main_qreInfo = {}, {}
    #         if file_main.filename:
    #             main_data, main_qreInfo = self.convert_by_mc(file_main)
    #
    #         data = {
    #             'screener': {
    #                 'data': scr_data,
    #                 'qreInfo': scr_qreInfo
    #             },
    #             'placement': {
    #                 'data': plm_data,
    #                 'qreInfo': plm_qreInfo
    #             },
    #             'main': {
    #                 'data': main_data,
    #                 'qreInfo': main_qreInfo
    #             }
    #         }
    #
    #         if not scr_data:
    #             return {
    #                 'isSuccess': False,
    #                 'strErr': 'Data screener is null'
    #             }
    #
    #         prj_rawdata = await self.rawdata_collection.find_one({'_ref_id': ObjectId(_id)}, {'_id': 1, '_ref_id': 1})
    #
    #         if not prj_rawdata:
    #             new_rawdata = new_rawdata_template
    #             new_rawdata['_id'] = ObjectId()
    #             new_rawdata['_ref_id'] = ObjectId(_id)
    #             new_rawdata['screener'] = data['screener']
    #             new_rawdata['placement'] = data['placement']
    #             new_rawdata['main'] = data['main']
    #             upload_prj_rawdata = await self.rawdata_collection.insert_one(new_rawdata)
    #         else:
    #             upload_prj_rawdata = await self.rawdata_collection.update_one({'_ref_id': ObjectId(_id)}, {'$set': data})
    #
    #         upload_prj = await self.prj_collection.update_one({'_id': ObjectId(_id)}, {
    #                 '$set': {
    #                     'screener': len(data['screener']['data']),
    #                     'placement': len(data['placement']['data']),
    #                     'main': len(data['main']['data'])
    #                 }
    #             }
    #         )
    #
    #         if not upload_prj_rawdata or not upload_prj:
    #             return {
    #                 'isSuccess': False,
    #                 'strErr': 'Upload unsuccessfully'
    #             }
    #
    #
    #         # Addin to process OE (11/4/2023)---------------------------------------------------------------------------
    #         if file_coding.filename and file_codelist.filename:
    #
    #             dict_oe_info = {
    #                 # store oe info from file_coding and file_codelist
    #                 'codelist': file_codelist.file.read(),
    #                 'coding': file_coding.file.read(),
    #             }
    #
    #             prj_openend = await self.openend_collection.find_one({'_ref_id': ObjectId(_id)}, {'_id': 1, '_ref_id': 1})
    #
    #             if not prj_openend:
    #                 new_openend = new_openend_template
    #                 new_openend['_id'] = ObjectId()
    #                 new_openend['_ref_id'] = ObjectId(_id)
    #                 new_openend['oe_info'] = dict_oe_info
    #                 upload_prj_openend = await self.openend_collection.insert_one(new_openend)
    #             else:
    #                 upload_prj_openend = await self.openend_collection.update_one({'_ref_id': ObjectId(_id)}, {'$set': {'oe_info': dict_oe_info}})
    #
    #             if not upload_prj_openend:
    #                 return {
    #                     'isSuccess': False,
    #                     'strErr': 'Upload coding/codelist unsuccessfully'
    #                 }
    #         # End Addin to process OE (11/4/2023)-----------------------------------------------------------------------
    #
    #         return {
    #             'isSuccess': True,
    #             'strErr': 'Upload successfully'
    #         }
    #
    #     except Exception:
    #         print(traceback.format_exc())
    #         return {
    #             'isSuccess': False,
    #             'strErr': traceback.format_exc()
    #         }
    #
    #
    # async def clear_prj_data(self, _id: str):
    #
    #     try:
    #         clear_prj_countData = await self.prj_collection.update_one(
    #             {'_id': ObjectId(_id)},
    #             {'$set': {'screener': 0, 'placement': 0, 'main': 0}}
    #         )
    #
    #         clear_prj_rawdata = await self.rawdata_collection.update_one(
    #             {'_ref_id': ObjectId(_id)},
    #             {'$set': {'screener': {}, 'placement': {}, 'main': {}}}
    #         )
    #
    #         clear_prj_oe = await self.openend_collection.update_one(
    #             {'_ref_id': ObjectId(_id)},
    #             {'$set': {'oe_info': {}}}
    #         )
    #
    #         if not clear_prj_rawdata or not clear_prj_countData or not clear_prj_oe:
    #             return {
    #                 'isSuccess': False,
    #                 'strErr': 'Upload unsuccessfully'
    #             }
    #
    #         return {
    #             'isSuccess': True,
    #             'strErr': 'Upload successfully'
    #         }
    #
    #     except Exception:
    #         print(traceback.format_exc())
    #         return {
    #             'isSuccess': False,
    #             'strErr': traceback.format_exc()
    #         }
    #
    #
    # async def data_export(self, _id: str, export_section: str, export_option: str) -> dict:
    #
    #     try:
    #         st = time.process_time()
    #
    #         prj = await self.prj_collection.find_one({'_id': ObjectId(_id)})
    #         rawdata = await self.rawdata_collection.find_one({'_ref_id': ObjectId(_id)}, {
    #             'screener': 1,
    #             'placement': 1,
    #             'main': 1})
    #
    #         prj['screener'] = rawdata['screener']
    #
    #         if prj['type'] == 'HUT':
    #             prj['placement'] = rawdata['placement']
    #         else:
    #             prj['placement'] = {}
    #
    #         prj['main'] = rawdata['main']
    #
    #         openend = await self.openend_collection.find_one({'_ref_id': ObjectId(_id)}, {'oe_info': 1})
    #         if openend:
    #             prj['oe_info'] = openend['oe_info']
    #         else:
    #             prj['oe_info'] = {
    #                 "coding": "",
    #                 "codelist": "",
    #             }
    #
    #         # if export_option == 'codeframe':
    #         #     openend = await self.openend_collection.find_one({'_ref_id': ObjectId(_id)}, {'codeframes': 1})
    #         #     prj['codeframes'] = openend['codeframes']
    #
    #         self.init_data_exporter_variables(prj=prj, export_section=export_section, export_option=export_option)
    #
    #         if export_option in ['codeframe']:
    #             isSuccess = self.export_data(is_export_raw=False, is_export_stacked=True, is_export_unstacked=False, is_fc_yn=False, is_export_sav=False)
    #         elif export_option in ['screener_only']:
    #             isSuccess = self.export_data(is_export_raw=False, is_export_stacked=True, is_export_unstacked=False, is_fc_yn=False, is_export_sav=True)
    #         else:
    #             isSuccess = self.export_data(is_fc_yn=False)
    #
    #         if not isSuccess[0]:
    #             return {
    #                 'isSuccess': False,
    #                 'strErr': isSuccess[1],
    #                 'zipName': None
    #             }
    #
    #         et = time.process_time()
    #         print('EXPORT DATA - TIME:', et - st, 'seconds')
    #
    #         return {
    #             'isSuccess': True,
    #             'strErr': None,
    #             # 'zipName': exp_data.zipName
    #             'zipName': self.zip_name
    #         }
    #
    #     except Exception:
    #         print(traceback.format_exc())
    #         return {
    #             'isSuccess': False,
    #             'strErr': traceback.format_exc(),
    #             'zipName': None
    #         }
    #
    #
    # async def topline_process(self, _id: str, export_section: str, export_sheets: list):
    #     # NEW FORMAT----------------------------------------------------------------------------------------------------
    #     try:
    #         st = time.process_time()
    #
    #         prj = await self.prj_collection.find_one({'_id': ObjectId(_id)}, {
    #             '_id': 1,
    #             'internal_id': 1,
    #             'name': 1,
    #             'categorical': 1,
    #             'type': 1,
    #             'status': 1,
    #             'detail': 1,
    #             'screener': 1,
    #             'placement': 1,
    #             'main': 1,
    #             'topline_exporter': 1
    #         })
    #
    #         rawdata = await self.rawdata_collection.find_one({'_ref_id': ObjectId(_id)}, {
    #             'screener': 1,
    #             'placement': 1,
    #             'main': 1
    #         })
    #
    #         prj['screener'] = rawdata['screener']
    #         prj['placement'] = rawdata['placement']
    #         prj['main'] = rawdata['main']
    #
    #         openend = await self.openend_collection.find_one({'_ref_id': ObjectId(_id)}, {'oe_info': 1})
    #         if openend:
    #             prj['oe_info'] = openend['oe_info']
    #         else:
    #             prj['oe_info'] = {
    #                 'coding': "",
    #                 'codelist': ""
    #             }
    #
    #         self.init_data_exporter_variables(prj=prj, export_section=export_section)
    #         isSuccess = self.export_data(is_export_raw=False, is_export_stacked=False, is_export_unstacked=True, is_fc_yn=True, is_export_sav=False)
    #
    #         if not isSuccess[0]:
    #             return {
    #                 'isSuccess': False,
    #                 'strErr': isSuccess[1]
    #             }
    #
    #         self.init_topline_exporter_variables(prj=prj, df_data=self.df_data_unstacked, df_info=self.df_info_unstacked, export_section=export_section)
    #
    #         # msn_topline_exporter = ToplineExporterV2(prj, df_data=self.df_data_unstacked, df_info=self.df_info_unstacked, export_section=export_section)
    #         # isSuccess = msn_topline_exporter.process_topline()
    #
    #         isSuccess = self.process_topline()
    #
    #         if not isSuccess[0]:
    #
    #             topline_exporter_data = {
    #                 export_section: {
    #                     'Ttest': False,
    #                     'errLbl': isSuccess[1]
    #                 }
    #             }
    #
    #             upload_prj_Ttest_UA = await self.prj_collection.update_one(
    #                 {'_id': ObjectId(_id)}, {'$set': {
    #                     'topline_exporter': topline_exporter_data
    #                 }
    #                 }
    #             )
    #
    #             if not upload_prj_Ttest_UA:
    #                 return {
    #                     'isSuccess': False,
    #                     'strErr': 'Upload Test & UA failed'
    #                 }
    #
    #             return {
    #                 'isSuccess': False,
    #                 'strErr': isSuccess[1]
    #             }
    #
    #
    #         # isSuccess = msn_topline_exporter.generate_topline_excel(export_sheets)
    #         isSuccess = self.generate_topline_excel(export_sheets)
    #
    #         if not isSuccess[0]:
    #
    #             topline_exporter_data = {
    #                 export_section: {
    #                     'Ttest': False,
    #                     'errLbl': isSuccess[1]
    #                 }
    #             }
    #
    #             upload_prj_Ttest_UA = await self.prj_collection.update_one(
    #                 {'_id': ObjectId(_id)}, {'$set': {
    #                     'topline_exporter': topline_exporter_data
    #                 }
    #                 }
    #             )
    #
    #             if not upload_prj_Ttest_UA:
    #                 return {
    #                     'isSuccess': False,
    #                     'strErr': 'Upload Test & UA failed'
    #                 }
    #
    #             return {
    #                 'isSuccess': False,
    #                 'strErr': isSuccess[1]
    #             }
    #
    #         await self.prj_collection.update_one(
    #             {'_id': ObjectId(_id)}, {'$set': {
    #                 'topline_exporter': {"file_name": self.topline_name}
    #             }
    #             }
    #         )
    #
    #         et = time.process_time()
    #         print('Topline process time:', et - st, 'seconds')
    #
    #         return {
    #             'isSuccess': True,
    #             'strErr': None
    #         }
    #
    #     except Exception:
    #         print(traceback.format_exc())
    #
    #         topline_exporter_data = {
    #             export_section: {
    #                 'Ttest': False,
    #                 'errLbl': traceback.format_exc()
    #             }
    #         }
    #
    #         await self.prj_collection.update_one(
    #             {'_id': ObjectId(_id)}, {'$set': {
    #                 'topline_exporter': topline_exporter_data
    #             }
    #             }
    #         )
    #
    #         return {
    #             'isSuccess': False,
    #             'strErr': traceback.format_exc()
    #         }
    #
    #
    # async def clear_topline_info(self, _id: str):
    #
    #     await self.prj_collection.update_one(
    #         {'_id': ObjectId(_id)}, {'$set': {
    #             'topline_exporter': {}
    #         }
    #         }
    #     )
    #
    #     await self.rawdata_collection.update_one(
    #         {'_ref_id': ObjectId(_id)}, {'$set': {
    #             'topline_structure': {}
    #         }
    #         }
    #     )
    #
    #
    # async def add_user(self, username, useremail, password):
    #     try:
    #
    #         lstUser = list()
    #         async for usr in self.user_collection.find({'email': useremail}, {'email': 1}):
    #             lstUser.append(usr)
    #
    #         if lstUser:
    #             return {
    #                 'isSuccess': False,
    #                 'strErr': 'Email is already registered.'
    #             }
    #
    #         new_user = new_user_template
    #         new_user['_id'] = ObjectId()
    #         new_user['email'] = useremail
    #         new_user['password'] = password
    #         new_user['name'] = username
    #         new_user['create_at'] = datetime.now()
    #         new_user['login_at'] = datetime.now()
    #
    #         user = await self.user_collection.insert_one(new_user)
    #
    #         if not user:
    #             return {
    #                 'isSuccess': False,
    #                 'strErr': 'Add user error'
    #             }
    #
    #         return {
    #             'isSuccess': True,
    #             'strErr': None
    #         }
    #
    #     except Exception:
    #         print(traceback.format_exc())
    #         return {
    #             'isSuccess': False,
    #             'strErr': traceback.format_exc()
    #         }
    #
    #
    # async def topline_bulkup_export(self, _id: str, strWhich: str):
    #
    #     try:
    #         prj = await self.prj_collection.find_one({'_id': ObjectId(_id)}, {
    #             '_id': 1,
    #             'internal_id': 1,
    #             'name': 1,
    #             'detail': 1
    #         })
    #
    #         csv_name = f"{prj['name']}_bulkup_{strWhich}.csv"
    #         df = pd.DataFrame.from_dict(prj['detail']['topline_design'][strWhich]).T
    #
    #         df.to_csv(csv_name, encoding='utf-8-sig', index=False)
    #
    #         return {
    #             'isSuccess': True,
    #             'strErr': None,
    #             'csvName': csv_name
    #         }
    #
    #     except Exception:
    #         print(traceback.format_exc())
    #         return {
    #             'isSuccess': False,
    #             'strErr': traceback.format_exc(),
    #             'csvName': None
    #         }
    #
    #
    # async def topline_side_auto_generate(self, _id: str):
    #
    #     try:
    #         prj = await self.prj_collection.find_one({'_id': ObjectId(_id)}, {
    #             '_id': 1,
    #             'detail': 1
    #         })
    #
    #         if not prj:
    #             return {
    #                 'isSuccess': False,
    #                 'strErr': 'Topline side-axis auto generate fail'
    #             }
    #
    #         topline_side_axis = self.auto_generate_topline_side_axis(prj=prj)
    #
    #         # tl_design = ToplineDesign(prj)
    #         # topline_side_axis = tl_design.auto_generate_topline_side_axis()
    #
    #         await self.prj_collection.update_one({'_id': ObjectId(_id)}, {'$set': {'detail.topline_design.side': topline_side_axis}})
    #
    #         return {
    #             'isSuccess': True,
    #             'strErr': 'Topline side-axis auto generate successfully'
    #         }
    #
    #     except Exception:
    #         print(traceback.format_exc())
    #         return {
    #             'isSuccess': False,
    #             'strErr': traceback.format_exc()
    #         }
    #
    #
    # async def create_codeframe(self, _id: str, strBody: str):
    #
    #     try:
    #
    #         # Convert body string to dictionary
    #         re_json = re.compile(r'\{"output".+}')
    #         str_json = re_json.search(strBody).group()
    #         dictBody = dict(json.loads(str_json))
    #         dictBody.pop('output')
    #
    #         # Restructure dictionary to upload to db
    #         dictBody_restructure = {'codeframes': dict()}
    #         for key, val in dictBody.items():
    #
    #             str_key = str(key).replace('codeframes.', '')
    #             lst_key = str_key.split('.')
    #
    #             str_dict_item = str_key.replace(".", "':{'")
    #             str_dict_item = '{' + f"'{str_dict_item}':'{val}'{'}' * len(lst_key)}"
    #             dict_item = eval(str_dict_item)
    #
    #             for key2, val2 in dict_item.items():
    #                 if key2 in dictBody_restructure['codeframes'].keys():
    #                     dictBody_restructure['codeframes'][key2].update(val2)
    #                 else:
    #                     dictBody_restructure['codeframes'].update({key2: val2})
    #
    #
    #         # Upload codeframe to bd
    #         prj_openend = await self.openend_collection.find_one({'_ref_id': ObjectId(_id)}, {'_id': 1, '_ref_id': 1})
    #
    #         if not prj_openend:
    #             new_openend = new_openend_template
    #             new_openend['_id'] = ObjectId()
    #             new_openend['_ref_id'] = ObjectId(_id)
    #             new_openend['codeframes'] = dictBody_restructure['codeframes']
    #             upload_prj_openend = await self.openend_collection.insert_one(new_openend)
    #         else:
    #             upload_prj_openend = await self.openend_collection.update_one({'_ref_id': ObjectId(_id)}, {'$set': dictBody_restructure})
    #
    #         if upload_prj_openend:
    #             return {
    #                 'isSuccess': True,
    #                 'strErr': 'Upload openend successfully'
    #             }
    #         else:
    #             return {
    #                 'isSuccess': False,
    #                 'strErr': 'Upload openend unsuccessfully'
    #             }
    #
    #     except Exception:
    #         print(traceback.format_exc())
    #         return {
    #             'isSuccess': False,
    #             'strErr': traceback.format_exc()
    #         }