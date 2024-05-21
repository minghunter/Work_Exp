from .MSN_Data_Exporter import DataExporter
from .MSN_Models import *
from bson.objectid import ObjectId
from datetime import datetime
from dotenv import load_dotenv
import motor.motor_asyncio
import pandas as pd
import numpy as np
import re
import json
import traceback
import time
import os
import io

from dotty_dict import dotty
from mergedeep import merge as merge_dict



class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)


class MsnPrj(DataExporter):

    def __init__(self):

        DataExporter.__init__(self)

        # --------------------------------------------------------------------------------------------------------------

        load_dotenv()
        MONGO_DETAILS = os.environ.get("MONGO_DETAILS")

        if not MONGO_DETAILS:
            # MONGO_DETAILS = 'mongodb://localhost:27017'
            with open("mongodb_cre.txt", 'r') as txt_mongodb_cre:
                MONGO_DETAILS = txt_mongodb_cre.readline()

        client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_DETAILS)

        db_msn = client.msn

        self.prj_collection = db_msn.get_collection('projects')
        self.rawdata_collection = db_msn.get_collection('rawdata')
        self.user_collection = db_msn.get_collection('users')
        self.openend_collection = db_msn.get_collection('openend')



    @staticmethod
    def prj_info(prj, isShort) -> dict:
        if isShort:
            return {
                'id': str(prj['_id']),
                'internal_id': prj['internal_id'],
                'name': prj['name'],
                'categorical': prj['categorical'],
                'type': prj['type'],
                'status': prj['status'],
                'lenOfScr': prj['screener'],
                'lenOfPlm': prj['placement'],
                'lenOfMain': prj['main'],
            }
        else:

            sec_topline_exporter = dict()
            download_file_name = ""

            if not prj['topline_exporter']:
                sec_topline_exporter = {}
            elif 'file_name' in prj['topline_exporter'].keys():
                download_file_name = prj['topline_exporter']['file_name']
            else:
                for key, val in prj['topline_exporter'].items():

                    if val['Ttest']:
                        strSecName = prj['detail']['sections'][str(key)]['name']
                        sec_topline_exporter.update({str(key): strSecName})
                    else:
                        sec_topline_exporter = {
                            'isErr': True,
                            'errLbl': val['errLbl']
                        }
                        break

            return {
                'id': str(prj['_id']),
                'internal_id': prj['internal_id'],
                'name': prj['name'],
                'categorical': prj['categorical'],
                'type': prj['type'],
                'status': prj['status'],
                'detail': prj['detail'],
                'lenOfScr': prj['screener'],
                'lenOfPlm': prj['placement'],
                'lenOfMain': prj['main'],
                # 'codeframes': prj['codeframes'],
                'sec_topline_exporter': sec_topline_exporter,
                'download_file_name': download_file_name,
                'is_has_oe_coding': prj['is_has_oe_coding'],
                'is_has_oe_codelist': prj['is_has_oe_coding'],
            }


    async def get_proc_time(self, _id):

        prj = await self.prj_collection.find_one({'_id': ObjectId(_id)}, {
            '_id': 1,
            'internal_id': 1,
            'detail': 1,
            'main': 1,
            'has_oe': 1
        })

        lenHeader = len(prj['detail']['topline_design']['header'].keys())
        lenSide = len(prj['detail']['topline_design']['side'].keys())

        secs = lenHeader * lenSide * prj['main'] * 3.5E-05
        secs = secs if secs >= 30 else 30
        secs = secs * 1.5 if prj['has_oe'] else secs

        return secs


    async def get_overView(self):

        try:
            lst_prj = list()
            async for prj in self.prj_collection.find({}, {
                "_id": 1,
                "internal_id": 1,
                "name": 1,
                "categorical": 1,
                "type": 1,
                "status": 1,
                "create_date": 1,
                "screener": 1,
                "placement": 1,
                "main": 1

            }):
                lst_prj.append(self.prj_info(prj, True))

            overView = {
                'total': len(lst_prj),
                'completed': 0,
                'on_going': 0,
                'pending': 0,
                'cancel': 0
            }

            for item in lst_prj:
                if item['status'] in ['Completed']:
                    overView['completed'] += 1
                elif item['status'] in ['On Going']:
                    overView['on_going'] += 1
                elif item['status'] in ['Pending']:
                    overView['pending'] += 1
                elif item['status'] in ['Cancel']:
                    overView['cancel'] += 1

            return {
                'isSuccess': True,
                'strErr': None,
                'overView': overView
            }

        except Exception:
            print(traceback.format_exc())
            return {
                'isSuccess': False,
                'strErr': traceback.format_exc(),
                'overView': None
            }


    @staticmethod
    def count_page(lst_prj, step):
        tuple_divMod = divmod(len(lst_prj), step)

        if tuple_divMod[1] != 0:
            page_count = tuple_divMod[0] + 1
        else:
            page_count = tuple_divMod[0]

        return page_count


    async def retrieve_search(self, page: int, kword: str):

        if kword == 'all':
            dict_search = {}
        else:
            if kword == 'has data':
                lst_key_search = [
                    {'screener': {'$gt': 0}},
                    {'placement': {'$gt': 0}},
                    {'main': {'$gt': 0}},
                ]
            elif 'type:' in kword:
                lst_key_search = [{'type': {'$regex': f'.*{kword.split(":")[1]}.*', '$options': 'i'}}]
            else:
                lst_key_search = [
                    {'name': {'$regex': f'.*{kword}.*', '$options': 'i'}},
                    {'internal_id': {'$regex': f'.*{kword}.*', '$options': 'i'}},
                    {'status': {'$regex': f'.*{kword}.*', '$options': 'i'}},
                ]

            dict_search = {'$or': lst_key_search}

        lst_prj = list()

        async for prj in self.prj_collection.find(dict_search, {
            "_id": 1,
            "internal_id": 1,
            "name": 1,
            "categorical": 1,
            "type": 1,
            "status": 1,
            "create_date": 1,
            "screener": 1,
            "placement": 1,
            "main": 1
        }).sort('create_date', -1):
            lst_prj.append(self.prj_info(prj, True))

        if len(lst_prj) > 5:
            step = 5
            page_count = self.count_page(lst_prj, step)
            lst_prj = lst_prj[((page - 1) * step):(page * step)]
        else:
            page_count = 1

        return {
            'isSuccess': True,
            'lst_prj': lst_prj,
            'page_count': page_count
        }



    async def retrieve(self, page):
        try:

            lst_prj = list()
            async for prj in self.prj_collection.find({}, {
                "_id": 1,
                "internal_id": 1,
                "name": 1,
                "categorical": 1,
                "type": 1,
                "status": 1,
                "create_date": 1,
                "screener": 1,
                "placement": 1,
                "main": 1
            }).sort('create_date', -1):
                lst_prj.append(self.prj_info(prj, True))

            step = 5
            page_count = self.count_page(lst_prj, step)

            lst_prj = lst_prj[((page - 1) * step):(page * step)]

            return {
                'isSuccess': True,
                'strErr': None,
                'lst_prj': lst_prj,
                'overView': None,
                'page_count': page_count
            }

        except Exception:
            print(traceback.format_exc())
            return {
                'isSuccess': False,
                'strErr': traceback.format_exc(),
                'lst_prj': None,
                'overView': None,
                'page_count': None
            }


    # async def search(self, str_key_search: str = ''):
    #     try:
    #
    #         if str_key_search == 'has data':
    #             lst_key_search = [
    #                 {'screener': {'$gt': 0}},
    #                 {'placement': {'$gt': 0}},
    #                 {'main': {'$gt': 0}},
    #             ]
    #         elif 'type:' in str_key_search:
    #             lst_key_search = [
    #                 {'type': {'$regex': f'.*{str_key_search.split(":")[1]}.*', '$options': 'i'}},
    #             ]
    #         else:
    #             lst_key_search = [
    #                 {'name': {'$regex': f'.*{str_key_search}.*', '$options': 'i'}},
    #                 {'internal_id': {'$regex': f'.*{str_key_search}.*', '$options': 'i'}},
    #                 {'status': {'$regex': f'.*{str_key_search}.*', '$options': 'i'}},
    #             ]
    #
    #         lst_prj = list()
    #         async for prj in self.prj_collection.find(
    #                 {
    #                     '$or': lst_key_search
    #                 },
    #                 {
    #                     "_id": 1,
    #                     "internal_id": 1,
    #                     "name": 1,
    #                     "categorical": 1,
    #                     "type": 1,
    #                     "status": 1,
    #                     "create_date": 1,
    #                     "screener": 1,
    #                     "placement": 1,
    #                     "main": 1
    #                 }
    #         ).sort('create_date', -1):
    #             lst_prj.append(self.prj_info(prj, True))
    #
    #         step = 10
    #         page_count = self.count_page(lst_prj, step)
    #
    #         return {
    #             'isSuccess': True,
    #             'strErr': None,
    #             'lst_prj': lst_prj,
    #             'overView': None,
    #             'page_count': page_count
    #         }
    #
    #     except Exception:
    #         print(traceback.format_exc())
    #         return {
    #             'isSuccess': False,
    #             'strErr': traceback.format_exc(),
    #             'lst_prj': None,
    #             'overView': None,
    #             'page_count': None
    #         }


    async def add(self, internal_id, prj_name, categorical, prj_type, prj_status):
        try:

            new_prj = new_prj_template
            new_prj['_id'] = ObjectId()
            new_prj['internal_id'] = internal_id
            new_prj['name'] = prj_name
            new_prj['categorical'] = categorical
            new_prj['type'] = prj_type
            new_prj['status'] = prj_status
            new_prj['create_date'] = datetime.now()

            prj = await self.prj_collection.insert_one(new_prj)

            if not prj:
                return {
                    'isSuccess': False,
                    'strErr': 'Add projects error'
                }

            return {
                'isSuccess': True,
                'strErr': None
            }

        except Exception:
            print(traceback.format_exc())
            return {
                'isSuccess': False,
                'strErr': traceback.format_exc()
            }


    async def copy_prj(self, _id):
        try:

            prj = await self.prj_collection.find_one({'_id': ObjectId(_id)})
            prj['_id'] = ObjectId()
            prj['name'] = f"{prj['name']} - copy at {datetime.now()}"
            prj['create_date'] = datetime.now()
            prj['screener'] = 0
            prj['placement'] = 0
            prj['main'] = 0

            new_prj = await self.prj_collection.insert_one(prj)

            if not new_prj:
                return {
                    'isSuccess': False,
                    'strErr': 'Copy msn project error'
                }

            return {
                'isSuccess': True,
                'strErr': None
            }

        except Exception:
            print(traceback.format_exc())
            return {
                'isSuccess': False,
                'strErr': traceback.format_exc()
            }


    async def delete(self, _id, email):
        try:
            user = await self.user_collection.find_one({'email': email}, {'email': 1, 'role': 1})

            if user['role'] != 'admin':
                return {
                    'isSuccess': False,
                    'strErr': 'You don\'t have permission to delete project.'
                }

            prj = await self.prj_collection.delete_one({'_id': ObjectId(_id)})
            rawdata = await self.rawdata_collection.delete_one({'_ref_id': ObjectId(_id)})
            openended = await self.openend_collection.delete_one({'_ref_id': ObjectId(_id)})

            if not prj:
                return {
                    'isSuccess': False,
                    'strErr': 'Delete project error'
                }

            if not rawdata:
                return {
                    'isSuccess': False,
                    'strErr': "Delete project's rawdata error"
                }

            if not openended:
                return {
                    'isSuccess': False,
                    'strErr': "Delete project's openended error"
                }

            return {
                'isSuccess': True,
                'strErr': None
            }

        except Exception:
            print(traceback.format_exc())
            return {
                'isSuccess': False,
                'strErr': traceback.format_exc()
            }


    async def retrieve_id(self, _id: str, is_openend: bool = False) -> dict:
        try:

            prj = await self.prj_collection.find_one({'_id': ObjectId(_id)}, {
                '_id': 1,
                'internal_id': 1,
                'name': 1,
                'categorical': 1,
                'type': 1,
                'status': 1,
                'detail': 1,
                'screener': 1,
                'placement': 1,
                'main': 1,
                'topline_exporter': 1
            })

            # prj['codeframes'] = dict()
            #
            # if is_openend:
            #     prj_openend = await self.openend_collection.find_one({'_ref_id': ObjectId(_id)}, {'_id': 1, '_ref_id': 1, 'codeframes': 1})
            #
            #     if prj_openend:
            #         prj['codeframes'] = prj_openend['codeframes']

            prj['is_has_oe_coding'] = 0
            prj['is_has_oe_codelist'] = 0

            prj_openend = await self.openend_collection.find_one({'_ref_id': ObjectId(_id)}, {'_id': 1, '_ref_id': 1, 'oe_info': 1})
            if prj_openend:
                prj['is_has_oe_coding'] = 1 if prj_openend['oe_info'].get('coding') else 0
                prj['is_has_oe_codelist'] = 1 if prj_openend['oe_info'].get('codelist') else 0

            if prj:
                prj = self.prj_info(prj, False)

            return {
                'isSuccess': True,
                'strErr': None,
                'prj': prj
            }

        except Exception:
            print(traceback.format_exc())
            return {
                'isSuccess': False,
                'strErr': traceback.format_exc(),
                'prj': None
            }


    async def update_prj(self, _id: str, strBody: str):

        try:
            prj = await self.prj_collection.find_one({'_id': ObjectId(_id)})

            lstSec = [v['name'] for v in prj['detail']['sections'].values()]

            data = self.body_to_json(strBody, lstSec)

            if len(data) < 1:
                return {
                    'isSuccess': False,
                    'strErr': 'Data is null'
                }

            if prj:

                prj_updated = await self.prj_collection.update_one({'_id': ObjectId(_id)}, {'$set': data})

                if prj_updated:
                    return {
                        'isSuccess': True,
                        'strErr': None
                    }

            return {
                'isSuccess': False,
                'strErr': 'Cannot update'
            }

        except Exception:
            print(traceback.format_exc())
            return {
                'isSuccess': False,
                'strErr': traceback.format_exc()
            }


    @staticmethod
    def body_to_json(strBody: str, lstSec: list):
        re_json = re.compile(r'\{"output".+}')
        str_json = re_json.search(strBody).group()
        dictBody = dict(json.loads(str_json))
        dictBody.pop('output')

        updateData = dict()
        for key, val in dictBody.items():
            if val is not None:

                if str(val).lower() in ['true', 'false']:
                    upVal = True if str(val).lower() == 'true' else False
                else:
                    upVal = val

                if '.' in key:
                    parentKey = str(key).rsplit('.', 1)[0]
                    childKey = str(key).rsplit('.', 1)[1]

                    if 'detail.oe_combine_cols' in parentKey or 'detail.scr_cols' in parentKey \
                            or 'detail.product_cols' in parentKey or 'detail.fc_cols' in parentKey \
                            or 'detail.plm_to_scr_cols' in parentKey or 'detail.plm_to_prod_cols' in parentKey:

                        if isinstance(upVal, list):

                            if parentKey in updateData.keys():
                                updateData[parentKey].update({childKey: upVal})
                            else:
                                updateData = {parentKey: {childKey: upVal}}

                        else:

                            lstKey = str(key).split('.')
                            parentKey2 = '.'.join(lstKey[:2])
                            childKey2 = lstKey[-2]

                            if updateData:

                                if parentKey2 in updateData.keys():
                                    if childKey2 in updateData[parentKey2].keys():
                                        updateData[parentKey2][childKey2].append(upVal)
                                    else:
                                        updateData[parentKey2].update({childKey2: [upVal]})
                                else:
                                    updateData[parentKey2] = {
                                        childKey2: [upVal]
                                    }

                            else:
                                updateData = {
                                    parentKey2:
                                        {
                                            childKey2: [upVal]
                                        }
                                }

                    elif 'detail.addin_vars' in parentKey:

                        lstKey = str(key).rsplit('.')

                        parentKey2 = '.'.join(lstKey[:2])
                        varIdx = lstKey[2]
                        varAtt = lstKey[3]

                        catIdx, catAtt = None, None
                        if len(lstKey) > 4:
                            catIdx = lstKey[4]
                            catAtt = lstKey[5]

                        if parentKey2 not in updateData.keys():
                            updateData[parentKey2] = dict()

                        if varIdx not in updateData[parentKey2].keys():
                            updateData[parentKey2][varIdx] = dict()

                        if varAtt not in updateData[parentKey2][varIdx].keys():
                            updateData[parentKey2][varIdx][varAtt] = dict()

                        if varAtt in ['name', 'lbl']:
                            updateData[parentKey2][varIdx][varAtt] = upVal
                        else:
                            if catIdx not in updateData[parentKey2][varIdx][varAtt].keys():
                                updateData[parentKey2][varIdx][varAtt][catIdx] = dict()

                            updateData[parentKey2][varIdx][varAtt][catIdx][catAtt] = upVal

                    elif 'topline_design.header' in parentKey:
                        lstKey = str(key).rsplit('.')
                        parentKey2 = '.'.join(lstKey[:3])
                        varIdx = lstKey[3]

                        if parentKey2 not in updateData.keys():
                            updateData[parentKey2] = dict()

                        if varIdx not in updateData[parentKey2].keys():
                            updateData[parentKey2][varIdx] = dict()

                        if isinstance(upVal, list):
                            updateData[parentKey2][varIdx] = {
                                'name': upVal[0],
                                'lbl': upVal[1],
                                'hidden_cats': upVal[2],
                                "filter": upVal[3],
                                'run_secs': upVal[4] if len(upVal) == 5 else ','.join(lstSec)
                            }

                        else:
                            varAtt = lstKey[4]
                            updateData[parentKey2][varIdx][varAtt] = upVal

                    elif 'topline_design.side' in parentKey:

                        lstKey = str(key).rsplit('.')
                        parentKey2 = '.'.join(lstKey[:3])
                        varIdx = lstKey[3]

                        if parentKey2 not in updateData.keys():
                            updateData[parentKey2] = dict()

                        if varIdx not in updateData[parentKey2].keys():
                            updateData[parentKey2][varIdx] = {
                                'group_lbl': '',
                                'name': '',
                                'lbl': '',
                                'type': '',
                                't2b': False,
                                'b2b': False,
                                'mean': False,
                                'ma_cats': '',
                                'hidden_cats': '',
                                'is_count': False,
                                'is_corr': False,
                                'is_ua': False
                            }

                        if isinstance(upVal, list):

                            lstAttKey = updateData[parentKey2][varIdx].keys()
                            for idx_AttKey, val_AttKey in enumerate(lstAttKey):

                                if str(upVal[idx_AttKey]).lower() in ['true', 'false']:
                                    newUpVal = True if str(upVal[idx_AttKey]).lower() == 'true' else False
                                else:
                                    newUpVal = upVal[idx_AttKey]

                                updateData[parentKey2][varIdx][val_AttKey] = newUpVal

                        else:
                            varAtt = lstKey[4]
                            updateData[parentKey2][varIdx][varAtt] = upVal

                    elif 'detail.prj_info' in parentKey:
                        parentKey2 = str(key).replace('prj.detail.prj_info', 'detail.prj_info')
                        updateData[parentKey2] = upVal

                    else:
                        updateData[key] = upVal

                else:
                    updateData[key] = upVal

        return updateData


    async def upload_prj_data(self, _id: str, file_scr, file_plm, file_main, file_coding, file_codelist) -> dict:

        try:
            # ----- MA by column ----- NEW FORMAT
            scr_data, scr_qreInfo = self.convert_by_mc(file_scr)

            plm_data, plm_qreInfo = {}, {}
            if file_plm is not None:
                if file_plm.filename:
                    plm_data, plm_qreInfo = self.convert_by_mc(file_plm)

            main_data, main_qreInfo = {}, {}
            if file_main.filename:
                main_data, main_qreInfo = self.convert_by_mc(file_main)

            data = {
                'screener': {
                    'data': scr_data,
                    'qreInfo': scr_qreInfo
                },
                'placement': {
                    'data': plm_data,
                    'qreInfo': plm_qreInfo
                },
                'main': {
                    'data': main_data,
                    'qreInfo': main_qreInfo
                }
            }

            if not scr_data:
                return {
                    'isSuccess': False,
                    'strErr': 'Data screener is null'
                }

            prj_rawdata = await self.rawdata_collection.find_one({'_ref_id': ObjectId(_id)}, {'_id': 1, '_ref_id': 1})

            if not prj_rawdata:
                new_rawdata = new_rawdata_template
                new_rawdata['_id'] = ObjectId()
                new_rawdata['_ref_id'] = ObjectId(_id)
                new_rawdata['screener'] = data['screener']
                new_rawdata['placement'] = data['placement']
                new_rawdata['main'] = data['main']
                upload_prj_rawdata = await self.rawdata_collection.insert_one(new_rawdata)
            else:
                upload_prj_rawdata = await self.rawdata_collection.update_one({'_ref_id': ObjectId(_id)}, {'$set': data})

            dict_upload_info = {
                'screener': len(data['screener']['data']),
                'placement': len(data['placement']['data']),
                'main': len(data['main']['data']),
                'has_oe': 0,
            }

            if file_coding.filename and file_codelist.filename:
                dict_upload_info['has_oe'] = 1

            upload_prj = await self.prj_collection.update_one({'_id': ObjectId(_id)}, {
                    '$set': dict_upload_info
                }
            )

            if not upload_prj_rawdata or not upload_prj:
                return {
                    'isSuccess': False,
                    'strErr': 'Upload unsuccessfully'
                }


            # Addin to process OE (11/4/2023)---------------------------------------------------------------------------
            if file_coding.filename and file_codelist.filename:

                dict_oe_info = {
                    # store oe info from file_coding and file_codelist
                    'codelist': file_codelist.file.read(),
                    'coding': file_coding.file.read(),
                }

                prj_openend = await self.openend_collection.find_one({'_ref_id': ObjectId(_id)}, {'_id': 1, '_ref_id': 1})

                if not prj_openend:
                    new_openend = new_openend_template
                    new_openend['_id'] = ObjectId()
                    new_openend['_ref_id'] = ObjectId(_id)
                    new_openend['oe_info'] = dict_oe_info
                    upload_prj_openend = await self.openend_collection.insert_one(new_openend)
                else:
                    upload_prj_openend = await self.openend_collection.update_one({'_ref_id': ObjectId(_id)}, {'$set': {'oe_info': dict_oe_info}})

                if not upload_prj_openend:
                    return {
                        'isSuccess': False,
                        'strErr': 'Upload coding/codelist unsuccessfully'
                    }
            # End Addin to process OE (11/4/2023)-----------------------------------------------------------------------

            return {
                'isSuccess': True,
                'strErr': 'Upload successfully'
            }

        except Exception:
            print(traceback.format_exc())
            return {
                'isSuccess': False,
                'strErr': traceback.format_exc()
            }


    async def clear_prj_data(self, _id: str):

        try:
            clear_prj_countData = await self.prj_collection.update_one(
                {'_id': ObjectId(_id)},
                {'$set': {'screener': 0, 'placement': 0, 'main': 0, 'has_oe': 0}}
            )

            clear_prj_rawdata = await self.rawdata_collection.update_one(
                {'_ref_id': ObjectId(_id)},
                {'$set': {'screener': {}, 'placement': {}, 'main': {}, 'topline_structure': {}, 'unstack_data': {}}}
            )

            clear_prj_oe = await self.openend_collection.update_one(
                {'_ref_id': ObjectId(_id)},
                {'$set': {'oe_info': {}}}
            )

            if not clear_prj_rawdata or not clear_prj_countData or not clear_prj_oe:
                return {
                    'isSuccess': False,
                    'strErr': 'Clear unsuccessfully'
                }

            return {
                'isSuccess': True,
                'strErr': 'Clear successfully'
            }

        except Exception:
            print(traceback.format_exc())
            return {
                'isSuccess': False,
                'strErr': traceback.format_exc()
            }


    async def data_export(self, _id: str, export_section: str, export_option: str) -> dict:

        try:
            st = time.process_time()

            # RUNNING STATUS
            await self.prj_collection.update_one({'_id': ObjectId(_id)}, {'$set': {'rawdata_exporter': {
                'isRunning': True,
                'isSuccess': False,
                'strErr': None,
                'zipName': None
            }}})

            prj = await self.prj_collection.find_one({'_id': ObjectId(_id)})
            rawdata = await self.rawdata_collection.find_one({'_ref_id': ObjectId(_id)}, {
                'screener': 1,
                'placement': 1,
                'main': 1})

            prj['screener'] = rawdata['screener']

            if prj['type'] == 'HUT':
                prj['placement'] = rawdata['placement']
            else:
                prj['placement'] = {}

            prj['main'] = rawdata['main']

            openend = await self.openend_collection.find_one({'_ref_id': ObjectId(_id)}, {'oe_info': 1})
            if openend:
                prj['oe_info'] = openend['oe_info']
            else:
                prj['oe_info'] = {
                    "coding": "",
                    "codelist": "",
                }

            self.init_data_exporter_variables(prj=prj, export_section=export_section, export_option=export_option)

            if export_option in ['codeframe']:
                isSuccess = self.export_data(is_export_raw=False, is_export_stacked=True, is_export_unstacked=False, is_fc_yn=False, is_export_sav=False)
            elif export_option in ['screener_only']:
                isSuccess = self.export_data(is_export_raw=False, is_export_stacked=True, is_export_unstacked=False, is_fc_yn=False, is_export_sav=True)
            else:
                isSuccess = self.export_data(is_fc_yn=False)

                # UPDATE ON 12/7/2023
                print('UPLOAD DATA UNSTACK TO MONGO DB')
                await self.rawdata_collection.update_one({'_ref_id': ObjectId(_id)}, {'$set': {'unstack_data': {
                    f'{export_section}': {
                        'df_data': self.df_data_unstacked.to_json(),
                        'df_info': self.df_info_unstacked.to_json()
                    }
                }}})

                await self.prj_collection.update_one({'_id': ObjectId(_id)}, {'$set': {'topline_exporter': {}}})


            if not isSuccess[0]:
                await self.prj_collection.update_one({'_id': ObjectId(_id)}, {'$set': {'rawdata_exporter': {
                    'isRunning': False,
                    'isSuccess': False,
                    'strErr': isSuccess[1],
                    'zipName': None
                }}})

                return {
                    'isSuccess': False,
                    'strErr': isSuccess[1],
                    'zipName': None
                }

            et = time.process_time()
            print('EXPORT DATA - TIME:', et - st, 'seconds')

            await self.prj_collection.update_one({'_id': ObjectId(_id)}, {'$set': {'rawdata_exporter': {
                'isRunning': False,
                'isSuccess': True,
                'strErr': None,
                'zipName': self.zip_name
            }}})

            return {
                'isSuccess': True,
                'strErr': None,
                'zipName': self.zip_name
            }

        except Exception:
            print(traceback.format_exc())

            await self.prj_collection.update_one({'_id': ObjectId(_id)}, {'$set': {'rawdata_exporter': {
                'isRunning': False,
                'isSuccess': False,
                'strErr': traceback.format_exc(),
                'zipName': None
            }}})

            return {
                'isSuccess': False,
                'strErr': traceback.format_exc(),
                'zipName': None
            }


    async def get_rawdata_exporter_stt(self, _id: str) -> dict:
        try:
            dict_rawdata_exporter = await self.prj_collection.find_one({'_id': ObjectId(_id)}, {'rawdata_exporter': 1})
            return dict_rawdata_exporter
        except Exception:
            print(traceback.format_exc())
            return {
                'isRunning': False,
                'isSuccess': False,
                'strErr': traceback.format_exc(),
                'zipName': None
            }


    async def clear_rawdata_exporter_stt(self, _id: str) -> dict:
        try:
            await self.prj_collection.update_one({'_id': ObjectId(_id)}, {'$set': {'rawdata_exporter': {
                'isRunning': None,
                'isSuccess': None,
                'strErr': None,
                'zipName': None
            }}})
            return {}
        except Exception:
            print(traceback.format_exc())
            return {
                'isRunning': False,
                'isSuccess': False,
                'strErr': traceback.format_exc(),
                'zipName': None
            }


    async def topline_process(self, _id: str, export_section: str, export_sheets: list):
        # NEW FORMAT----------------------------------------------------------------------------------------------------
        try:
            st = time.process_time()

            prj = await self.prj_collection.find_one({'_id': ObjectId(_id)}, {
                '_id': 1,
                'internal_id': 1,
                'name': 1,
                'categorical': 1,
                'type': 1,
                'status': 1,
                'detail': 1,
                'screener': 1,
                'placement': 1,
                'main': 1,
                'topline_exporter': 1,
                'has_oe': 1
            })

            rawdata = await self.rawdata_collection.find_one({'_ref_id': ObjectId(_id)}, {
                'screener': 1,
                'placement': 1,
                'main': 1
            })

            prj['screener'] = rawdata['screener']
            prj['placement'] = rawdata['placement']
            prj['main'] = rawdata['main']

            openend = await self.openend_collection.find_one({'_ref_id': ObjectId(_id)}, {'oe_info': 1})
            if openend:
                prj['oe_info'] = openend['oe_info']
            else:
                prj['oe_info'] = {
                    'coding': "",
                    'codelist': ""
                }

            # OLD
            # self.init_data_exporter_variables(prj=prj, export_section=export_section)
            # isSuccess = self.export_data(is_export_raw=False, is_export_stacked=False, is_export_unstacked=True, is_fc_yn=True, is_export_sav=False)
            # if not isSuccess[0]:
            #     return {
            #         'isSuccess': False,
            #         'strErr': isSuccess[1]
            #     }

            # NEW ON 12/07/2023
            print('HERE - topline_process')
            rawdata_collection = await self.rawdata_collection.find_one({'_ref_id': ObjectId(_id)}, {'unstack_data': 1})
            unstack_data = rawdata_collection.get('unstack_data').get(export_section)

            if not unstack_data:
                print("Please export rawdata first")

                topline_exporter_data = {
                    export_section: {
                        'Ttest': False,
                        'errLbl': "Please export rawdata first"
                    }
                }

                await self.prj_collection.update_one({'_id': ObjectId(_id)}, {'$set': {'topline_exporter': topline_exporter_data}})

                return {
                    'isSuccess': False,
                    'strErr': topline_exporter_data.get(export_section).get('errLbl')
                }

            await self.rawdata_collection.update_one({'_ref_id': ObjectId(_id)}, {'$set': {'unstack_data': {f'{export_section}': {}}}})

            self.df_data_unstacked = pd.DataFrame.from_dict(json.loads(unstack_data['df_data']))
            self.df_info_unstacked = pd.DataFrame.from_dict(json.loads(unstack_data['df_info']))

            obj_section = prj['detail']['sections'][export_section]
            self.lst_force_choice = obj_section['force_choice']['qres'][0].split('|')

            self.df_data_unstacked, self.df_info_unstacked = self.addin_yn_fc(self.df_data_unstacked, self.df_info_unstacked)

            if self.prj['has_oe']:
                df_oe_codelist = pd.read_csv(io.BytesIO(self.prj['oe_info']['codelist']))

                for idx in df_oe_codelist.index:
                    oe_name, oe_num_col = df_oe_codelist.at[idx, 'COL_NAME'].rsplit('|', 1)

                    dict_val_lbl = eval(df_oe_codelist.at[idx, 'CODELIST'])

                    for idx2 in self.df_info_unstacked.loc[(self.df_info_unstacked['var_name'].str.contains(oe_name) & self.df_info_unstacked['var_type'].str.contains('MA')), :].index:
                        self.df_info_unstacked.at[idx2, 'val_lbl'] = dict_val_lbl

            self.convert_df_info_unstacked_val_lbl()
            # ----------------------------------------------------------------------------------------------------------



            self.init_topline_exporter_variables(prj=prj, df_data=self.df_data_unstacked, df_info=self.df_info_unstacked, export_section=export_section)

            # msn_topline_exporter = ToplineExporterV2(prj, df_data=self.df_data_unstacked, df_info=self.df_info_unstacked, export_section=export_section)
            # isSuccess = msn_topline_exporter.process_topline()

            isSuccess = self.process_topline()

            if not isSuccess[0]:

                topline_exporter_data = {
                    export_section: {
                        'Ttest': False,
                        'errLbl': isSuccess[1]
                    }
                }

                upload_prj_Ttest_UA = await self.prj_collection.update_one(
                    {'_id': ObjectId(_id)}, {'$set': {
                        'topline_exporter': topline_exporter_data
                    }
                    }
                )

                if not upload_prj_Ttest_UA:
                    return {
                        'isSuccess': False,
                        'strErr': 'Upload Test & UA failed'
                    }

                return {
                    'isSuccess': False,
                    'strErr': isSuccess[1]
                }


            # isSuccess = msn_topline_exporter.generate_topline_excel(export_sheets)
            isSuccess = self.generate_topline_excel(export_sheets)

            if not isSuccess[0]:

                topline_exporter_data = {
                    export_section: {
                        'Ttest': False,
                        'errLbl': isSuccess[1]
                    }
                }

                upload_prj_Ttest_UA = await self.prj_collection.update_one(
                    {'_id': ObjectId(_id)}, {'$set': {
                        'topline_exporter': topline_exporter_data
                    }
                    }
                )

                if not upload_prj_Ttest_UA:
                    return {
                        'isSuccess': False,
                        'strErr': 'Upload Test & UA failed'
                    }

                return {
                    'isSuccess': False,
                    'strErr': isSuccess[1]
                }

            await self.prj_collection.update_one(
                {'_id': ObjectId(_id)}, {'$set': {
                    'topline_exporter': {"file_name": self.topline_name}
                }
                }
            )

            et = time.process_time()
            print('Topline process time:', et - st, 'seconds')

            return {
                'isSuccess': True,
                'strErr': None
            }

        except Exception:
            print(traceback.format_exc())

            topline_exporter_data = {
                export_section: {
                    'Ttest': False,
                    'errLbl': traceback.format_exc()
                }
            }

            await self.prj_collection.update_one(
                {'_id': ObjectId(_id)}, {'$set': {
                    'topline_exporter': topline_exporter_data
                }
                }
            )

            return {
                'isSuccess': False,
                'strErr': traceback.format_exc()
            }


    async def clear_topline_info(self, _id: str):

        await self.prj_collection.update_one(
            {'_id': ObjectId(_id)}, {'$set': {
                'topline_exporter': {}
            }
            }
        )

        await self.rawdata_collection.update_one(
            {'_ref_id': ObjectId(_id)}, {'$set': {
                'topline_structure': {}
            }
            }
        )


    async def add_user(self, username, useremail, password):
        try:

            lstUser = list()
            async for usr in self.user_collection.find({'email': useremail}, {'email': 1}):
                lstUser.append(usr)

            if lstUser:
                return {
                    'isSuccess': False,
                    'strErr': 'Email is already registered.'
                }

            new_user = new_user_template
            new_user['_id'] = ObjectId()
            new_user['email'] = useremail
            new_user['password'] = password
            new_user['name'] = username
            new_user['create_at'] = datetime.now()
            new_user['login_at'] = datetime.now()

            user = await self.user_collection.insert_one(new_user)

            if not user:
                return {
                    'isSuccess': False,
                    'strErr': 'Add user error'
                }

            return {
                'isSuccess': True,
                'strErr': None
            }

        except Exception:
            print(traceback.format_exc())
            return {
                'isSuccess': False,
                'strErr': traceback.format_exc()
            }


    async def topline_bulkup_export(self, _id: str, strWhich: str):

        try:
            prj = await self.prj_collection.find_one({'_id': ObjectId(_id)}, {
                '_id': 1,
                'internal_id': 1,
                'name': 1,
                'detail': 1
            })

            csv_name = f"{prj['name']}_bulkup_{strWhich}.csv"
            df = pd.DataFrame.from_dict(prj['detail']['topline_design'][strWhich]).T

            df.to_csv(csv_name, encoding='utf-8-sig', index=False)

            return {
                'isSuccess': True,
                'strErr': None,
                'csvName': csv_name
            }

        except Exception:
            print(traceback.format_exc())
            return {
                'isSuccess': False,
                'strErr': traceback.format_exc(),
                'csvName': None
            }


    async def topline_side_auto_generate(self, _id: str):

        try:
            prj = await self.prj_collection.find_one({'_id': ObjectId(_id)}, {
                '_id': 1,
                'detail': 1
            })

            if not prj:
                return {
                    'isSuccess': False,
                    'strErr': 'Topline side-axis auto generate fail'
                }

            topline_side_axis = self.auto_generate_topline_side_axis(prj=prj)

            # tl_design = ToplineDesign(prj)
            # topline_side_axis = tl_design.auto_generate_topline_side_axis()

            await self.prj_collection.update_one({'_id': ObjectId(_id)}, {'$set': {'detail.topline_design.side': topline_side_axis}})

            return {
                'isSuccess': True,
                'strErr': 'Topline side-axis auto generate successfully'
            }

        except Exception:
            print(traceback.format_exc())
            return {
                'isSuccess': False,
                'strErr': traceback.format_exc()
            }


    async def create_codeframe(self, _id: str, strBody: str):

        try:

            # Convert body string to dictionary
            re_json = re.compile(r'\{"output".+}')
            str_json = re_json.search(strBody).group()
            dictBody = dict(json.loads(str_json))
            dictBody.pop('output')

            # Restructure dictionary to upload to db
            dictBody_restructure = {'codeframes': dict()}
            for key, val in dictBody.items():

                str_key = str(key).replace('codeframes.', '')
                lst_key = str_key.split('.')

                str_dict_item = str_key.replace(".", "':{'")
                str_dict_item = '{' + f"'{str_dict_item}':'{val}'{'}' * len(lst_key)}"
                dict_item = eval(str_dict_item)

                for key2, val2 in dict_item.items():
                    if key2 in dictBody_restructure['codeframes'].keys():
                        dictBody_restructure['codeframes'][key2].update(val2)
                    else:
                        dictBody_restructure['codeframes'].update({key2: val2})


            # Upload codeframe to bd
            prj_openend = await self.openend_collection.find_one({'_ref_id': ObjectId(_id)}, {'_id': 1, '_ref_id': 1})

            if not prj_openend:
                new_openend = new_openend_template
                new_openend['_id'] = ObjectId()
                new_openend['_ref_id'] = ObjectId(_id)
                new_openend['codeframes'] = dictBody_restructure['codeframes']
                upload_prj_openend = await self.openend_collection.insert_one(new_openend)
            else:
                upload_prj_openend = await self.openend_collection.update_one({'_ref_id': ObjectId(_id)}, {'$set': dictBody_restructure})

            if upload_prj_openend:
                return {
                    'isSuccess': True,
                    'strErr': 'Upload openend successfully'
                }
            else:
                return {
                    'isSuccess': False,
                    'strErr': 'Upload openend unsuccessfully'
                }

        except Exception:
            print(traceback.format_exc())
            return {
                'isSuccess': False,
                'strErr': traceback.format_exc()
            }



    # START ON 07/07/2023
    # NEW---------------------------------------------------------------------------------------------------------------


    async def get_msn_prj_info(self, _id: str) -> dict:

        msn_prj_info = await self.prj_collection.find_one({'_id': ObjectId(_id)}, {
            '_id': 1,
            'internal_id': 1,
            'name': 1,
            'categorical': 1,
            'type': 1,
            'status': 1,
            'create_date': 1,
            'detail.prj_info': 1,
        })

        model_msn_prj_info = ModelMsnPrjInfo()
        model_msn_prj_info.set_prj_info(msn_prj_info)

        return model_msn_prj_info.dict()


    async def put_msn_prj_info(self, _id: str, model_msn_prj_info: ModelMsnPrjInfo) -> dict:

        dict_update = model_msn_prj_info.dict()

        for k, v in dict_update.get('detail_prj_info').items():
            dict_update.update({f'detail.prj_info.{k}.val': v})

        dict_update.pop('create_date')
        dict_update.pop('detail_prj_info')

        await self.prj_collection.update_one({'_id': ObjectId(_id)}, {'$set': dict_update})

        return {
            'is_success': True,
            'stt': 'Update info successful!'
        }


    async def get_msn_prj_sections(self, _id: str) -> dict:

        msn_prj_sections = await self.prj_collection.find_one({'_id': ObjectId(_id)}, {
            '_id': 1,
            'type': 1,
            'detail.join_col': 1,
            'detail.order_col': 1,
            'detail.sections': 1,
        })

        model_msn_prj_sections = ModelMsnPrjSections()
        model_msn_prj_sections.set_prj_sections(msn_prj_sections)

        return model_msn_prj_sections.dict() | {'type': msn_prj_sections['type']}


    async def put_msn_prj_sections(self, _id: str, model_msn_prj_sections: ModelMsnPrjSections) -> dict:

        try:

            dict_update = model_msn_prj_sections.dict()

            dict_sections = dict()
            for key, val in dict_update['sections'].items():
                dot = dotty()
                str_key = f'SEC@{str(key)}'.replace('.0', '.cat@0').replace('.1', '.cat@1').replace('.2', '.cat@2')
                dot[str_key] = str(val)
                merge_dict(dict_sections, dot.to_dict())

            for key, val in dict_sections.copy().items():
                key_new = key.replace('SEC@', '')
                val_new = val.copy()

                # ROTATION----------------------------------------------------------------------------------------------
                val_new['rotation']['qres'] = [val_new['rotation']['qres']]

                for k_cat, v_cat in val_new['rotation']['cats'].copy().items():
                    val_new['rotation']['cats'].update({k_cat.replace('cat@', ''): list(v_cat.values())})
                    val_new['rotation']['cats'].pop(k_cat)
                # END ROTATION------------------------------------------------------------------------------------------

                # PRODUCT-----------------------------------------------------------------------------------------------
                for k_cat, v_cat in val_new['product']['cats'].copy().items():
                    val_new['product']['cats'].update({k_cat.replace('cat@', ''): list(v_cat.values())})
                    val_new['product']['cats'].pop(k_cat)

                lst_prod_qres = list()
                for v_qre in val_new['product']['qres'].copy().values():
                    lst_prod_qres.append('|'.join(v_qre.values()))

                val_new['product']['qres'] = lst_prod_qres
                # END PRODUCT-------------------------------------------------------------------------------------------

                # FORCE_CHOICE------------------------------------------------------------------------------------------
                val_new['force_choice']['qres'] = ['|'.join(val_new['force_choice']['qres']['cat@0'].values())]
                # END FORCE_CHOICE--------------------------------------------------------------------------------------

                dict_sections.update({key_new: val_new})
                dict_sections.pop(key)

            dict_update['sections'] = dict_sections

            dict_update_mongodb = dict()
            for k, v in dict_update.items():
                dict_update_mongodb[f'detail.{k}'] = v

            await self.prj_collection.update_one({'_id': ObjectId(_id)}, {'$set': dict_update_mongodb})

            return {
                'is_success': True,
                'stt': 'Update section successful!'
            }

        except Exception:
            print(traceback.format_exc())
            return {
                'is_success': False,
                'stt': f'<b>Update section fail!</b><br/>{traceback.format_exc()}'
            }



    async def get_msn_prj_structure(self, _id: str, tab_key: int) -> dict:

        dict_structure = {
            1: 'oe_combine_cols',
            2: 'scr_cols',
            3: 'plm_to_scr_cols',
            4: 'plm_to_prod_cols',
            5: 'product_cols',
            6: 'fc_cols',
        }

        msn_prj_structure = await self.prj_collection.find_one({'_id': ObjectId(_id)}, {
            '_id': 1,
            'type': 1,
            f'detail.{dict_structure[tab_key]}': 1,
        })

        return {
            'tab_name': dict_structure.get(tab_key),
            'type': msn_prj_structure['type'],
            'obj_data': msn_prj_structure['detail'][dict_structure[tab_key]]
        }
    # ------------------------------------------------------------------------------------------------------------------