from .MSN_Addin_Variables_v2 import AddinVariables
from .MSN_Topline_To_Excel import ToplineToExcel
from .MSN_Topline_Design import ToplineDesign

from scipy import stats
import pandas as pd
import numpy as np
import traceback
import time


# from .MSN_Data_Table import DataTable


class ToplineExporterV2(AddinVariables, ToplineToExcel, ToplineDesign):

    def __init__(self, prj: dict, df_data: pd.DataFrame, df_info: pd.DataFrame, export_section: str):

        self.prj = prj
        self.obj_section = self.prj['detail']['sections'][export_section]
        self.obj_prj_info = self.prj['detail']['prj_info']

        self.export_section_name = self.obj_section['name']

        dict_cats = self.obj_section['product']['cats']
        self.dict_prod_cats = {str(k): v[0] for k, v in dict_cats.items()}

        self.isJR3Factors = self.prj['detail']['topline_design']['is_jar_scale_3']

        self.dictHeader, self.dictSide = dict(), dict()

        # init Class AddinVariables
        # lstProductCode = [int(i) for i in list(self.dict_prod_cats.values())] OLD
        lstProductCode = [i for i in list(self.dict_prod_cats.values())]
        AddinVariables.__init__(self, prj_addVars=self.prj['detail']['addin_vars'],
                                df_data=df_data, df_info=df_info, lstProductCode=lstProductCode)

        # init Class ToplineToExcel
        ToplineToExcel.__init__(self,
                                topline_name=f"{self.prj['internal_id']}_{self.prj['name']}_{self.export_section_name}_Topline.xlsx",
                                topline_title=f"{self.prj['name']}_{self.export_section_name}",
                                is_display_pct_sign=self.prj['detail']['topline_design']['is_display_pct_sign'],
                                is_jar_scale_3=self.prj['detail']['topline_design']['is_jar_scale_3'],
                                dictTtest=dict(), dictUA=dict(), dictOE=dict())

        # init Class ToplineDesign
        ToplineDesign.__init__(self)

    def process_topline(self):

        try:
            isSuccess = self.get_topline_info()

            if not isSuccess[0]:
                return isSuccess

            self.generate_topline_container()

            return True, None

        except Exception:
            print(traceback.format_exc())
            return False, traceback.format_exc()


    def get_topline_info(self):

        try:
            # COMPUTE NEW VARS------------------------------------------------------------------------------------------
            print('COMPUTE NEW VARS')
            df_info = self.df_info.copy()
            df_info['idx_var_name'] = df_info.loc[:, ['var_name']]
            df_info.set_index('idx_var_name', inplace=True)

            isSuccess = self.addin_vars()
            if not isSuccess[0]:
                return isSuccess

            # END COMPUTE NEW VARS--------------------------------------------------------------------------------------

            # TOPLINE'S HEADER------------------------------------------------------------------------------------------
            print('GENERATE TOPLINE HEADER')
            for key, val in self.prj['detail']['topline_design']['header'].items():

                excl_hder = str(val['hidden_cats']).split('|') if len(val['hidden_cats']) > 0 else []
                excl_hder = list(map(int, excl_hder))

                self.dictHeader[str(key)] = {
                    'qre': val['name'],
                    'lbl': val['lbl'],
                    'excl': excl_hder,
                    'filter': val['filter'],
                    'run_secs': val['run_secs'].split(',') if '|' not in val['run_secs'] else val['run_secs'].split('|'),
                }
            # END TOPLINE'S HEADER--------------------------------------------------------------------------------------

            # TOPLINE'S SIDE AXIS---------------------------------------------------------------------------------------
            print('GENERATE TOPLINE SIDE AXIS')
            for key, val in self.prj['detail']['topline_design']['side'].items():
                stt = str(key)

                self.dictSide[stt] = {
                    'groupLbl': val['group_lbl'],
                    'qre': val['name'],
                    'qreLbl': val['lbl'],
                    'type': val['type'],
                    'atts': list(),
                    'MACats': list(),
                    'excl': list(),  # hidden cats
                    'isCount': val['is_count'],
                    'isCorr': val['is_corr'],
                    'isUA': val['is_ua'],
                    'qres': list(),
                }

                if val['type'] in ['MA']:
                    lst_ma_cats = str(val['ma_cats']).split('|') if len(val['ma_cats']) > 0 else []
                    lst_ma_cats = list(map(int, lst_ma_cats))
                    self.dictSide[stt]['MACats'] = lst_ma_cats


                excl = str(val['hidden_cats']).split('|') if len(val['hidden_cats']) > 0 else []


                excl = list(map(int, excl))
                self.dictSide[stt]['excl'] = excl

                for item in ['T2B', 'B2B', 'Mean']:
                    if val[str(item).lower()]:
                        self.dictSide[stt]['atts'].append(item)

                if self.dictSide[stt]['isUA']:
                    self.dictSide[stt]['qres'] = [self.dictSide[stt]['qre']]
                else:
                    self.dictSide[stt]['qres'] = [
                        f"{self.dictSide[stt]['qre']}_{self.dict_prod_cats['1']}", # prod code with value = 1
                        f"{self.dictSide[stt]['qre']}_{self.dict_prod_cats['2']}"  # prod code with value = 2
                    ]
            # TOPLINE'S SIDE AXIS---------------------------------------------------------------------------------------

            return True, None

        except Exception:
            print(traceback.format_exc())
            return False, traceback.format_exc()


    def generate_topline_container(self):

        print('GENERATE TOPLINE CONTAINER')
        st = time.process_time()

        df_data = self.df_data.copy()
        df_info = self.df_info.copy()

        dictTtest = dict()
        dictUA = dict()
        dictOE = dict()

        for hd_key, hd_val in self.dictHeader.items():

            print(f"Formatting - {hd_val['qre']}")

            if hd_val['qre'] in ['Total', 'TOTAL']:

                strSubGroupName = hd_val['qre']
                dfFil = df_data.copy()

                dictTtest[strSubGroupName] = {'subGroupLbl': hd_val['lbl'], 'subCodeLbl': hd_val['lbl'], 'sideQres': {}}

                dictUA[strSubGroupName] = {'subGroupLbl': hd_val['lbl'], 'subCodeLbl': hd_val['lbl'], 'sideQres': {}}

                dictOE[strSubGroupName] = {'subGroupLbl': hd_val['lbl'], 'subCodeLbl': hd_val['lbl'], 'sideQres': {}}

                dictTtest, dictUA, dictOE = self.pull_data_to_container(dictTtest, dictUA, dictOE, strSubGroupName, dfFil)

            else:

                if self.export_section_name in hd_val['run_secs']:

                    if '$' in hd_val['qre']:
                        str_hd_val_qre = f"{hd_val['qre'][1:]}_1"
                    else:
                        str_hd_val_qre = hd_val['qre']

                    dict_val_lbl_temp = df_info.loc[df_info['var_name'] == str_hd_val_qre, ['val_lbl']].values[0, 0]

                    dict_val_lbl = dict()

                    if hd_val['excl']:
                        for k, v in dict_val_lbl_temp.items():
                            if k not in hd_val['excl']:
                                dict_val_lbl[k] = v
                    else:
                        dict_val_lbl = dict_val_lbl_temp

                    for cat, catLbl in dict_val_lbl.items():

                        # strSubGroupName = f"{hd_val['qre']}_{int(cat)}"
                        strSubGroupName = f"{hd_val['lbl']}_{int(cat)}"

                        if '$' in hd_val['qre']:

                            lst_col_ma = df_info.loc[df_info['var_name'].str.contains(f"{hd_val['qre'][1:]}_[0-9]+"), 'var_name'].copy().values.tolist()
                            lst_col_ma = [f"{i} == {cat}" for i in lst_col_ma]
                            str_query_ma = ' | '.join(lst_col_ma)
                            dfFil = df_data.query(str_query_ma).copy()

                        else:
                            dfFil = df_data.loc[df_data[hd_val['qre']] == cat, :].copy()


                        if hd_val['filter'] != 'FULL':
                            dfFil = dfFil.query(hd_val['filter'])

                        dictTtest[strSubGroupName] = {'subGroupLbl': hd_val['lbl'], 'subCodeLbl': catLbl, 'sideQres': {}}

                        dictUA[strSubGroupName] = {'subGroupLbl': hd_val['lbl'], 'subCodeLbl': catLbl, 'sideQres': {}}

                        dictOE[strSubGroupName] = {'subGroupLbl': hd_val['lbl'], 'subCodeLbl': catLbl, 'sideQres': {}}

                        dictTtest, dictUA, dictOE = self.pull_data_to_container(dictTtest, dictUA, dictOE, strSubGroupName, dfFil)


        # Final Result container
        self.dictTtest = dictTtest
        self.dictUA = dictUA
        self.dictOE = dictOE

        et = time.process_time()
        print('GENERATED TOPLINE CONTAINER - TIME:', et - st, 'seconds')


    def pull_data_to_container(self, dictTtest: dict, dictUA: dict, dictOE: dict, strSubGroupName: str, dfFil: pd.DataFrame):

        for key, val in self.dictSide.items():

            if not val['isUA']:

                if '_OE' in val['qre'] and val['type'] == 'MA':
                    if self.prj['has_oe']:
                        dictOE[strSubGroupName]['sideQres'][key] = {
                            'groupLbl': val['groupLbl'],
                            'qreLbl': val['qreLbl'],
                            'type': val['type'],
                            'isCount': val['isCount'],
                            'prod_cats': self.dict_prod_cats,
                            'result': self.pull_data(dfFil, val),
                        }
                else:
                    dictTtest[strSubGroupName]['sideQres'][key] = {
                        'groupLbl': val['groupLbl'],
                        'qreLbl': val['qreLbl'],
                        'type': val['type'],
                        'isCount': val['isCount'],
                        'prod_cats': self.dict_prod_cats,
                        'result': self.pull_data(dfFil, val),
                    }

            else:

                dictUA[strSubGroupName]['sideQres'][key] = {
                    'qre': val['qre'],
                    'groupLbl': val['groupLbl'],
                    'qreLbl': val['qreLbl'],
                    'type': val['type'],
                    'isCount': val['isCount'],
                    'result': self.pull_data(dfFil, val),
                }

        return dictTtest, dictUA, dictOE


    def pull_data(self, df: pd.DataFrame, val: dict):

        df = df.copy()
        qType = val['type']
        dict_result = dict()

        lst_qres_new = list()
        for i in val['qres']:
            lst_qres_new.extend([str(i).replace('|', '_')])

        val['qres'] = lst_qres_new

        if qType == 'MA':
            dict_result = self.pull_data_ma(df, dict_result, val)

        elif qType == 'NUM':
            dict_result = self.pull_data_num(df, dict_result, val)

        else:  # SA OL JR
            dict_result = self.pull_data_single_qre(df, dict_result, val)

        return dict_result


    def pull_data_ma(self, df: pd.DataFrame, dict_result: dict, val: dict):

        qres, atts, excl, qType, isCount = val['qres'], val['atts'], val['excl'], val['type'], val['isCount']
        df_info = self.df_info.copy()
        df_info['idx_by_var_name'] = df_info['var_name']
        df_info.set_index('idx_by_var_name', inplace=True)

        # Get MA columns name
        # Apr 12 2023---------------------------------------------------------------------------------------------------
        dict_val_lbl = df_info.loc[f'{qres[0]}_1', ['val_lbl']].values.tolist()[0]
        dict_qres_name = {qre: df_info.loc[df_info['var_name'].str.contains(f'{qre}_[0-9]+'), 'var_name'].values.tolist() for qre in qres}

        del df_info

        for idx_qre, qre in enumerate(qres):
            # lst_qres_name = df_info.loc[df_info['var_name'].str.contains(f'{qre}_[0-9]+'), 'var_name'].values.tolist()

            lst_qres_name = dict_qres_name[qre]

            total = int(df[lst_qres_name[0]].count())

            # Base MA
            if 'Base' not in excl:

                if 'base' not in dict_result.keys():
                    dict_result.update({'base': {
                        'catLbl': 'Base',
                        f'val{idx_qre}': total,
                        f'sig{idx_qre}': 0,
                    }})
                else:
                    dict_result['base'].update({
                        f'val{idx_qre}': total,
                        f'sig{idx_qre}': 0,
                    })

            # Categories MA
            df_val_count = df[lst_qres_name].melt(value_name=qre).loc[:, [qre]].apply(pd.value_counts)
            for cat, lbl in dict_val_lbl.items():

                if '|' in str(cat):
                    lst_net_info = cat.upper().split('|')

                    if str(cat) not in dict_result.keys():
                        dict_result.update({str(cat): {
                            'catLbl': f'{lst_net_info[-1]}({lst_net_info[-2]})',
                            f'val{idx_qre}': 0,
                            f'sig{idx_qre}': 0,
                        }})
                    else:
                        dict_result[str(cat)].update({
                            f'val{idx_qre}': 0,
                            f'sig{idx_qre}': 0,
                        })

                    df_net_count = df[lst_qres_name]
                    df_net_count = df_net_count.isin(list(lbl.keys()))
                    df_net_count = df_net_count[df_net_count.any(axis=1)]
                    net_val = int(df_net_count.shape[0])

                    if not isCount:
                        net_val = net_val / total if total > 0 else 0

                    dict_result[str(cat)][f'val{idx_qre}'] = net_val

                    if idx_qre > 0 and not isCount and total >= 30 and '_OE_' in qres[0]:
                        dict_result = self.cal_ma_and_sig(dict_result, cat, dict_val_lbl, dict_qres_name, qres, df, lst_sub_cat=list(lbl.keys()))


                    if lst_net_info[-2] == 'NET':
                        for cat2, val2 in lbl.items():
                            dict_result = self.cal_ma_cat(dict_result, df_val_count, isCount, total, qre, idx_qre, cat2, val2)

                            if idx_qre > 0 and not isCount and total >= 30 and '_OE_' in qres[0]:
                                dict_result = self.cal_ma_and_sig(dict_result, cat2, dict_val_lbl, dict_qres_name, qres, df, lst_sub_cat=[cat2])


                else:
                    if cat == 99999:
                        continue

                    dict_result = self.cal_ma_cat(dict_result, df_val_count, isCount, total, qre, idx_qre, cat, lbl)

                    if idx_qre > 0 and not isCount and total >= 30 and '_OE_' in qres[0]:
                        dict_result = self.cal_ma_and_sig(dict_result, cat, dict_val_lbl, dict_qres_name, qres, df, lst_sub_cat=[cat])


        # END Apr 12 2023-----------------------------------------------------------------------------------------------

        return dict_result



    def cal_ma_and_sig(self, dict_result, cat, dict_val_lbl, dict_qres_name, qres, df, lst_sub_cat) -> dict:

        dict_recode = {None: np.nan}
        for rekey, reval in dict_val_lbl.items():
            if isinstance(reval, dict):
                lst_reval = reval.keys()
            else:
                lst_reval = [rekey]

            for re_i in lst_reval:
                dict_recode[re_i] = 1 if re_i in lst_sub_cat else 0

        lst_col_arr = [dict_qres_name.get(qres[0]), dict_qres_name.get(qres[1])]

        sig_arrs = [df[lst_col_arr[0]].copy(), df[lst_col_arr[1]].copy()]

        sig_arrs[0].replace(dict_recode, inplace=True)
        sig_arrs[1].replace(dict_recode, inplace=True)

        # if 'F2' in qres[0]:
        #     a = 1

        for i, arr in enumerate(sig_arrs):
            arr.loc[arr[lst_col_arr[i][0]] >= 0, [f'sum{i}']] = arr.loc[
                arr[lst_col_arr[i][0]] >= 0, lst_col_arr[i]].sum(axis=1)
            arr.loc[arr[f'sum{i}'] > 1, [f'sum{i}']] = [1]

        dict_result[str(cat)] = self.run_ttest_oe(dict_result[str(cat)], sig_arrs[0]['sum0'], sig_arrs[1]['sum1'])

        return dict_result



    @staticmethod
    def cal_ma_cat(dict_result, df_val_count, isCount, total, qre, idx_qre, cat, lbl) -> dict:

        if str(cat) not in dict_result.keys():
            dict_result.update({str(cat): {
                'catLbl': lbl,
                f'val{idx_qre}': 0,
                f'sig{idx_qre}': 0,
            }})
        else:
            dict_result[str(cat)].update({
                f'val{idx_qre}': 0,
                f'sig{idx_qre}': 0,
            })

        if cat in list(df_val_count.index):
            cat_val = df_val_count.at[cat, qre]

            if not isCount:
                cat_val = cat_val / total if total > 0 else 0

            dict_result[str(cat)][f'val{idx_qre}'] = cat_val

        return dict_result


    @staticmethod
    def pull_data_num(df: pd.DataFrame, dict_result: dict, val: dict):

        qres, atts, excl, qType, isCount = val['qres'], val['atts'], val['excl'], val['type'], val['isCount']

        # Update in 14/04/2023------------------------------------------------------------------------------------------
        for idx_qre, qre in enumerate(qres):
            total = int(df[qre].count())

            df[qre] = df[qre].astype(float)

            # Base
            if 'Base' not in excl:

                if 'base' not in dict_result.keys():
                    dict_result.update({'base': {
                        'catLbl': 'Base',
                        f'val{idx_qre}': total,
                        f'sig{idx_qre}': 0,
                    }})
                else:
                    dict_result['base'].update({
                        f'val{idx_qre}': total,
                        f'sig{idx_qre}': 0,
                    })

            # Mean
            if 'mean' not in dict_result.keys():
                dict_result.update({'mean': {
                    'catLbl': 'Mean',
                    f'val{idx_qre}': df[qre].mean() if total else 0,
                    f'sig{idx_qre}': 0,

                }})
            else:
                dict_result['mean'].update({
                    f'val{idx_qre}': df[qre].mean() if total else 0,
                    f'sig{idx_qre}': 0,
                })

            # Standard Deviation
            if 'std' not in dict_result.keys():
                dict_result.update({'std': {
                    'catLbl': 'Standard Deviation',
                    f'val{idx_qre}': df[qre].std() if total else 0,
                    f'sig{idx_qre}': 0,
                }})
            else:
                dict_result['std'].update({
                    f'val{idx_qre}': df[qre].std() if total else 0,
                    f'sig{idx_qre}': 0,
                })

            # Min
            if 'min' not in dict_result.keys():
                dict_result.update({'min': {
                    'catLbl': 'Min',
                    f'val{idx_qre}': df[qre].min() if total else 0,
                    f'sig{idx_qre}': 0,
                }})
            else:
                dict_result['min'].update({
                    f'val{idx_qre}': df[qre].min() if total else 0,
                    f'sig{idx_qre}': 0,
                })

            # Max
            if 'max' not in dict_result.keys():
                dict_result.update({'max': {
                    'catLbl': 'Max',
                    f'val{idx_qre}': df[qre].max() if total else 0,
                    f'sig{idx_qre}': 0,
                }})
            else:
                dict_result['max'].update({
                    f'val{idx_qre}': df[qre].max() if total else 0,
                    f'sig{idx_qre}': 0,
                })


            # Median
            if 'median' not in dict_result.keys():
                dict_result.update({'median': {
                    'catLbl': 'Median',
                    f'val{idx_qre}': df[qre].median() if total else 0,
                    f'sig{idx_qre}': 0,
                }})
            else:
                dict_result['median'].update({
                    f'val{idx_qre}': df[qre].median() if total else 0,
                    f'sig{idx_qre}': 0,
                })


            # First quartile
            if 'first_quartile' not in dict_result.keys():
                dict_result.update({'first_quartile': {
                    'catLbl': 'First quartile',
                    f'val{idx_qre}': df[qre].quantile(0.25) if total else 0,
                    f'sig{idx_qre}': 0,
                }})
            else:
                dict_result['first_quartile'].update({
                    f'val{idx_qre}': df[qre].quantile(0.25) if total else 0,
                    f'sig{idx_qre}': 0,
                })


            # Third quartile
            if 'third_quartile' not in dict_result.keys():
                dict_result.update({'third_quartile': {
                    'catLbl': 'Third quartile',
                    f'val{idx_qre}': df[qre].quantile(0.75) if total else 0,
                    f'sig{idx_qre}': 0,
                }})
            else:
                dict_result['third_quartile'].update({
                    f'val{idx_qre}': df[qre].quantile(0.75) if total else 0,
                    f'sig{idx_qre}': 0,
                })
        # Update in 14/04/2023------------------------------------------------------------------------------------------

        return dict_result


    def pull_data_single_qre(self, df: pd.DataFrame, dict_result: dict, val: dict):
        qres, atts, excl, qType, isCount = val['qres'], val['atts'], val['excl'], val['type'], val['isCount']
        df_info = self.df_info.copy()
        df_info['idx_by_var_name'] = df_info['var_name']
        df_info.set_index('idx_by_var_name', inplace=True)

        # Update 14/04/2023---------------------------------------------------------------------------------------------
        dict_val_lbl = df_info.loc[qres[0], ['val_lbl']].values.tolist()[0]

        del df_info

        lstCats = list(dict_val_lbl.keys())
        lstCats.sort()
        lst_total = list()

        for idx_qre, qre in enumerate(qres):
            total = int(df[qre].count())
            lst_total.append(total)

            # Base
            if 'Base' not in excl:

                if 'base' not in dict_result.keys():
                    dict_result.update({'base': {
                        'catLbl': 'Base',
                        f'val{idx_qre}': total,
                        f'sig{idx_qre}': 0,
                    }})
                else:
                    dict_result['base'].update({
                        f'val{idx_qre}': total,
                        f'sig{idx_qre}': 0,
                    })

            # Categories
            df_val_count = df.loc[:, [qre]].apply(pd.value_counts)
            for cat, lbl in dict_val_lbl.items():

                if cat in excl:
                    continue

                dict_result = self.cal_ma_cat(dict_result, df_val_count, isCount, total, qre, idx_qre, cat, lbl)

                if idx_qre > 0 and not isCount and lst_total[0] == total >= 30 and qType in ['OL', 'FC']:
                    df['temp0'] = [np.nan if pd.isnull(a) else (1 if a == cat else 0) for a in df[qres[0]]]
                    df['temp1'] = [np.nan if pd.isnull(a) else (1 if a == cat else 0) for a in df[qres[1]]]

                    df_cats = df.loc[:, ['temp0', 'temp1']].copy()
                    df_cats.dropna(how='all', inplace=True)

                    dict_result[str(cat)] = self.run_ttest_rel(dict_result[str(cat)], df_cats['temp0'], df_cats['temp1'])

            # T2B & B2B
            for item in ['t2b', 'b2b']:
                if str(item).upper() in atts:

                    minVal, maxVal = -999, -999

                    if item == 't2b':
                        if qType == 'JR' and len(lstCats) == 6:
                            minVal, maxVal = lstCats[-3], lstCats[-2]
                        else:
                            minVal, maxVal = lstCats[-2], lstCats[-1]
                    elif item == 'b2b':
                        minVal, maxVal = lstCats[:2]

                    val_count = df_val_count.loc[df_val_count.index.isin([minVal, maxVal])].sum(axis=0).values[0]

                    if item not in dict_result.keys():
                        dict_result.update({item: {
                            'catLbl': str(item).upper(),
                            f'val{idx_qre}': val_count / total if total > 0 else 0,
                            f'sig{idx_qre}': 0,
                        }})
                    else:
                        dict_result[item].update({
                            f'val{idx_qre}': val_count / total if total > 0 else 0,
                            f'sig{idx_qre}': 0,
                        })

                    minVal, maxVal = int(minVal), int(maxVal)

                    if idx_qre > 0 and not isCount and lst_total[0] == total >= 30 and qType in ['OL']:

                        df['temp0'] = [np.nan if pd.isnull(a) else (1 if maxVal >= a >= minVal else 0) for a in df[qres[0]]]
                        df['temp1'] = [np.nan if pd.isnull(a) else (1 if maxVal >= a >= minVal else 0) for a in df[qres[1]]]

                        df_t2b_b2b = df.loc[:, ['temp0', 'temp1']].copy()
                        df_t2b_b2b.dropna(how='all', inplace=True)

                        dict_result[item] = self.run_ttest_rel(dict_result[item], df_t2b_b2b['temp0'], df_t2b_b2b['temp1'])


            if len(lstCats) == 11:

                dict_grp_scale_11 = {
                    'Group: 0-4': [1, 2, 3, 4, 5],
                    'Group: 5': [6],
                    'Group: 6-10': [7, 8, 9, 10, 11],
                }

                for grp_key, grp_val in dict_grp_scale_11.items():

                    val_count = df_val_count.loc[df_val_count.index.isin(grp_val)].sum(axis=0).values[0]

                    if str(grp_key).lower() not in dict_result.keys():
                        dict_result.update({str(grp_key).lower(): {
                            'catLbl': grp_key,
                            f'val{idx_qre}': val_count / total if total > 0 else 0,
                            f'sig{idx_qre}': 0,
                        }})
                    else:
                        dict_result[str(grp_key).lower()].update({
                            f'val{idx_qre}': val_count / total if total > 0 else 0,
                            f'sig{idx_qre}': 0,
                        })

                    if idx_qre > 0 and not isCount and lst_total[0] == total >= 30 and qType in ['OL']:
                        df['temp0'] = [np.nan if pd.isnull(a) else (1 if a in grp_val else 0) for a in df[qres[0]]]
                        df['temp1'] = [np.nan if pd.isnull(a) else (1 if a in grp_val else 0) for a in df[qres[1]]]

                        df_grp_nps = df.loc[:, ['temp0', 'temp1']].copy()
                        df_grp_nps.dropna(how='all', inplace=True)

                        dict_result[str(grp_key).lower()] = self.run_ttest_rel(dict_result[str(grp_key).lower()], df_grp_nps['temp0'], df_grp_nps['temp1'])


            # Mean & Std
            if 'Mean' in atts:

                df_mean = df.loc[:, qres].copy()
                df_mean.dropna(how='all', inplace=True)

                if qType == 'JR':
                    dict_replace_for_mean = dict()

                    if self.isJR3Factors:
                        dict_replace_for_mean = {4: 2, 5: 1}

                    if len(lstCats) == 6:
                        dict_replace_for_mean.update({6: np.nan})

                    df_mean[qre].replace(dict_replace_for_mean, inplace=True)

                else:
                    if len(lstCats) == 11:  # Scale 0-10
                        df_mean[qre].replace({1: 0, 2: 1, 3: 2, 4: 3, 5: 4, 6: 5, 7: 6, 8: 7, 9: 8, 10: 9, 11: 10}, inplace=True)


                if 'mean' not in dict_result.keys():
                    dict_result.update({
                        'mean': {
                            'catLbl': 'Mean',
                            f'val{idx_qre}': df_mean[qre].mean() if total > 0 else 0,
                            f'sig{idx_qre}': 0,
                        },
                        'std': {
                            'catLbl': 'Standard Deviation',
                            f'val{idx_qre}': df_mean[qre].std() if total > 0 else 0,
                            f'sig{idx_qre}': 0,
                        }
                    })
                else:
                    dict_result['mean'].update({
                        f'val{idx_qre}': df_mean[qre].mean() if total > 0 else 0,
                        f'sig{idx_qre}': 0,
                    })

                    dict_result['std'].update({
                        f'val{idx_qre}': df_mean[qre].std() if total > 0 else 0,
                        f'sig{idx_qre}': 0,
                    })


                if idx_qre > 0 and lst_total[0] == total >= 30:

                    if qType in ['OL']:
                        dict_result['mean'] = self.run_ttest_rel(dict_result['mean'], df_mean[qres[0]], df_mean[qres[1]])

                    if qType in ['JR'] and not self.isJR3Factors:
                        dict_result['mean'] = self.run_ttest_jar_scale_5_with_mean_3(dict_result['mean'], df_mean[qres[0]], df_mean[qres[1]])


        # Update 14/04/2023---------------------------------------------------------------------------------------------

        return dict_result


    def run_ttest_rel(self, dictToSig: dict, arr1, arr2):

        if len(arr1) >= 30 <= len(arr2):
            sigResult = stats.ttest_rel(arr1, arr2)

            if sigResult.statistic > 0:
                dictToSig['sig0'] = self.convertSigValue(sigResult.pvalue)
            else:
                dictToSig['sig1'] = self.convertSigValue(sigResult.pvalue)

        return dictToSig


    def run_ttest_jar_scale_5_with_mean_3(self, dictToSig: dict, arr1, arr2):

        if len(arr1) >= 30 <= len(arr2):
            arr_mean_3 = pd.Series(data=[3] * len(arr1), index=arr1.index)

            sigResult_0 = stats.ttest_rel(arr1, arr_mean_3)
            sigResult_1 = stats.ttest_rel(arr2, arr_mean_3)

            if sigResult_0.statistic:
                dictToSig['sig0'] = self.convertSigValue(sigResult_0.pvalue)

            if sigResult_1.statistic:
                dictToSig['sig1'] = self.convertSigValue(sigResult_1.pvalue)

        return dictToSig


    def run_ttest_oe(self, dictToSig: dict, arr1, arr2):

        if len(arr1) >= 30 <= len(arr2):
            sigResult = stats.ttest_rel(arr1, arr2)

            if np.isnan(sigResult.statistic) and np.isnan(sigResult.pvalue) and np.isnan(sigResult.df):
                sigResult = stats.ttest_ind(arr1, arr2, nan_policy='omit')

            if sigResult.statistic > 0:
                dictToSig['sig0'] = self.convertSigValue(sigResult.pvalue)
            else:
                dictToSig['sig1'] = self.convertSigValue(sigResult.pvalue)

        return dictToSig


    @staticmethod
    def convertSigValue(pval):
        if pval <= .05:
            return 95
        elif pval <= .1:
            return 90
        elif pval <= .2:
            return 80
        else:
            return 0


    def generate_topline_excel(self, lstSheet):
        st = time.process_time()

        self.topline_name = self.topline_name.replace('Topline', 'Handcount') if 'Handcount' in lstSheet else self.topline_name

        tl_to_excel_result = self.export_topline_excel_file(lstSheet, self.obj_prj_info)

        if 'Handcount' in lstSheet and tl_to_excel_result[0]:
            # ADD Unstack raw data to handcount file
            df_data = self.df_data.copy()
            df_recode = self.df_info.loc[self.df_info['val_lbl'] != {}, ['var_name', 'val_lbl']].copy()

            df_recode.set_index('var_name', inplace=True)

            df_recode['val_lbl'] = [{int(cat): lbl for cat, lbl in dict_val.items() if '|' not in str(cat)} for dict_val in df_recode['val_lbl']]

            dict_recode = df_recode.loc[:, 'val_lbl'].to_dict()

            df_data.replace(dict_recode, inplace=True)

            with pd.ExcelWriter(self.topline_name, engine="openpyxl", mode='a') as writer:
                df_data.to_excel(writer, sheet_name='Verbatim', index=False)  # encoding='utf-8-sig'


        et = time.process_time()
        print('EXPORT EXCEL - TIME:', et - st, 'seconds')

        return tl_to_excel_result
