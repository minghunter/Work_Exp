import pandas as pd
import numpy as np
import pyreadstat
import zipfile
import traceback
import os
from functools import reduce

# warnings.simplefilter(action='ignore', category=pd.errors.PerformanceWarning)


class ExportMSNData:

    def __init__(self, prj: dict, isExportSPCode: bool, export_section: str, isExportForceChoiceYN: bool):

        self.prj = prj
        self.obj_section = prj['detail']['sections'][export_section]

        self.isExportForceChoiceYN = isExportForceChoiceYN
        self.isExportSPCode = isExportSPCode

        self.strProjectName = str()
        self.strRidColName = str()
        self.strOrderColName = str()
        self.strFilter = str()
        self.lstSPCodes = list()

        self.dictReProductCode = dict()
        self.dictNewProductCodeQres = dict()

        self.lstNewProductCodeQres = list()

        self.dictReForceChoice = dict()

        self.dfCombinedOE = pd.DataFrame()

        self.lstScrFormat = list()

        self.lstPlmToScrFormat = list()
        self.lstPlmToMainFormat = list()

        self.lstSP1Format = list()
        self.lstSP2Format = list()

        self.lstPreFormat = list()

        self.dfMerge = pd.DataFrame()
        self.dictMerge_column_labels, self.dictMerge_variable_value_labels = dict(), dict()

        self.dfSP1, self.dfSP2 = pd.DataFrame(), pd.DataFrame()

        self.dfStacked, self.dfUnstacked = pd.DataFrame(), pd.DataFrame()
        self.dictUnstack_column_labels, self.dictUnstack_variable_value_labels = dict(), dict()

        self.savStackName = str()
        self.savStackFile = None

        self.savUnstackName = str()
        self.savUnstackFile = None

        self.xlsxName = str()
        self.xlsxFile = None

        self.zipName = str()
        self.zipFile = None


    def export_data(self):
        isSuccess = self.get_prj_info()
        if not isSuccess[0]:
            return isSuccess

        isSuccess = self.create_dfMerge()
        if not isSuccess[0]:
            return isSuccess

        isSuccess = self.create_dfSP1_dfSP2()
        if not isSuccess[0]:
            return isSuccess

        isSuccess = self.export_stackSav(isCreateSavFile=True)
        if not isSuccess[0]:
            return isSuccess

        isSuccess = self.export_unstackSav(isCreateSavFile=True)
        if not isSuccess[0]:
            return isSuccess

        isSuccess = self.export_excelFile()
        if not isSuccess[0]:
            return isSuccess

        lstFile = [self.savStackName, self.savUnstackName, self.xlsxName]
        isSuccess = self.zipfiles(lstFile)
        if not isSuccess[0]:
            return isSuccess

        return True, None


    def export_dfOnly(self):

        isSuccess = self.get_prj_info()
        if not isSuccess[0]:
            return isSuccess

        isSuccess = self.create_dfMerge()
        if not isSuccess[0]:
            return isSuccess

        isSuccess = self.create_dfSP1_dfSP2()
        if not isSuccess[0]:
            return isSuccess

        # isSuccess = self.export_stackSav(isCreateSavFile=False)
        # if not isSuccess[0]:
        #     return isSuccess

        isSuccess = self.export_unstackSav(isCreateSavFile=False)
        if not isSuccess[0]:
            return isSuccess

        return True, None


    def get_prj_info(self):

        try:
            self.strProjectName = f"{self.prj['internal_id']}_{self.prj['name']}_{self.obj_section['name']}"
            self.strRidColName = self.prj['detail']['join_col']

            if self.prj['type'] == 'HUT':
                self.strOrderColName = self.prj['detail']['order_col']

            self.strFilter = self.obj_section['filter']
            self.lstSPCodes = [self.obj_section['product_qres']['1']['cats']['1']['lbl'],
                               self.obj_section['product_qres']['1']['cats']['2']['lbl']]

            # {'Main_P0c_2_Ma_san_pham_HN': {1: '426', 2: '705'}, 'Main_O0c_2_Ma_san_pham_HN': {1: '426', 2: '705'}}
            self.dictReProductCode = dict()

            # {'Main_P0c_Ma_san_pham': {'label': 'P0c. MÃ SẢN PHẨM ĐƯỢC ĐÁNH GIÁ TƯƠNG ỨNG',
            #                           'qres': ['Main_P0c_2_Ma_san_pham_HN', 'Main_P0c_2_Ma_san_pham_HN'],
            #                           'cats': {1: '426', 2: '705'}},
            #  'Main_O0c_Ma_san_pham': {'label': 'P0c. MÃ SẢN PHẨM ĐƯỢC ĐÁNH GIÁ TƯƠNG ỨNG',
            #                           'qres': ['Main_O0c_2_Ma_san_pham_HN', 'Main_O0c_2_Ma_san_pham_HN'],
            #                           'cats': {1: '426', 2: '705'}}}
            self.dictNewProductCodeQres = dict()

            dictCats = dict()

            for key, val in self.obj_section['product_qres'].items():
                dictCats = {
                    int(val['cats']['1']['val']): str(val['cats']['1']['lbl']),
                    int(val['cats']['2']['val']): str(val['cats']['2']['lbl']),
                }

                self.dictReProductCode[val['qres'][0]] = dictCats

                self.dictNewProductCodeQres[val['name']] = {
                    'label': val['lbl'],
                    'qres': [val['qres'][0], val['qres'][0]],
                    'cats': dictCats
                }

            self.lstNewProductCodeQres = list(self.dictNewProductCodeQres.keys())

            dictSPCodes = dict()
            if self.isExportSPCode:
                for item in self.lstSPCodes:
                    dictSPCodes[int(item)] = str(item)
            else:
                dictSPCodes = dictCats


            # self.dictReForceChoice = {
            #     self.obj_section['fc_qres'][0]: {
            #         'oldVal': {
            #             1: self.lstNewProductCodeQres[0],
            #             2: self.lstNewProductCodeQres[1]},
            #         'newVal': dictSPCodes
            #     }
            # }

            self.dictReForceChoice = dict()
            for item in self.obj_section['fc_qres'][0].split('|'):

                self.dictReForceChoice[item] = {
                    'oldVal': {
                        1: self.lstNewProductCodeQres[0],
                        2: self.lstNewProductCodeQres[0] if self.prj['type'] == 'HUT' else self.lstNewProductCodeQres[1]
                    },
                    'newVal': dictSPCodes
                }


            self.dfCombinedOE = pd.DataFrame.from_dict(self.prj['detail']['oe_combine_cols'],
                                                       orient='index',
                                                       columns=['Combined name', 'Qre name 1', 'Qre name 2'])

            self.lstScrFormat = [[self.strRidColName, self.strRidColName]]
            for key, val in self.prj['detail']['scr_cols'].items():
                self.lstScrFormat.append([val[1], val[0]])

            if self.prj['type'] == 'HUT':
                self.lstPlmToScrFormat = list()
                for key, val in self.prj['detail']['plm_to_scr_cols'].items():
                    self.lstPlmToScrFormat.append([val[1], val[0]])

                self.lstPlmToMainFormat = list()
                for key, val in self.prj['detail']['plm_to_prod_cols'].items():
                    self.lstPlmToMainFormat.append([val[1], val[0]])

            self.lstSP1Format = [[self.lstNewProductCodeQres[0], self.lstNewProductCodeQres[0]]]
            self.lstSP2Format = [[self.lstNewProductCodeQres[0] if self.prj['type'] == 'HUT' else self.lstNewProductCodeQres[1], self.lstNewProductCodeQres[0]]]
            for key, val in self.prj['detail']['product_cols'].items():
                self.lstSP1Format.append([val[1], val[0]])
                self.lstSP2Format.append([val[2], val[0]])

            self.lstPreFormat = list()
            for key, val in self.prj['detail']['fc_cols'].items():
                self.lstPreFormat.append([val[1], val[0]])

            return True, None

        except Exception:
            print(traceback.format_exc())
            return False, traceback.format_exc()


    def create_dfMerge(self):
        try:
            # Load excel file to dataframe----------------------------------------------------------------------------------

            dfScr = pd.DataFrame.from_dict(self.prj['screener']['data'])
            dictScr_column_labels = self.prj['screener']['varLbl']
            dictScr_variable_value_labels = {key: {int(k): v for k, v in val.items()} for key, val in self.prj['screener']['valLbl'].items()}
            dfScr[self.strRidColName] = dfScr[self.strRidColName].apply(pd.to_numeric)

            dfPlm, dictPlm_column_labels, dictPlm_variable_value_labels = None, None, None
            if self.prj['type'] == 'HUT':
                dfPlm = pd.DataFrame.from_dict(self.prj['placement']['data'])
                dictPlm_column_labels = self.prj['placement']['varLbl']
                dictPlm_variable_value_labels = {key: {int(k): v for k, v in val.items()} for key, val in self.prj['placement']['valLbl'].items()}
                dfPlm[self.strRidColName] = dfPlm[self.strRidColName].apply(pd.to_numeric)


            dfMain = pd.DataFrame.from_dict(self.prj['main']['data'])
            dictMain_column_labels = self.prj['main']['varLbl']
            dictMain_variable_value_labels = {key: {int(k): v for k, v in val.items()} for key, val in self.prj['main']['valLbl'].items()}
            dfMain[self.strRidColName] = dfMain[self.strRidColName].apply(pd.to_numeric)

            print('Check ID')
            if self.prj['type'] == 'HUT':

                dfMain1st = dfMain.loc[dfMain[self.strOrderColName] == 1, self.strRidColName].copy()
                dfPlm1st = dfPlm.loc[dfPlm[self.strOrderColName] == 1, self.strRidColName].copy()
                isIdValid = self.check_RID(dfScr[self.strRidColName].copy(), dfMain1st, dfPlm1st)

                if not isIdValid[0]:
                    return isIdValid

                dfMain2nd = dfMain.loc[dfMain[self.strOrderColName] == 2, self.strRidColName].copy()
                dfPlm2nd = dfPlm.loc[dfPlm[self.strOrderColName] == 2, self.strRidColName].copy()
                isIdValid = self.check_RID(dfScr[self.strRidColName].copy(), dfMain2nd, dfPlm2nd)

                if not isIdValid[0]:
                    return isIdValid

            else:
                isIdValid = self.check_RID(dfScr[self.strRidColName].copy(), dfMain[self.strRidColName].copy(), None)

                if not isIdValid[0]:
                    return isIdValid

            print('Load Q&Me excel files to dataframe')

            # Combined OE columns for dfMain----------------------------------------------------------------------------
            mainCols = list(dfMain.columns)

            for idx in self.dfCombinedOE.index:
                strCombineName = self.dfCombinedOE.at[idx, 'Combined name']
                strSP1Name = self.dfCombinedOE.at[idx, 'Qre name 1']
                strSP2Name = self.dfCombinedOE.at[idx, 'Qre name 2']

                if strSP1Name in mainCols or strSP2Name in mainCols:
                    df_toCombined = dfMain
                else:
                    df_toCombined = dfPlm

                df_toCombined[strSP1Name].replace({np.nan: 'NULL'}, inplace=True)
                df_toCombined[strSP2Name].replace({np.nan: 'NULL'}, inplace=True)

                lst_CombinedOE = [(a if a != 'NULL' else (b if b != 'NULL' else np.nan)) for a, b in
                                  zip(df_toCombined[strSP1Name], df_toCombined[strSP2Name])]

                df_CombinedOE = pd.DataFrame(lst_CombinedOE, columns=[strCombineName])
                df_CombinedOE[self.strRidColName] = df_toCombined[self.strRidColName].values
                lstColToMerge = [self.strRidColName]

                if self.prj['type'] == 'HUT':
                    df_CombinedOE[self.strOrderColName] = df_toCombined[self.strOrderColName].values
                    lstColToMerge.append(self.strOrderColName)

                df_toCombined = pd.merge(df_toCombined, df_CombinedOE, how='inner', on=lstColToMerge)

                df_toCombined[strSP1Name].replace({'NULL': np.nan}, inplace=True)
                df_toCombined[strSP2Name].replace({'NULL': np.nan}, inplace=True)

                if strSP1Name in mainCols or strSP2Name in mainCols:
                    dfMain = df_toCombined

                    dictMain_variable_value_labels[strCombineName] = {}
                    dictMain_column_labels[strCombineName] = dictMain_column_labels[strSP1Name]
                else:
                    dfPlm = df_toCombined

                    dictPlm_variable_value_labels[strCombineName] = {}
                    dictPlm_column_labels[strCombineName] = dictPlm_column_labels[strSP1Name]



            # self.dfMain.drop(self.dfDroppedOE['Delete columns name'].values.tolist(), inplace=True, axis=1)

            print('Combined OE columns for dfMain & dfPlm')

            # Create dfMerge--------------------------------------------------------------------------------------------
            dfPlm_scr = pd.DataFrame()
            dfMainInScr = pd.DataFrame()
            dict_rename_plm_1 = dict()
            dict_rename_plm_2 = dict()
            dict_rename_prod_1 = dict()
            dict_rename_prod_2 = dict()
            dfFc = pd.DataFrame()

            if self.prj['type'] == 'HUT':

                lst_loc_scr = list()
                lst_loc_MainInScr = list()
                for row in self.lstScrFormat:
                    if 'RECALL' in str(row[1]).upper() or 'MAIN' in str(row[0]).upper():
                        lst_loc_MainInScr.append(row[0])
                    else:
                        lst_loc_scr.append(row[0])

                dfScr = dfScr.loc[:, lst_loc_scr]

                dfMainInScr = dfMain.loc[dfMain[self.strOrderColName] == 1, [self.strRidColName] + lst_loc_MainInScr]

                dfPlm_scr = dfPlm.loc[dfPlm[self.strOrderColName] == 1, [self.strRidColName] + [row[0] for row in self.lstPlmToScrFormat]]

                lst_loc_plm_main = list()
                dict_rename_plm_1 = dict()
                dict_rename_plm_2 = dict()
                for row in self.lstPlmToMainFormat:
                    lst_loc_plm_main.append(row[0])
                    dict_rename_plm_1[row[0]] = f'SP1_{row[0]}'
                    dict_rename_plm_2[row[0]] = f'SP2_{row[0]}'

                dfPlm_prod_1 = dfPlm.loc[dfPlm[self.strOrderColName] == 1, [self.strRidColName] + lst_loc_plm_main]
                dfPlm_prod_2 = dfPlm.loc[dfPlm[self.strOrderColName] == 2, [self.strRidColName] + lst_loc_plm_main]

                dfPlm_prod_1.rename(columns=dict_rename_plm_1, inplace=True)
                dfPlm_prod_2.rename(columns=dict_rename_plm_2, inplace=True)


                lst_loc_main = list()
                dict_rename_prod_1 = dict()
                dict_rename_prod_2 = dict()
                for row in self.lstSP1Format:

                    val = row[0]

                    if row[0] in self.dictNewProductCodeQres.keys():
                        qreProd = self.dictNewProductCodeQres[row[0]]['qres'][0]
                        if '|' in qreProd:

                            for qre in qreProd.split('|'):
                                lst_loc_main.append(qre)
                                dict_rename_prod_1[qre] = f'SP1_{qre}'
                                dict_rename_prod_2[qre] = f'SP2_{qre}'

                            continue

                        else:
                            val = qreProd

                    lst_loc_main.append(val)
                    dict_rename_prod_1[val] = f'SP1_{val}'
                    dict_rename_prod_2[val] = f'SP2_{val}'

                dfMain_prod_1 = dfMain.loc[dfMain[self.strOrderColName] == 1, [self.strRidColName] + lst_loc_main]
                dfMain_prod_2 = dfMain.loc[dfMain[self.strOrderColName] == 2, [self.strRidColName] + lst_loc_main]

                dfMain_prod_1.rename(columns=dict_rename_prod_1, inplace=True)
                dfMain_prod_2.rename(columns=dict_rename_prod_2, inplace=True)

                dfFc = dfMain.loc[dfMain[self.strOrderColName] == 2, [self.strRidColName] + [row[0] for row in self.lstPreFormat]]


                data_frames = [dfScr, dfPlm_scr, dfMainInScr, dfPlm_prod_1, dfPlm_prod_2, dfMain_prod_1, dfMain_prod_2, dfFc]
                self.dfMerge = reduce(lambda left, right: pd.merge(left, right, on=[self.strRidColName], how='left'), data_frames)

            else:
                self.dfMerge = pd.merge(dfScr, dfMain, on=self.strRidColName, how='left')

            self.dfMerge.sort_values(by=self.strRidColName, inplace=True)

            print('Create dfMerge')

            # Get filter for dfMerge------------------------------------------------------------------------------------
            filterResult = self.get_filter_data(self.dfMerge.copy(), self.strFilter)

            if not filterResult['isSuccess']:
                return filterResult['isSuccess'], filterResult['err']

            self.dfMerge = filterResult['dfMerge']

            # Reorder index for dfMerge
            self.dfMerge = self.dfMerge.reset_index(drop=True)

            print('Get filter from dataframe')

            # Process dfMerge ------------------------------------------------------------------------------------------
            if self.prj['type'] == 'HUT':

                self.dictMerge_column_labels = dictScr_column_labels
                self.dictMerge_variable_value_labels = dictScr_variable_value_labels

                for item in dfPlm_scr.columns:
                    self.dictMerge_column_labels.update({item: dictPlm_column_labels[item]})
                    self.dictMerge_variable_value_labels.update({item: dictPlm_variable_value_labels[item]})

                for item in dfMainInScr.columns:
                    self.dictMerge_column_labels.update({item: dictMain_column_labels[item]})
                    self.dictMerge_variable_value_labels.update({item: dictMain_variable_value_labels[item]})

                for (key1, val1), (key2, val2) in zip(dict_rename_plm_1.items(), dict_rename_plm_2.items()):
                    self.dictMerge_column_labels.update({
                        val1: dictPlm_column_labels[key1],
                        val2: dictPlm_column_labels[key2]
                    })

                    self.dictMerge_variable_value_labels.update({
                        val1: dictPlm_variable_value_labels[key1],
                        val2: dictPlm_variable_value_labels[key2]
                    })

                for (key1, val1), (key2, val2) in zip(dict_rename_prod_1.items(), dict_rename_prod_2.items()):
                    self.dictMerge_column_labels.update({
                        val1: dictMain_column_labels[key1],
                        val2: dictMain_column_labels[key2]
                    })

                    self.dictMerge_variable_value_labels.update({
                        val1: dictMain_variable_value_labels[key1],
                        val2: dictMain_variable_value_labels[key2]
                    })

                for item in dfFc.columns:
                    self.dictMerge_column_labels.update({item: dictMain_column_labels[item]})
                    self.dictMerge_variable_value_labels.update({item: dictMain_variable_value_labels[item]})

                dictReProductCode = dict()
                for key, val in self.dictReProductCode.items():
                    dictReProductCode.update({
                        f'SP1_{key}': val,
                        f'SP2_{key}': val
                    })
                self.dictReProductCode = dictReProductCode

                dictReForceChoice = dict()
                for key, val in self.dictReForceChoice.items():
                    dictReForceChoice.update({
                        key: {
                            'oldVal': {
                                1: val['oldVal'][1],
                                2: f"SP2_{val['oldVal'][2]}"
                            },
                            'newVal': val['newVal']
                        }
                    })
                self.dictReForceChoice = dictReForceChoice

                dictNewProductCodeQres = dict()
                for key, val in self.dictNewProductCodeQres.items():
                    dictNewProductCodeQres.update({
                        key: {
                            'label': val['label'],
                            'qres': [f"SP1_{i.replace('|', '|SP1_')}" for i in val['qres']],
                            'cats': val['cats']
                        },
                        f'SP2_{key}': {
                            'label': val['label'],
                            'qres': [f"SP2_{i.replace('|', '|SP2_')}" for i in val['qres']],
                            'cats': val['cats']
                        }
                    })
                self.dictNewProductCodeQres = dictNewProductCodeQres


                # HERE
                # dfPlm_prod_1, dfPlm_prod_2, dfMain_prod_1, dfMain_prod_2, dfFc
                self.lstScrFormat = self.lstScrFormat + self.lstPlmToScrFormat

                lstSP1Format, lstSP2Format = list(), list()

                lstSP1Format.append(self.lstSP1Format[0])
                lstSP2Format.append(self.lstSP2Format[0])

                lstSP1Format[0][0] = list(self.dictNewProductCodeQres.keys())[0]
                lstSP2Format[0][0] = list(self.dictNewProductCodeQres.keys())[1]

                self.lstSP1Format.remove(self.lstSP1Format[0])
                self.lstSP2Format.remove(self.lstSP2Format[0])

                for row in self.lstPlmToMainFormat:
                    lstSP1Format.append([f'SP1_{row[0]}', row[1]])
                    lstSP2Format.append([f'SP2_{row[0]}', row[1]])

                for row1, row2 in zip(self.lstSP1Format, self.lstSP2Format):
                    lstSP1Format.append([f'SP1_{row1[0]}', row1[1]])
                    lstSP2Format.append([f'SP2_{row2[0]}', row2[1]])

                self.lstSP1Format = lstSP1Format
                self.lstSP2Format = lstSP2Format

            else:
                self.dictMerge_column_labels = dictScr_column_labels | dictMain_column_labels
                self.dictMerge_variable_value_labels = dictScr_variable_value_labels | dictMain_variable_value_labels



            for key, val in self.dictReProductCode.items():
                self.dictMerge_variable_value_labels[key] = val

            for key, val in self.dictReForceChoice.items():
                self.dictMerge_variable_value_labels[key] = val['newVal']

            if self.isExportSPCode:
                for key in self.dictReProductCode.keys():
                    self.dfMerge[key].replace(self.dictReProductCode[key], inplace=True)

            for key, val in self.dictNewProductCodeQres.items():

                # self.dfMerge[key] = [a if float(a) > 0 else b for a, b in zip(self.dfMerge[val['qres'][0]], self.dfMerge[val['qres'][1]])]

                # Fix for performance
                if key in self.dfMerge.columns:
                    self.dfMerge[key] = [a if float(a) > 0 else b for a, b in zip(self.dfMerge[val['qres'][0]], self.dfMerge[val['qres'][1]])]
                else:

                    if '|' in val['qres'][0] or '|' in val['qres'][1]:
                        for qComb in val['qres']:

                            if '|' in qComb:
                                arr_q = qComb.split('|')

                                # not yet fix performance
                                # self.dfMerge[qComb] = [0] * self.dfMerge.shape[0]

                                if qComb not in self.dfMerge.columns:
                                    newQCombCol = pd.DataFrame([0] * self.dfMerge.shape[0], columns=[qComb])
                                    self.dfMerge = pd.concat([self.dfMerge, newQCombCol], axis=1)

                                    for q in arr_q:
                                        self.dfMerge[q].replace({np.nan: 0}, inplace=True)

                                        self.dfMerge[qComb] = self.dfMerge[qComb] + self.dfMerge[q]

                                    self.dfMerge[qComb] = self.dfMerge[qComb].apply(pd.to_numeric)

                    newCol = pd.DataFrame([a if float(a) > 0 else b for a, b in zip(self.dfMerge[val['qres'][0]], self.dfMerge[val['qres'][1]])], columns=[key])
                    self.dfMerge = pd.concat([self.dfMerge, newCol], axis=1)


                self.dictMerge_column_labels[key] = val['label']

                self.dictMerge_variable_value_labels[key] = val['cats']

            for key, val in self.dictReForceChoice.items():
                for idx in self.dfMerge.index:
                    for key2, val2 in val['oldVal'].items():
                        if self.dfMerge.at[idx, key] == key2:

                            self.dfMerge.at[idx, key] = self.dfMerge.at[idx, val2]

                            break

            # self.dfMerge.to_csv('dfMerge.csv', encoding='utf-8-sig')

            print('Process dfMerge')

            return True, None

        except Exception:
            print(traceback.format_exc())
            return False, traceback.format_exc()


    @staticmethod
    def check_RID(dfRidScr, dfRidMain, dfRidPlm=None):

        dfRidScr = pd.DataFrame(dfRidScr)
        dfRidMain = pd.DataFrame(dfRidMain)

        dfRidScrDup = pd.DataFrame(dfRidScr[dfRidScr.astype(int).duplicated()])
        if not dfRidScrDup.empty:
            return False, f'Duplicated id in screener: {dfRidScrDup.values}'

        dfRidMainDup = pd.DataFrame(dfRidMain[dfRidMain.astype(int).duplicated()])
        if not dfRidMainDup.empty:
            return False, f'Duplicated id in main: {dfRidMainDup.values}'

        if dfRidPlm is not None:
            dfRidPlm = pd.DataFrame(dfRidPlm)
            dfRidPlmDup = pd.DataFrame(dfRidPlm[dfRidPlm.astype(int).duplicated()])
            if not dfRidPlmDup.empty:
                return False, f'Duplicated id in placement: {dfRidScrDup.values}'


        setRidScr = set(dfRidScr.iloc[:, 0])
        setRidMain = set(dfRidMain.iloc[:, 0])

        setRidPlm = None
        if dfRidPlm is not None:
            setRidPlm = set(dfRidPlm.iloc[:, 0])

        diffRid_Scr_Main = setRidScr.difference(setRidMain)
        if len(diffRid_Scr_Main):
            return False, f'ID in screener but not in main: {diffRid_Scr_Main}'

        diffRidMain = setRidMain.difference(setRidScr)
        if len(diffRidMain):
            return False, f'ID in main but not in screener: {diffRidMain}'

        if setRidPlm is not None:
            diffRid_Scr_Plm = setRidScr.difference(setRidPlm)
            if len(diffRid_Scr_Plm):
                return False, f'ID in screener but not in placement: {diffRid_Scr_Plm}'

            diffRid_Plm_Scr = setRidPlm.difference(setRidScr)
            if len(diffRid_Plm_Scr):
                return False, f'ID in placement but not in screener: {diffRid_Plm_Scr}'

            diffRid_Main_Plm = setRidMain.difference(setRidPlm)
            if len(diffRid_Main_Plm):
                return False, f'ID in main but not in placement: {diffRid_Main_Plm}'

            diffRid_Plm_Main = setRidPlm.difference(setRidMain)
            if len(diffRid_Plm_Main):
                return False, f'ID in placement but not in main: {diffRid_Plm_Main}'

        return True, None


    @staticmethod
    def get_filter_data(dfMerge: pd.DataFrame, strFilter: str):
        try:

            strFilter = strFilter.strip()
            strFilter = "{}{}{}".format('{', strFilter.replace(' ', '} {'), '}')
            strFilter = strFilter.replace('} {=} {', "'] == ")
            strFilter = strFilter.replace('} {<>} {', "'] != ")
            strFilter = strFilter.replace('} {>} {', "'] > ")
            strFilter = strFilter.replace('} {>=} {', "'] >= ")
            strFilter = strFilter.replace('} {<} {', "'] < ")
            strFilter = strFilter.replace('} {<=} {', "'] <= ")
            strFilter = strFilter.replace('{OR}', '|')
            strFilter = strFilter.replace('{AND}', '&')
            strFilter = strFilter.replace('{', "(dfMerge['")
            strFilter = strFilter.replace('}', ")")

            dfMerge = eval(f"dfMerge.loc[({strFilter})]")

            return {
                'isSuccess': True,
                'err': None,
                'dfMerge': dfMerge
            }

        except Exception:
            print(traceback.format_exc())
            return {
                'isSuccess': False,
                'err': traceback.format_exc(),
                'dfMerge': None
            }


    def create_dfSP1_dfSP2(self):
        try:
            lstSP1_toLoc = [row[0] for row in self.lstScrFormat + self.lstSP1Format + self.lstPreFormat]
            lstSP2_toLoc = [row[0] for row in self.lstScrFormat + self.lstSP2Format + self.lstPreFormat]

            self.dfSP1 = self.dfMerge.loc[:, lstSP1_toLoc].copy()
            self.dfSP2 = self.dfMerge.loc[:, lstSP2_toLoc].copy()

            self.dfSP1.set_axis([row[1] for row in self.lstScrFormat + self.lstSP1Format + self.lstPreFormat], axis=1, inplace=True)
            self.dfSP2.set_axis([row[1] for row in self.lstScrFormat + self.lstSP2Format + self.lstPreFormat], axis=1, inplace=True)

            print('Created dfSP1 & dfSP2')
            return True, None

        except Exception:
            print(traceback.format_exc())
            return False, traceback.format_exc()


    def export_stackSav(self, isCreateSavFile):

        try:

            self.savStackName = f"{self.strProjectName}_Stack.sav"

            self.dfStacked = pd.concat([self.dfSP1, self.dfSP2], ignore_index=True)

            lstStack_column_labels = [self.dictMerge_column_labels[row[0]] for row in self.lstScrFormat + self.lstSP1Format + self.lstPreFormat]

            dictStack_variable_value_labels = dict()
            dictStack_variable_measure = dict()

            for row in self.lstScrFormat + self.lstSP1Format + self.lstPreFormat:
                dictStack_variable_value_labels[row[1]] = self.dictMerge_variable_value_labels[row[0]]
                dictStack_variable_measure[row[1]] = 'nominal'

            lstForceChoiceYN = list()
            for key, val in self.dictReForceChoice.items():
                strForceChoice = key  # list(self.dictReForceChoice.keys())[0]
                strForceChoiceYN = f'{strForceChoice}_YN'
                lstForceChoiceYN.append(strForceChoiceYN)
                strProductCodeQre = val['oldVal'][1]  # self.dictReForceChoice[strForceChoice]['oldVal'][1]

                # Fix for performance
                # self.dfStacked[strForceChoiceYN] = [1 if a == b else 0 for a, b in zip(self.dfStacked[strForceChoice], self.dfStacked[strProductCodeQre])]
                newCol = pd.DataFrame([1 if a == b else 0 for a, b in zip(self.dfStacked[strForceChoice], self.dfStacked[strProductCodeQre])], columns=[strForceChoiceYN])
                self.dfStacked = pd.concat([self.dfStacked, newCol], axis=1)

                lstStack_column_labels.append(self.dictMerge_column_labels[strForceChoice])
                dictStack_variable_value_labels[strForceChoiceYN] = {0: 'No', 1: 'Yes'}

            self.dfStacked.sort_values(by=self.lstScrFormat[0][1], inplace=True)

            self.dfStacked.replace({-99999: np.nan}, inplace=True)

            # dfStacked.to_csv(f'{strFormatedPath}/dfStacked.csv', encoding='utf-8-sig')

            print('Created dfStacked')

            if self.isExportForceChoiceYN:
                if isCreateSavFile:
                    self.savStackFile = pyreadstat.write_sav(self.dfStacked, self.savStackName,
                                                             column_labels=lstStack_column_labels,
                                                             variable_value_labels=dictStack_variable_value_labels,
                                                             variable_measure=dictStack_variable_measure)

            else:
                # self.dfStacked.drop(columns=[strForceChoiceYN], inplace=True)
                self.dfStacked.drop(columns=lstForceChoiceYN, inplace=True)

                lstStack_column_labels = lstStack_column_labels[:-(len(lstForceChoiceYN))]

                for i in lstForceChoiceYN:
                    dictStack_variable_value_labels.pop(i)  # .pop(strForceChoiceYN)

                if isCreateSavFile:
                    self.savStackFile = pyreadstat.write_sav(self.dfStacked, self.savStackName,
                                                             column_labels=lstStack_column_labels,
                                                             variable_value_labels=dictStack_variable_value_labels,
                                                             variable_measure=dictStack_variable_measure)

            print('Exported Stacked Sav file')

            return True, None

        except Exception:
            print(traceback.format_exc())
            return False, traceback.format_exc()


    def export_unstackSav(self, isCreateSavFile):
        try:

            self.savUnstackName = f"{self.strProjectName}_Unstack.sav"

            self.dfUnstacked = pd.DataFrame()

            lstUnstack_column_labels = list()
            dictUnstack_variable_value_labels = dict()
            dictUnstack_variable_measure = dict()

            for row in self.lstScrFormat:

                if self.dfUnstacked.empty:
                    self.dfUnstacked[row[1]] = self.dfMerge[row[0]].copy()
                else:
                    dfToMerge = pd.DataFrame(self.dfMerge[row[0]].copy())
                    dfToMerge.rename(columns={row[0]: row[1]}, inplace=True)

                    self.dfUnstacked = pd.concat([self.dfUnstacked, dfToMerge], axis=1)

                lstUnstack_column_labels.append(self.dictMerge_column_labels[row[0]])
                dictUnstack_variable_value_labels[row[1]] = self.dictMerge_variable_value_labels[row[0]]
                dictUnstack_variable_measure[row[1]] = 'nominal'

                self.dictUnstack_column_labels[row[1]] = self.dictMerge_column_labels[row[0]]
                self.dictUnstack_variable_value_labels[row[1]] = self.dictMerge_variable_value_labels[row[0]]


            arr_empty = np.empty(self.dfUnstacked.shape[0])
            arr_empty[:] = np.nan
            arr_empty = list(arr_empty)

            for row in self.lstSP1Format:

                if not row[0] in list(self.dictReProductCode.keys())[0]:  # row[0] != self.lstNewProductCodeQres[0]:
                    for spCode in self.lstSPCodes:
                        
                        dfToMerge = pd.DataFrame(arr_empty, columns=[f'{row[1]}_{spCode}'])

                        self.dfUnstacked = pd.concat([self.dfUnstacked, dfToMerge], axis=1)

                        # self.dfUnstacked[f'{row[1]}_{spCode}'] = [np.nan] * self.dfUnstacked.shape[0]

                        lstUnstack_column_labels.append(f'{self.dictMerge_column_labels[row[0]]}_{spCode}')
                        dictUnstack_variable_value_labels[f'{row[1]}_{spCode}'] = self.dictMerge_variable_value_labels[row[0]]
                        dictUnstack_variable_measure[f'{row[1]}_{spCode}'] = 'nominal'

                        self.dictUnstack_column_labels[f'{row[1]}_{spCode}'] = f'{self.dictMerge_column_labels[row[0]]}_{spCode}'
                        self.dictUnstack_variable_value_labels[f'{row[1]}_{spCode}'] = self.dictMerge_variable_value_labels[row[0]]


            for row in self.lstPreFormat:

                dfToMerge = pd.DataFrame(self.dfMerge[row[0]].copy())
                dfToMerge.rename(columns={row[0]: row[1]}, inplace=True)

                self.dfUnstacked = pd.concat([self.dfUnstacked, dfToMerge], axis=1)

                # self.dfUnstacked[row[1]] = self.dfMerge[row[0]].copy()

                lstUnstack_column_labels.append(self.dictMerge_column_labels[row[0]])
                dictUnstack_variable_value_labels[row[1]] = self.dictMerge_variable_value_labels[row[0]]
                dictUnstack_variable_measure[row[1]] = 'nominal'

                self.dictUnstack_column_labels[row[1]] = self.dictMerge_column_labels[row[0]]
                self.dictUnstack_variable_value_labels[row[1]] = self.dictMerge_variable_value_labels[row[0]]


            for idx in self.dfUnstacked.index:

                for row in self.lstSP1Format:

                    if not row[0] in list(self.dictReProductCode.keys())[0]:

                        qreMaSP = self.lstNewProductCodeQres[0]

                        masp = self.dictMerge_variable_value_labels[qreMaSP][int(self.dfMerge.at[idx, qreMaSP])]

                        val = self.dfMerge.at[idx, row[0]]

                        self.dfUnstacked.at[idx, f'{row[1]}_{masp}'] = val


                for row_idx, row in enumerate(self.lstSP2Format):

                    if row_idx == 0:
                        continue

                    if not row[0] in list(self.dictReProductCode.keys())[1]:

                        qreMaSP = f"SP2_{self.lstNewProductCodeQres[0]}" if self.prj['type'] == 'HUT' else self.lstNewProductCodeQres[1]

                        masp = self.dictMerge_variable_value_labels[qreMaSP][int(self.dfMerge.at[idx, qreMaSP])]

                        val = self.dfMerge.at[idx, row[0]]

                        self.dfUnstacked.at[idx, f'{row[1]}_{masp}'] = val


            self.dfUnstacked.sort_values(by=self.lstScrFormat[0][1], inplace=True)

            self.dfUnstacked.replace({-99999: np.nan}, inplace=True)

            # dfUnstacked.to_csv(f'{strFormatedPath}/dfUnstacked.csv', encoding='utf-8-sig')

            print('Created dfUnstacked')

            if self.isExportForceChoiceYN:

                for key, val in self.dictReForceChoice.items():

                    strForceChoice = key  # list(self.dictReForceChoice.keys())[0]
                    dictProductCode = val['newVal']  # self.dictReForceChoice[strForceChoice]['newVal']

                    strForceChoiceSP1 = f'{strForceChoice}_{int(list(dictProductCode.values())[0])}'
                    strForceChoiceSP2 = f'{strForceChoice}_{int(list(dictProductCode.values())[1])}'

                    self.dfUnstacked[strForceChoiceSP1] = [1 if int(a) == list(dictProductCode.keys())[0] else 0 for a in self.dfUnstacked[strForceChoice]]
                    self.dfUnstacked[strForceChoiceSP2] = [1 if int(a) == list(dictProductCode.keys())[1] else 0 for a in self.dfUnstacked[strForceChoice]]

                    self.dictUnstack_column_labels[strForceChoiceSP1] = strForceChoiceSP1
                    self.dictUnstack_column_labels[strForceChoiceSP2] = strForceChoiceSP2

                    self.dictUnstack_variable_value_labels[strForceChoiceSP1] = {0: 'No', 1: 'Yes'}
                    self.dictUnstack_variable_value_labels[strForceChoiceSP2] = {0: 'No', 1: 'Yes'}

                if isCreateSavFile:
                    self.savUnstackFile = pyreadstat.write_sav(self.dfUnstacked, self.savUnstackName,
                                                               column_labels=list(self.dictUnstack_column_labels.values()),
                                                               variable_value_labels=self.dictUnstack_variable_value_labels,
                                                               variable_measure=dictUnstack_variable_measure)

            else:
                if isCreateSavFile:
                    self.savUnstackFile = pyreadstat.write_sav(self.dfUnstacked, self.savUnstackName,
                                                               column_labels=lstUnstack_column_labels,
                                                               variable_value_labels=dictUnstack_variable_value_labels,
                                                               variable_measure=dictUnstack_variable_measure)


            print('Exported Unstacked Sav file')

            return True, None

        except Exception:
            print(traceback.format_exc())
            return False, traceback.format_exc()


    def export_excelFile(self):

        try:

            self.xlsxName = f"{self.strProjectName}_ExcelData.xlsx"
            dictRecodeScrForXlsx = dict()

            for row in self.lstScrFormat:
                dictRecodeScrForXlsx[row[1]] = self.dictMerge_variable_value_labels[row[0]]

            dfUnstacked = self.dfUnstacked.replace(dictRecodeScrForXlsx)
            dfStacked = self.dfStacked.replace(dictRecodeScrForXlsx)

            with pd.ExcelWriter(self.xlsxName) as writer:
                dfUnstacked.to_excel(writer, sheet_name='Product - Unstacked', index=False, encoding='utf-8-sig')
                dfStacked.to_excel(writer, sheet_name='Product - Stacked', index=False, encoding='utf-8-sig')

            print('Exported Excel data files')

            return True, None

        except Exception:
            print(traceback.format_exc())
            return False, traceback.format_exc()


    def zipfiles(self, lstFile: list):
        try:
            self.zipName = f"{self.strProjectName}.zip"

            with zipfile.ZipFile(self.zipName, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
                for f in lstFile:
                    zf.write(f)
                    os.remove(f)

            self.zipFile = zf

            return True, None

        except Exception:
            print(traceback.format_exc())
            return False, traceback.format_exc()











