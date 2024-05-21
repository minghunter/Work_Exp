import pandas as pd
import numpy as np
import re
import io


class QMeFileConvert:

    def __init__(self):
        pass
        # self.dfData = pd.DataFrame()
        # self.dictData, self.dictVarLbl, self.dictValLbl = dict(), dict(), dict()


    def convert_by_md(self, file):

        dfData, dfQres = self.read_file(file)

        dictQres = dict()
        for idx in dfQres.index:

            strMatrix = '' if dfQres.loc[idx, 'Question(Matrix)'] is None else f"{dfQres.loc[idx, 'Question(Matrix)']}_"
            strNormal = dfQres.loc[idx, 'Question(Normal)'] if strMatrix == '' else f"{strMatrix}{dfQres.loc[idx, 'Question(Normal)']}"
            strQreName = str(dfQres.loc[idx, 'Name of items'])
            strQreName = strQreName.replace('Rank_', 'Rank') if 'Rank_' in strQreName else strQreName

            dictQres[strQreName] = {
                'type': dfQres.loc[idx, 'Question type'],
                'label': f'{self.cleanhtml(strNormal)}',
                'isMAMatrix': True if strMatrix != '' and dfQres.loc[idx, 'Question type'] == 'MA' else False,
                'cats': {
                }
            }

            lstHeaderCol = list(dfQres.columns)
            lstHeaderCol.remove('Name of items')
            lstHeaderCol.remove('Question type')
            lstHeaderCol.remove('Question(Matrix)')
            lstHeaderCol.remove('Question(Normal)')

            for col in lstHeaderCol:
                # if col not in ['Name of items', 'Question type', 'Question(Matrix)', 'Question(Normal)'] \
                #         and dfQres.loc[idx, col] is not None and len(dfQres.loc[idx, col]) > 0:
                if dfQres.loc[idx, col] is not None and len(str(dfQres.loc[idx, col])) > 0:
                    dictQres[strQreName]['cats'].update({str(col): self.cleanhtml(str(dfQres.loc[idx, col]))})

        lstMatrixHeader = list()
        for k in dictQres.keys():
            if dictQres[k]['isMAMatrix'] and len(dictQres[k]['cats'].keys()):
                lstMatrixHeader.append(k)

        if len(lstMatrixHeader):
            for i in lstMatrixHeader:
                for code in dictQres[i]['cats'].keys():
                    lstLblMatrixMA = dictQres[f'{i}_{code}']['label'].split('_')
                    dictQres[f'{i}_{code}']['cats'].update({'1': self.cleanhtml(lstLblMatrixMA[1])})
                    dictQres[f'{i}_{code}']['label'] = f"{dictQres[i]['label']}_{lstLblMatrixMA[1]}"

        dictVarLbl = dict()
        dictValLbl = dict()

        for col in dfData.columns:
            if col in dictQres.keys():
                dictVarLbl[col] = self.cleanhtml(dictQres[col]['label'])

                dictValLbl[col] = dictQres[col]['cats']

            else:
                dictVarLbl[col] = col

        dfData.replace({None: np.nan}, inplace=True)
        dictData = dfData.to_dict('records')

        return dictData, dictVarLbl, dictValLbl


    def convert_by_mc(self, file):  # convert data with MA questions format by columns instead of code

        dfData, dfQres = self.read_file(file)
        dfData.replace({None: np.nan}, inplace=True)

        lstFullCodelist = list(dfQres.columns)
        lstFullCodelist.remove('Name of items')
        lstFullCodelist.remove('Question type')
        lstFullCodelist.remove('Question(Matrix)')
        lstFullCodelist.remove('Question(Normal)')

        dictQres = dict()
        for idx in dfQres.index:

            strQreName = str(dfQres.loc[idx, 'Name of items'])
            strQreName = strQreName.replace('Rank_', 'Rank') if 'Rank_' in strQreName else strQreName
            strQreType = dfQres.loc[idx, 'Question type']
            isMatrix = False if dfQres.loc[idx, 'Question(Matrix)'] is None else True
            strMatrix = '' if dfQres.loc[idx, 'Question(Matrix)'] is None else self.cleanhtml(f"{dfQres.loc[idx, 'Question(Matrix)']}")
            strNormal = '' if dfQres.loc[idx, 'Question(Normal)'] is None else self.cleanhtml(f"{dfQres.loc[idx, 'Question(Normal)']}")

            if strQreName not in dictQres.keys():

                if strQreType == 'MA':

                    if isMatrix:

                        ser_codelist = dfQres.loc[idx, lstFullCodelist]
                        ser_codelist.dropna(inplace=True)
                        dict_codelist = ser_codelist.to_dict()

                        if not ser_codelist.empty:
                            dictQres[strQreName] = {
                                'type': strQreType,
                                'label': f'{strMatrix}_{strNormal}' if isMatrix else strNormal,
                                'isMatrix': isMatrix,
                                'MA_Matrix_Header': strQreName,
                                'MA_cols': [f'{strQreName}_{k}' for k in dict_codelist.keys()],
                                'cats': {str(k): self.cleanhtml(v) for k, v in dict_codelist.items()},
                            }
                    else:

                        maName, maCode = strQreName.rsplit('_', 1)
                        maLbl = self.cleanhtml(dfQres.at[idx, 1])

                        if maName not in dictQres.keys():

                            dictQres[maName] = {
                                'type': strQreType,
                                'label': strNormal,
                                'isMatrix': isMatrix,
                                'MA_cols': [strQreName],
                                'cats': {str(maCode): maLbl}
                            }

                        else:

                            dict_qre = dictQres[maName]
                            dict_qre['MA_cols'].append(strQreName)
                            dict_qre['cats'].update({str(maCode): maLbl})


                else:  # ['SA', 'RANKING', 'FT']

                    dictQres[strQreName] = {
                        'type': strQreType,
                        'label': str(),
                        'isMatrix': isMatrix,
                        'cats': dict(),
                    }

                    dict_qre = dictQres[strQreName]
                    dict_qre['label'] = f'{strMatrix}_{strNormal}' if isMatrix else strNormal

                    if strQreType in ['SA', 'RANKING']:
                        ser_codelist = dfQres.loc[idx, lstFullCodelist]
                        ser_codelist.dropna(inplace=True)
                        dict_qre['cats'] = {str(k): self.cleanhtml(v) for k, v in ser_codelist.to_dict().items()}


        dfDataOutput, dfQreInfo = pd.DataFrame(), pd.DataFrame(columns=['var_name', 'var_lbl', 'var_type', 'val_lbl'])
        dfDataOutput.index = dfData.index

        for qre, qre_info in dictQres.items():

            if qre_info['type'] == 'MA':

                dfMA = dfData.loc[:, qre_info['MA_cols']]

                for col_name in qre_info['MA_cols']:
                    maName, maCode = col_name.rsplit('_', 1)
                    dfMA[col_name].replace({1: int(maCode)}, inplace=True)

                dfMA['combined'] = [[e for e in row if e == e] for row in dfMA[qre_info['MA_cols']].values.tolist()]
                dfMA = pd.DataFrame(dfMA['combined'].tolist(), index=dfMA.index)

                for i, col_name in enumerate(qre_info['MA_cols']):

                    if i in list(dfMA.columns):
                        dfColMA = dfMA[i].to_frame()
                        dfColMA.rename(columns={i: col_name}, inplace=True)
                    else:
                        dfColMA = pd.DataFrame([np.nan] * dfMA.shape[0], columns=[col_name])

                    dfDataOutput = pd.concat([dfDataOutput, dfColMA], axis=1)

                    dfInfoRow = pd.DataFrame([[col_name, qre_info['label'], 'MA_mtr' if qre_info['isMatrix'] else 'MA', qre_info['cats']]], columns=['var_name', 'var_lbl', 'var_type', 'val_lbl'])

                    dfQreInfo = pd.concat([dfQreInfo, dfInfoRow], axis=0)

            else:
                if qre in dfData.columns:
                    dfDataOutput = pd.concat([dfDataOutput, dfData[qre]], axis=1)

                    dfInfoRow = pd.DataFrame([[qre, qre_info['label'], f"{qre_info['type']}_mtr" if qre_info['isMatrix'] else qre_info['type'], qre_info['cats']]], columns=['var_name', 'var_lbl', 'var_type', 'val_lbl'])

                    dfQreInfo = pd.concat([dfQreInfo, dfInfoRow], axis=0)

        # dfQreInfo.set_index('var_name', inplace=True)
        dfQreInfo.reset_index(drop=True, inplace=True)

        # dfDataOutput.to_csv('dfDataOutput.csv', encoding='utf-8-sig')
        # dfQreInfo.to_csv('dfQreInfo.csv', encoding='utf-8-sig')

        dictDataOutput = dfDataOutput.to_dict('records')
        dictQreInfo = dfQreInfo.to_dict('records')

        return dictDataOutput, dictQreInfo


    # @staticmethod
    # def read_file_old(file):
    #
    #     xlsx = io.BytesIO(file.file.read())
    #     wb = openpyxl.load_workbook(xlsx, data_only=True)
    #
    #     wsData = wb['Data']
    #     wsQres = wb['Question']
    #
    #     mergedCells = list()
    #     for group in wsData.merged_cells.ranges:
    #         mergedCells.append(group)
    #
    #     for group in mergedCells:
    #         wsData.unmerge_cells(str(group))
    #
    #     wsData.delete_rows(1, 4)
    #     wsData.delete_rows(2, 1)
    #
    #     for icol in range(1, wsData.max_column + 1):
    #         if wsData.cell(row=1, column=icol).value is None:
    #             wsData.cell(row=1,
    #                         column=icol).value = f'{wsData.cell(row=1, column=icol - 1).value}_{wsData.cell(row=2, column=icol).value}'
    #
    #     wsData.delete_rows(2, 1)
    #
    #     data = wsData.values
    #     columns = next(data)[0:]
    #     dfData = pd.DataFrame(data, columns=columns)
    #
    #     lstDrop = [
    #     ]
    #
    #     for col in dfData.columns:
    #         if col in lstDrop or '_Images' in col:
    #             dfData.drop(col, inplace=True, axis=1)
    #
    #     data = wsQres.values
    #     columns = next(data)[0:]
    #     dfQres = pd.DataFrame(data, columns=columns)
    #
    #     wb.close()
    #
    #     return dfData, dfQres


    @staticmethod
    def read_file(file) -> (pd.DataFrame, pd.DataFrame):

        lstDrop = [
            'Approve',
            'Reject',
            'Re - do request', 'Re-do request',
            'Reason to reject',
            'Memo'
            'No.',
            'Date',
            'ID',
            'Country',
            'Channel',
            'Chain / Type',
            'Distributor',
            'Method',
            'Panel ID',
            'Panel FB',
            'Panel Email',
            'Panel Phone',
            'Panel Age',
            'Panel Gender',
            'Panel Area',
            'Panel Income',
            'Login ID',
            'User name',
            'Store ID',
            'Store Code',
            'Store name',
            'Store level',
            'District',
            'Ward',
            'Store address',
            'Area group',
            'Store ranking',
            'Region 2',
            'Nhóm cửa hàng',
            'Nhà phân phối',
            'Manager',
            'Telephone number',
            'Contact person',
            'Email',
            'Others 1',
            'Others 2',
            'Others 3',
            'Others 4',
            'Check in',
            'Store Latitude',
            'Store Longitude',
            'User Latitude',
            'User Longitude',
            'Check out',
            'Distance',
            'Task duration',

            'InterviewerID',
            'InterviewerName',
            'RespondentName',
            'RespondentEmail',
            'RespondentPhone',
            'RespondentCellPhone',
            'RespondentAddress',

            'Memo',
            'No.'
        ]

        xlsx = io.BytesIO(file.file.read())

        df_data = pd.read_excel(xlsx, sheet_name='Data')

        # Update 01072023
        df_data_header = df_data.iloc[[3, 4, 5], :].copy().T
        df_data_header.loc[((pd.isnull(df_data_header[3])) & (df_data_header[5] == 'Images')), 3] = ['Images']
        df_data_header[3].fillna(method='ffill', inplace=True)

        df_temp = df_data_header.loc[(df_data_header[3].duplicated(keep=False)) & ~(pd.isnull(df_data_header[3])) & ~(
            pd.isnull(df_data_header[4])), :].copy()

        for idx in df_temp.index:
            df_data_header.at[idx, 3] = f"{df_data_header.at[idx, 3]}_{df_data_header.at[idx, 4].rsplit('_', 1)[1]}"

        df_data_header.loc[pd.isnull(df_data_header[3]), 3] = df_data_header.loc[pd.isnull(df_data_header[3]), 5]
        # Update 01072023

        # df_data_header = df_data.iloc[[3, 5], :].copy().T
        # df_data_header.loc[pd.isnull(df_data_header[3]), 3] = df_data_header.loc[pd.isnull(df_data_header[3]), 5]
        dict_header = df_data_header[3].to_dict()
        df_data.rename(columns=dict_header, inplace=True)
        df_data.drop(list(range(6)), inplace=True)

        set_drop = set(dict_header.values()).intersection(set(lstDrop))
        df_data.drop(list(set_drop), inplace=True, axis=1)

        df_qres = pd.read_excel(xlsx, sheet_name='Question')
        df_qres.replace({np.nan: None}, inplace=True)

        df_data.reset_index(drop=True, inplace=True)
        df_qres.reset_index(drop=True, inplace=True)


        return df_data, df_qres


    @staticmethod
    def cleanhtml(raw_html):

        if isinstance(raw_html, str):
            CLEANR = re.compile('{.*?}|<.*?>|&([a-z0-9]+|#[0-9]{1,6}|#x[0-9a-f]{1,6});|\n|(?i)(showcard|showphoto|show photo)')
            cleantext = re.sub(CLEANR, '', raw_html)
            return cleantext

        return raw_html