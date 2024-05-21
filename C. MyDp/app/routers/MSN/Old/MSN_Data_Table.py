import pandas as pd
import numpy as np
import time
from scipy import stats


class DataTable:

    def __init__(self, dictSide, dictHeader, export_section_name, productCode, dictValLbl, isJR3Factors, isDisplayPctSign, df):

        self.dictSide, self.dictHeader = dictSide, dictHeader
        self.export_section_name = export_section_name
        self.productCode = productCode
        self.dictValLbl = dictValLbl
        self.isJR3Factors = isJR3Factors
        self.isDisplayPctSign = isDisplayPctSign
        self.df = df

        self.df_ttest = pd.DataFrame()
        self.df_ua = pd.DataFrame()


    def create_tbl(self):

        st = time.process_time()

        print('Data table processing...')

        lstSideCol = ['SideQre', 'SideGroupLbl', 'SideQreLbl', 'SideQreType', 'isCount', 'runCode', 'SideCode', 'SideLbl']
        lstNanSideCol = [np.nan] * len(lstSideCol)

        dict_side_ttest = {
            'subGroupLbl': lstNanSideCol,
            'subCodeLbl': lstNanSideCol,
            'subGrpFilter.qre': lstNanSideCol,
            'subGrpFilter.val': lstNanSideCol
        }

        dict_side_ua = {
            'subGroupLbl': lstNanSideCol,
            'subCodeLbl': lstNanSideCol,
            'subGrpFilter.qre': lstNanSideCol,
            'subGrpFilter.val': lstNanSideCol
        }

        st_create_tbl_side = time.process_time()

        for s_key, s_val in self.dictSide.items():
            if s_val['isUA']:
                dict_side_ua = self.create_tbl_side(dict_side_ua, s_val, lstNanSideCol)
            else:
                dict_side_ttest = self.create_tbl_side(dict_side_ttest, s_val, lstNanSideCol)

        et_create_tbl_side = time.process_time()
        print('Create table side - processed time:', et_create_tbl_side - st_create_tbl_side, 'seconds\n')

        df_side_ua = pd.DataFrame.from_dict(dict_side_ua, orient='index', columns=[lstSideCol])
        df_side_ttest = pd.DataFrame.from_dict(dict_side_ttest, orient='index', columns=[lstSideCol])

        self.df_ttest.index = df_side_ttest.index

        self.df_ua.index = df_side_ua.index

        lst_nan_ttest = [np.nan] * (df_side_ttest.shape[0] - 5)
        lst_nan_ua = [np.nan] * (df_side_ua.shape[0] - 4)

        for h_key, h_val in self.dictHeader.items():

            if self.export_section_name in h_val['run_secs']:

                st_populate_tbl_data = time.process_time()

                if h_val['qre'] == 'Total':

                    col_data = {
                        f"{h_val['qre']}#{self.productCode[0]}#val": ['Total', 'Total', np.nan, np.nan, self.productCode[0]] + lst_nan_ttest,
                        f"{h_val['qre']}#{self.productCode[0]}#sig": ['Total', 'Total', np.nan, np.nan, self.productCode[0]] + lst_nan_ttest,
                        f"{h_val['qre']}#{self.productCode[1]}#val": ['Total', 'Total', np.nan, np.nan, self.productCode[1]] + lst_nan_ttest,
                        f"{h_val['qre']}#{self.productCode[1]}#sig": ['Total', 'Total', np.nan, np.nan, self.productCode[1]] + lst_nan_ttest
                    }

                    self.df_ttest = pd.concat([self.df_ttest, self.populate_tbl_data(df_side_ttest, col_data, isUA=False)], axis=1)

                    col_data = {
                        f"{h_val['qre']}": ['Total', 'Total', np.nan, np.nan] + lst_nan_ua,
                    }

                    self.df_ua = pd.concat([self.df_ua, self.populate_tbl_data(df_side_ua, col_data, isUA=True)], axis=1)

                else:
                    for cat, lbl in self.dictValLbl[h_val['qre']].items():
                        if cat not in h_val['excl']:

                            lst_prod_1 = [h_val['lbl'], lbl, h_val['qre'], cat, self.productCode[0]] + lst_nan_ttest
                            lst_prod_2 = [h_val['lbl'], lbl, h_val['qre'], cat, self.productCode[1]] + lst_nan_ttest

                            col_data = {
                                f"{h_val['qre']}#{cat}#{self.productCode[0]}#val": lst_prod_1,
                                f"{h_val['qre']}#{cat}#{self.productCode[0]}#sig": lst_prod_1,
                                f"{h_val['qre']}#{cat}#{self.productCode[1]}#val": lst_prod_2,
                                f"{h_val['qre']}#{cat}#{self.productCode[1]}#sig": lst_prod_2
                            }

                            df_col_data = self.populate_tbl_data(df_side_ttest, col_data, isUA=False)

                            self.df_ttest = pd.concat([self.df_ttest, df_col_data.loc[:, col_data.keys()]], axis=1)

                            col_data = {
                                f"{h_val['qre']}#{cat}": [h_val['lbl'], lbl, h_val['qre'], cat] + lst_nan_ua
                            }

                            df_col_data = self.populate_tbl_data(df_side_ua, col_data, isUA=True)
                            self.df_ua = pd.concat([self.df_ua, df_col_data.loc[:, col_data.keys()]], axis=1)

                et_populate_tbl_data = time.process_time()
                print(f"Populate table data - {h_val['qre']} - processed time:", et_populate_tbl_data - st_populate_tbl_data, 'seconds')

        self.df_ua.to_csv('df_ua.csv', encoding='utf-8-sig')
        self.df_ttest.to_csv('df_ttest.csv', encoding='utf-8-sig')

        et = time.process_time()
        print('\nData table processed time:', et - st, 'seconds\n')



    def create_tbl_side(self, dict_side: dict, s_val: dict, lstNanSideCol: list):

        if not s_val['isUA'] and 'prod_code' not in dict_side.keys():  # is Ttest
            dict_side.update({'prod_code': lstNanSideCol})

        k_sub_grp = f"Side#{'count' if s_val['isCount'] else 'pct'}#{s_val['qre']}"
        lstTopCol = [s_val['qre'], s_val['groupLbl'], s_val['qreLbl'], s_val['type'], 1 if s_val['isCount'] else 0, np.nan]

        prodCode = '' if s_val['isUA'] else f"_{self.productCode[0]}"

        if s_val['type'] in ['MA']:

            lstTopCol[-1] = s_val['MACats']

            dict_side.update({f"{k_sub_grp}#base": lstTopCol + ['base', 'Base']})

            lstTopCol[-1] = np.nan

            for cat in s_val['MACats']:
                dict_side.update({
                    f"{k_sub_grp}#{cat}": lstTopCol + [cat, self.dictValLbl[f"{s_val['qre']}_{cat}{prodCode}"][1]]
                })

        elif s_val['type'] in ['NUM']:

            dict_side.update({f"{k_sub_grp}#base": lstTopCol + ['base', 'Base']})

            dict_side.update({f"{k_sub_grp}#mean": lstTopCol + ['mean', 'Mean']})

        else:

            dict_side.update({f"{k_sub_grp}#base": lstTopCol + ['base', 'Base']})

            lstCats = list()
            for cat, lbl in self.dictValLbl[f"{s_val['qre']}{prodCode}"].items():
                if cat not in s_val['excl']:
                    dict_side.update({f"{k_sub_grp}#{cat}": lstTopCol + [cat, lbl]})
                    lstCats.append(cat)

            lst_atts = s_val['atts']
            if s_val['type'] in ['OL', 'JR']:
                lst_atts.extend(['std'])

                if s_val['type'] == 'OL':
                    lst_atts.insert(1, 'Medium')
                else:
                    lst_atts.insert(1, 'JR')


            for att in lst_atts:
                att_code = str(att).lower()
                att_lbl = att

                if att in ['B2B']:
                    lstTopCol[-1] = [1, 2]
                elif att in ['T2B']:

                    if len(lstCats) == 6:
                        lstTopCol[-1] = lstCats[-3:-2]
                    else:
                        lstTopCol[-1] = lstCats[-2:]

                elif att in ['Mean', 'std']:

                    if self.isJR3Factors and s_val['type'] == 'JR':

                        if len(lstCats) == 6:
                            lstTopCol[-1] = {4: 2, 5: 1, 6: np.nan}
                        else:
                            lstTopCol[-1] = {4: 2, 5: 1}
                    else:
                        lstTopCol[-1] = np.nan

                    if att == 'std':
                        att_lbl = 'Standard Deviation'

                elif att in ['Medium', 'JR']:

                    lstTopCol[-1] = [3]

                else:
                    lstTopCol[-1] = np.nan

                dict_side.update({f"{k_sub_grp}#{att_code}": lstTopCol + [att_code, att_lbl]})

        return dict_side



    def populate_tbl_data(self, df_side: pd.DataFrame, col_data: dict, isUA: bool):

        df_col_data = pd.DataFrame.from_dict(col_data, orient='columns')
        df_col_data.index = df_side.index

        df_col_data = pd.concat([df_side, df_col_data], axis=1)
        df_col_data.columns = df_col_data.columns.map(''.join)

        if isUA:
            lst_full_col = list(df_col_data.columns)[-1:]
            lst_col_val_name = lst_full_col
            lst_col_sig_name = None
        else:
            lst_full_col = list(df_col_data.columns)[-4:]
            lst_col_val_name = [lst_full_col[0], lst_full_col[2]]
            lst_col_sig_name = [lst_full_col[1], lst_full_col[3]]

        subGrpFilter_qre = df_col_data.at['subGrpFilter.qre', lst_full_col[0]]
        subGrpFilter_val = df_col_data.at['subGrpFilter.val', lst_full_col[0]]

        if np.isnan(subGrpFilter_val):
            df_data = self.df
        else:
            df_data = self.df.loc[self.df[subGrpFilter_qre] == subGrpFilter_val, :]


        # # Test -----------------------------------------------------------------------------------------------
        # df_full_qre = df_col_data.loc[df_col_data['SideCode'] == 'base', ['SideQre', 'SideQreType', 'isCount']]
        #
        # lst_prod = None
        # if not isUA:
        #     lst_prod = [int(i.split('#')[-2]) for i in lst_col_val_name]
        #
        # for idx_qre in df_full_qre.index:
        #     qreName = df_full_qre.at[idx_qre, 'SideQre']
        #     isCount = df_full_qre.at[idx_qre, 'isCount']
        #     qreType = df_full_qre.at[idx_qre, 'SideQreType']
        #
        #     left_of_idx = str(idx_qre).rsplit('#', 1)[0]
        #
        #     if lst_prod:
        #         lst_qre_prod = [f"{qreName}_{i}" for i in lst_prod]
        #     else:
        #         lst_qre_prod = [qreName]
        #
        #     df_col_data_qre = df_col_data.loc[((df_col_data['SideQre'] == qreName) & (df_col_data['isCount'] == isCount)), :]
        #
        #     if qreType in ['OL', 'JR', 'SA', 'NUM', 'FC']:
        #
        #         df_raw_data_qre = df_data.loc[:, lst_qre_prod]
        #         lst_side_code = list(df_col_data_qre['SideCode'])
        #
        #         lst_cat = list()
        #         if qreType not in ['NUM']:
        #             lst_cat = [val for val in lst_side_code if isinstance(val, (int, float))]
        #
        #         df_raw_data_qre_mean_std = df_raw_data_qre.copy()
        #         if 'mean' in lst_side_code:
        #             if not pd.isna(df_col_data_qre.at[f"{left_of_idx}#mean", 'runCode']):
        #                 df_raw_data_qre_mean_std.replace(df_col_data_qre.at[f"{left_of_idx}#mean", 'runCode'], inplace=True)
        #
        #         df_describe = df_raw_data_qre_mean_std.describe()
        #
        #         # Significant
        #         base_sig1 = df_describe.at['count', lst_qre_prod[0]]
        #
        #         if len(lst_qre_prod) == 1:
        #             base_sig2 = 0
        #         else:
        #             base_sig2 = df_describe.at['count', lst_qre_prod[1]]
        #
        #         if lst_col_sig_name and isCount == 0 and base_sig1 == base_sig2 >= 30:
        #
        #             if qreType in ['OL']:
        #                 if 'mean' in lst_side_code:
        #                     sig_idx, sig_val = self.run_ttest_rel(df_raw_data_qre_mean_std)
        #                     if sig_val:
        #                         df_col_data.at[f"{left_of_idx}#mean", lst_col_sig_name[sig_idx]] = sig_val
        #
        #                 if 't2b' in lst_side_code or 'b2b' in lst_side_code:
        #
        #                     for item in ['t2b', 'b2b']:
        #                         lstGroupCat = df_col_data_qre.at[f"{left_of_idx}#{item}", 'runCode']
        #
        #                         lst_val = list()
        #                         for prod in lst_qre_prod:
        #                             lst_val.append([np.nan if pd.isna(a) else (1 if lstGroupCat[0] <= a <= lstGroupCat[1] else 0) for a in df_raw_data_qre[prod]])
        #
        #                         df_raw_data_qre_t2b_b2b = pd.DataFrame(np.array(lst_val).T, columns=lst_qre_prod)
        #                         sig_idx, sig_val = self.run_ttest_rel(df_raw_data_qre_t2b_b2b)
        #                         if sig_val:
        #                             df_col_data.at[f"{left_of_idx}#{item}", lst_col_sig_name[sig_idx]] = sig_val
        #
        #             if qreType in ['FC']:
        #                 sig_idx, sig_val = self.run_ttest_rel(df_raw_data_qre)
        #                 if sig_val:
        #                     df_col_data.at[f"{left_of_idx}#1", lst_col_sig_name[sig_idx]] = sig_val
        #         # END Significant
        #
        #         # Populate data ['OL', 'JR', 'SA', 'NUM', 'FC']
        #         for col_idx, col_name in enumerate(lst_col_val_name):
        #
        #             base = df_describe.at['count', lst_qre_prod[col_idx]]
        #
        #             if base > 0 and not pd.isna(base):
        #                 df_col_data.at[f"{left_of_idx}#base", col_name] = base
        #
        #                 if 'mean' in lst_side_code:
        #                     df_col_data.at[f"{left_of_idx}#mean", col_name] = df_describe.at['mean', lst_qre_prod[col_idx]]
        #
        #                 if 'std' in lst_side_code:
        #                     df_col_data.at[f"{left_of_idx}#std", col_name] = df_describe.at['std', lst_qre_prod[col_idx]]
        #
        #                 if isCount > 0:
        #                     df_freq = df_raw_data_qre[lst_qre_prod[col_idx]].value_counts(normalize=False)
        #                 else:
        #                     df_freq = df_raw_data_qre[lst_qre_prod[col_idx]].value_counts(normalize=True)
        #
        #                 if df_describe.at['count', lst_qre_prod[col_idx]] > 0:
        #
        #                     if lst_cat:
        #                         for cat in lst_cat:
        #                             df_col_data.at[f"{left_of_idx}#{cat}", col_name] = 0
        #
        #                             if cat in df_freq.index:
        #                                 cat_val = df_freq.at[cat]
        #                                 if not self.isDisplayPctSign and isCount < 1:
        #                                     cat_val = cat_val * 100
        #
        #                                 df_col_data.at[f"{left_of_idx}#{cat}", col_name] = cat_val
        #
        #                     lst_att = list()
        #                     for item in lst_side_code:
        #                         if item in ['t2b', 'b2b', 'medium', 'jr']:
        #                             lst_att.append(item)
        #
        #                     if lst_att:
        #                         for item in lst_att:
        #                             val_t2b_b2b = 0
        #                             for cat in df_col_data_qre.at[f"{left_of_idx}#{item}", 'runCode']:
        #                                 val_t2b_b2b += df_col_data.at[f"{left_of_idx}#{cat}", col_name]
        #
        #                             df_col_data.at[f"{left_of_idx}#{item}", col_name] = val_t2b_b2b
        #         # END Populate data ['OL', 'JR', 'SA', 'NUM', 'FC']
        #
        #
        #
        #     else:
        #
        #         # Populate data MA
        #         lst_cat = df_col_data_qre.at[f"{left_of_idx}#base", 'runCode']
        #
        #         if isUA:
        #             lstMACol = [[f"{qreName}_{cat}" for cat in lst_cat]]
        #         else:
        #             lstMACol = list()
        #             for prod in lst_prod:
        #                 lstMACol.append([f"{qreName}_{cat}_{prod}" for cat in lst_cat])
        #
        #         for col_idx, col_name in enumerate(lst_col_val_name):
        #
        #             df_raw_data_qre = df_data.loc[:, lstMACol[col_idx]]
        #
        #             df_raw_data_qre['SumMAQre'] = df_raw_data_qre[lstMACol[col_idx]].sum(axis=1)
        #             df_raw_data_qre['SumMAQre'].replace({0: np.nan}, inplace=True)
        #
        #             base = df_raw_data_qre['SumMAQre'].count()
        #
        #             if base > 0 and not pd.isna(base):
        #                 df_col_data.at[f"{left_of_idx}#base", col_name] = base
        #
        #                 for cat, colMA in zip(lst_cat, lstMACol[col_idx]):
        #
        #                     df_freq = df_raw_data_qre[colMA].value_counts(normalize=False)
        #
        #                     if 1 in df_freq.index:
        #
        #                         if isCount < 1:
        #                             cat_val = df_freq.at[1] / base
        #                             if not self.isDisplayPctSign:
        #                                 cat_val = cat_val * 100
        #                         else:
        #                             cat_val = df_freq.at[1]
        #
        #                     else:
        #                         cat_val = 0
        #
        #                     df_col_data.at[f"{left_of_idx}#{cat}", col_name] = cat_val
        #         # END Populate data MA
        #
        # # Test -----------------------------------------------------------------------------------------------


        num_slide = 4 if isUA else 5
        for idx in df_col_data.iloc[num_slide:, :].index:

            left_of_idx = str(idx).rsplit('#', 1)[0]

            df_sig = pd.DataFrame()

            for item_idx, item in enumerate(lst_col_val_name):

                qre = df_col_data.at[idx, 'SideQre']

                if isUA:
                    prod = None
                    qre_prod = qre
                else:
                    prod = df_col_data.at['prod_code', item]
                    qre_prod = f"{qre}_{prod}"

                if '#base' in idx:

                    if df_col_data.at[idx, 'SideQreType'] in ['MA']:

                        if isUA:
                            lstMACol = [f"{qre}_{cat}" for cat in df_col_data.at[idx, 'runCode']]
                        else:
                            lstMACol = [f"{qre}_{cat}_{prod}" for cat in df_col_data.at[idx, 'runCode']]

                        df_ma_data = df_data.loc[:, lstMACol].copy()

                        df_ma_data['SumMAQre'] = df_ma_data[lstMACol].sum(axis=1)
                        df_ma_data['SumMAQre'].replace({0: np.nan}, inplace=True)

                        base = df_ma_data['SumMAQre'].count()

                    else:
                        base = df_data[qre_prod].count()

                    if base:
                        df_col_data.at[idx, item] = base

                else:  # Not Base

                    base = df_col_data.at[f"{left_of_idx}#base", item]

                    if base:

                        if '#t2b' in idx or '#b2b' in idx or '#medium' in idx or '#jr' in idx:

                            lstGroupCat = df_col_data.at[idx, 'runCode']
                            val_left = df_col_data.at[f"{left_of_idx}#{lstGroupCat[0]}", item]

                            val_right = 0
                            if len(lstGroupCat) > 1:
                                val_right = df_col_data.at[f"{left_of_idx}#{lstGroupCat[1]}", item]

                            df_col_data.at[idx, item] = val_left + val_right

                            # significant
                            if ('#t2b' in idx or '#b2b' in idx) and not isUA and df_col_data.at[idx, 'SideQreType'] in ['OL'] and base >= 30:

                                lst_val = [np.nan if pd.isna(a) else (1 if lstGroupCat[0] <= a <= lstGroupCat[1] else 0) for a in df_data[qre_prod]]

                                df_sig = pd.concat([df_sig, pd.DataFrame(lst_val, columns=[qre_prod])], axis=1)

                                if item_idx == 1 and df_sig.shape[0] >= 30 and df_sig.shape[1] == 2:

                                    sig_idx, sig_val = self.run_ttest_rel(df_sig)
                                    if sig_val:
                                        df_col_data.at[idx, lst_col_sig_name[sig_idx]] = sig_val

                        elif '#mean' in idx or '#std' in idx:

                            if pd.isna([df_col_data.at[idx, 'runCode']]):
                                df_mean_std = df_data[qre_prod].copy()
                            else:
                                df_mean_std = df_data[qre_prod].replace(df_col_data.at[idx, 'runCode']).copy()

                            if '#mean' in idx:
                                df_col_data.at[idx, item] = df_mean_std.mean()

                                # significant
                                if not isUA and df_col_data.at[idx, 'SideQreType'] in ['OL'] and base >= 30:
                                    df_sig = pd.concat([df_sig, df_mean_std], axis=1)

                                    if item_idx == 1 and df_sig.shape[0] >= 30 and df_sig.shape[1] == 2:
                                        sig_idx, sig_val = self.run_ttest_rel(df_sig)
                                        if sig_val:
                                            df_col_data.at[idx, lst_col_sig_name[sig_idx]] = sig_val

                            else:
                                df_col_data.at[idx, item] = df_mean_std.std()

                        else:

                            if df_col_data.at[idx, 'SideQreType'] in ['MA']:

                                if isUA:
                                    qre_MA = f"{qre}_{df_col_data.at[idx, 'SideCode']}"
                                else:
                                    qre_MA = f"{qre}_{df_col_data.at[idx, 'SideCode']}_{prod}"

                                count = df_data[qre_MA].count()
                            else:
                                count = df_data.loc[df_data[qre_prod] == df_col_data.at[idx, 'SideCode'], qre_prod].count()

                            if df_col_data.at[idx, 'isCount'] == 1:
                                df_col_data.at[idx, item] = count if base else np.nan
                            else:
                                if self.isDisplayPctSign:
                                    pct = count / float(base) if base else np.nan
                                else:
                                    pct = count / float(base) * 100 if base else np.nan

                                df_col_data.at[idx, item] = pct

                                # significant
                                if not isUA and df_col_data.at[idx, 'SideQreType'] in ['FC'] and base >= 30:

                                    df_sig = pd.concat([df_sig, pd.DataFrame(df_data[qre_prod], columns=[qre_prod])], axis=1)

                                    if item_idx == 1 and df_sig.shape[0] >= 30 and df_sig.shape[1] == 2:
                                        sig_idx, sig_val = self.run_ttest_rel(df_sig)
                                        if sig_val:
                                            df_col_data.at[idx, lst_col_sig_name[sig_idx]] = sig_val




        return df_col_data







    def run_ttest_rel(self, df_sig: pd.DataFrame):

        sig_idx = None
        sig_val = None

        arr1 = df_sig.iloc[:, 0]
        arr2 = df_sig.iloc[:, 1]

        if len(arr1) == len(arr2) >= 30:

            sigResult = stats.ttest_rel(arr1, arr2)

            if not pd.isna(sigResult.statistic):

                if sigResult.statistic > 0:
                    sig_idx = 0
                    sig_val = self.convertSigValue(sigResult.pvalue)
                else:
                    sig_idx = 1
                    sig_val = self.convertSigValue(sigResult.pvalue)

        return sig_idx, sig_val


    @staticmethod
    def convertSigValue(pval):
        if pval <= .05:
            return 95
        elif pval <= .1:
            return 90
        elif pval <= .2:
            return 80
        else:
            return None