from app.classes.AP_DataConverter import APDataConverter
from app.routers.Online_Survey.export_online_survey_data_table import DataTableGenerator
import pandas as pd
import numpy as np


class VN8168TvcVncf(APDataConverter, DataTableGenerator):

    def convert_tvc_to_sav(self):
        df_data_output, df_qres_info_output = self.convert_df_mc()

        lst_col_scr = [
            'ID',
            'S1',
            'S1_o3',
            'S2',
            'S3b',
            'S3a',
            'S4',
            'S5_1',
            'S5_2',
            'S5_3',
            'S5_4',
            'S5_5',
            'S5_6',
            'S5_7',
            'S5_8',
            'S6',
            'S6_o15',
            'S7',
            'S8',
            'S9_1',
            'S9_2',
            'S9_3',
            'S9_4',
            'S9_5',
            'S9_6',
            'S9_7',
            'S9_8',
            'S9_9',
            'S9_10',
            'S10_1',
            'S10_2',
            'S10_3',
            'S10_4',
            'S10_5',
            'S10_6',
            'S10_7',
            'S10_o7',
            'S11',
            'S12',
            'S13_1',
            'S13_2',
            'S13_3',
            'S13_4',
            'S13_5',
            'S13_6',
            'S13_7',
            'S13_8',
            'S13_9',
            'S13_10',
            'S13_11',
            'S13_12',
            'S13_o10',
            'S13_o11',
            'S13_o12',
            'S14',
            'get_survey',
            'Q1a',
            'Q1b',
        ]

        lst_col_tvc_1 = [
            'Q2',
            'Q3',
            'Q3a_1',
            'Q3b',
            'Q3a_2',
            'Q4',
            'Q5',
            'Q6_1_01',
            'Q6_1_02',
            'Q6_1_03',
            'Q6_2_01',
            'Q6_2_02',
            'Q6_2_03',
            'Q6_2_04',
            'Q6_2_05',
            'Q6_2_06',
            'Q6_2_07',
        ]

        lst_col_tvc_2 = [
            'O2',
            'O3',
            'O3a_1',
            'O3b',
            'O3a_2',
            'O4',
            'O5',
            'O6_1_01',
            'O6_1_02',
            'O6_1_03',
            'O6_2_01',
            'O6_2_02',
            'O6_2_03',
            'O6_2_04',
            'O6_2_05',
            'O6_2_06',
            'O6_2_07',
        ]

        df_data_tvc_1 = df_data_output.loc[:, lst_col_scr + lst_col_tvc_1].copy()
        df_data_tvc_2 = df_data_output.loc[:, lst_col_scr + lst_col_tvc_2].copy()

        dict_tvc_2_rename = {b: a for a, b in zip(lst_col_tvc_1, lst_col_tvc_2)}
        df_data_tvc_2.rename(columns=dict_tvc_2_rename, inplace=True)

        df_data_tvc_1.insert(len(lst_col_scr), 'Order', [1] * df_data_tvc_1.shape[0])
        df_data_tvc_2.insert(len(lst_col_scr), 'Order', [2] * df_data_tvc_2.shape[0])

        df_data_tvc_1.insert(len(lst_col_scr) + 1, 'TVC_Asked', [2 if a == 6 else a for a in df_data_tvc_1['Q1a']])
        df_data_tvc_2.insert(len(lst_col_scr) + 1, 'TVC_Asked', [np.nan] * df_data_tvc_2.shape[0])

        for idx in df_data_tvc_2.index:

            if df_data_tvc_2.at[idx, 'Q1a'] in [1, 3, 4, 5]:
                df_data_tvc_2.at[idx, 'TVC_Asked'] = 2

            if df_data_tvc_2.at[idx, 'Q1a'] in [2] and df_data_tvc_2.at[idx, 'Q1b'] in [1, 3, 4, 5]:
                df_data_tvc_2.at[idx, 'TVC_Asked'] = df_data_tvc_2.at[idx, 'Q1b']

            if df_data_tvc_2.at[idx, 'Q1a'] in [2] and df_data_tvc_2.at[idx, 'Q1b'] in [6]:
                df_data_tvc_2.at[idx, 'TVC_Asked'] = np.nan

            if df_data_tvc_2.at[idx, 'Q1a'] in [6]:
                df_data_tvc_2.at[idx, 'TVC_Asked'] = np.nan

        df_data_tvc = pd.concat([df_data_tvc_1, df_data_tvc_2], axis=0)

        for i in range(1, 4):
            df_data_tvc[f'Q6_1_0{i}'] = [b if pd.isnull(a) else a for a, b in zip(df_data_tvc[f'Q6_1_0{i}'], df_data_tvc[f'Q6_2_0{i}'])]

        df_data_tvc['Q3a_1'] = [b if pd.isnull(a) else a for a, b in zip(df_data_tvc['Q3a_1'], df_data_tvc['Q3a_2'])]

        df_data_tvc.drop(columns=['Q3a_2', 'Q6_2_01', 'Q6_2_02', 'Q6_2_03'], inplace=True)

        df_data_tvc.rename(columns={'Q3a_1': 'Q3a'}, inplace=True)

        df_data_tvc.sort_values(by=['ID', 'Order'], inplace=True)

        df_data_tvc.reset_index(drop=True, inplace=True)

        df_qres_info_tvc = df_qres_info_output.copy()
        df_qres_info_tvc.loc[df_qres_info_tvc['var_name'] == 'Q3a_1', 'var_name'] = 'Q3a'

        lst_data_tvc_asked = [
            ['Order', 'Order', 'SA', {1: '1st', 2: '2nd'}],
            ['TVC_Asked', 'TVC_Asked', 'SA', {1: 'Quảng cáo 1', 2: 'Quảng cáo 2', 3: 'Quảng cáo 3', 4: 'Quảng cáo 4', 5: 'Quảng cáo 5'}]
        ]

        df_qres_info_tvc = pd.concat([df_qres_info_tvc, pd.DataFrame(columns=['var_name', 'var_lbl', 'var_type', 'val_lbl'], data=lst_data_tvc_asked)], axis=0)

        df_qres_info_tvc['idx_var_name'] = df_qres_info_tvc['var_name']
        df_qres_info_tvc.set_index('idx_var_name', inplace=True)

        df_qres_info_tvc = df_qres_info_tvc.loc[list(df_data_tvc.columns), :]
        df_qres_info_tvc.reset_index(drop=True, inplace=True)

        lst_qre_group = [
            ['Q3', {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}}],
            ['Q3a', {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}}],
            ['Q3b', {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}}],
            ['Q4', {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}}],
            ['Q5', {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}}],
            ['Q6_1_01', {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}}],
            ['Q6_1_02', {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}}],
            ['Q6_1_03', {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}}],
            ['Q6_2_04', {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}}],
            ['Q6_2_05', {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}}],
            ['Q6_2_06', {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}}],
            ['Q6_2_07', {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}}],

        ]

        lst_qre_mean = [
            ['Q3', {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}],
            ['Q3a', {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}],
            ['Q3b', {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}],
            ['Q4', {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}],
            ['Q5', {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}],
            ['Q6_1_01', {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}],
            ['Q6_1_02', {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}],
            ['Q6_1_03', {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}],
            ['Q6_2_04', {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}],
            ['Q6_2_05', {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}],
            ['Q6_2_06', {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}],
            ['Q6_2_07', {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}],
        ]

        DataTableGenerator.__init__(self, df_data=df_data_tvc, df_info=df_qres_info_tvc,
                                    xlsx_name=self.str_file_name.replace('.xlsx', '_Topline.xlsx'),
                                    lst_qre_group=lst_qre_group, lst_qre_mean=lst_qre_mean)

        # self.run_tables(is_standard=True, is_matrix_by_answers=False, is_matrix_by_qres=False)

        lst_func_to_run = [
            {
                'json_file': './app/routers/Online_Survey/tables_standard.json',
                'func_name': 'run_multi_standard_header',
                'tables_to_run': ['T0', 'T1', 'T2'],  # null for FULL tables
            },

        ]
        self.run_tables_by_js_files(lst_func_to_run)

        self.format_table()

        self.generate_sav_sps(df_data=df_data_tvc, df_qres_info=df_qres_info_tvc, is_md=False, is_export_xlsx=True)

        # df_data_tvc.to_csv('zzz_df_data_tvc.csv', encoding='utf-8-sig')
        # df_qres_info_tvc.to_csv('zzz_df_qres_info_tvc.csv', encoding='utf-8-sig')


    def convert_tvc_to_sav_v2(self):
        df_data_output, df_qres_info_output = self.convert_df_mc()

        lst_col_scr = [
            'ID',
            'S1',
            'S1_o3',
            'S2',
            'S3b',
            'S3a',
            'S4',
            'S5_1',
            'S5_2',
            'S5_3',
            'S5_4',
            'S5_5',
            'S5_6',
            'S5_7',
            'S5_8',
            'S6',
            'S6_o15',
            'S7',
            'S8',
            'S9_1',
            'S9_2',
            'S9_3',
            'S9_4',
            'S9_5',
            'S9_6',
            'S9_7',
            'S9_8',
            'S9_9',
            'S9_10',
            'S10_1',
            'S10_2',
            'S10_3',
            'S10_4',
            'S10_5',
            'S10_6',
            'S10_7',
            'S10_o7',
            'S11',
            'S12',
            'S13_1',
            'S13_2',
            'S13_3',
            'S13_4',
            'S13_5',
            'S13_6',
            'S13_7',
            'S13_8',
            'S13_9',
            'S13_10',
            'S13_11',
            'S13_12',
            'S13_o10',
            'S13_o11',
            'S13_o12',
            'S14',
            'get_survey',
            'Q1a',
            'Q1b',
            'Q1c',
            'C1',
        ]

        lst_col_tvc_1 = [
            'QC2a',
            'QC1a',
            'QC3a',
            'QC4a',
            'QC5a',
            'Q2',
            'Q3',
            'Q3a_1',
            'Q3b',
            'Q3a_2',
            'Q4',
            'Q5',
            'Q6_1_01',
            'Q6_1_02',
            'Q6_1_03',
            'Q6_2_01',
            'Q6_2_02',
            'Q6_2_03',
            'Q6_2_04',
            'Q6_2_05',
            'Q6_2_06',
            'Q6_2_07',
        ]

        lst_col_tvc_2 = [
            'QC2b',
            'QC1b',
            'QC3b',
            'QC4b',
            'QC5b',
            'O2',
            'O3',
            'O3a_1',
            'O3b',
            'O3a_2',
            'O4',
            'O5',
            'O6_1_01',
            'O6_1_02',
            'O6_1_03',
            'O6_2_01',
            'O6_2_02',
            'O6_2_03',
            'O6_2_04',
            'O6_2_05',
            'O6_2_06',
            'O6_2_07',
        ]

        df_data_tvc_1 = df_data_output.loc[:, lst_col_scr + lst_col_tvc_1].copy()
        df_data_tvc_2 = df_data_output.loc[:, lst_col_scr + lst_col_tvc_2].copy()

        dict_tvc_2_rename = {b: a for a, b in zip(lst_col_tvc_1, lst_col_tvc_2)}
        df_data_tvc_2.rename(columns=dict_tvc_2_rename, inplace=True)

        df_data_tvc_1.insert(len(lst_col_scr), 'Order', [1] * df_data_tvc_1.shape[0])
        df_data_tvc_2.insert(len(lst_col_scr), 'Order', [2] * df_data_tvc_2.shape[0])

        df_data_tvc_1.insert(len(lst_col_scr) + 1, 'TVC_Asked', [np.nan] * df_data_tvc_1.shape[0])
        df_data_tvc_2.insert(len(lst_col_scr) + 1, 'TVC_Asked', [np.nan] * df_data_tvc_2.shape[0])

        df_data_tvc = pd.concat([df_data_tvc_1, df_data_tvc_2], axis=0)

        df_data_tvc.replace(to_replace={
            'QC1a': {np.nan: 0},
            'QC2a': {np.nan: 0, 1: 2},
            'QC3a': {np.nan: 0, 1: 3},
            'QC4a': {np.nan: 0, 1: 4},
            'QC5a': {np.nan: 0, 1: 5},
        }, inplace=True)

        df_data_tvc['TVC_Asked'] = [a1 + a2 + a3 + a4 + a5 for a1, a2, a3, a4, a5 in zip(df_data_tvc['QC1a'], df_data_tvc['QC2a'], df_data_tvc['QC3a'], df_data_tvc['QC4a'], df_data_tvc['QC5a'])]

        for i in range(1, 4):
            df_data_tvc[f'Q6_1_0{i}'] = [b if pd.isnull(a) else a for a, b in zip(df_data_tvc[f'Q6_1_0{i}'], df_data_tvc[f'Q6_2_0{i}'])]

        df_data_tvc['Q3a_1'] = [b if pd.isnull(a) else a for a, b in zip(df_data_tvc['Q3a_1'], df_data_tvc['Q3a_2'])]

        # ["TVC1_Rank", "TVC2_Rank", "TVC3_Rank", "TVC4_Rank", "TVC5_Rank"]
        for i in range(1, 6):
            df_data_tvc[f'TVC{i}'] = [1 if a == i else (2 if b == i else (3 if c == i else 99)) for a, b, c in zip(df_data_tvc['Q1a'], df_data_tvc['Q1b'], df_data_tvc['Q1c'])]

        df_data_tvc.drop(columns=['QC1a', 'QC2a', 'QC3a', 'QC4a', 'QC5a', 'Q3a_2', 'Q6_2_01', 'Q6_2_02', 'Q6_2_03'], inplace=True)

        df_data_tvc.rename(columns={'Q3a_1': 'Q3a'}, inplace=True)

        df_data_tvc.sort_values(by=['ID', 'Order'], inplace=True)

        df_data_tvc.reset_index(drop=True, inplace=True)

        # Edit df_qres_info_tvc
        df_qres_info_tvc = df_qres_info_output.copy()
        df_qres_info_tvc.loc[df_qres_info_tvc['var_name'] == 'Q3a_1', 'var_name'] = 'Q3a'

        lst_data_tvc_asked = [
            ['Order', 'Order', 'SA', {1: '1st', 2: '2nd'}],
            ['TVC_Asked', 'TVC_Asked', 'SA', {1: 'QC 1', 2: 'QC 2', 3: 'QC 3', 4: 'QC 4', 5: 'QC 5'}],
            ['TVC1', 'TVC Pepsi_Rank', 'SA', {1: '1st', 2: '2nd', 3: '3rd', 99: 'None'}],
            ['TVC2', 'TVC VNCF_Rank', 'SA', {1: '1st', 2: '2nd', 3: '3rd', 99: 'None'}],
            ['TVC3', 'TVC O Long Tea_Rank', 'SA', {1: '1st', 2: '2nd', 3: '3rd', 99: 'None'}],
            ['TVC4', 'TVC Nescafe_Rank', 'SA', {1: '1st', 2: '2nd', 3: '3rd', 99: 'None'}],
            ['TVC5', 'TVC Omachi_Rank', 'SA', {1: '1st', 2: '2nd', 3: '3rd', 99: 'None'}],
        ]

        df_qres_info_tvc = pd.concat([df_qres_info_tvc, pd.DataFrame(columns=['var_name', 'var_lbl', 'var_type', 'val_lbl'], data=lst_data_tvc_asked)], axis=0)

        df_qres_info_tvc['idx_var_name'] = df_qres_info_tvc['var_name']
        df_qres_info_tvc.set_index('idx_var_name', inplace=True)

        df_qres_info_tvc = df_qres_info_tvc.loc[list(df_data_tvc.columns), :]
        df_qres_info_tvc.reset_index(drop=True, inplace=True)

        lst_qre_group = [
            ['Q3', {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}}],
            ['Q3a', {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}}],
            ['Q3b', {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}}],
            ['Q4', {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}}],
            ['Q5', {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}}],
            ['Q6_1_01', {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}}],
            ['Q6_1_02', {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}}],
            ['Q6_1_03', {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}}],
            ['Q6_2_04', {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}}],
            ['Q6_2_05', {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}}],
            ['Q6_2_06', {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}}],
            ['Q6_2_07', {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}}],

        ]

        lst_qre_mean = [
            ['Q3', {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}],
            ['Q3a', {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}],
            ['Q3b', {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}],
            ['Q4', {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}],
            ['Q5', {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}],
            ['Q6_1_01', {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}],
            ['Q6_1_02', {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}],
            ['Q6_1_03', {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}],
            ['Q6_2_04', {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}],
            ['Q6_2_05', {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}],
            ['Q6_2_06', {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}],
            ['Q6_2_07', {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}],
        ]

        DataTableGenerator.__init__(self, df_data=df_data_tvc, df_info=df_qres_info_tvc,
                                    xlsx_name=self.str_file_name.replace('.xlsx', '_Topline.xlsx'),
                                    lst_qre_group=lst_qre_group, lst_qre_mean=lst_qre_mean)

        # self.run_tables(is_standard=True, is_matrix_by_answers=False, is_matrix_by_qres=True)

        lst_func_to_run = [
            {
                'json_file': './app/routers/Online_Survey/tables_standard.json',
                'func_name': 'run_multi_standard_header',
                'tables_to_run': ['T0', 'T1', 'T2'],  # null for FULL tables
            },
            {
                'json_file': './app/routers/Online_Survey/tables_matrix_by_qres.json',
                'func_name': 'run_multi_matrix_header_by_qres',
                'tables_to_run': ['MT01'],  # null for FULL tables
            },
        ]

        self.run_tables_by_js_files(lst_func_to_run)

        self.format_table()

        self.generate_sav_sps(df_data=df_data_tvc, df_qres_info=df_qres_info_tvc, is_md=False, is_export_xlsx=True)