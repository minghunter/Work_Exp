from app.classes.AP_DataConverter import APDataConverter
from app.routers.Online_Survey.export_online_survey_data_table import DataTableGenerator
import pandas as pd
import numpy as np

import warnings
warnings.filterwarnings('ignore')


class VN9999TrueID(APDataConverter, DataTableGenerator):

    def convert_vn9999_trueid(self, coding_file):

        df_data_output, df_qres_info_output = self.convert_df_mc()

        # Q6 & Q7 & Q8
        for item in ['Q6', 'Q7', 'Q8']:
            df_data_output[f'{item}_Temp'] = df_data_output[item]


        df_data_output['Q6'].replace({3: 2}, inplace=True)
        df_qres_info_output.loc[df_qres_info_output['var_name'] == 'Q6', ['var_lbl', 'val_lbl']] = ['Are you a True ID User?', {'1': 'Yes', '2': 'No'}]

        df_data_output['Q7'].replace({1: np.nan, 2: np.nan}, inplace=True)
        df_qres_info_output.loc[df_qres_info_output['var_name'] == 'Q7', ['val_lbl']] = [{'3': 'Once month', '4': '1-3 times a week', '5': '4-5 times a week', '6': 'Daily '}]

        df_data_output['Q8'].replace({3: 2}, inplace=True)
        df_qres_info_output.loc[df_qres_info_output['var_name'] == 'Q8', ['val_lbl']] = [{'1': 'Yes', '2': 'No'}]

        # Q17
        df_data_output['Q17'].replace({5: np.nan}, inplace=True)
        df_qres_info_output.loc[df_qres_info_output['var_name'] == 'Q17', ['val_lbl']] = [{'1': 'Less than 5 minutes', '2': '5-15 minutes', '3': '20-45 minutes', '4': 'More than 60 minutes'}]

        # Q1_AgeGroup
        df_data_output['Q1_AgeGroup'] = df_data_output['Q1']
        df_data_output['Q1_AgeGroup'] = [1 if 10 <= a <= 19 else (2 if 20 <= a <= 29 else (3 if 30 <= a <= 39 else (4 if 40 <= a <= 75 else np.nan)))for a in df_data_output['Q1_AgeGroup']]
        df_qres_info_output = df_qres_info_output.append({
            'var_name': 'Q1_AgeGroup',
            'var_lbl': 'Age group',
            'var_type': 'SA',
            'val_lbl': {'1': '10’s', '2': '20’s', '3': '30’s', '4': '40’s and above'}}, ignore_index=True)

        # Clear Q20_Rank[1..11] & Q21 base on Q6 & Q7
        lst_clear = [f'Q20_Rank{a}' for a in range(1, 12)]
        lst_clear.append('Q21')
        for item in lst_clear:
            df_data_output[item] = [qclear if q6 == 1 and q7 >= 1 else np.nan for q6, q7, qclear in zip(df_data_output['Q6'], df_data_output['Q7'], df_data_output[item])]



        # # data csv
        # df_data_output.to_csv('zzzz_df_data_output.csv', encoding='utf-8-sig')
        # df_qres_info_output.to_csv('zzzz_df_qres_info_output.csv', encoding='utf-8-sig')

        dict_qre_group_mean = {
            'Q21': {
                'range': [],
                'group': {'cats': {1: '1', 2: '2', 3: '3'}, 'recode': {1: 1, 2: 2, 3: 3}},
                'mean': {1: 3, 2: 2, 3: 1}
            },
        }

        # ADD MEAN & GROUP----------------------------------------------------------------------------------------------
        print('ADD MEAN & GROUP')

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

        # End ADD MEAN & GROUP------------------------------------------------------------------------------------------

        # Export data tables--------------------------------------------------------------------------------------------
        str_topline_file_name = self.str_file_name.replace('.xlsx', '_Topline.xlsx')

        DataTableGenerator.__init__(self, df_data=df_data_output, df_info=df_qres_info_output, xlsx_name=str_topline_file_name,
                                    lst_qre_group=lst_qre_group, lst_qre_mean=lst_qre_mean)

        lst_func_to_run = [
            {
                'json_file': './app/routers/Online_Survey/tables_standard.json',
                'func_name': 'run_multi_standard_header',
                'tables_to_run': ['TrueID_Data_Table', 'TrueID_Data_Table_count'],
            },
        ]

        self.run_tables_by_js_files(lst_func_to_run)

        self.format_table()

        df_data_output.drop(['Q6_Temp', 'Q7_Temp', 'Q8_Temp', 'Q6_OE'], inplace=True, axis=1)

        df_qres_info_output['idx_var_name'] = df_qres_info_output['var_name']
        df_qres_info_output.set_index('idx_var_name', inplace=True)
        df_qres_info_output = df_qres_info_output.loc[list(df_data_output.columns), :]
        df_qres_info_output.reset_index(drop=True, inplace=True)

        self.generate_sav_sps(df_data=df_data_output, df_qres_info=df_qres_info_output, is_md=False, is_export_xlsx=True)

        # End Export data tables----------------------------------------------------------------------------------------





        # # End Define structure------------------------------------------------------------------------------------------
        #
        # # Data stack format---------------------------------------------------------------------------------------------
        # print('Data stack format')
        #
        # df_data_scr = df_data_output.loc[:, lst_scr].copy()
        # df_data_tag_on = df_data_output.loc[:, ['Q0a_RespondentID'] + lst_tag_on].copy()
        #
        # df_data_sp1_nc = df_data_output.loc[:, ['Q0a_RespondentID'] + list(dict_sp1_nc.keys())].copy()
        # df_data_sp2_nc = df_data_output.loc[:, ['Q0a_RespondentID'] + list(dict_sp2_nc.keys())].copy()
        # df_data_sp3_nc = df_data_output.loc[:, ['Q0a_RespondentID'] + list(dict_sp3_nc.keys())].copy()
        # df_data_sp4_nc = df_data_output.loc[:, ['Q0a_RespondentID'] + list(dict_sp4_nc.keys())].copy()
        # df_data_sp5_nc = df_data_output.loc[:, ['Q0a_RespondentID'] + list(dict_sp5_nc.keys())].copy()
        #
        # df_data_sp1_pl = df_data_output.loc[:, ['Q0a_RespondentID'] + list(dict_sp1_pl.keys())].copy()
        # df_data_sp2_pl = df_data_output.loc[:, ['Q0a_RespondentID'] + list(dict_sp2_pl.keys())].copy()
        # df_data_sp3_pl = df_data_output.loc[:, ['Q0a_RespondentID'] + list(dict_sp3_pl.keys())].copy()
        # df_data_sp4_pl = df_data_output.loc[:, ['Q0a_RespondentID'] + list(dict_sp4_pl.keys())].copy()
        # df_data_sp5_pl = df_data_output.loc[:, ['Q0a_RespondentID'] + list(dict_sp5_pl.keys())].copy()
        #
        # df_data_sp1_nc.rename(columns=dict_sp1_nc, inplace=True)
        # df_data_sp2_nc.rename(columns=dict_sp2_nc, inplace=True)
        # df_data_sp3_nc.rename(columns=dict_sp3_nc, inplace=True)
        # df_data_sp4_nc.rename(columns=dict_sp4_nc, inplace=True)
        # df_data_sp5_nc.rename(columns=dict_sp5_nc, inplace=True)
        #
        # df_data_sp1_pl.rename(columns=dict_sp1_pl, inplace=True)
        # df_data_sp2_pl.rename(columns=dict_sp2_pl, inplace=True)
        # df_data_sp3_pl.rename(columns=dict_sp3_pl, inplace=True)
        # df_data_sp4_pl.rename(columns=dict_sp4_pl, inplace=True)
        # df_data_sp5_pl.rename(columns=dict_sp5_pl, inplace=True)
        #
        # df_data_nc = pd.concat([df_data_sp1_nc, df_data_sp2_nc, df_data_sp3_nc, df_data_sp4_nc, df_data_sp5_nc], axis=0, ignore_index=True)
        # df_data_pl = pd.concat([df_data_sp1_pl, df_data_sp2_pl, df_data_sp3_pl, df_data_sp4_pl, df_data_sp5_pl], axis=0, ignore_index=True)
        #
        # df_data_stack = df_data_nc.merge(df_data_pl, how='left', on=['Q0a_RespondentID', 'Ma_san_pham'])
        # df_data_stack.reset_index(drop=True, inplace=True)
        #
        # df_data_stack.sort_values(by=['Q0a_RespondentID', 'Ma_san_pham'], inplace=True)
        # df_data_stack.reset_index(drop=True, inplace=True)
        #
        # df_data_stack = df_data_scr.merge(df_data_stack, how='right', on=['Q0a_RespondentID'])
        # df_data_stack = df_data_stack.merge(df_data_tag_on, how='left', on=['Q0a_RespondentID'])
        #
        # df_info_stack = df_qres_info_output.copy()
        #
        # for key, val in dict_sp1_nc.items():
        #     df_info_stack.loc[df_info_stack['var_name'] == key, ['var_name']] = [val]
        #
        # for key, val in dict_sp1_pl.items():
        #     df_info_stack.loc[df_info_stack['var_name'] == key, ['var_name']] = [val]
        #
        # df_info_stack.drop_duplicates(subset=['var_name'], keep='first', inplace=True)
        #
        # # if dict_qre_OE_info:
        # #
        # #     # ADD OE to Data stack--------------------------------------------------------------------------------------
        # #     lst_OE_col = list(dict_qre_OE_info.keys())
        # #
        # #     df_data_stack[lst_OE_col] = pd.DataFrame([[np.nan] * len(lst_OE_col)], index=df_data_stack.index)
        # #
        # #     # Remember edit this
        # #     for item in lst_addin_OE_value:
        # #         df_data_stack.loc[(df_data_stack[item[0]] == item[1]) & (df_data_stack[item[2]] == item[3]), [item[4]]] = [item[5]]
        # #
        # #     df_data_stack['Main_Q5a_OE_Ly_do_thich_Y1_1'] = [9999 if pd.isnull(a) else a for a in df_data_stack['Main_Q5a_OE_Ly_do_thich_Y1_1']]
        # #     df_data_stack['Main_Q5b_OE_Ly_do_khong_thich_Y1_1'] = [9999 if pd.isnull(a) else a for a in df_data_stack['Main_Q5b_OE_Ly_do_khong_thich_Y1_1']]
        # #
        # #
        # #     # END ADD OE to Data stack----------------------------------------------------------------------------------
        # #
        # #     # ADD OE to Info stack--------------------------------------------------------------------------------------
        # #     df_info_stack = pd.concat([df_info_stack,
        # #                                pd.DataFrame(columns=['var_name', 'var_lbl', 'var_type', 'val_lbl'],
        # #                                             data=list(dict_qre_OE_info.values()))], axis=0)
        # #     # END ADD OE to Info stack----------------------------------------------------------------------------------
        #
        # # if dict_qre_MA_info:
        # #
        # #     # ADD MA OE to Data stack-----------------------------------------------------------------------------------
        # #     # Remember edit this
        # #     for item in lst_addin_MA_value:
        # #
        # #         idx_item = df_data_stack.loc[(df_data_stack[item[0]] == item[1]) & (df_data_stack[item[2]] == item[3]), item[0]].index[0]
        # #         str_ma_oe_name = item[4].rsplit('_', 1)[0]
        # #         int_ma_oe_code = int(item[4].rsplit('_', 1)[1].replace('o', ''))
        # #
        # #         lst_ma_oe_col = df_info_stack.loc[df_info_stack['var_name'].str.contains(f'{str_ma_oe_name}_[0-9]+'), 'var_name'].values.tolist()
        # #
        # #         is_found = False
        # #         for col in lst_ma_oe_col:
        # #             if df_data_stack.at[idx_item, col] == int_ma_oe_code:
        # #                 is_found = True
        # #                 df_data_stack.at[idx_item, col] = item[5]
        # #                 break
        # #
        # #         if not is_found:
        # #             for col in lst_ma_oe_col:
        # #                 if pd.isnull(df_data_stack.at[idx_item, col]):
        # #                     df_data_stack.at[idx_item, col] = item[5]
        # #                     break
        # #     # END ADD MA OE to Data stack-------------------------------------------------------------------------------
        # #
        # #     # ADD MA OE to Info stack--------------------------------------------------------------------------------------
        # #     for key, val in dict_qre_MA_info.items():
        # #         df_info_stack.loc[df_info_stack['var_name'].str.contains(key), 'val_lbl'] = [val] * df_info_stack.loc[df_info_stack['var_name'].str.contains(key), 'val_lbl'].shape[0]
        # #
        # #     # END ADD MA OE to Info stack----------------------------------------------------------------------------------
        # #
        #
        # # Reset df_info_stack index
        # df_info_stack['idx_var_name'] = df_info_stack['var_name']
        # df_info_stack.set_index('idx_var_name', inplace=True)
        # df_info_stack = df_info_stack.loc[list(df_data_stack.columns), :]
        # df_info_stack.reset_index(drop=True, inplace=True)
        #
        # # df_data_stack.to_csv('zzz_df_data_stack.csv', encoding='utf-8-sig')
        # # df_info_stack.to_csv('zzz_df_info_stack.csv', encoding='utf-8-sig')
        # # End Data stack format-----------------------------------------------------------------------------------------
        #
        # # Data unstack format-------------------------------------------------------------------------------------------
        # print('Data unstack format')
        #
        # dict_sp1 = dict_sp1_nc | dict_sp1_pl
        #
        # lst_data_sp_by_code_col = list()
        # lst_ignore = list()
        #
        # for k, i in dict_sp1.items():
        #
        #     if i == 'Ma_san_pham':
        #         continue
        #
        #     for j in dict_sp_code.values():
        #
        #         var_type = df_qres_info_output.loc[df_qres_info_output['var_name'] == k, 'var_type'].values[0]
        #         val_lbl = df_qres_info_output.loc[df_qres_info_output['var_name'] == k, 'val_lbl'].values[0]
        #
        #         if 'MA' in var_type:
        #             if f'{i.rsplit("_", 1)[0]}_{j}' in lst_ignore:
        #                 continue
        #
        #             for c in val_lbl.keys():
        #                 lst_data_sp_by_code_col.append(f'{i.rsplit("_", 1)[0]}_{j}_{c}')
        #
        #             lst_ignore.append(f'{i.rsplit("_", 1)[0]}_{j}')
        #         else:
        #             lst_data_sp_by_code_col.append(f'{i}_{j}')
        #
        #
        # df_data_sp_by_order_nc = df_data_output.loc[:, ['Q0a_RespondentID'] + list(dict_sp1_nc.keys()) + list(dict_sp2_nc.keys()) + list(dict_sp3_nc.keys()) + list(dict_sp4_nc.keys()) + list(dict_sp5_nc.keys())].copy()
        # df_data_sp_by_order_pl = df_data_output.loc[:, ['Q0a_RespondentID'] + list(dict_sp1_pl.keys()) + list(dict_sp2_pl.keys()) + list(dict_sp3_pl.keys()) + list(dict_sp4_pl.keys()) + list(dict_sp5_pl.keys())].copy()
        #
        # sample_size = df_data_sp_by_order_nc.shape[0]
        #
        # df_data_sp_by_code = pd.DataFrame(data=[[np.nan] * len(lst_data_sp_by_code_col)] * sample_size, columns=lst_data_sp_by_code_col)
        #
        # df_data_sp_by_code.index = df_data_sp_by_order_nc.index
        #
        # df_data_sp_by_code['Q0a_RespondentID'] = df_data_sp_by_order_nc['Q0a_RespondentID']
        #
        # dict_sp_nc = {
        #     1: dict_sp1_nc,
        #     2: dict_sp2_nc,
        #     3: dict_sp3_nc,
        #     4: dict_sp4_nc,
        #     5: dict_sp5_nc,
        # }
        #
        # dict_sp_code_int = {1: '361', 2: '387', 3: '499', 4: '515', 5: '789'}
        #
        # for idx in df_data_sp_by_order_nc.index:
        #     for key, val in dict_sp_nc.items():
        #
        #         ma_sp_code = df_data_sp_by_order_nc.at[idx, f'Main_SP{key}_NC0b_Ma_san_pham']
        #         ma_sp_lbl = dict_sp_code_int[ma_sp_code]
        #
        #         for k_sp, v_sp in val.items():
        #             # k_sp: original name
        #             # v_sp: new name
        #
        #             if pd.isnull(df_data_sp_by_order_nc.at[idx, k_sp]):
        #                 continue
        #
        #             if k_sp == f'Main_SP{key}_NC0b_Ma_san_pham':
        #                 continue
        #
        #             var_type = df_qres_info_output.loc[df_qres_info_output['var_name'] == k_sp, 'var_type'].values[0]
        #
        #             if 'MA' in var_type:
        #                 v_sp_name = v_sp.rsplit('_', 1)[0]
        #                 v_sp_code = v_sp.rsplit('_', 1)[1]
        #
        #                 df_data_sp_by_code.at[idx, f'{v_sp_name}_{ma_sp_lbl}_{v_sp_code}'] = df_data_sp_by_order_nc.at[idx, k_sp]
        #             else:
        #                 df_data_sp_by_code.at[idx, f'{v_sp}_{ma_sp_lbl}'] = df_data_sp_by_order_nc.at[idx, k_sp]
        #
        # dict_sp_pl = {
        #     1: dict_sp1_pl,
        #     2: dict_sp2_pl,
        #     3: dict_sp3_pl,
        #     4: dict_sp4_pl,
        #     5: dict_sp5_pl,
        # }
        #
        # for idx in df_data_sp_by_order_pl.index:
        #     for key, val in dict_sp_pl.items():
        #
        #         ma_sp_code = df_data_sp_by_order_pl.at[idx, f'Main_SP{key}_PL0b_Ma_san_pham']
        #         ma_sp_lbl = dict_sp_code_int[ma_sp_code]
        #
        #         for k_sp, v_sp in val.items():
        #             # k_sp: original name
        #             # v_sp: new name
        #
        #             if pd.isnull(df_data_sp_by_order_pl.at[idx, k_sp]):
        #                 continue
        #
        #             if k_sp == f'Main_SP{key}_PL0b_Ma_san_pham':
        #                 continue
        #
        #             var_type = df_qres_info_output.loc[df_qres_info_output['var_name'] == k_sp, 'var_type'].values[0]
        #
        #             if 'MA' in var_type:
        #                 v_sp_name = v_sp.rsplit('_', 1)[0]
        #                 v_sp_code = v_sp.rsplit('_', 1)[1]
        #
        #                 df_data_sp_by_code.at[idx, f'{v_sp_name}_{ma_sp_lbl}_{v_sp_code}'] = df_data_sp_by_order_pl.at[idx, k_sp]
        #             else:
        #                 df_data_sp_by_code.at[idx, f'{v_sp}_{ma_sp_lbl}'] = df_data_sp_by_order_pl.at[idx, k_sp]
        #
        #
        # df_data_unstack = df_data_scr.merge(df_data_sp_by_code, how='right', on=['Q0a_RespondentID'])
        # df_data_unstack = df_data_unstack.merge(df_data_tag_on, how='left', on=['Q0a_RespondentID'])
        # df_data_unstack.reset_index(drop=True, inplace=True)
        #
        #
        # lst_info_unstack = list()
        # for key, val in dict_sp_nc[1].items():
        #
        #     if 'Ma_san_pham' in val:
        #         continue
        #
        #     # key: original name
        #     # val: new name
        #     for sp_name in dict_sp_code_int.values():
        #
        #         var_name = f'{val}_{sp_name}'
        #         var_lbl = f"{df_qres_info_output.loc[df_qres_info_output['var_name'] == key, 'var_lbl'].values[0]}_{sp_name}"
        #         var_type = df_qres_info_output.loc[df_qres_info_output['var_name'] == key, 'var_type'].values[0]
        #         val_lbl = df_qres_info_output.loc[df_qres_info_output['var_name'] == key, 'val_lbl'].values[0]
        #
        #         if 'MA' in var_type:
        #             val_name = val.rsplit('_', 1)[0]
        #             val_code = val.rsplit('_', 1)[1]
        #             var_name = f'{val_name}_{sp_name}_{val_code}'
        #
        #         lst_info_unstack.append([var_name, var_lbl, var_type, val_lbl])
        #
        #
        # for key, val in dict_sp_pl[1].items():
        #
        #     if 'Ma_san_pham' in val:
        #         continue
        #
        #     # key: original name
        #     # val: new name
        #     for sp_name in dict_sp_code_int.values():
        #
        #         var_name = f'{val}_{sp_name}'
        #         var_lbl = f"{df_qres_info_output.loc[df_qres_info_output['var_name'] == key, 'var_lbl'].values[0]}_{sp_name}"
        #         var_type = df_qres_info_output.loc[df_qres_info_output['var_name'] == key, 'var_type'].values[0]
        #         val_lbl = df_qres_info_output.loc[df_qres_info_output['var_name'] == key, 'val_lbl'].values[0]
        #
        #         if 'MA' in var_type:
        #             val_name = val.rsplit('_', 1)[0]
        #             val_code = val.rsplit('_', 1)[1]
        #             var_name = f'{val_name}_{sp_name}_{val_code}'
        #
        #         lst_info_unstack.append([var_name, var_lbl, var_type, val_lbl])
        #
        #
        # df_qres_info_output_reindex = df_qres_info_output.copy()
        # df_qres_info_output_reindex['idx_var_name'] = df_qres_info_output_reindex['var_name']
        # df_qres_info_output_reindex.set_index('idx_var_name', inplace=True)
        #
        # df_info_unstack = pd.DataFrame(data=lst_info_unstack, columns=['var_name', 'var_lbl', 'var_type', 'val_lbl'])
        # df_info_unstack = pd.concat([df_qres_info_output_reindex.loc[lst_scr, :], df_info_unstack, df_qres_info_output_reindex.loc[lst_tag_on, :]], axis=0)
        #
        # df_info_unstack['idx_var_name'] = df_info_unstack['var_name']
        # df_info_unstack.reindex(list(df_data_unstack.columns))
        #
        # df_info_unstack.reset_index(drop=True, inplace=True)
        #
        # # df_data_unstack.to_csv('zzz_df_data_unstack.csv', encoding='utf-8-sig')
        # # df_info_unstack.to_csv('zzz_df_info_unstack.csv', encoding='utf-8-sig')
        #
        # # End Data unstack format---------------------------------------------------------------------------------------
        #
        #
        # # ADD MEAN & GROUP----------------------------------------------------------------------------------------------
        # print('ADD MEAN & GROUP')
        #
        # lst_qre_mean = list()
        # lst_qre_group = list()
        #
        # if dict_qre_group_mean:
        #     for key, val in dict_qre_group_mean.items():
        #
        #         if val['range']:
        #             for i in val['range']:
        #                 lst_qre_mean.append([f'{key}_{i}', val['mean']])
        #                 lst_qre_group.append([f'{key}_{i}', val['group']])
        #         else:
        #             lst_qre_mean.append([key, val['mean']])
        #             lst_qre_group.append([key, val['group']])
        #
        # # End ADD MEAN & GROUP------------------------------------------------------------------------------------------
        #
        #
        #
        # print('Generate SAV files')
        #
        # # Remove net_code to export sav---------------------------------------------------------------------------------
        # df_info_stack_without_net = df_info_stack.copy()
        #
        # for idx in df_info_stack_without_net.index:
        #     val_lbl = df_info_stack_without_net.at[idx, 'val_lbl']
        #
        #     if 'net_code' in val_lbl.keys():
        #         df_info_stack_without_net.at[idx, 'val_lbl'] = self.unnetted_qre_val(val_lbl)
        # # END Remove net_code to export sav-----------------------------------------------------------------------------
        #
        #
        # # self.generate_sav_sps(df_data=df_data_stack, df_qres_info=df_info_stack_without_net, is_md=False, is_export_xlsx=True)
        #
        # self.generate_sav_sps(df_data=df_data_stack, df_qres_info=df_info_stack_without_net, is_md=False, is_export_xlsx=True,
        #                       df_data_2=df_data_unstack, df_qres_info_2=df_info_unstack)


        print('COMPLETED')
