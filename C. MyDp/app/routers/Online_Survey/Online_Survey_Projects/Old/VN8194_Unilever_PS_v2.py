from app.classes.AP_DataConverter import APDataConverter
from app.routers.Online_Survey.export_online_survey_data_table import DataTableGenerator
from app.routers.Online_Survey.calculate_lsm import LSMCalculation
import pandas as pd
import numpy as np


class VN8194UnileverPSv2(APDataConverter, DataTableGenerator, LSMCalculation):

    def convert_unilever_ps_v2(self):
        df_data_output, df_qres_info_output = self.convert_df_mc()

        # LSM calculation
        df_data_output, df_qres_info_output = self.cal_lsm_6(df_data_output, df_qres_info_output)

        # Defines variables---------------------------------------------------------------------------------------------
        dict_qre_OE_info = {}

        lst_addin_OE_value = []

        dict_qre_group_mean = {
            'Q10': {
                'range': [f'0{i}' for i in range(1, 4)],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {1: 2, 2: 2, 4: 1, 5: 1}},
                'mean': {1: 5, 2: 4, 3: 3, 4: 2, 5: 1}
            },
            'Q11': {
                'range': [f'0{i}' for i in range(1, 6)],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {1: 2, 2: 2, 4: 1, 5: 1}},
                'mean': {1: 5, 2: 4, 3: 3, 4: 2, 5: 1}
            },
            'Q12': {
                'range': [f'0{i}' for i in range(1, 6)],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {1: 2, 2: 2, 4: 1, 5: 1}},
                'mean': {1: 5, 2: 4, 3: 3, 4: 2, 5: 1}
            },
            'Q13': {
                'range': [f'0{i}' for i in range(1, 6)],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {1: 2, 2: 2, 4: 1, 5: 1}},
                'mean': {1: 5, 2: 4, 3: 3, 4: 2, 5: 1}
            },

        }
        # END Defines variables-----------------------------------------------------------------------------------------

        if dict_qre_OE_info:
            # ADD OE to df_data_output
            lst_OE_col = list(dict_qre_OE_info.keys())
            df_data_output[lst_OE_col] = pd.DataFrame([[np.nan] * len(lst_OE_col)], index=df_data_output.index)

            for item in lst_addin_OE_value:
                df_data_output.loc[df_data_output['ID'] == item[0], [item[1]]] = [item[2]]

            # ADD OE to df_qres_info_output
            lst_data_addin = list(dict_qre_OE_info.values())
            df_qres_info_output = pd.concat([df_qres_info_output,
                                             pd.DataFrame(columns=['var_name', 'var_lbl', 'var_type', 'val_lbl'],
                                                          data=lst_data_addin)], axis=0)

        # ADD MEAN & GROUP
        lst_qre_mean = list()
        lst_qre_group = list()

        for key, val in dict_qre_group_mean.items():

            if val['range']:
                for i in val['range']:
                    lst_qre_mean.append([f'{key}_{i}', val['mean']])
                    lst_qre_group.append([f'{key}_{i}', val['group']])
            else:
                lst_qre_mean.append([key, val['mean']])
                lst_qre_group.append([key, val['group']])


        # Reset index of df_qres_info_output
        df_qres_info_output['idx_var_name'] = df_qres_info_output['var_name']
        df_qres_info_output.set_index('idx_var_name', inplace=True)
        df_qres_info_output = df_qres_info_output.loc[list(df_data_output.columns), :]
        df_qres_info_output.reset_index(drop=True, inplace=True)


        DataTableGenerator.__init__(self, df_data=df_data_output, df_info=df_qres_info_output,
                                    xlsx_name=self.str_file_name.replace('.xlsx', '_Topline.xlsx'),
                                    lst_qre_group=lst_qre_group, lst_qre_mean=lst_qre_mean)

        lst_func_to_run = [
            {
                'json_file': './app/routers/Online_Survey/tables_standard.json',
                'func_name': 'run_multi_standard_header',
                'tables_to_run': ['T10_v2', 'T11_v2'],
            }
        ]

        self.run_tables_by_js_files(lst_func_to_run)

        self.format_table()

        self.generate_sav_sps(df_data=df_data_output, df_qres_info=df_qres_info_output, is_md=False, is_export_xlsx=True)