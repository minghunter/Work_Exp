from app.classes.AP_DataConverter import APDataConverter
from app.routers.Online_Survey.export_online_survey_data_table import DataTableGenerator
from app.routers.Online_Survey.convert_unstack import ConvertUnstack
import pandas as pd
import numpy as np
import time
import traceback
import datetime


class VN8333CharityF1(APDataConverter, DataTableGenerator, ConvertUnstack):

    def convert_vn8333_charity_f1(self, py_script_file, tables_format_file, codelist_file, coding_file):
        """DO NOT EDIT THIS FUNCTION"""

        try:
            start_time = time.time()

            if py_script_file:
                exec(py_script_file.file.read())
                exit()

            self.processing_script_vn8333_charity_f1(tables_format_file, codelist_file, coding_file)

            self.logger.warning('DATA PROCESSING COMPLETED in %d seconds' % (time.time() - start_time))

        except Exception:
            self.logger.error(traceback.format_exc())
            str_err_log_name = self.str_file_name.replace('.xlsx', '_Errors.txt')
            with open(str_err_log_name, 'w') as err_log_txt:
                err_log_txt.writelines(traceback.format_exc())


    def processing_script_vn8333_charity_f1(self, tables_format_file, codelist_file, coding_file):
        """
        :param tables_format_file: txt uploaded table format file
        :param codelist_file: txt uploaded codelist file
        :param coding_file: txt uploaded coding file
        :return: zip file include files: .sav, .sps, .xlsx(rawdata, topline)

        EDIT FROM HERE
        """

        # Convert system rawdata to dataframes--------------------------------------------------------------------------
        self.lstDrop.remove('Date')
        self.lstDrop.remove('Task duration')
        self.lstDrop.extend(['Q41_Respondent_Name', 'Q41_Intervirew_name'])

        new_rows = [
            ['Date', 'DATE', None, 'Date'] + [None] * 27,
            ['Task duration', 'DATE', None, 'Task duration'] + [None] * 27,
        ]

        df_data_output, df_info_output = self.convert_df_mc(lst_new_row=new_rows)

        df_data_output.rename(columns={'Task duration': 'Task_duration'}, inplace=True)
        df_info_output.loc[df_info_output['var_name'] == 'Task duration',  ['var_name']] = ['Task_duration']

        lst_name = [f"Q29_2_{i}" for i in range(1, 11)]
        for col_name in lst_name:
            df_data_output[col_name].replace({9: 10, 10: 9}, inplace=True)

        lst_name = [f"Q29_1_{i}" for i in range(1, 10)] + lst_name
        df_q29_1 = df_data_output[lst_name].copy()
        for col_name in lst_name:
            df_q29_1[col_name].replace({np.nan: 999}, inplace=True)

        lst_q29 = df_q29_1.values.tolist()

        lst_q29_new = list()
        for i in lst_q29:
            lst = list(dict.fromkeys(i))
            lst.sort()
            lst.remove(999)
            if len(lst) > 0 and 9 in lst:
                lst.remove(9)
            lst_q29_new.append(lst)

        df_q29 = pd.DataFrame(lst_q29_new)

        for i in range(1, 11):
            df_data_output.loc[:, [f'Q29_Dummy_{i}']] = [np.nan]

            if i - 1 in df_q29.columns:
                df_data_output.loc[:, f'Q29_Dummy_{i}'] = df_q29.loc[:, i - 1]

        df_data_output.loc[df_data_output['Q29_Dummy_1'].isnull(), ['Q29_Dummy_1']] = [9]

        # df_data_output.to_excel('zzzz_df_data_output.xlsx')

        # Pre processing------------------------------------------------------------------------------------------------
        self.logger.info('Pre processing')

        dict_export_col = {
            'SbjNum': 'ID',
            'SbjNam': 'nan',
            'Duration': 'Task_duration',
            'Status': 'nan',
            'VStart': 'Date',
            'VEnd': 'nan',
            'Q1': 'Q1',
            'Q1_o6': 'Q1_o6',
            'Q2': 'Q2',
            'Q3': 'Q3',
            'Q4': 'Q4',
            'Q5': 'Q5',
            'Q6': 'Q6',
            'Q7': 'Q7',
            'Q8_1': 'Q8_1',
            'Q8_2': 'Q8_2',
            'Q8_3': 'Q8_3',
            'Q8_4': 'Q8_4',
            'Q9_Count': 'nan',
            'Q9_01': 'Q9_01',
            'Q9_02': 'Q9_02',
            'Q9_03': 'Q9_03',
            'Q9_04': 'Q9_04',
            'Q9_05': 'Q9_05',
            'Q9_06': 'Q9_06',
            'Q9_07': 'Q9_07',
            'Q9_08': 'Q9_08',
            'Q9_09': 'Q9_09',
            'Q9_10': 'Q9_10',
            'Q9_11': 'Q9_11',
            'Q9_12': 'Q9_12',
            'Q9_13': 'Q9_13',
            'Q9_14': 'Q9_14',
            'Q9_15': 'Q9_15',
            'Q9_16': 'Q9_16',
            'Q9_17': 'Q9_17',
            'Q9_18': 'Q9_18',
            'Q9_19': 'Q9_19',
            'Q9_20': 'Q9_20',
            'Q9_21': 'Q9_21',
            'Q9_22': 'Q9_22',
            'Q10_1': 'Q10_1',
            'Q10_2': 'Q10_2',
            'Q10_3': 'Q10_3',
            'Q10_o11': 'Q10_o11',
            'Q10_o12': 'Q10_o12',
            'Q10_o13': 'Q10_o13',
            'Q11': 'Q11',
            'Q12': 'Q12',
            'Q13_1': 'Q13_1',
            'Q13_2': 'Q13_2',
            'Q13_3': 'Q13_3',
            'Q13_4': 'Q13_4',
            'Q14': 'Q14',
            'Q15_1': 'Q15_1',
            'Q15_2': 'Q15_2',
            'Q15_3': 'Q15_3',
            'Q15_o7': 'Q15_o7',
            'Q15_o8': 'Q15_o8',
            'Q15_o9': 'Q15_o9',
            'Q16_1': 'Q16_1',
            'Q16_2': 'Q16_2',
            'Q16_3': 'Q16_3',
            'Q16_4': 'Q16_4',
            'Q16_5': 'Q16_5',
            'Q16_6': 'Q16_6',
            'Q16_o4': 'Q16_o4',
            'Q16_o5': 'Q16_o5',
            'Q16_o6': 'Q16_o6',
            'Q17_01': 'Q17_01',
            'Q17_02': 'Q17_02',
            'Q17_03': 'Q17_03',
            'Q18': 'Q18',
            'Q19_1': 'Q19_1',
            'Q19_2': 'Q19_2',
            'Q19_3': 'Q19_3',
            'Q20': 'Q20',
            'Q21': 'Q21',
            'Q22': 'Q22',
            'Q22_o5': 'Q22_o5',
            'Q23': 'Q23',
            'Q24_1': 'Q24_1',
            'Q24_2': 'Q24_2',
            'Q24_3': 'Q24_3',
            'Q24_4': 'Q24_4',
            'Q24_5': 'Q24_5',
            'Q24_6': 'Q24_6',
            'Q24_7': 'Q24_7',
            'Q24_8': 'Q24_8',
            'Q24_o6': 'Q24_o6',
            'Q24_o7': 'Q24_o7',
            'Q24_o8': 'Q24_o8',
            'Q25': 'Q25',
            'Q26': 'Q26',
            'Q27_o2': 'Q27_o2',
            'Q28_1': 'Q28_1',
            'Q28_2': 'Q28_2',
            'Q28_3': 'Q28_3',
            'Q28_4': 'Q28_4',
            'Q28_5': 'Q28_5',
            'Q28_6': 'Q28_6',
            'Q28_7': 'nan',
            'Q28_8': 'nan',
            'Q28_9': 'nan',
            'Q28_o2': 'Q28_o2',
            'Q28_o3': 'Q28_o3',
            'Q28_o4': 'Q28_o4',
            'Q28_o5': 'Q28_o5',
            'Q28_o6': 'Q28_o6',
            'Q28_o7': 'nan',
            'Q28_o8': 'nan',
            'Q28_o9': 'nan',
            'Q28count': 'nan',
            'Q28T_1': 'Q28T_1',
            'Q28T_2': 'Q28T_2',
            'Q28T_3': 'Q28T_3',
            'Q28T_4': 'Q28T_4',
            'Q28T_5': 'Q28T_5',
            'Q28T_6': 'Q28T_6',
            'Q28T_7': 'Q28T_7',
            'Q28T_8': 'Q28T_8',
            'Q28T_9': 'Q28T_9',
            'Q28T_10': 'nan',
            'Q28T_11': 'nan',
            'Q28T_12': 'nan',
            'Q28T_13': 'nan',
            'Q28T_o9': 'Q28T_o9',
            'Q29_1': 'Q29_Dummy_1',
            'Q29_2': 'Q29_Dummy_2',
            'Q29_3': 'Q29_Dummy_3',
            'Q29_4': 'Q29_Dummy_4',
            'Q29_5': 'Q29_Dummy_5',
            'Q29_6': 'Q29_Dummy_6',
            'Q29_7': 'Q29_Dummy_7',
            'Q29_8': 'Q29_Dummy_8',
            'Q30_1': 'Q30_1',
            'Q30_2': 'Q30_2',
            'Q30_3': 'Q30_3',
            'Q30_4': 'Q30_4',
            'Q30_5': 'Q30_5',
            'Q30_6': 'Q30_6',
            'Q30_7': 'Q30_7',
            'Q30_8': 'Q30_8',
            'Q31': 'Q31',
            'Q32_1': 'Q32_1',
            'Q32_2': 'Q32_2',
            'Q32_3': 'Q32_3',
            'Q32_4': 'Q32_4',
            'Q32_5': 'Q32_5',
            'Q32_6': 'Q32_6',
            'Q32_7': 'Q32_7',
            'Q32_8': 'Q32_8',
            'Q32_9': 'Q32_9',
            'Q32_10': 'Q32_10',
            'Q32_11': 'Q32_11',
            'Q32_o11': 'Q32_o11',
            'Q33_1': 'Q33_1',
            'Q33_2': 'Q33_2',
            'Q33_3': 'Q33_3',
            'Q33_4': 'Q33_4',
            'Q33_5': 'Q33_5',
            'Q33_6': 'Q33_6',
            'Q33_7': 'Q33_7',
            'Q33_8': 'Q33_8',
            'Q33_9': 'Q33_9',
            'Q33_10': 'nan',
            'Q33_11': 'nan',
            'Q33_o9': 'Q33_o9',
            'Q33_o10': 'nan',
            'Q33_o11': 'nan',
            'Q34_01_1': 'Q34_01_1',
            'Q34_01_2': 'Q34_01_2',
            'Q34_01_3': 'Q34_01_3',
            'Q34_01_4': 'Q34_01_4',
            'Q34_01_5': 'Q34_01_5',
            'Q34_01_6': 'Q34_01_6',
            'Q34_01_7': 'Q34_01_7',
            'Q34_01_8': 'Q34_01_8',
            'Q34_01_9': 'Q34_01_9',
            'Q34_01_10': 'Q34_01_10',
            'Q34_01_11': 'nan',
            'Q34_01_12': 'nan',
            'Q34_02_1': 'Q34_02_1',
            'Q34_02_2': 'Q34_02_2',
            'Q34_02_3': 'Q34_02_3',
            'Q34_02_4': 'Q34_02_4',
            'Q34_02_5': 'Q34_02_5',
            'Q34_02_6': 'Q34_02_6',
            'Q34_02_7': 'Q34_02_7',
            'Q34_02_8': 'Q34_02_8',
            'Q34_02_9': 'Q34_02_9',
            'Q34_02_10': 'Q34_02_10',
            'Q34_02_11': 'nan',
            'Q34_02_12': 'nan',
            'Q34_03_1': 'Q34_03_1',
            'Q34_03_2': 'Q34_03_2',
            'Q34_03_3': 'Q34_03_3',
            'Q34_03_4': 'Q34_03_4',
            'Q34_03_5': 'Q34_03_5',
            'Q34_03_6': 'Q34_03_6',
            'Q34_03_7': 'Q34_03_7',
            'Q34_03_8': 'Q34_03_8',
            'Q34_03_9': 'Q34_03_9',
            'Q34_03_10': 'Q34_03_10',
            'Q34_03_11': 'nan',
            'Q34_03_12': 'nan',
            'Q34_04_1': 'Q34_04_1',
            'Q34_04_2': 'Q34_04_2',
            'Q34_04_3': 'Q34_04_3',
            'Q34_04_4': 'Q34_04_4',
            'Q34_04_5': 'Q34_04_5',
            'Q34_04_6': 'Q34_04_6',
            'Q34_04_7': 'Q34_04_7',
            'Q34_04_8': 'Q34_04_8',
            'Q34_04_9': 'Q34_04_9',
            'Q34_04_10': 'Q34_04_10',
            'Q34_04_11': 'nan',
            'Q34_04_12': 'nan',
            'Q34_05_1': 'Q34_05_1',
            'Q34_05_2': 'Q34_05_2',
            'Q34_05_3': 'Q34_05_3',
            'Q34_05_4': 'Q34_05_4',
            'Q34_05_5': 'Q34_05_5',
            'Q34_05_6': 'Q34_05_6',
            'Q34_05_7': 'Q34_05_7',
            'Q34_05_8': 'Q34_05_8',
            'Q34_05_9': 'Q34_05_9',
            'Q34_05_10': 'Q34_05_10',
            'Q34_05_11': 'nan',
            'Q34_05_12': 'nan',
            'Q34_06_1': 'Q34_06_1',
            'Q34_06_2': 'Q34_06_2',
            'Q34_06_3': 'Q34_06_3',
            'Q34_06_4': 'Q34_06_4',
            'Q34_06_5': 'Q34_06_5',
            'Q34_06_6': 'Q34_06_6',
            'Q34_06_7': 'Q34_06_7',
            'Q34_06_8': 'Q34_06_8',
            'Q34_06_9': 'Q34_06_9',
            'Q34_06_10': 'Q34_06_10',
            'Q34_06_11': 'nan',
            'Q34_06_12': 'nan',
            'Q34_07_1': 'Q34_07_1',
            'Q34_07_2': 'Q34_07_2',
            'Q34_07_3': 'Q34_07_3',
            'Q34_07_4': 'Q34_07_4',
            'Q34_07_5': 'Q34_07_5',
            'Q34_07_6': 'Q34_07_6',
            'Q34_07_7': 'Q34_07_7',
            'Q34_07_8': 'Q34_07_8',
            'Q34_07_9': 'Q34_07_9',
            'Q34_07_10': 'Q34_07_10',
            'Q34_07_11': 'nan',
            'Q34_07_12': 'nan',
            'Q34_08_1': 'Q34_08_1',
            'Q34_08_2': 'Q34_08_2',
            'Q34_08_3': 'Q34_08_3',
            'Q34_08_4': 'Q34_08_4',
            'Q34_08_5': 'Q34_08_5',
            'Q34_08_6': 'Q34_08_6',
            'Q34_08_7': 'Q34_08_7',
            'Q34_08_8': 'Q34_08_8',
            'Q34_08_9': 'Q34_08_9',
            'Q34_08_10': 'Q34_08_10',
            'Q34_08_11': 'nan',
            'Q34_08_12': 'nan',
            'Q34_09_1': 'Q34_09_1',
            'Q34_09_2': 'Q34_09_2',
            'Q34_09_3': 'Q34_09_3',
            'Q34_09_4': 'Q34_09_4',
            'Q34_09_5': 'Q34_09_5',
            'Q34_09_6': 'Q34_09_6',
            'Q34_09_7': 'Q34_09_7',
            'Q34_09_8': 'Q34_09_8',
            'Q34_09_9': 'Q34_09_9',
            'Q34_09_10': 'Q34_09_10',
            'Q34_09_11': 'nan',
            'Q34_09_12': 'nan',
            'Q34_10_1': 'Q34_10_1',
            'Q34_10_2': 'Q34_10_2',
            'Q34_10_3': 'Q34_10_3',
            'Q34_10_4': 'Q34_10_4',
            'Q34_10_5': 'Q34_10_5',
            'Q34_10_6': 'Q34_10_6',
            'Q34_10_7': 'Q34_10_7',
            'Q34_10_8': 'Q34_10_8',
            'Q34_10_9': 'Q34_10_9',
            'Q34_10_10': 'Q34_10_10',
            'Q34_10_11': 'nan',
            'Q34_10_12': 'nan',
            'Q34_11_1': 'Q34_11_1',
            'Q34_11_2': 'Q34_11_2',
            'Q34_11_3': 'Q34_11_3',
            'Q34_11_4': 'Q34_11_4',
            'Q34_11_5': 'Q34_11_5',
            'Q34_11_6': 'Q34_11_6',
            'Q34_11_7': 'Q34_11_7',
            'Q34_11_8': 'Q34_11_8',
            'Q34_11_9': 'Q34_11_9',
            'Q34_11_10': 'Q34_11_10',
            'Q34_11_11': 'nan',
            'Q34_11_12': 'nan',
            'Q35': 'Q35',
            'Q35a': 'Q35a',
            'Q36_1': 'Q36_1',
            'Q36_2': 'Q36_2',
            'Q36_3': 'Q36_3',
            'Q36_4': 'Q36_4',
            'Q36_5': 'Q36_5',
            'Q36_6': 'Q36_6',
            'Q36_7': 'Q36_7',
            'Q36_8': 'nan',
            'Q36_9': 'nan',
            'Q36_o7': 'Q36_o7',
            'Q36_o8': 'nan',
            'Q36_o9': 'nan',
            'Q37': 'Q37',
            'Q38': 'Q38',
            'Q38_o11': 'Q38_o11',
            'Q39': 'Q39',
        }

        info_col = ['var_name', 'var_lbl', 'var_type', 'val_lbl']
        df_data, df_info = pd.DataFrame(), pd.DataFrame(columns=info_col, data=[])

        for key, val in dict_export_col.items():
            if val == 'nan':
                df_data[key] = [np.nan] * df_data.shape[0]

                if key in ['SbjNam', 'Status']:
                    df_info = pd.concat([df_info, pd.DataFrame(columns=info_col, data=[[key, key, 'FT', {}]])], axis=0)
                elif key in ['VEnd']:
                    df_info = pd.concat([df_info, pd.DataFrame(columns=info_col, data=[[key, key, 'DATE', {}]])], axis=0)
                elif key in ['Q9_Count', 'Q28count']:
                    df_info = pd.concat([df_info, pd.DataFrame(columns=info_col, data=[[key, key, 'NUM', {}]])], axis=0)
                else:
                    qre_name, qre_col = str(key).rsplit('_', 1)
                    if 'o' in qre_col:
                        new_row = df_info_output.loc[df_info_output['var_name'].str.contains(f"{qre_name}_o"), :].values.tolist()[0]
                    else:
                        new_row = df_info_output.loc[df_info_output['var_name'] == f'{qre_name}_1', :].values.tolist()[0]

                    new_row[0] = key
                    df_info = pd.concat([df_info, pd.DataFrame(columns=info_col, data=[new_row])], axis=0)

            else:
                df_data[key] = df_data_output[val]
                new_row = df_info_output.loc[df_info_output['var_name'] == val, :].values.tolist()[0]
                new_row[0] = key
                df_info = pd.concat([df_info, pd.DataFrame(columns=info_col, data=[new_row])], axis=0)

        df_data['VEnd'] = df_data['VStart'].astype('datetime64[ns]') + df_data['Duration'].astype('timedelta64[ns]')
        df_data['Q28count'] = df_data[[f'Q28_{i}' for i in range(1, 10)]].count(axis='columns')

        df_data['Duration_new'] = df_data['Duration'].astype('timedelta64[ns]')
        df_data = df_data.query("datetime.timedelta(minutes=60) > Duration_new > datetime.timedelta(minutes=14)")

        df_data['Q20_new'] = df_data['Q20'].replace({np.nan: 0})
        df_data['Q21_new'] = df_data['Q21'].replace({np.nan: 0})

        df_data['Q20_new'] = df_data['Q20_new'].astype(int)
        df_data['Q21_new'] = df_data['Q21_new'].astype(int)

        df_data = df_data.query("~(Q21_new < Q20_new & Q21_new > 0 & Q20_new > 0)")

        dict_q9_col = {
            'Q9_01': 'Q9_new_01',
            'Q9_02': 'Q9_new_02',
            'Q9_03': 'Q9_new_03',
            'Q9_04': 'Q9_new_04',
            'Q9_05': 'Q9_new_05',
            'Q9_06': 'Q9_new_06',
            'Q9_07': 'Q9_new_07',
            'Q9_08': 'Q9_new_08',
            'Q9_09': 'Q9_new_09',
            'Q9_10': 'Q9_new_10',
            'Q9_11': 'Q9_new_11',
            'Q9_12': 'Q9_new_12',
            'Q9_13': 'Q9_new_13',
            'Q9_14': 'Q9_new_14',
            'Q9_15': 'Q9_new_15',
            'Q9_16': 'Q9_new_16',
            'Q9_17': 'Q9_new_17',
            'Q9_18': 'Q9_new_18',
            'Q9_19': 'Q9_new_19',
            'Q9_20': 'Q9_new_20',
            'Q9_21': 'Q9_new_21',
            'Q9_22': 'Q9_new_22',
        }
        
        df_data['Q9_mean'] = df_data[list(dict_q9_col.keys())].mean(axis='columns')
        # df_data = df_data.query("Q9_mean != Q9_01")
        df_data = df_data.query("~(Q9_01 == Q9_02 == Q9_03 == Q9_04 == Q9_05 == Q9_06 == Q9_07 == Q9_08 == Q9_09 == Q9_10 == Q9_11 == Q9_12 == Q9_13 == Q9_14 == Q9_15 == Q9_16 == Q9_17 == Q9_18 == Q9_19 == Q9_20 == Q9_21 == Q9_22)")

        df_data[list(dict_q9_col.values())] = df_data[list(dict_q9_col.keys())]
        for col in list(dict_q9_col.values()):
            df_data[col].replace({3: np.nan, 4: np.nan, 5: np.nan}, inplace=True)

        df_data['Q9_Count_1_2'] = df_data[list(dict_q9_col.values())].count(axis='columns')
        df_data = df_data.query("Q9_Count_1_2 < 9")


        df_data.drop(columns=['Duration_new', 'Q20_new', 'Q21_new', 'Q9_mean', 'Q9_Count_1_2'] + list(dict_q9_col.values()), inplace=True)

        df_data.reset_index(drop=True, inplace=True)
        df_info.reset_index(drop=True, inplace=True)


        # df_data.to_excel('zzz_df_data.xlsx')
        # df_info.to_excel('zzz_df_info.xlsx')

        # Define structure----------------------------------------------------------------------------------------------
        self.logger.info('Define structure')

        dict_qre_group_mean = {
            # 'Q17': {
            #     'range': [f'0{i}' for i in range(1, 4)],
            #     'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
            #     'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            # },
        }

        # dict_qre_OE_info_org = {'Main_Q3_Cai_thien_New_OE|1-3': ['Q3. lbl', 'MA',  {'net_code': {'999': 'Không có/Không cần', '90001|SỢI PHỞ (NET)': {'101': 'Thêm nội dung về sợi phở/làm nổi bật/nhấn mạnh ý tưởng về sợi phở', '102': 'Được làm từ gạo nếp nương tạo cảm giác thiên nhiên/tự nhiên/sạch/không thuốc', '103': 'Được làm từ gạo nếp nương giúp sợi phở thanh lành (hơn)', '104': 'Được làm từ gạo nếp nương vùng tây bắc ', '105': 'Sợi phở dẻo bùi', '106': 'Sợi phở thơm', '107': 'Sợi phở ngon', '108': 'Đề cập đến độ dai', '109': 'Gạo nếp nương đặc sắc của vùng cao tạo cảm giác thoải mái', '110': 'Gạo nếp nương đặc sắc của vùng cao tạo cảm giác thanh nhẹ', '111': 'Gạo nếp nương đặc sắc của vùng cao tạo cảm giác món phở có vị đậm đà'}, '90002|NGUYÊN LIỆU (NET)': {'201': 'Đề cập chi tiết hơn về các nguyên liệu truyền thống', '202': 'Đề cập về thịt', '203': 'Đông trùng hạ thảo giúp cân bằng cảm xúc'}, '90003|NƯỚC DÙNG (NET)': {'301': 'Được nấu theo công thức của quán phở Thìn Bờ Hồ danh tiếng 70 năm làm cho vị sợi phở ngon hấp dẫn', '302': 'Được nấu từ công thức đặc biệt của quán phở trứ danh của Hà Nội tạo cảm giác hương vị ngon, sợi phở thanh lành', '303': 'Sẽ có/mang lại vị thực tế hơn', '304': 'Được nấu theo công thức quản phở Bờ Hồ lâu đời', '305': 'Thêm nội dung về nước dùng/làm nổi bật/nhấn mạnh ý tưởng về nước dùng', '306': 'Được nấu theo công thức quản phở nổi tiếng'}, '90004|SỢI PHỞ KẾT HỢP NƯỚC DÙNG (NET)': {'401': 'Tạo sự kết hợp ấn tượng giữa sợi phở và nước dùng'}, '90005|KHÁC (NET)': {'601': 'Bổ sung thêm lợi ích/chất dinh dưỡng mang lại cho cơ thể', '610': 'Bổ sung thêm công thức nước phở Thìn đã có ngay trong gói phở ăn liền giúp tiết kiệm thời gian hơn khi ăn ngoài hàng'}, '90006|ĐIỀU CHỈNH NỘI DUNG (NET)': {'602': 'Kết hợp độc đáo đổi thành kết hợp hài hòa sẽ hay hơn', '603': 'Gạo nếp nương của vùng cao đổi thành gạo nếp nương đặc sản của vùng cao', '604': 'Giúp cân bằng cảm xúc ngay cả lúc mệt mỏi, cho cơ thể nhẹ nhàng và tạo nên một lối sống lành mạnh, thư thái đổi thành sản phẩm mang đến sức khỏe tốt cho cơ thể, giúp cơ thể hồi phục sức nhanh'}}}],}
        dict_qre_OE_info_org = dict()
        if codelist_file.filename:
            dict_qre_OE_info_org = eval(codelist_file.file.read())

        dict_qre_OE_info = dict()
        for k, v in dict_qre_OE_info_org.items():
            oe_name, oe_num_col = k.rsplit('|', 1)
            oe_num_col = oe_num_col.split('-')
            oe_num_col = range(int(oe_num_col[0]), int(oe_num_col[1]) + 1)

            for i in oe_num_col:
                dict_qre_OE_info.update({
                    f'{oe_name}_{i}': [f'{oe_name}_{i}'] + v
                })

        # lst_addin_OE_value = [['Q0a_RespondentID', 1056, 'Main_Ma_san_pham_Q3', 1, 'Main_Q5a_OE_Ly_do_thich_Y1_1', 101],]
        lst_addin_OE_value = list()
        if coding_file.filename:
            lst_addin_OE_value = eval(coding_file.file.read())

        # net code for exist qres
        dict_qre_net_info = {
            # 'Main_MA_vi_ngot_Q12_[0-9]+': {
            #     '2': 'Đường mía',
            #     '3': 'Đường cát/ đường trắng',
            #     '4': 'Đường phèn', '5': 'Đường bột',
            #     'net_code': {
            #
            #         '90001|TRÁI CÂY (NET)': {
            #             '1': 'Trái cây',
            #             '13': 'Trái cây, vui lòng ghi rõ loại trái cây',
            #             '201': 'Chanh dây', '202': 'Cam/ chanh/ quýt',
            #         },
            #     }
            # }
        }
        # End Define structure------------------------------------------------------------------------------------------


        # OE RUNNING
        if dict_qre_OE_info:

            # ADD OE to Data stack--------------------------------------------------------------------------------------
            lst_OE_col = list(dict_qre_OE_info.keys())
            df_data[lst_OE_col] = pd.DataFrame([[np.nan] * len(lst_OE_col)], index=df_data.index)

            # Remember edit this
            for item in lst_addin_OE_value:
                df_data.loc[(df_data[item[0]] == item[1]) & (df_data[item[2]] == item[3]), [item[4]]] = [item[5]]

            # END ADD OE to Data stack----------------------------------------------------------------------------------

            # ADD OE to Info stack--------------------------------------------------------------------------------------
            df_info = pd.concat([df_info, pd.DataFrame(columns=['var_name', 'var_lbl', 'var_type', 'val_lbl'], data=list(dict_qre_OE_info.values()))], axis=0)
            # END ADD OE to Info stack----------------------------------------------------------------------------------

        if dict_qre_net_info:
            pass
            # # ADD MA OE to Data stack-----------------------------------------------------------------------------------
            # # Remember edit this
            # for item in lst_addin_MA_value:
            #
            #     idx_item = df_data_stack.loc[(df_data_stack[item[0]] == item[1]) & (df_data_stack[item[2]] == item[3]), item[0]].index[0]
            #     str_ma_oe_name = item[4].rsplit('_', 1)[0]
            #     int_ma_oe_code = int(item[4].rsplit('_', 1)[1].replace('o', ''))
            #
            #     lst_ma_oe_col = df_info_stack.loc[df_info_stack['var_name'].str.contains(f'{str_ma_oe_name}_[0-9]+'), 'var_name'].values.tolist()
            #
            #     is_found = False
            #     for col in lst_ma_oe_col:
            #         if df_data_stack.at[idx_item, col] == int_ma_oe_code:
            #             is_found = True
            #             df_data_stack.at[idx_item, col] = item[5]
            #             break
            #
            #     if not is_found:
            #         for col in lst_ma_oe_col:
            #             if pd.isnull(df_data_stack.at[idx_item, col]):
            #                 df_data_stack.at[idx_item, col] = item[5]
            #                 break
            # # END ADD MA OE to Data stack-------------------------------------------------------------------------------

            # # ADD MA OE to Info stack--------------------------------------------------------------------------------------
            # for key, val in dict_qre_net_info.items():
            #     df_info_stack.loc[df_info_stack['var_name'].str.contains(key), 'val_lbl'] = [val] * df_info_stack.loc[df_info_stack['var_name'].str.contains(key), 'val_lbl'].shape[0]
            # # END ADD MA OE to Info stack----------------------------------------------------------------------------------


        # Export data tables--------------------------------------------------------------------------------------------
        if tables_format_file.filename:

            # ADD MEAN & GROUP------------------------------------------------------------------------------------------
            self.logger.info('ADD MEAN & GROUP')

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

            # End ADD MEAN & GROUP--------------------------------------------------------------------------------------

            str_topline_file_name = self.str_file_name.replace('.xlsx', '_Topline.xlsx')
            DataTableGenerator.__init__(self, df_data=df_data, df_info=df_info,
                                        xlsx_name=str_topline_file_name, logger=self.logger,
                                        lst_qre_group=lst_qre_group, lst_qre_mean=lst_qre_mean, is_md=False)

            lst_func_to_run = eval(tables_format_file.file.read())
            self.run_tables_by_js_files(lst_func_to_run)
            self.format_sig_table()
        # End Export data tables----------------------------------------------------------------------------------------

        # Generate SAV files--------------------------------------------------------------------------------------------
        self.logger.info('Generate SAV files')

        # Remove net_code to export sav---------------------------------------------------------------------------------
        df_info_without_net = df_info.copy()

        for idx in df_info_without_net.index:
            val_lbl = df_info_without_net.at[idx, 'val_lbl']

            if 'net_code' in val_lbl.keys():
                df_info_without_net.at[idx, 'val_lbl'] = self.unnetted_qre_val(val_lbl)
        # END Remove net_code to export sav-----------------------------------------------------------------------------

        dict_dfs = {
            1: {
                'data': df_data,
                'info': df_info_without_net,
                'tail_name': 'code',
                'sheet_name': 'code',
                'is_recode_to_lbl': False,
            },
            2: {
                'data': df_data,
                'info': df_info_without_net,
                'tail_name': 'label',
                'sheet_name': 'label',
                'is_recode_to_lbl': True,
            },
        }

        self.generate_multiple_sav_sps(dict_dfs=dict_dfs, is_md=False, is_export_xlsx=True)
        # END Generate SAV files----------------------------------------------------------------------------------------


