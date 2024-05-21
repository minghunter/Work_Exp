from app.classes.AP_DataConverter import APDataConverter
from app.routers.Online_Survey.export_online_survey_data_table import DataTableGenerator
from app.routers.Online_Survey.convert_unstack import ConvertUnstack
import pandas as pd
import numpy as np
import time



class VN8189Acecook(APDataConverter, DataTableGenerator, ConvertUnstack):

    def convert_vn8189_acecook(self, py_script_file, tables_format_file, codelist_file, coding_file):

        # exec(py_script_file.file.read())

        start_time = time.time()

        df_data_output, df_info_output = self.convert_df_mc()

        self.logger.info('Pre processing')

        dict_fc_yn = {
            'Main_Recall_P100_thich_hon_SP1': ['Force choice', 'SA', {'1': 'Yes', '2': 'No'}, 1],
            'Main_Recall_P100_thich_hon_SP2': ['Force choice', 'SA', {'1': 'Yes', '2': 'No'}, 2],
            'Main_Recall_P100_thich_hon_SP3': ['Force choice', 'SA', {'1': 'Yes', '2': 'No'}, 3],
        }

        for key, val in dict_fc_yn.items():
            df_info_output = pd.concat([df_info_output,
                                        pd.DataFrame(columns=['var_name', 'var_lbl', 'var_type', 'val_lbl'],
                                                     data=[[key, val[0], val[1], val[2]]])], axis=0, ignore_index=True)

            df_data_output = pd.concat([df_data_output, pd.DataFrame(columns=[key], data=[2] * df_data_output.shape[0])], axis=1)
            df_data_output[key] = [1 if a == val[-1] else 2 for a in df_data_output['Main_Recall_P100_thich_hon']]

        df_info_output = pd.concat([df_info_output,
                                    pd.DataFrame(columns=['var_name', 'var_lbl', 'var_type', 'val_lbl'],
                                                 data=[['Main_Recall_P100_thich_hon_by_Ma_SP', 'Force choice (by product code)', 'SA', {'1': 'THÍCH Y22 (HANDY)', '2': 'THÍCH H30 (OMACHI)', '3': 'THÍCH L85 (KOOL)'}]])], axis=0, ignore_index=True)

        for pair in [
            [1, 'Main_P0b_Ma_san_pham'],
            [2, 'Main_Q0b_Ma_san_pham'],
            [3, 'Main_R0b_Ma_san_pham'],
        ]:
            df_fil = df_data_output.query(f"Main_Recall_P100_thich_hon == {pair[0]}")
            df_data_output.loc[df_fil.index, ['Main_Recall_P100_thich_hon_by_Ma_SP']] = df_data_output.loc[df_fil.index, [pair[1]]].values

        lst_combine_oe = [
            ['Main_P1_1a1_OE_Ngoai_quan_thich', 'Main_P1_1a2_OE_Ngoai_quan_thich'],
            ['Main_Q1_1a1_OE_Ngoai_quan_thich', 'Main_Q1_1a2_OE_Ngoai_quan_thich'],
            ['Main_R1_1a1_OE_Ngoai_quan_thich', 'Main_R1_1a2_OE_Ngoai_quan_thich'],
            ['Main_P2a1_OE_Ly_do_thich_o4', 'Main_P2a2_OE_Ly_do_thich_o4'],
            ['Main_P2a1_OE_Ly_do_thich_o5', 'Main_P2a2_OE_Ly_do_thich_o5'],
            ['Main_P2a1_OE_Ly_do_thich_o6', 'Main_P2a2_OE_Ly_do_thich_o6'],
            ['Main_P2a1_OE_Ly_do_thich_o7', 'Main_P2a2_OE_Ly_do_thich_o7'],
            ['Main_P2a1_OE_Ly_do_thich_o8', 'Main_P2a2_OE_Ly_do_thich_o8'],
            ['Main_P2a1_OE_Ly_do_thich_o9', 'Main_P2a2_OE_Ly_do_thich_o9'],
            ['Main_P2a1_OE_Ly_do_thich_o10', 'Main_P2a2_OE_Ly_do_thich_o10'],
            ['Main_P2a1_OE_Ly_do_thich_o11', 'Main_P2a2_OE_Ly_do_thich_o11'],
            ['Main_P2a1_OE_Ly_do_thich_o12', 'Main_P2a2_OE_Ly_do_thich_o12'],
            ['Main_P2a1_OE_Ly_do_thich_o13', 'Main_P2a2_OE_Ly_do_thich_o13'],
            ['Main_P2a1_OE_Ly_do_thich_o14', 'Main_P2a2_OE_Ly_do_thich_o14'],
            ['Main_P2a1_OE_Ly_do_thich_o15', 'Main_P2a2_OE_Ly_do_thich_o15'],
            ['Main_P2a1_OE_Ly_do_thich_o16', 'Main_P2a2_OE_Ly_do_thich_o16'],
            ['Main_P2a1_OE_Ly_do_thich_o17', 'Main_P2a2_OE_Ly_do_thich_o17'],
            ['Main_P2a1_OE_Ly_do_thich_o18', 'Main_P2a2_OE_Ly_do_thich_o18'],
            ['Main_Q2a1_OE_Ly_do_thich_o4', 'Main_Q2a2_OE_Ly_do_thich_o4'],
            ['Main_Q2a1_OE_Ly_do_thich_o5', 'Main_Q2a2_OE_Ly_do_thich_o5'],
            ['Main_Q2a1_OE_Ly_do_thich_o6', 'Main_Q2a2_OE_Ly_do_thich_o6'],
            ['Main_Q2a1_OE_Ly_do_thich_o7', 'Main_Q2a2_OE_Ly_do_thich_o7'],
            ['Main_Q2a1_OE_Ly_do_thich_o8', 'Main_Q2a2_OE_Ly_do_thich_o8'],
            ['Main_Q2a1_OE_Ly_do_thich_o9', 'Main_Q2a2_OE_Ly_do_thich_o9'],
            ['Main_Q2a1_OE_Ly_do_thich_o10', 'Main_Q2a2_OE_Ly_do_thich_o10'],
            ['Main_Q2a1_OE_Ly_do_thich_o11', 'Main_Q2a2_OE_Ly_do_thich_o11'],
            ['Main_Q2a1_OE_Ly_do_thich_o12', 'Main_Q2a2_OE_Ly_do_thich_o12'],
            ['Main_Q2a1_OE_Ly_do_thich_o13', 'Main_Q2a2_OE_Ly_do_thich_o13'],
            ['Main_Q2a1_OE_Ly_do_thich_o14', 'Main_Q2a2_OE_Ly_do_thich_o14'],
            ['Main_Q2a1_OE_Ly_do_thich_o15', 'Main_Q2a2_OE_Ly_do_thich_o15'],
            ['Main_Q2a1_OE_Ly_do_thich_o16', 'Main_Q2a2_OE_Ly_do_thich_o16'],
            ['Main_Q2a1_OE_Ly_do_thich_o17', 'Main_Q2a2_OE_Ly_do_thich_o17'],
            ['Main_Q2a1_OE_Ly_do_thich_o18', 'Main_Q2a2_OE_Ly_do_thich_o18'],
            ['Main_R2a1_OE_Ly_do_thich_o4', 'Main_R2a2_OE_Ly_do_thich_o4'],
            ['Main_R2a1_OE_Ly_do_thich_o5', 'Main_R2a2_OE_Ly_do_thich_o5'],
            ['Main_R2a1_OE_Ly_do_thich_o6', 'Main_R2a2_OE_Ly_do_thich_o6'],
            ['Main_R2a1_OE_Ly_do_thich_o7', 'Main_R2a2_OE_Ly_do_thich_o7'],
            ['Main_R2a1_OE_Ly_do_thich_o8', 'Main_R2a2_OE_Ly_do_thich_o8'],
            ['Main_R2a1_OE_Ly_do_thich_o9', 'Main_R2a2_OE_Ly_do_thich_o9'],
            ['Main_R2a1_OE_Ly_do_thich_o10', 'Main_R2a2_OE_Ly_do_thich_o10'],
            ['Main_R2a1_OE_Ly_do_thich_o11', 'Main_R2a2_OE_Ly_do_thich_o11'],
            ['Main_R2a1_OE_Ly_do_thich_o12', 'Main_R2a2_OE_Ly_do_thich_o12'],
            ['Main_R2a1_OE_Ly_do_thich_o13', 'Main_R2a2_OE_Ly_do_thich_o13'],
            ['Main_R2a1_OE_Ly_do_thich_o14', 'Main_R2a2_OE_Ly_do_thich_o14'],
            ['Main_R2a1_OE_Ly_do_thich_o15', 'Main_R2a2_OE_Ly_do_thich_o15'],
            ['Main_R2a1_OE_Ly_do_thich_o16', 'Main_R2a2_OE_Ly_do_thich_o16'],
            ['Main_R2a1_OE_Ly_do_thich_o17', 'Main_R2a2_OE_Ly_do_thich_o17'],
            ['Main_R2a1_OE_Ly_do_thich_o18', 'Main_R2a2_OE_Ly_do_thich_o18'],
        ]
        for item in lst_combine_oe:
            df_fil = df_data_output.query(f"{item[0]}.isnull() & ~{item[1]}.isnull()").copy()

            if not df_fil.empty:
                df_data_output.loc[df_fil.index, [item[0]]] = df_data_output.loc[df_fil.index, [item[1]]].values
        del df_fil

        dict_recode_scale_qre = {
            'Main_P1_2_OL_Mau_sac': [{'1': 'Do not like totally', '2': 'Do not like', '3': 'Normal', '4': 'Like', '5': 'Very like'}, {1: 3, 2: 4, 3: 5, 4: 1, 5: 2}],
            'Main_P1_3_OL_Do_bong': [{'1': 'Do not like totally', '2': 'Do not like', '3': 'Normal', '4': 'Like', '5': 'Very like'}, {1: 3, 2: 4, 3: 5, 4: 1, 5: 2}],
            'Main_P1_4_OL_Mui_mi_truoc_khi_an': [{'1': 'Do not like totally', '2': 'Do not like', '3': 'Normal', '4': 'Like', '5': 'Very like'}, {1: 3, 2: 4, 3: 5, 4: 1, 5: 2}],
            'Main_P3_OL_Mui_trong_khi_an': [{'1': 'Do not like totally', '2': 'Do not like', '3': 'Normal', '4': 'Like', '5': 'Very like'}, {1: 3, 2: 4, 3: 5, 4: 1, 5: 2}],
            'Main_P5_OL_Hau_vi': [{'1': 'Do not like totally', '2': 'Do not like', '3': 'Normal', '4': 'Like', '5': 'Very like'}, {1: 3, 2: 4, 3: 5, 4: 1, 5: 2}],
            'Main_P7_OL_Topping': [{'1': 'Do not like totally', '2': 'Do not like', '3': 'Normal', '4': 'Like', '5': 'Very like'}, {1: 3, 2: 4, 3: 5, 4: 1, 5: 2}],
            'Main_P10_PI_Y_dinh_mua': [{'1': 'Definitely not purchase', '2': 'Might not purchase', '3': 'Might purchase or not', '4': 'Might purchase', '5': 'Definitely purchase'}, {1: 3, 2: 4, 3: 5, 4: 1, 5: 2}],
        }

        # Define structure----------------------------------------------------------------------------------------------
        self.logger.info('Define structure')

        id_col = 'RespondentID'
        sp_col = 'Ma_SP'

        lst_scr = [
            'Recruit_S1_City',
            'Recruit_S1_City_o5',
            'Recruit_S2_Gender',
            'Recruit_S3_Year',
            'Recruit_S3_Age',
            'Recruit_S4_MHHI',
            'Recruit_S5_Marial_status',
            'Recruit_S6_Occupation',
            'Recruit_S6_Occupation_o12',
            'Recruit_S6_Occupation_o13',
            'Recruit_S6_Occupation_o14',
            'Recruit_S7_Forbidden_1',
            'Recruit_S7_Forbidden_2',
            'Recruit_S7_Forbidden_3',
            'Recruit_S7_Forbidden_4',
            'Recruit_S7_Forbidden_5',
            'Recruit_S7_Forbidden_6',
            'Recruit_S7_Forbidden_7',
            'Recruit_S8_Join_in_P3M',
            'Recruit_S9_Decision_maker',
            'Recruit_S10_Noodle_eating_frequency',
            'Recruit_S11_Mixed_noodle_eating_frequency',
            'Recruit_S12_TOM_noodle_1',
            'Recruit_S12_TOM_noodle_2',
            'Recruit_S12_TOM_noodle_3',
            'Recruit_S12_TOM_noodle_4',
            'Recruit_S12_TOM_noodle_5',
            'Recruit_S12_TOM_noodle_6',
            'Recruit_S12_TOM_noodle_7',
            'Recruit_S12_TOM_noodle_8',
            'Recruit_S12_TOM_noodle_9',
            'Recruit_S12_TOM_noodle_10',
            'Recruit_S12_TOM_noodle_11',
            'Recruit_S12_TOM_noodle_12',
            'Recruit_S12_TOM_noodle_13',
            'Recruit_S12_TOM_noodle_14',
            'Recruit_S12_TOM_noodle_15',
            'Recruit_S12_TOM_noodle_16',
            'Recruit_S12_TOM_noodle_17',
            'Recruit_S12_TOM_noodle_18',
            'Recruit_S12_TOM_noodle_19',
            'Recruit_S12_TOM_noodle_20',
            'Recruit_S12_TOM_noodle_21',
            'Recruit_S12_TOM_noodle_22',
            'Recruit_S12_TOM_noodle_23',
            'Recruit_S12_TOM_noodle_24',
            'Recruit_S12_TOM_noodle_25',
            'Recruit_S12_TOM_noodle_26',
            'Recruit_S12_TOM_noodle_27',
            'Recruit_S12_TOM_noodle_28',
            'Recruit_S12_TOM_noodle_o28',
            'Recruit_S12_TOM_noodle2_1',
            'Recruit_S12_TOM_noodle2_2',
            'Recruit_S12_TOM_noodle2_3',
            'Recruit_S12_TOM_noodle2_4',
            'Recruit_S12_TOM_noodle2_5',
            'Recruit_S12_TOM_noodle2_6',
            'Recruit_S12_TOM_noodle2_7',
            'Recruit_S12_TOM_noodle2_8',
            'Recruit_S12_TOM_noodle2_9',
            'Recruit_S12_TOM_noodle2_10',
            'Recruit_S12_TOM_noodle2_11',
            'Recruit_S12_TOM_noodle2_12',
            'Recruit_S12_TOM_noodle2_13',
            'Recruit_S12_TOM_noodle2_14',
            'Recruit_S12_TOM_noodle2_15',
            'Recruit_S12_TOM_noodle2_16',
            'Recruit_S12_TOM_noodle2_17',
            'Recruit_S12_TOM_noodle2_18',
            'Recruit_S12_TOM_noodle2_19',
            'Recruit_S12_TOM_noodle2_20',
            'Recruit_S12_TOM_noodle2_21',
            'Recruit_S12_TOM_noodle2_22',
            'Recruit_S12_TOM_noodle2_23',
            'Recruit_S12_TOM_noodle2_24',
            'Recruit_S12_TOM_noodle2_25',
            'Recruit_S12_TOM_noodle2_26',
            'Recruit_S12_TOM_noodle2_27',
            'Recruit_S12_TOM_noodle2_28',
            'Recruit_S12_TOM_noodle2_29',
            'Recruit_S12_TOM_noodle2_30',
            'Recruit_S12_TOM_noodle2_o30',
            'Recruit_S13_TOM_bowl_mixed_noodle_1',
            'Recruit_S13_TOM_bowl_mixed_noodle_2',
            'Recruit_S13_TOM_bowl_mixed_noodle_3',
            'Recruit_S13_TOM_bowl_mixed_noodle_4',
            'Recruit_S13_TOM_bowl_mixed_noodle_5',
            'Recruit_S13_TOM_bowl_mixed_noodle_6',
            'Recruit_S13_TOM_bowl_mixed_noodle_7',
            'Recruit_S13_TOM_bowl_mixed_noodle_8',
            'Recruit_S13_TOM_bowl_mixed_noodle_9',
            'Recruit_S13_TOM_bowl_mixed_noodle_10',
            'Recruit_S13_TOM_bowl_mixed_noodle_11',
            'Recruit_S13_TOM_bowl_mixed_noodle_12',
            'Recruit_S13_TOM_bowl_mixed_noodle_13',
            'Recruit_S13_TOM_bowl_mixed_noodle_14',
            'Recruit_S13_TOM_bowl_mixed_noodle_15',
            'Recruit_S13_TOM_bowl_mixed_noodle_16',
            'Recruit_S13_TOM_bowl_mixed_noodle_17',
            'Recruit_S13_TOM_bowl_mixed_noodle_18',
            'Recruit_S13_TOM_bowl_mixed_noodle_19',
            'Recruit_S13_TOM_bowl_mixed_noodle_20',
            'Recruit_S13_TOM_bowl_mixed_noodle_21',
            'Recruit_S13_TOM_bowl_mixed_noodle_22',
            'Recruit_S13_TOM_bowl_mixed_noodle_23',
            'Recruit_S13_TOM_bowl_mixed_noodle_24',
            'Recruit_S13_TOM_bowl_mixed_noodle_25',
            'Recruit_S13_TOM_bowl_mixed_noodle_26',
            'Recruit_S13_TOM_bowl_mixed_noodle_27',
            'Recruit_S13_TOM_bowl_mixed_noodle_28',
            'Recruit_S13_TOM_bowl_mixed_noodle_29',
            'Recruit_S13_TOM_bowl_mixed_noodle_o29',
            'Recruit_S13_TOM_bowl_mixed_noodle2_1',
            'Recruit_S13_TOM_bowl_mixed_noodle2_2',
            'Recruit_S13_TOM_bowl_mixed_noodle2_3',
            'Recruit_S13_TOM_bowl_mixed_noodle2_4',
            'Recruit_S13_TOM_bowl_mixed_noodle2_5',
            'Recruit_S13_TOM_bowl_mixed_noodle2_6',
            'Recruit_S13_TOM_bowl_mixed_noodle2_7',
            'Recruit_S13_TOM_bowl_mixed_noodle2_8',
            'Recruit_S13_TOM_bowl_mixed_noodle2_9',
            'Recruit_S13_TOM_bowl_mixed_noodle2_10',
            'Recruit_S13_TOM_bowl_mixed_noodle2_11',
            'Recruit_S13_TOM_bowl_mixed_noodle2_12',
            'Recruit_S13_TOM_bowl_mixed_noodle2_13',
            'Recruit_S13_TOM_bowl_mixed_noodle2_14',
            'Recruit_S13_TOM_bowl_mixed_noodle2_15',
            'Recruit_S13_TOM_bowl_mixed_noodle2_16',
            'Recruit_S13_TOM_bowl_mixed_noodle2_17',
            'Recruit_S13_TOM_bowl_mixed_noodle2_18',
            'Recruit_S13_TOM_bowl_mixed_noodle2_19',
            'Recruit_S13_TOM_bowl_mixed_noodle2_20',
            'Recruit_S13_TOM_bowl_mixed_noodle2_21',
            'Recruit_S13_TOM_bowl_mixed_noodle2_22',
            'Recruit_S13_TOM_bowl_mixed_noodle2_23',
            'Recruit_S13_TOM_bowl_mixed_noodle2_24',
            'Recruit_S13_TOM_bowl_mixed_noodle2_25',
            'Recruit_S13_TOM_bowl_mixed_noodle2_26',
            'Recruit_S13_TOM_bowl_mixed_noodle2_27',
            'Recruit_S13_TOM_bowl_mixed_noodle2_28',
            'Recruit_S13_TOM_bowl_mixed_noodle2_29',
            'Recruit_S13_TOM_bowl_mixed_noodle2_30',
            'Recruit_S13_TOM_bowl_mixed_noodle2_31',
            'Recruit_S13_TOM_bowl_mixed_noodle2_o31',
            'Recruit_S14_Awareness_1',
            'Recruit_S14_Awareness_2',
            'Recruit_S14_Awareness_3',
            'Recruit_S14_Awareness_4',
            'Recruit_S14_Awareness_5',
            'Recruit_S14_Awareness_6',
            'Recruit_S14_Awareness_7',
            'Recruit_S14_Awareness_8',
            'Recruit_S14_Awareness_9',
            'Recruit_S14_Awareness_10',
            'Recruit_S14_Awareness_11',
            'Recruit_S14_Awareness_12',
            'Recruit_S14_Awareness_13',
            'Recruit_S14_Awareness_14',
            'Recruit_S14_Awareness_15',
            'Recruit_S14_Awareness_16',
            'Recruit_S14_Awareness_17',
            'Recruit_S14_Awareness_18',
            'Recruit_S14_Awareness_19',
            'Recruit_S14_Awareness_20',
            'Recruit_S14_Awareness_21',
            'Recruit_S14_Awareness_22',
            'Recruit_S14_Awareness_23',
            'Recruit_S14_Awareness_24',
            'Recruit_S14_Awareness_25',
            'Recruit_S14_Awareness_26',
            'Recruit_S14_Awareness_27',
            'Recruit_S14_Awareness_28',
            'Recruit_S14_Awareness_o28',
            'Recruit_S15_Brand_usage_P3M_1',
            'Recruit_S15_Brand_usage_P3M_2',
            'Recruit_S15_Brand_usage_P3M_3',
            'Recruit_S15_Brand_usage_P3M_4',
            'Recruit_S15_Brand_usage_P3M_5',
            'Recruit_S15_Brand_usage_P3M_6',
            'Recruit_S15_Brand_usage_P3M_7',
            'Recruit_S15_Brand_usage_P3M_8',
            'Recruit_S15_Brand_usage_P3M_9',
            'Recruit_S15_Brand_usage_P3M_10',
            'Recruit_S15_Brand_usage_P3M_11',
            'Recruit_S15_Brand_usage_P3M_12',
            'Recruit_S15_Brand_usage_P3M_13',
            'Recruit_S15_Brand_usage_P3M_14',
            'Recruit_S15_Brand_usage_P3M_15',
            'Recruit_S15_Brand_usage_P3M_16',
            'Recruit_S15_Brand_usage_P3M_17',
            'Recruit_S15_Brand_usage_P3M_18',
            'Recruit_S15_Brand_usage_P3M_19',
            'Recruit_S15_Brand_usage_P3M_20',
            'Recruit_S15_Brand_usage_P3M_21',
            'Recruit_S15_Brand_usage_P3M_22',
            'Recruit_S15_Brand_usage_P3M_23',
            'Recruit_S15_Brand_usage_P3M_24',
            'Recruit_S15_Brand_usage_P3M_25',
            'Recruit_S15_Brand_usage_P3M_26',
            'Recruit_S15_Brand_usage_P3M_27',
            'Recruit_S15_Brand_usage_P3M_28',
            'Recruit_S15_Brand_usage_P3M_29',
            'Recruit_S15_Brand_usage_P3M_30',
            'Recruit_S15_Brand_usage_P3M_31',
            'Recruit_S15_Brand_usage_P3M_32',
            'Recruit_S15_Brand_usage_P3M_33',
            'Recruit_S15_Brand_usage_P3M_34',
            'Recruit_S15_Brand_usage_P3M_35',
            'Recruit_S15_Brand_usage_P3M_36',
            'Recruit_S15_Brand_usage_P3M_37',
            'Recruit_S15_Brand_usage_P3M_38',
            'Recruit_S15_Brand_usage_P3M_39',
            'Recruit_S15_Brand_usage_P3M_40',
            'Recruit_S15_Brand_usage_P3M_o40',
            'S15a',
            'S15b',
            'S15c',
            'S15d',
            'S15e',
            'S15f',
            'S15g',
            'S15h',
            'S15i',
            'S15j',
            'S15k',
            'S15l',
            'S15m',
            'S15n',
            'Recruit_S16_BUMO',
            'Recruit_S17_BUMO_purchase_factor_1',
            'Recruit_S17_BUMO_purchase_factor_2',
            'Recruit_S17_BUMO_purchase_factor_3',
            'Recruit_S17_BUMO_purchase_factor_4',
            'Recruit_S17_BUMO_purchase_factor_5',
            'Recruit_S17_BUMO_purchase_factor_6',
            'Recruit_S17_BUMO_purchase_factor_7',
            'Recruit_S17_BUMO_purchase_factor_8',
            'Recruit_S17_BUMO_purchase_factor_9',
            'Recruit_S17_BUMO_purchase_factor_10',
            'Recruit_S17_BUMO_purchase_factor_11',
            'Recruit_S17_BUMO_purchase_factor_12',
            'Recruit_S17_BUMO_purchase_factor_13',
            'Recruit_S17_BUMO_purchase_factor_14',
            'Recruit_S17_BUMO_purchase_factor_15',
            'Recruit_S17_BUMO_purchase_factor_16',
            'Recruit_S17_BUMO_purchase_factor_17',
            'Recruit_S17_BUMO_purchase_factor_18',
            'Recruit_S17_BUMO_purchase_factor_19',
            'Recruit_S17_BUMO_purchase_factor_20',
            'Recruit_S17_BUMO_purchase_factor_21',
            'Recruit_S17_BUMO_purchase_factor_22',
            'Recruit_S17_BUMO_purchase_factor_o22',
            'Recruit_S18_SKU_P3M_1',
            'Recruit_S18_SKU_P3M_2',
            'Recruit_S18_SKU_P3M_3',
            'Recruit_S18_SKU_P3M_4',
            'Recruit_S18_SKU_P3M_5',
            'Recruit_S18_SKU_P3M_6',
            'Recruit_S18_SKU_P3M_7',
            'Recruit_S18_SKU_P3M_8',
            'Recruit_S18_SKU_P3M_9',
            'Recruit_S18_SKU_P3M_10',
            'Recruit_S18_SKU_P3M_11',
            'Recruit_S18_SKU_P3M_12',
            'Recruit_S18_SKU_P3M_13',
            'Recruit_S18_SKU_P3M_14',
            'Recruit_S18_SKU_P3M_15',
            'Recruit_S18_SKU_P3M_16',
            'Recruit_S18_SKU_P3M_17',
            'Recruit_S18_SKU_P3M_18',
            'Recruit_S18_SKU_P3M_19',
            'Recruit_S18_SKU_P3M_20',
            'Recruit_S18_SKU_P3M_21',
            'Recruit_S18_SKU_P3M_22',
            'Recruit_S18_SKU_P3M_23',
            'Recruit_S18_SKU_P3M_24',
            'Recruit_S18_SKU_P3M_25',
            'Recruit_S18_SKU_P3M_26',
            'Recruit_S18_SKU_P3M_27',
            'Recruit_S18_SKU_P3M_28',
            'Recruit_S18_SKU_P3M_29',
            'Recruit_S18_SKU_P3M_30',
            'Recruit_S18_SKU_P3M_31',
            'Recruit_S18_SKU_P3M_32',
            'Recruit_S18_SKU_P3M_33',
            'Recruit_S18_SKU_P3M_34',
            'Recruit_S18_SKU_P3M_35',
            'Recruit_S18_SKU_P3M_36',
            'Recruit_S18_SKU_P3M_37',
            'Recruit_S18_SKU_P3M_38',
            'Recruit_S18_SKU_P3M_39',
            'Recruit_S18_SKU_P3M_40',
            'Recruit_S18_SKU_P3M_41',
            'Recruit_S18_SKU_P3M_42',
            'Recruit_S18_SKU_P3M_43',
            'Recruit_S18_SKU_P3M_44',
            'Recruit_S18_SKU_P3M_45',
            'Recruit_S18_SKU_P3M_46',
            'Recruit_S18_SKU_P3M_47',
            'Recruit_S18_SKU_P3M_48',
            'Recruit_S18_SKU_P3M_49',
            'Recruit_S18_SKU_P3M_50',
            'Recruit_S18_SKU_P3M_51',
            'Recruit_S18_SKU_P3M_52',
            'Recruit_S18_SKU_P3M_o52',
            'Recruit_S19_SKU_P1M_1',
            'Recruit_S19_SKU_P1M_2',
            'Recruit_S19_SKU_P1M_3',
            'Recruit_S19_SKU_P1M_4',
            'Recruit_S19_SKU_P1M_5',
            'Recruit_S19_SKU_P1M_6',
            'Recruit_S19_SKU_P1M_7',
            'Recruit_S19_SKU_P1M_8',
            'Recruit_S19_SKU_P1M_9',
            'Recruit_S19_SKU_P1M_10',
            'Recruit_S19_SKU_P1M_11',
            'Recruit_S19_SKU_P1M_12',
            'Recruit_S19_SKU_P1M_13',
            'Recruit_S19_SKU_P1M_14',
            'Recruit_S19_SKU_P1M_15',
            'Recruit_S19_SKU_P1M_16',
            'Recruit_S19_SKU_P1M_17',
            'Recruit_S19_SKU_P1M_18',
            'Recruit_S19_SKU_P1M_19',
            'Recruit_S19_SKU_P1M_20',
            'Recruit_S19_SKU_P1M_21',
            'Recruit_S19_SKU_P1M_22',
            'Recruit_S19_SKU_P1M_23',
            'Recruit_S19_SKU_P1M_24',
            'Recruit_S19_SKU_P1M_25',
            'Recruit_S19_SKU_P1M_26',
            'Recruit_S19_SKU_P1M_27',
            'Recruit_S19_SKU_P1M_28',
            'Recruit_S19_SKU_P1M_29',
            'Recruit_S19_SKU_P1M_30',
            'Recruit_S19_SKU_P1M_31',
            'Recruit_S19_SKU_P1M_32',
            'Recruit_S19_SKU_P1M_33',
            'Recruit_S19_SKU_P1M_34',
            'Recruit_S19_SKU_P1M_35',
            'Recruit_S19_SKU_P1M_36',
            'Recruit_S19_SKU_P1M_37',
            'Recruit_S19_SKU_P1M_38',
            'Recruit_S19_SKU_P1M_39',
            'Recruit_S19_SKU_P1M_40',
            'Recruit_S19_SKU_P1M_41',
            'Recruit_S19_SKU_P1M_42',
            'Recruit_S19_SKU_P1M_43',
            'Recruit_S19_SKU_P1M_44',
            'Recruit_S19_SKU_P1M_45',
            'Recruit_S19_SKU_P1M_46',
            'Recruit_S19_SKU_P1M_47',
            'Recruit_S19_SKU_P1M_48',
            'Recruit_S19_SKU_P1M_49',
            'Recruit_S19_SKU_P1M_50',
            'Recruit_S19_SKU_P1M_51',
            'Recruit_S19_SKU_P1M_52',
            'Recruit_S20_Brand_deny_1',
            'Recruit_S20_Brand_deny_2',
            'Recruit_S20_Brand_deny_3',
            'Recruit_S20_Brand_deny_4',
            'Recruit_S20_Brand_deny_5',
            'Recruit_S20_Brand_deny_6',
            'Recruit_S20_Brand_deny_7',
            'Recruit_S20_Brand_deny_8',
            'Recruit_S20_Brand_deny_9',
            'Recruit_S20_Brand_deny_10',
            'Recruit_S20_Brand_deny_o10',
            'Recruit_S21_HH_start_reason_1',
            'Recruit_S21_HH_start_reason_2',
            'Recruit_S21_HH_start_reason_3',
            'Recruit_S21_HH_start_reason_4',
            'Recruit_S21_HH_start_reason_5',
            'Recruit_S21_HH_start_reason_6',
            'Recruit_S21_HH_start_reason_7',
            'Recruit_S21_HH_start_reason_8',
            'Recruit_S21_HH_start_reason_9',
            'Recruit_S21_HH_start_reason_10',
            'Recruit_S21_HH_start_reason_11',
            'Recruit_S21_HH_start_reason_12',
            'Recruit_S21_HH_start_reason_o12',
            'Recruit_S22_HH_frequency',
            'Recruit_S23_HH_usage_intention',
            'Recruit_S24_HH_increase_intention',
            'Recruit_S25_Other_brands_decrease_01_1',
            'Recruit_S25_Other_brands_decrease_01_2',
            'Recruit_S25_Other_brands_decrease_01_3',
            'Recruit_S25_Other_brands_decrease_01_4',
            'Recruit_S25_Other_brands_decrease_02_1',
            'Recruit_S25_Other_brands_decrease_02_2',
            'Recruit_S25_Other_brands_decrease_02_3',
            'Recruit_S25_Other_brands_decrease_02_4',
            'Recruit_S25_Other_brands_decrease_03_1',
            'Recruit_S25_Other_brands_decrease_03_2',
            'Recruit_S25_Other_brands_decrease_03_3',
            'Recruit_S25_Other_brands_decrease_03_4',
            'Recruit_S25_Other_brands_decrease_04_1',
            'Recruit_S25_Other_brands_decrease_04_2',
            'Recruit_S25_Other_brands_decrease_04_3',
            'Recruit_S25_Other_brands_decrease_04_4',
            'Recruit_S25_Other_brands_decrease_05_1',
            'Recruit_S25_Other_brands_decrease_05_2',
            'Recruit_S25_Other_brands_decrease_05_3',
            'Recruit_S25_Other_brands_decrease_05_4',
            'Recruit_S25_Other_brands_decrease_06_1',
            'Recruit_S25_Other_brands_decrease_06_2',
            'Recruit_S25_Other_brands_decrease_06_3',
            'Recruit_S25_Other_brands_decrease_06_4',
            'Recruit_S25_Other_brands_decrease_07_1',
            'Recruit_S25_Other_brands_decrease_07_2',
            'Recruit_S25_Other_brands_decrease_07_3',
            'Recruit_S25_Other_brands_decrease_07_4',
            'Recruit_S25_Other_brands_decrease_08_1',
            'Recruit_S25_Other_brands_decrease_08_2',
            'Recruit_S25_Other_brands_decrease_08_3',
            'Recruit_S25_Other_brands_decrease_08_4',
            'Recruit_S25_Other_brands_decrease_09_1',
            'Recruit_S25_Other_brands_decrease_09_2',
            'Recruit_S25_Other_brands_decrease_09_3',
            'Recruit_S25_Other_brands_decrease_09_4',
            'Recruit_S25_Other_brands_decrease_10_1',
            'Recruit_S25_Other_brands_decrease_10_2',
            'Recruit_S25_Other_brands_decrease_10_3',
            'Recruit_S25_Other_brands_decrease_10_4',
            'Recruit_S25_Other_brands_decrease_11_1',
            'Recruit_S25_Other_brands_decrease_11_2',
            'Recruit_S25_Other_brands_decrease_11_3',
            'Recruit_S25_Other_brands_decrease_11_4',
            'Recruit_S25_Other_brands_decrease_12_1',
            'Recruit_S25_Other_brands_decrease_12_2',
            'Recruit_S25_Other_brands_decrease_12_3',
            'Recruit_S25_Other_brands_decrease_12_4',
            'Recruit_S25_Other_brands_decrease_13_1',
            'Recruit_S25_Other_brands_decrease_13_2',
            'Recruit_S25_Other_brands_decrease_13_3',
            'Recruit_S25_Other_brands_decrease_13_4',
            'Recruit_S25_Other_brands_decrease_14_1',
            'Recruit_S25_Other_brands_decrease_14_2',
            'Recruit_S25_Other_brands_decrease_14_3',
            'Recruit_S25_Other_brands_decrease_14_4',
            'Recruit_S25_Other_brands_decrease_15_1',
            'Recruit_S25_Other_brands_decrease_15_2',
            'Recruit_S25_Other_brands_decrease_15_3',
            'Recruit_S25_Other_brands_decrease_15_4',
            'Recruit_S25_Other_brands_decrease_16_1',
            'Recruit_S25_Other_brands_decrease_16_2',
            'Recruit_S25_Other_brands_decrease_16_3',
            'Recruit_S25_Other_brands_decrease_16_4',
            'Recruit_S25_Other_brands_decrease_17_1',
            'Recruit_S25_Other_brands_decrease_17_2',
            'Recruit_S25_Other_brands_decrease_17_3',
            'Recruit_S25_Other_brands_decrease_17_4',
            'Recruit_S25_Other_brands_decrease_18_1',
            'Recruit_S25_Other_brands_decrease_18_2',
            'Recruit_S25_Other_brands_decrease_18_3',
            'Recruit_S25_Other_brands_decrease_18_4',
            'Recruit_S25_Other_brands_decrease_19_1',
            'Recruit_S25_Other_brands_decrease_19_2',
            'Recruit_S25_Other_brands_decrease_19_3',
            'Recruit_S25_Other_brands_decrease_19_4',
            'Recruit_S25_Other_brands_decrease_20_1',
            'Recruit_S25_Other_brands_decrease_20_2',
            'Recruit_S25_Other_brands_decrease_20_3',
            'Recruit_S25_Other_brands_decrease_20_4',
            'Recruit_S25_Other_brands_decrease_21_1',
            'Recruit_S25_Other_brands_decrease_21_2',
            'Recruit_S25_Other_brands_decrease_21_3',
            'Recruit_S25_Other_brands_decrease_21_4',
            'Recruit_S25_Other_brands_decrease_22_1',
            'Recruit_S25_Other_brands_decrease_22_2',
            'Recruit_S25_Other_brands_decrease_22_3',
            'Recruit_S25_Other_brands_decrease_22_4',
            'Recruit_S25_Other_brands_decrease_23_1',
            'Recruit_S25_Other_brands_decrease_23_2',
            'Recruit_S25_Other_brands_decrease_23_3',
            'Recruit_S25_Other_brands_decrease_23_4',
            'Recruit_S25_Other_brands_decrease_24_1',
            'Recruit_S25_Other_brands_decrease_24_2',
            'Recruit_S25_Other_brands_decrease_24_3',
            'Recruit_S25_Other_brands_decrease_24_4',
            'Recruit_S25_Other_brands_decrease_25_1',
            'Recruit_S25_Other_brands_decrease_25_2',
            'Recruit_S25_Other_brands_decrease_25_3',
            'Recruit_S25_Other_brands_decrease_25_4',
            'Recruit_S25_Other_brands_decrease_26_1',
            'Recruit_S25_Other_brands_decrease_26_2',
            'Recruit_S25_Other_brands_decrease_26_3',
            'Recruit_S25_Other_brands_decrease_26_4',
            'Recruit_S25_Other_brands_decrease_27_1',
            'Recruit_S25_Other_brands_decrease_27_2',
            'Recruit_S25_Other_brands_decrease_27_3',
            'Recruit_S25_Other_brands_decrease_27_4',
            'Recruit_S25_Other_brands_decrease_28_1',
            'Recruit_S25_Other_brands_decrease_28_2',
            'Recruit_S25_Other_brands_decrease_28_3',
            'Recruit_S25_Other_brands_decrease_28_4',
            'Recruit_S25_Other_brands_decrease_29_1',
            'Recruit_S25_Other_brands_decrease_29_2',
            'Recruit_S25_Other_brands_decrease_29_3',
            'Recruit_S25_Other_brands_decrease_29_4',
            'Recruit_S25_Other_brands_decrease_30_1',
            'Recruit_S25_Other_brands_decrease_30_2',
            'Recruit_S25_Other_brands_decrease_30_3',
            'Recruit_S25_Other_brands_decrease_30_4',
            'Recruit_S25_Other_brands_decrease_31_1',
            'Recruit_S25_Other_brands_decrease_31_2',
            'Recruit_S25_Other_brands_decrease_31_3',
            'Recruit_S25_Other_brands_decrease_31_4',
            'Recruit_S25_Other_brands_decrease_32_1',
            'Recruit_S25_Other_brands_decrease_32_2',
            'Recruit_S25_Other_brands_decrease_32_3',
            'Recruit_S25_Other_brands_decrease_32_4',
            'Recruit_S25_Other_brands_decrease_33_1',
            'Recruit_S25_Other_brands_decrease_33_2',
            'Recruit_S25_Other_brands_decrease_33_3',
            'Recruit_S25_Other_brands_decrease_33_4',
            'Recruit_S25_Other_brands_decrease_34_1',
            'Recruit_S25_Other_brands_decrease_34_2',
            'Recruit_S25_Other_brands_decrease_34_3',
            'Recruit_S25_Other_brands_decrease_34_4',
            'Recruit_S25_Other_brands_decrease_35_1',
            'Recruit_S25_Other_brands_decrease_35_2',
            'Recruit_S25_Other_brands_decrease_35_3',
            'Recruit_S25_Other_brands_decrease_35_4',
            'Recruit_S25_Other_brands_decrease_36_1',
            'Recruit_S25_Other_brands_decrease_36_2',
            'Recruit_S25_Other_brands_decrease_36_3',
            'Recruit_S25_Other_brands_decrease_36_4',
            'Recruit_S25_Other_brands_decrease_37_1',
            'Recruit_S25_Other_brands_decrease_37_2',
            'Recruit_S25_Other_brands_decrease_37_3',
            'Recruit_S25_Other_brands_decrease_37_4',
            'Recruit_S25_Other_brands_decrease_38_1',
            'Recruit_S25_Other_brands_decrease_38_2',
            'Recruit_S25_Other_brands_decrease_38_3',
            'Recruit_S25_Other_brands_decrease_38_4',
            'Recruit_S25_Other_brands_decrease_39_1',
            'Recruit_S25_Other_brands_decrease_39_2',
            'Recruit_S25_Other_brands_decrease_39_3',
            'Recruit_S25_Other_brands_decrease_39_4',
            'Recruit_S25_Other_brands_decrease_40_1',
            'Recruit_S25_Other_brands_decrease_40_2',
            'Recruit_S25_Other_brands_decrease_40_3',
            'Recruit_S25_Other_brands_decrease_40_4',
            'Recruit_S26_HH_increase_reason_1',
            'Recruit_S26_HH_increase_reason_2',
            'Recruit_S26_HH_increase_reason_3',
            'Recruit_S26_HH_increase_reason_4',
            'Recruit_S26_HH_increase_reason_5',
            'Recruit_S26_HH_increase_reason_6',
            'Recruit_S26_HH_increase_reason_7',
            'Recruit_S26_HH_increase_reason_8',
            'Recruit_S26_HH_increase_reason_9',
            'Recruit_S26_HH_increase_reason_10',
            'Recruit_S26_HH_increase_reason_11',
            'Recruit_S26_HH_increase_reason_12',
            'Recruit_S26_HH_increase_reason_o12',
            'Recruit_S27_HH_decrease_reason_1',
            'Recruit_S27_HH_decrease_reason_2',
            'Recruit_S27_HH_decrease_reason_3',
            'Recruit_S27_HH_decrease_reason_4',
            'Recruit_S27_HH_decrease_reason_5',
            'Recruit_S27_HH_decrease_reason_6',
            'Recruit_S27_HH_decrease_reason_7',
            'Recruit_S27_HH_decrease_reason_8',
            'Recruit_S27_HH_decrease_reason_9',
            'Recruit_S27_HH_decrease_reason_10',
            'Recruit_S27_HH_decrease_reason_11',
            'Recruit_S27_HH_decrease_reason_12',
            'Recruit_S27_HH_decrease_reason_o12',
            'Recruit_S28_HH_nonuser',
            'Recruit_S28_HH_nonuser_o2',
            'Recruit_S28_HH_nonuser_o3',
            'Recruit_S29_Cooking_Difficulty',
            'Recruit_S29_Cooking_Difficulty_o2',
            'Recruit_S30_Non_Cooking_Difficulty',
            'Recruit_S30_Non_Cooking_Difficulty_o2',
            'Recruit_S31_Indomie_cooking_method',
            'Recruit_S31_Indomie_cooking_method_o3',
            'Recruit_S32_Indomie_no_cook_reason',
            'Recruit_S33_Purchase_factor_1',
            'Recruit_S33_Purchase_factor_2',
            'Recruit_S33_Purchase_factor_3',
            'Recruit_S33_Purchase_factor_4',
            'Recruit_S33_Purchase_factor_5',
            'Recruit_S33_Purchase_factor_6',
            'Recruit_S33_Purchase_factor_7',
            'Recruit_S33_Purchase_factor_8',
            'Recruit_S33_Purchase_factor_9',
            'Recruit_S33_Purchase_factor_10',
            'Recruit_S33_Purchase_factor_11',
            'Recruit_S33_Purchase_factor_12',
            'Recruit_S33_Purchase_factor_13',
            'Recruit_S33_Purchase_factor_14',
            'Recruit_S33_Purchase_factor_15',
            'Recruit_S33_Purchase_factor_16',
            'Recruit_S33_Purchase_factor_17',
            'Recruit_S33_Purchase_factor_18',
            'Recruit_S33_Purchase_factor_19',
            'Recruit_S33_Purchase_factor_20',
            'Recruit_S33_Purchase_factor_21',
            'Recruit_S33_Purchase_factor_22',
            'Recruit_S33_Purchase_factor_o22',
            'Recruit_S34_Type_usage_1',
            'Recruit_S34_Type_usage_2',
            'Recruit_S34_Type_usage_3',
            'Recruit_S34_Type_usage_4',
            'Recruit_S34_Type_usage_o4',
            'Recruit_S35_Bowl_type_purchase_factor_1',
            'Recruit_S35_Bowl_type_purchase_factor_2',
            'Recruit_S35_Bowl_type_purchase_factor_3',
            'Recruit_S35_Bowl_type_purchase_factor_4',
            'Recruit_S35_Bowl_type_purchase_factor_5',
            'Recruit_S35_Bowl_type_purchase_factor_6',
            'Recruit_S35_Bowl_type_purchase_factor_7',
            'Recruit_S35_Bowl_type_purchase_factor_8',
            'Recruit_S35_Bowl_type_purchase_factor_9',
            'Recruit_S35_Bowl_type_purchase_factor_10',
            'Recruit_S35_Bowl_type_purchase_factor_11',
            'Recruit_S35_Bowl_type_purchase_factor_12',
            'Recruit_S35_Bowl_type_purchase_factor_13',
            'Recruit_S35_Bowl_type_purchase_factor_14',
            'Recruit_S35_Bowl_type_purchase_factor_15',
            'Recruit_S35_Bowl_type_purchase_factor_16',
            'Recruit_S35_Bowl_type_purchase_factor_17',
            'Recruit_S35_Bowl_type_purchase_factor_18',
            'Recruit_S35_Bowl_type_purchase_factor_19',
            'Recruit_S35_Bowl_type_purchase_factor_20',
            'Recruit_S35_Bowl_type_purchase_factor_21',
            'Recruit_S35_Bowl_type_purchase_factor_22',
            'Recruit_S35_Bowl_type_purchase_factor_23',
            'Recruit_S35_Bowl_type_purchase_factor_24',
            'Recruit_S35_Bowl_type_purchase_factor_o24',
            'Recruit_S36_Meal_1',
            'Recruit_S36_Meal_2',
            'Recruit_S36_Meal_3',
            'Recruit_S36_Meal_4',
            'Recruit_S36_Meal_5',
            'Recruit_S36_Meal_6',
            'Recruit_S37_Activity_1',
            'Recruit_S37_Activity_2',
            'Recruit_S37_Activity_3',
            'Recruit_S37_Activity_4',
            'Recruit_S37_Activity_5',
            'Recruit_S37_Activity_6',
            'Recruit_S37_Activity_7',
            'Recruit_S37_Activity_o7',
            'Recruit_S38_Health_1',
            'Recruit_S38_Health_2',
            'Recruit_S38_Health_3',
            'Recruit_S38_Health_4',
            'Recruit_S38_Health_5',
            'Recruit_S38_Health_6',
            'Recruit_S38_Health_7',
            'Recruit_S38_Health_8',
            'Recruit_S38_Health_9',
            'Recruit_S39_Allergy_1',
            'Recruit_S39_Allergy_2',
            'Recruit_S39_Allergy_3',
            'Recruit_S39_Allergy_4',
            'Recruit_S39_Allergy_5',
            'Recruit_S39_Allergy_6',
            'Recruit_S39_Allergy_7',
            'Recruit_S39_Allergy_8',
            'Recruit_S39_Allergy_9',
            'Recruit_S39_Allergy_10',
            'Recruit_S39_Allergy_o10',
            'Recruit_S40_Leave_plan',
        ]

        lst_fc = [
            'Main_Recall_P100_thich_hon',
            'Main_Recall_P100_thich_hon_by_Ma_SP',
            'Main_P100a_OE_Ly_do_thich_hon_o3',
            'Main_P100a_OE_Ly_do_thich_hon_o4',
            'Main_P100a_OE_Ly_do_thich_hon_o5',
            'Main_P100a_OE_Ly_do_thich_hon_o6',
            'Main_P100a_OE_Ly_do_thich_hon_o7',
            'Main_P100a_OE_Ly_do_thich_hon_o8',
            'Main_P100a_OE_Ly_do_thich_hon_o9',
            'Main_P100a_OE_Ly_do_thich_hon_o10',
            'Main_P100a_OE_Ly_do_thich_hon_o11',
            'Main_P100a_OE_Ly_do_thich_hon_o12',
            'Main_P100a_OE_Ly_do_thich_hon_o13',
            'Main_P100a_OE_Ly_do_thich_hon_o14',
            'Main_P100a_OE_Ly_do_thich_hon_o15',
            'Main_P100a_OE_Ly_do_thich_hon_o16',
            'Main_P100a_OE_Ly_do_thich_hon_o17',
            'Main_P11_OL_Bao_bi',
            'Main_P11a_OE_Ly_do_thich_bao_bi',
            'Main_P12a_OE_Bao_bi_Handy_thich',
            'Main_P12b_OE_Bao_bi_Handy_Khong_thich',
        ]

        dict_sp1 = {
            'Main_P0b_Ma_san_pham': 'Ma_SP',
            'Main_P1_1_OL_Ngoai_quan': 'Main_P1_1_OL_Ngoai_quan',
            'Main_P1_1a1_OE_Ngoai_quan_thich': 'Main_P1_1a1_OE_Ngoai_quan_thich',
            'Main_P1_1b_OE_Ngoai_quan_khong_thich': 'Main_P1_1b_OE_Ngoai_quan_khong_thich',
            'Main_P1_2_OL_Mau_sac': 'Main_P1_2_OL_Mau_sac',
            'Main_P1_2_OL_Mau_sac_o4': 'Main_P1_2_OL_Mau_sac_o4',
            'Main_P1_2_OL_Mau_sac_o5': 'Main_P1_2_OL_Mau_sac_o5',
            'Main_P1_3_OL_Do_bong': 'Main_P1_3_OL_Do_bong',
            'Main_P1_3_OL_Do_bong_o4': 'Main_P1_3_OL_Do_bong_o4',
            'Main_P1_3_OL_Do_bong_o5': 'Main_P1_3_OL_Do_bong_o5',
            'Main_P1_4_OL_Mui_mi_truoc_khi_an': 'Main_P1_4_OL_Mui_mi_truoc_khi_an',
            'Main_P1_4_OL_Mui_mi_truoc_khi_an_o4': 'Main_P1_4_OL_Mui_mi_truoc_khi_an_o4',
            'Main_P1_4_OL_Mui_mi_truoc_khi_an_o5': 'Main_P1_4_OL_Mui_mi_truoc_khi_an_o5',
            'Main_P2_OL_noi_chung': 'Main_P2_OL_noi_chung',
            'Main_P2a1_OE_Ly_do_thich_o4': 'Main_P2a1_OE_Ly_do_thich_o4',
            'Main_P2a1_OE_Ly_do_thich_o5': 'Main_P2a1_OE_Ly_do_thich_o5',
            'Main_P2a1_OE_Ly_do_thich_o6': 'Main_P2a1_OE_Ly_do_thich_o6',
            'Main_P2a1_OE_Ly_do_thich_o7': 'Main_P2a1_OE_Ly_do_thich_o7',
            'Main_P2a1_OE_Ly_do_thich_o8': 'Main_P2a1_OE_Ly_do_thich_o8',
            'Main_P2a1_OE_Ly_do_thich_o9': 'Main_P2a1_OE_Ly_do_thich_o9',
            'Main_P2a1_OE_Ly_do_thich_o10': 'Main_P2a1_OE_Ly_do_thich_o10',
            'Main_P2a1_OE_Ly_do_thich_o11': 'Main_P2a1_OE_Ly_do_thich_o11',
            'Main_P2a1_OE_Ly_do_thich_o12': 'Main_P2a1_OE_Ly_do_thich_o12',
            'Main_P2a1_OE_Ly_do_thich_o13': 'Main_P2a1_OE_Ly_do_thich_o13',
            'Main_P2a1_OE_Ly_do_thich_o14': 'Main_P2a1_OE_Ly_do_thich_o14',
            'Main_P2a1_OE_Ly_do_thich_o15': 'Main_P2a1_OE_Ly_do_thich_o15',
            'Main_P2a1_OE_Ly_do_thich_o16': 'Main_P2a1_OE_Ly_do_thich_o16',
            'Main_P2a1_OE_Ly_do_thich_o17': 'Main_P2a1_OE_Ly_do_thich_o17',
            'Main_P2a1_OE_Ly_do_thich_o18': 'Main_P2a1_OE_Ly_do_thich_o18',
            'Main_P2b_OE_Ly_do_khong_thich_o4': 'Main_P2b_OE_Ly_do_khong_thich_o4',
            'Main_P2b_OE_Ly_do_khong_thich_o5': 'Main_P2b_OE_Ly_do_khong_thich_o5',
            'Main_P2b_OE_Ly_do_khong_thich_o6': 'Main_P2b_OE_Ly_do_khong_thich_o6',
            'Main_P2b_OE_Ly_do_khong_thich_o7': 'Main_P2b_OE_Ly_do_khong_thich_o7',
            'Main_P2b_OE_Ly_do_khong_thich_o8': 'Main_P2b_OE_Ly_do_khong_thich_o8',
            'Main_P2b_OE_Ly_do_khong_thich_o9': 'Main_P2b_OE_Ly_do_khong_thich_o9',
            'Main_P2b_OE_Ly_do_khong_thich_o10': 'Main_P2b_OE_Ly_do_khong_thich_o10',
            'Main_P2b_OE_Ly_do_khong_thich_o11': 'Main_P2b_OE_Ly_do_khong_thich_o11',
            'Main_P2b_OE_Ly_do_khong_thich_o12': 'Main_P2b_OE_Ly_do_khong_thich_o12',
            'Main_P2b_OE_Ly_do_khong_thich_o13': 'Main_P2b_OE_Ly_do_khong_thich_o13',
            'Main_P2b_OE_Ly_do_khong_thich_o14': 'Main_P2b_OE_Ly_do_khong_thich_o14',
            'Main_P2b_OE_Ly_do_khong_thich_o15': 'Main_P2b_OE_Ly_do_khong_thich_o15',
            'Main_P2b_OE_Ly_do_khong_thich_o16': 'Main_P2b_OE_Ly_do_khong_thich_o16',
            'Main_P2b_OE_Ly_do_khong_thich_o17': 'Main_P2b_OE_Ly_do_khong_thich_o17',
            'Main_P2b_OE_Ly_do_khong_thich_o18': 'Main_P2b_OE_Ly_do_khong_thich_o18',
            'Main_P3_OL_Mui_trong_khi_an': 'Main_P3_OL_Mui_trong_khi_an',
            'Main_P3_OL_Mui_trong_khi_an_o4': 'Main_P3_OL_Mui_trong_khi_an_o4',
            'Main_P3_OL_Mui_trong_khi_an_o5': 'Main_P3_OL_Mui_trong_khi_an_o5',
            'Main_P4_OL_Vi_noi_chung': 'Main_P4_OL_Vi_noi_chung',
            'Main_P4a_JAR_Vi_man': 'Main_P4a_JAR_Vi_man',
            'Main_P4b_JAR_Vi_ngot': 'Main_P4b_JAR_Vi_ngot',
            'Main_P4c_JAR_Vi_chua': 'Main_P4c_JAR_Vi_chua',
            'Main_P4d_JAR_Vi_cay': 'Main_P4d_JAR_Vi_cay',
            'Main_P4e_JAR_Vi_beo': 'Main_P4e_JAR_Vi_beo',
            'Main_P5_OL_Hau_vi': 'Main_P5_OL_Hau_vi',
            'Main_P5_OL_Hau_vi_o4': 'Main_P5_OL_Hau_vi_o4',
            'Main_P5_OL_Hau_vi_o5': 'Main_P5_OL_Hau_vi_o5',
            'Main_P6_Do_hoa_quyen': 'Main_P6_Do_hoa_quyen',
            'Main_P7_OL_Topping': 'Main_P7_OL_Topping',
            'Main_P7_OL_Topping_o4': 'Main_P7_OL_Topping_o4',
            'Main_P7_OL_Topping_o5': 'Main_P7_OL_Topping_o5',
            'Main_P8_OL_Soi_mi': 'Main_P8_OL_Soi_mi',
            'Main_P8a_JAR_Kich_thuoc_soi_mi': 'Main_P8a_JAR_Kich_thuoc_soi_mi',
            'Main_P8b_JAR_Do_dai_soi_mi': 'Main_P8b_JAR_Do_dai_soi_mi',
            'Main_P9_JAR_Luong_mi': 'Main_P9_JAR_Luong_mi',
            'Main_P10_PI_Y_dinh_mua': 'Main_P10_PI_Y_dinh_mua',
            'Main_P10_PI_Y_dinh_mua_o4': 'Main_P10_PI_Y_dinh_mua_o4',
            'Main_P10_PI_Y_dinh_mua_o5': 'Main_P10_PI_Y_dinh_mua_o5',
            'Main_P10a_PI_Y_dinh_mua_1': 'Main_P10a_PI_Y_dinh_mua_1',
            'Main_Recall_P100_thich_hon_SP1': 'Main_Recall_P100_thich_hon_YN',
        }

        dict_sp2 = {
            'Main_Q0b_Ma_san_pham': 'Ma_SP',
            'Main_Q1_1_OL_Ngoai_quan': 'Main_P1_1_OL_Ngoai_quan',
            'Main_Q1_1a1_OE_Ngoai_quan_thich': 'Main_P1_1a1_OE_Ngoai_quan_thich',
            'Main_Q1_1b_OE_Ngoai_quan_khong_thich': 'Main_P1_1b_OE_Ngoai_quan_khong_thich',
            'Main_Q1_2_OL_Mau_sac': 'Main_P1_2_OL_Mau_sac',
            'Main_Q1_2_OL_Mau_sac_o4': 'Main_P1_2_OL_Mau_sac_o4',
            'Main_Q1_2_OL_Mau_sac_o5': 'Main_P1_2_OL_Mau_sac_o5',
            'Main_Q1_3_OL_Do_bong': 'Main_P1_3_OL_Do_bong',
            'Main_Q1_3_OL_Do_bong_o4': 'Main_P1_3_OL_Do_bong_o4',
            'Main_Q1_3_OL_Do_bong_o5': 'Main_P1_3_OL_Do_bong_o5',
            'Main_Q1_4_OL_Mui_mi_truoc_khi_an': 'Main_P1_4_OL_Mui_mi_truoc_khi_an',
            'Main_Q1_4_OL_Mui_mi_truoc_khi_an_o4': 'Main_P1_4_OL_Mui_mi_truoc_khi_an_o4',
            'Main_Q1_4_OL_Mui_mi_truoc_khi_an_o5': 'Main_P1_4_OL_Mui_mi_truoc_khi_an_o5',
            'Main_Q2_OL_noi_chung': 'Main_P2_OL_noi_chung',
            'Main_Q2a1_OE_Ly_do_thich_o4': 'Main_P2a1_OE_Ly_do_thich_o4',
            'Main_Q2a1_OE_Ly_do_thich_o5': 'Main_P2a1_OE_Ly_do_thich_o5',
            'Main_Q2a1_OE_Ly_do_thich_o6': 'Main_P2a1_OE_Ly_do_thich_o6',
            'Main_Q2a1_OE_Ly_do_thich_o7': 'Main_P2a1_OE_Ly_do_thich_o7',
            'Main_Q2a1_OE_Ly_do_thich_o8': 'Main_P2a1_OE_Ly_do_thich_o8',
            'Main_Q2a1_OE_Ly_do_thich_o9': 'Main_P2a1_OE_Ly_do_thich_o9',
            'Main_Q2a1_OE_Ly_do_thich_o10': 'Main_P2a1_OE_Ly_do_thich_o10',
            'Main_Q2a1_OE_Ly_do_thich_o11': 'Main_P2a1_OE_Ly_do_thich_o11',
            'Main_Q2a1_OE_Ly_do_thich_o12': 'Main_P2a1_OE_Ly_do_thich_o12',
            'Main_Q2a1_OE_Ly_do_thich_o13': 'Main_P2a1_OE_Ly_do_thich_o13',
            'Main_Q2a1_OE_Ly_do_thich_o14': 'Main_P2a1_OE_Ly_do_thich_o14',
            'Main_Q2a1_OE_Ly_do_thich_o15': 'Main_P2a1_OE_Ly_do_thich_o15',
            'Main_Q2a1_OE_Ly_do_thich_o16': 'Main_P2a1_OE_Ly_do_thich_o16',
            'Main_Q2a1_OE_Ly_do_thich_o17': 'Main_P2a1_OE_Ly_do_thich_o17',
            'Main_Q2a1_OE_Ly_do_thich_o18': 'Main_P2a1_OE_Ly_do_thich_o18',
            'Main_Q2b_OE_Ly_do_khong_thich_o4': 'Main_P2b_OE_Ly_do_khong_thich_o4',
            'Main_Q2b_OE_Ly_do_khong_thich_o5': 'Main_P2b_OE_Ly_do_khong_thich_o5',
            'Main_Q2b_OE_Ly_do_khong_thich_o6': 'Main_P2b_OE_Ly_do_khong_thich_o6',
            'Main_Q2b_OE_Ly_do_khong_thich_o7': 'Main_P2b_OE_Ly_do_khong_thich_o7',
            'Main_Q2b_OE_Ly_do_khong_thich_o8': 'Main_P2b_OE_Ly_do_khong_thich_o8',
            'Main_Q2b_OE_Ly_do_khong_thich_o9': 'Main_P2b_OE_Ly_do_khong_thich_o9',
            'Main_Q2b_OE_Ly_do_khong_thich_o10': 'Main_P2b_OE_Ly_do_khong_thich_o10',
            'Main_Q2b_OE_Ly_do_khong_thich_o11': 'Main_P2b_OE_Ly_do_khong_thich_o11',
            'Main_Q2b_OE_Ly_do_khong_thich_o12': 'Main_P2b_OE_Ly_do_khong_thich_o12',
            'Main_Q2b_OE_Ly_do_khong_thich_o13': 'Main_P2b_OE_Ly_do_khong_thich_o13',
            'Main_Q2b_OE_Ly_do_khong_thich_o14': 'Main_P2b_OE_Ly_do_khong_thich_o14',
            'Main_Q2b_OE_Ly_do_khong_thich_o15': 'Main_P2b_OE_Ly_do_khong_thich_o15',
            'Main_Q2b_OE_Ly_do_khong_thich_o16': 'Main_P2b_OE_Ly_do_khong_thich_o16',
            'Main_Q2b_OE_Ly_do_khong_thich_o17': 'Main_P2b_OE_Ly_do_khong_thich_o17',
            'Main_Q2b_OE_Ly_do_khong_thich_o18': 'Main_P2b_OE_Ly_do_khong_thich_o18',
            'Main_Q3_OL_Mui_trong_khi_an': 'Main_P3_OL_Mui_trong_khi_an',
            'Main_Q3_OL_Mui_trong_khi_an_o4': 'Main_P3_OL_Mui_trong_khi_an_o4',
            'Main_Q3_OL_Mui_trong_khi_an_o5': 'Main_P3_OL_Mui_trong_khi_an_o5',
            'Main_Q4_OL_Vi_noi_chung': 'Main_P4_OL_Vi_noi_chung',
            'Main_Q4a_JAR_Vi_man': 'Main_P4a_JAR_Vi_man',
            'Main_Q4b_JAR_Vi_ngot': 'Main_P4b_JAR_Vi_ngot',
            'Main_Q4c_JAR_Vi_chua': 'Main_P4c_JAR_Vi_chua',
            'Main_Q4d_JAR_Vi_cay': 'Main_P4d_JAR_Vi_cay',
            'Main_Q4e_JAR_Vi_beo': 'Main_P4e_JAR_Vi_beo',
            'Main_Q5_OL_Hau_vi': 'Main_P5_OL_Hau_vi',
            'Main_Q5_OL_Hau_vi_o4': 'Main_P5_OL_Hau_vi_o4',
            'Main_Q5_OL_Hau_vi_o5': 'Main_P5_OL_Hau_vi_o5',
            'Main_Q6_Do_hoa_quyen': 'Main_P6_Do_hoa_quyen',
            'Main_Q7_OL_Topping': 'Main_P7_OL_Topping',
            'Main_Q7_OL_Topping_o4': 'Main_P7_OL_Topping_o4',
            'Main_Q7_OL_Topping_o5': 'Main_P7_OL_Topping_o5',
            'Main_Q8_OL_Soi_mi': 'Main_P8_OL_Soi_mi',
            'Main_Q8a_JAR_Kich_thuoc_soi_mi': 'Main_P8a_JAR_Kich_thuoc_soi_mi',
            'Main_Q8b_JAR_Do_dai_soi_mi': 'Main_P8b_JAR_Do_dai_soi_mi',
            'Main_Q9_JAR_Luong_mi': 'Main_P9_JAR_Luong_mi',
            'Main_Q10_PI_Y_dinh_mua': 'Main_P10_PI_Y_dinh_mua',
            'Main_Q10_PI_Y_dinh_mua_o4': 'Main_P10_PI_Y_dinh_mua_o4',
            'Main_Q10_PI_Y_dinh_mua_o5': 'Main_P10_PI_Y_dinh_mua_o5',
            'Main_Q10a_PI_Y_dinh_mua_1': 'Main_P10a_PI_Y_dinh_mua_1',
            'Main_Recall_P100_thich_hon_SP2': 'Main_Recall_P100_thich_hon_YN',
        }

        dict_sp3 = {
            'Main_R0b_Ma_san_pham': 'Ma_SP',
            'Main_R1_1_OL_Ngoai_quan': 'Main_P1_1_OL_Ngoai_quan',
            'Main_R1_1a1_OE_Ngoai_quan_thich': 'Main_P1_1a1_OE_Ngoai_quan_thich',
            'Main_R1_1b_OE_Ngoai_quan_khong_thich': 'Main_P1_1b_OE_Ngoai_quan_khong_thich',
            'Main_R1_2_OL_Mau_sac': 'Main_P1_2_OL_Mau_sac',
            'Main_R1_2_OL_Mau_sac_o4': 'Main_P1_2_OL_Mau_sac_o4',
            'Main_R1_2_OL_Mau_sac_o5': 'Main_P1_2_OL_Mau_sac_o5',
            'Main_R1_3_OL_Do_bong': 'Main_P1_3_OL_Do_bong',
            'Main_R1_3_OL_Do_bong_o4': 'Main_P1_3_OL_Do_bong_o4',
            'Main_R1_3_OL_Do_bong_o5': 'Main_P1_3_OL_Do_bong_o5',
            'Main_R1_4_OL_Mui_mi_truoc_khi_an': 'Main_P1_4_OL_Mui_mi_truoc_khi_an',
            'Main_R1_4_OL_Mui_mi_truoc_khi_an_o4': 'Main_P1_4_OL_Mui_mi_truoc_khi_an_o4',
            'Main_R1_4_OL_Mui_mi_truoc_khi_an_o5': 'Main_P1_4_OL_Mui_mi_truoc_khi_an_o5',
            'Main_R2_OL_noi_chung': 'Main_P2_OL_noi_chung',
            'Main_R2a1_OE_Ly_do_thich_o4': 'Main_P2a1_OE_Ly_do_thich_o4',
            'Main_R2a1_OE_Ly_do_thich_o5': 'Main_P2a1_OE_Ly_do_thich_o5',
            'Main_R2a1_OE_Ly_do_thich_o6': 'Main_P2a1_OE_Ly_do_thich_o6',
            'Main_R2a1_OE_Ly_do_thich_o7': 'Main_P2a1_OE_Ly_do_thich_o7',
            'Main_R2a1_OE_Ly_do_thich_o8': 'Main_P2a1_OE_Ly_do_thich_o8',
            'Main_R2a1_OE_Ly_do_thich_o9': 'Main_P2a1_OE_Ly_do_thich_o9',
            'Main_R2a1_OE_Ly_do_thich_o10': 'Main_P2a1_OE_Ly_do_thich_o10',
            'Main_R2a1_OE_Ly_do_thich_o11': 'Main_P2a1_OE_Ly_do_thich_o11',
            'Main_R2a1_OE_Ly_do_thich_o12': 'Main_P2a1_OE_Ly_do_thich_o12',
            'Main_R2a1_OE_Ly_do_thich_o13': 'Main_P2a1_OE_Ly_do_thich_o13',
            'Main_R2a1_OE_Ly_do_thich_o14': 'Main_P2a1_OE_Ly_do_thich_o14',
            'Main_R2a1_OE_Ly_do_thich_o15': 'Main_P2a1_OE_Ly_do_thich_o15',
            'Main_R2a1_OE_Ly_do_thich_o16': 'Main_P2a1_OE_Ly_do_thich_o16',
            'Main_R2a1_OE_Ly_do_thich_o17': 'Main_P2a1_OE_Ly_do_thich_o17',
            'Main_R2a1_OE_Ly_do_thich_o18': 'Main_P2a1_OE_Ly_do_thich_o18',
            'Main_R2b_OE_Ly_do_khong_thich_o4': 'Main_P2b_OE_Ly_do_khong_thich_o4',
            'Main_R2b_OE_Ly_do_khong_thich_o5': 'Main_P2b_OE_Ly_do_khong_thich_o5',
            'Main_R2b_OE_Ly_do_khong_thich_o6': 'Main_P2b_OE_Ly_do_khong_thich_o6',
            'Main_R2b_OE_Ly_do_khong_thich_o7': 'Main_P2b_OE_Ly_do_khong_thich_o7',
            'Main_R2b_OE_Ly_do_khong_thich_o8': 'Main_P2b_OE_Ly_do_khong_thich_o8',
            'Main_R2b_OE_Ly_do_khong_thich_o9': 'Main_P2b_OE_Ly_do_khong_thich_o9',
            'Main_R2b_OE_Ly_do_khong_thich_o10': 'Main_P2b_OE_Ly_do_khong_thich_o10',
            'Main_R2b_OE_Ly_do_khong_thich_o11': 'Main_P2b_OE_Ly_do_khong_thich_o11',
            'Main_R2b_OE_Ly_do_khong_thich_o12': 'Main_P2b_OE_Ly_do_khong_thich_o12',
            'Main_R2b_OE_Ly_do_khong_thich_o13': 'Main_P2b_OE_Ly_do_khong_thich_o13',
            'Main_R2b_OE_Ly_do_khong_thich_o14': 'Main_P2b_OE_Ly_do_khong_thich_o14',
            'Main_R2b_OE_Ly_do_khong_thich_o15': 'Main_P2b_OE_Ly_do_khong_thich_o15',
            'Main_R2b_OE_Ly_do_khong_thich_o16': 'Main_P2b_OE_Ly_do_khong_thich_o16',
            'Main_R2b_OE_Ly_do_khong_thich_o17': 'Main_P2b_OE_Ly_do_khong_thich_o17',
            'Main_R2b_OE_Ly_do_khong_thich_o18': 'Main_P2b_OE_Ly_do_khong_thich_o18',
            'Main_R3_OL_Mui_trong_khi_an': 'Main_P3_OL_Mui_trong_khi_an',
            'Main_R3_OL_Mui_trong_khi_an_o4': 'Main_P3_OL_Mui_trong_khi_an_o4',
            'Main_R3_OL_Mui_trong_khi_an_o5': 'Main_P3_OL_Mui_trong_khi_an_o5',
            'Main_R4_OL_Vi_noi_chung': 'Main_P4_OL_Vi_noi_chung',
            'Main_R4a_JAR_Vi_man': 'Main_P4a_JAR_Vi_man',
            'Main_R4b_JAR_Vi_ngot': 'Main_P4b_JAR_Vi_ngot',
            'Main_R4c_JAR_Vi_chua': 'Main_P4c_JAR_Vi_chua',
            'Main_R4d_JAR_Vi_cay': 'Main_P4d_JAR_Vi_cay',
            'Main_R4e_JAR_Vi_beo': 'Main_P4e_JAR_Vi_beo',
            'Main_R5_OL_Hau_vi': 'Main_P5_OL_Hau_vi',
            'Main_R5_OL_Hau_vi_o4': 'Main_P5_OL_Hau_vi_o4',
            'Main_R5_OL_Hau_vi_o5': 'Main_P5_OL_Hau_vi_o5',
            'Main_R6_Do_hoa_quyen': 'Main_P6_Do_hoa_quyen',
            'Main_R7_OL_Topping': 'Main_P7_OL_Topping',
            'Main_R7_OL_Topping_o4': 'Main_P7_OL_Topping_o4',
            'Main_R7_OL_Topping_o5': 'Main_P7_OL_Topping_o5',
            'Main_R8_OL_Soi_mi': 'Main_P8_OL_Soi_mi',
            'Main_R8a_JAR_Kich_thuoc_soi_mi': 'Main_P8a_JAR_Kich_thuoc_soi_mi',
            'Main_R8b_JAR_Do_dai_soi_mi': 'Main_P8b_JAR_Do_dai_soi_mi',
            'Main_R9_JAR_Luong_mi': 'Main_P9_JAR_Luong_mi',
            'Main_R10_PI_Y_dinh_mua': 'Main_P10_PI_Y_dinh_mua',
            'Main_R10_PI_Y_dinh_mua_o4': 'Main_P10_PI_Y_dinh_mua_o4',
            'Main_R10_PI_Y_dinh_mua_o5': 'Main_P10_PI_Y_dinh_mua_o5',
            'Main_R10a_PI_Y_dinh_mua_1': 'Main_P10a_PI_Y_dinh_mua_1',
            'Main_Recall_P100_thich_hon_SP3': 'Main_Recall_P100_thich_hon_YN',
        }

        dict_qre_group_mean = {

            'Main_P1_1_OL_Ngoai_quan': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 0, 4: 2, 5: 2}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Main_P1_2_OL_Mau_sac': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 0, 4: 2, 5: 2}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Main_P1_3_OL_Do_bong': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 0, 4: 2, 5: 2}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Main_P1_4_OL_Mui_mi_truoc_khi_an': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 0, 4: 2, 5: 2}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Main_P2_OL_noi_chung': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 0, 4: 2, 5: 2}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Main_P3_OL_Mui_trong_khi_an': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 0, 4: 2, 5: 2}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Main_P4_OL_Vi_noi_chung': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 0, 4: 2, 5: 2}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Main_P4a_JAR_Vi_man': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 0, 4: 2, 5: 2}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Main_P4b_JAR_Vi_ngot': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 0, 4: 2, 5: 2}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Main_P4c_JAR_Vi_chua': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 0, 4: 2, 5: 2}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Main_P4d_JAR_Vi_cay': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 0, 4: 2, 5: 2}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Main_P4e_JAR_Vi_beo': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 0, 4: 2, 5: 2}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Main_P4e_JAR_Vi_beo_Exc_6': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 0, 4: 2, 5: 2}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Main_P5_OL_Hau_vi': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 0, 4: 2, 5: 2}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Main_P6_Do_hoa_quyen': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 0, 4: 2, 5: 2}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Main_P7_OL_Topping': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 0, 4: 2, 5: 2}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Main_P8_OL_Soi_mi': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 0, 4: 2, 5: 2}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Main_P8a_JAR_Kich_thuoc_soi_mi': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 0, 4: 2, 5: 2}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Main_P8b_JAR_Do_dai_soi_mi': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 0, 4: 2, 5: 2}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Main_P9_JAR_Luong_mi': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 0, 4: 2, 5: 2}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Main_P10_PI_Y_dinh_mua': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 0, 4: 2, 5: 2}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },

        }

        # dict_qre_new_vars_info = {
        #     'Main_Q1_Y_tuong_nho_nhat_New': ['Main_Q1_Y_tuong_nho_nhat_New', 'Q1. Sau khi đã nghe qua các ý tưởng phở ăn liền mới này, hiện tại Anh/Chị NHỚ NHẤT VÀ THÍCH NHẤT ý tưởng nào?', 'SA', {'1': 'Yes', '2': 'No'}],
        #     'Main_Q2_Y_tuong_phu_hop_nhat_New': ['Main_Q2_Y_tuong_phu_hop_nhat_New', 'Q2. Ý tưởng sản phẩm phở ăn liền nào phù hợp nhất với lợi ích ‘Một loại phở ăn liền có hương vị thơm ngon và sợi phở thanh lành, giúp cân bằng cảm xúc ngay cả lúc mệt mỏi, cho cơ thể nhẹ nhàng và tạo nên một lối sống lành mạnh, thư thái’?', 'SA', {'1': 'Yes', '2': 'No'}],
        #     'Main_Q3_Cai_thien_New': ['Main_Q3_Cai_thien_New', 'Q3. Vậy có điểm nào anh/chị muốn cải thiện, sửa đổi để anh/chị thích/hài lòng hơn nữa?', 'FT', {}],
        #     'Main_Q4_Ten_SP_New': ['Main_Q4_Ten_SP_New', 'Q4. Sau đây là một số tên sản phẩm phở ăn liền mới, theo bạn tên nào là phù hợp nhất với ý tưởng sản phẩm _?', 'SA', {'1': 'Phở Bờ Hồ', '2': 'Phở Gánh Phố Cổ', '3': 'Phở Ngõ Nhỏ', '4': 'Phở An Nhiên', '5': 'Phở Quốc Hương'}],
        #
        # }

        # # dict_qre_OE_info_org = {'Main_Q3_Cai_thien_New_OE|1-3': ['Q3. Vậy có điểm nào anh/chị muốn cải thiện, sửa đổi để anh/chị thích/hài lòng hơn nữa? ', 'MA',  {'net_code': {'999': 'Không có/Không cần', '90001|SỢI PHỞ (NET)': {'101': 'Thêm nội dung về sợi phở/làm nổi bật/nhấn mạnh ý tưởng về sợi phở', '102': 'Được làm từ gạo nếp nương tạo cảm giác thiên nhiên/tự nhiên/sạch/không thuốc', '103': 'Được làm từ gạo nếp nương giúp sợi phở thanh lành (hơn)', '104': 'Được làm từ gạo nếp nương vùng tây bắc ', '105': 'Sợi phở dẻo bùi', '106': 'Sợi phở thơm', '107': 'Sợi phở ngon', '108': 'Đề cập đến độ dai', '109': 'Gạo nếp nương đặc sắc của vùng cao tạo cảm giác thoải mái', '110': 'Gạo nếp nương đặc sắc của vùng cao tạo cảm giác thanh nhẹ', '111': 'Gạo nếp nương đặc sắc của vùng cao tạo cảm giác món phở có vị đậm đà'}, '90002|NGUYÊN LIỆU (NET)': {'201': 'Đề cập chi tiết hơn về các nguyên liệu truyền thống', '202': 'Đề cập về thịt', '203': 'Đông trùng hạ thảo giúp cân bằng cảm xúc'}, '90003|NƯỚC DÙNG (NET)': {'301': 'Được nấu theo công thức của quán phở Thìn Bờ Hồ danh tiếng 70 năm làm cho vị sợi phở ngon hấp dẫn', '302': 'Được nấu từ công thức đặc biệt của quán phở trứ danh của Hà Nội tạo cảm giác hương vị ngon, sợi phở thanh lành', '303': 'Sẽ có/mang lại vị thực tế hơn', '304': 'Được nấu theo công thức quản phở Bờ Hồ lâu đời', '305': 'Thêm nội dung về nước dùng/làm nổi bật/nhấn mạnh ý tưởng về nước dùng', '306': 'Được nấu theo công thức quản phở nổi tiếng'}, '90004|SỢI PHỞ KẾT HỢP NƯỚC DÙNG (NET)': {'401': 'Tạo sự kết hợp ấn tượng giữa sợi phở và nước dùng'}, '90005|KHÁC (NET)': {'601': 'Bổ sung thêm lợi ích/chất dinh dưỡng mang lại cho cơ thể', '610': 'Bổ sung thêm công thức nước phở Thìn đã có ngay trong gói phở ăn liền giúp tiết kiệm thời gian hơn khi ăn ngoài hàng'}, '90006|ĐIỀU CHỈNH NỘI DUNG (NET)': {'602': 'Kết hợp độc đáo đổi thành kết hợp hài hòa sẽ hay hơn', '603': 'Gạo nếp nương của vùng cao đổi thành gạo nếp nương đặc sản của vùng cao', '604': 'Giúp cân bằng cảm xúc ngay cả lúc mệt mỏi, cho cơ thể nhẹ nhàng và tạo nên một lối sống lành mạnh, thư thái đổi thành sản phẩm mang đến sức khỏe tốt cho cơ thể, giúp cơ thể hồi phục sức nhanh'}}}],}
        # dict_qre_OE_info_org = dict()
        # if codelist_file.filename:
        #     dict_qre_OE_info_org = eval(codelist_file.file.read())
        #
        #
        # dict_qre_OE_info = dict()
        # for k, v in dict_qre_OE_info_org.items():
        #     oe_name, oe_num_col = k.rsplit('|', 1)
        #     oe_num_col = oe_num_col.split('-')
        #     oe_num_col = range(int(oe_num_col[0]), int(oe_num_col[1]) + 1)
        #
        #     for i in oe_num_col:
        #         dict_qre_OE_info.update({
        #             f'{oe_name}_{i}': [f'{oe_name}_{i}'] + v
        #         })
        #
        #
        # # lst_addin_OE_value = [['Q0a_RespondentID', 1056, 'Main_Ma_san_pham_Q3', 1, 'Main_Q5a_OE_Ly_do_thich_Y1_1', 101],]
        # lst_addin_OE_value = list()
        # if coding_file.filename:
        #     lst_addin_OE_value = eval(coding_file.file.read())

        # net code for exist qres
        # dict_qre_net_info = {
        #     # 'Main_MA_vi_ngot_Q12_[0-9]+': {
        #     #     '2': 'Đường mía',
        #     #     '3': 'Đường cát/ đường trắng',
        #     #     '4': 'Đường phèn', '5': 'Đường bột',
        #     #     'net_code': {
        #     #
        #     #         '90001|TRÁI CÂY (NET)': {
        #     #             '1': 'Trái cây',
        #     #             '13': 'Trái cây, vui lòng ghi rõ loại trái cây',
        #     #             '201': 'Chanh dây', '202': 'Cam/ chanh/ quýt',
        #     #         },
        #     #     }
        #     # }
        # }
        # End Define structure------------------------------------------------------------------------------------------

        # Data stack format---------------------------------------------------------------------------------------------
        self.logger.info('Data stack format')

        # df_data_stack generate
        df_data_scr = df_data_output.loc[:, [id_col] + lst_scr].copy()
        df_data_fc = df_data_output.loc[:, [id_col] + lst_fc].copy()

        df_data_sp1 = df_data_output.loc[:, [id_col] + list(dict_sp1.keys())].copy()
        df_data_sp2 = df_data_output.loc[:, [id_col] + list(dict_sp2.keys())].copy()
        df_data_sp3 = df_data_output.loc[:, [id_col] + list(dict_sp3.keys())].copy()

        df_data_sp1.rename(columns=dict_sp1, inplace=True)
        df_data_sp2.rename(columns=dict_sp2, inplace=True)
        df_data_sp3.rename(columns=dict_sp3, inplace=True)

        df_data_stack = pd.concat([df_data_sp1, df_data_sp2, df_data_sp3], axis=0, ignore_index=True)

        df_data_stack = df_data_scr.merge(df_data_stack, how='left', on=[id_col])
        df_data_stack.reset_index(drop=True, inplace=True)

        df_data_stack = df_data_stack.merge(df_data_fc, how='left', on=[id_col])

        df_data_stack.sort_values(by=[id_col, sp_col], inplace=True)
        df_data_stack.reset_index(drop=True, inplace=True)

        df_info_stack = df_info_output.copy()

        for key, val in dict_sp1.items():
            df_info_stack.loc[df_info_stack['var_name'] == key, ['var_name']] = [val]

        df_info_stack.drop_duplicates(subset=['var_name'], keep='first', inplace=True)

        # if dict_qre_OE_info:
        #
        #     # ADD OE to Data stack--------------------------------------------------------------------------------------
        #     lst_OE_col = list(dict_qre_OE_info.keys())
        #     df_data_stack[lst_OE_col] = pd.DataFrame([[np.nan] * len(lst_OE_col)], index=df_data_stack.index)
        #
        #     # Remember edit this
        #     for item in lst_addin_OE_value:
        #         df_data_stack.loc[(df_data_stack[item[0]] == item[1]) & (df_data_stack[item[2]] == item[3]), [item[4]]] = [item[5]]
        #
        #     # This project only
        #     df_data_stack.loc[(df_data_stack['ID'] == '712022_2322160') & (df_data_stack['Ma_YT'] == 1), ['Main_R1a_Thich_OE_1']] = [999]
        #
        #     # END ADD OE to Data stack----------------------------------------------------------------------------------
        #
        #     # ADD OE to Info stack--------------------------------------------------------------------------------------
        #     df_info_stack = pd.concat([df_info_stack,
        #                                pd.DataFrame(columns=['var_name', 'var_lbl', 'var_type', 'val_lbl'],
        #                                             data=list(dict_qre_OE_info.values()))], axis=0)
        #     # END ADD OE to Info stack----------------------------------------------------------------------------------

        # if dict_qre_net_info:
        #
        #     # ADD MA OE to Data stack-----------------------------------------------------------------------------------
        #     # Remember edit this
        #     for item in lst_addin_MA_value:
        #
        #         idx_item = df_data_stack.loc[(df_data_stack[item[0]] == item[1]) & (df_data_stack[item[2]] == item[3]), item[0]].index[0]
        #         str_ma_oe_name = item[4].rsplit('_', 1)[0]
        #         int_ma_oe_code = int(item[4].rsplit('_', 1)[1].replace('o', ''))
        #
        #         lst_ma_oe_col = df_info_stack.loc[df_info_stack['var_name'].str.contains(f'{str_ma_oe_name}_[0-9]+'), 'var_name'].values.tolist()
        #
        #         is_found = False
        #         for col in lst_ma_oe_col:
        #             if df_data_stack.at[idx_item, col] == int_ma_oe_code:
        #                 is_found = True
        #                 df_data_stack.at[idx_item, col] = item[5]
        #                 break
        #
        #         if not is_found:
        #             for col in lst_ma_oe_col:
        #                 if pd.isnull(df_data_stack.at[idx_item, col]):
        #                     df_data_stack.at[idx_item, col] = item[5]
        #                     break
        #     # END ADD MA OE to Data stack-------------------------------------------------------------------------------
        #
        #     # ADD MA OE to Info stack--------------------------------------------------------------------------------------
        #     for key, val in dict_qre_net_info.items():
        #         df_info_stack.loc[df_info_stack['var_name'].str.contains(key), 'val_lbl'] = [val] * df_info_stack.loc[df_info_stack['var_name'].str.contains(key), 'val_lbl'].shape[0]
        #
        #     # END ADD MA OE to Info stack----------------------------------------------------------------------------------

        for key, val in dict_recode_scale_qre.items():
            df_info_stack.loc[df_info_stack['var_name'] == key, ['val_lbl']] = [val[0]]
            df_data_stack[key].replace(val[1], inplace=True)

        df_data_stack['Main_P4e_JAR_Vi_beo_Exc_6'] = df_data_stack['Main_P4e_JAR_Vi_beo']
        df_data_stack['Main_P4e_JAR_Vi_beo_Exc_6'].replace({6: np.nan}, inplace=True)

        df_info_stack = pd.concat([df_info_stack,
                                   pd.DataFrame(columns=['var_name', 'var_lbl', 'var_type', 'val_lbl'],
                                                data=[['Main_P4e_JAR_Vi_beo_Exc_6', 'How do you evaluate the RICHNESS of this product? (SA)', 'SA', {'1': 'Too little richness to my liking', '2': 'Quite little richness to my liking', '3': 'Just right to my liking', '4': 'Quite much richness to my liking', '5': 'Too much richness to my liking'}]])], axis=0, ignore_index=True)


        # Reset df_info_stack index
        df_info_stack['idx_var_name'] = df_info_stack['var_name']
        df_info_stack.set_index('idx_var_name', inplace=True)

        df_info_stack = df_info_stack.loc[list(df_data_stack.columns), :]
        df_info_stack.reindex(list(df_data_stack.columns))

        df_info_stack.loc['Main_P10a_PI_Y_dinh_mua_1', ['var_type']] = ['NUM']

        df_info_stack.reset_index(drop=True, inplace=True)

        # df_data_stack.to_excel('zzzz_df_data_stack.csv')
        # df_info_stack.to_excel('zzzz_df_info_stack.csv')
        # End Data stack format-----------------------------------------------------------------------------------------


        # # Data unstack format-------------------------------------------------------------------------------------------
        # logger.info('Data unstack format')
        #
        # lst_col_part_body = list(dict_sp1.values())
        # lst_col_part_body.remove(sp_col)
        #
        # dict_stack_structure = {
        #     'id_col': id_col,
        #     'sp_col': sp_col,
        #     'lst_col_part_head': lst_scr,
        #     'lst_col_part_body': lst_col_part_body,
        #     'lst_col_part_tail': lst_fc,
        # }
        #
        # df_data_unstack, df_info_unstack = self.convert_to_unstack(df_data_stack, df_info_stack, dict_stack_structure)
        #
        # # df_data_unstack.to_csv('zzzz_df_data_unstack.csv', encoding='utf-8-sig')
        # # df_info_unstack.to_csv('zzzz_df_info_unstack.csv', encoding='utf-8-sig')
        # # End Data unstack format---------------------------------------------------------------------------------------

        # ADD MEAN & GROUP----------------------------------------------------------------------------------------------
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

        # End ADD MEAN & GROUP------------------------------------------------------------------------------------------

        # Export data tables--------------------------------------------------------------------------------------------
        str_topline_file_name = self.str_file_name.replace('.xlsx', '_Topline.xlsx')

        DataTableGenerator.__init__(self, logger=self.logger, df_data=df_data_stack, df_info=df_info_stack,
                                    xlsx_name=str_topline_file_name,
                                    lst_qre_group=lst_qre_group, lst_qre_mean=lst_qre_mean, is_md=False)

        if tables_format_file.filename:
            lst_func_to_run = eval(tables_format_file.file.read())

            self.run_tables_by_js_files(lst_func_to_run)

            self.format_sig_table()

        # End Export data tables----------------------------------------------------------------------------------------

        self.logger.info('Generate SAV files')

        # Remove net_code to export sav---------------------------------------------------------------------------------
        df_info_stack_without_net = df_info_stack.copy()

        for idx in df_info_stack_without_net.index:
            val_lbl = df_info_stack_without_net.at[idx, 'val_lbl']

            if 'net_code' in val_lbl.keys():
                df_info_stack_without_net.at[idx, 'val_lbl'] = self.unnetted_qre_val(val_lbl)
        # END Remove net_code to export sav-----------------------------------------------------------------------------

        self.generate_sav_sps(df_data=df_data_stack, df_qres_info=df_info_stack_without_net, is_md=False, is_export_xlsx=True)

        # df_data_2 = df_data_unstack, df_qres_info_2 = df_info_unstack

        self.logger.warning('DATA PROCESSING COMPLETED in %d seconds' % (time.time() - start_time))
