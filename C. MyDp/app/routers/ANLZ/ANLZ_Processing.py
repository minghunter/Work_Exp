from app.routers.Online_Survey.export_online_survey_data_table import DataTableGenerator
from app.routers.Online_Survey.convert_unstack import ConvertUnstack
from app.routers.Online_Survey.calculate_lsm import LSMCalculation
import traceback
import pandas as pd
import numpy as np
import time


class AnlzProcessing(DataTableGenerator, ConvertUnstack, LSMCalculation):

    def __int__(self):
        self.txt_pre_processing_script: str = ''


    def run_pre_processing(self) -> dict:
        try:
            start_time = time.time()
            df_data, df_info = self.df_data, self.df_info

            exec(self.txt_pre_processing_script)

            self.logger.warning('DATA PROCESSING COMPLETED in %d seconds' % (time.time() - start_time))

            yield {'data': df_data, 'info': df_info}

        except Exception:
            self.logger.error(traceback.format_exc())




    # def convert_vn8286_crust_clt(self, py_script_file, tables_format_file, codelist_file, coding_file):
    #     """DO NOT EDIT THIS FUNCTION"""
    #     try:
    #         start_time = time.time()
    #
    #         if py_script_file:
    #             exec(py_script_file.file.read())
    #             exit()
    #
    #
    #         # self.processing_script_vn8286_crust_clt(tables_format_file, codelist_file, coding_file)
    #
    #         self.logger.warning('DATA PROCESSING COMPLETED in %d seconds' % (time.time() - start_time))
    #
    #     except Exception:
    #         self.logger.error(traceback.format_exc())
    #         str_err_log_name = self.str_prj_name.replace('.xlsx', '_Errors.txt')
    #         with open(str_err_log_name, 'w') as err_log_txt:
    #             err_log_txt.writelines(traceback.format_exc())


    # def processing_script_vn8286_crust_clt(self, tables_format_file, codelist_file, coding_file):
    #     """
    #     :param tables_format_file: txt uploaded table format file
    #     :param codelist_file: txt uploaded codelist file
    #     :param coding_file: txt uploaded coding file
    #     :return: zip file include files: .sav, .sps, .xlsx(rawdata, topline)
    #
    #     EDIT FROM HERE
    #     """
    #
    #     # Convert system rawdata to dataframes--------------------------------------------------------------------------
    #     df_data_output, df_info_output = self.convert_df_mc()
    #
    #     # Pre processing------------------------------------------------------------------------------------------------
    #     self.logger.info('Pre processing')
    #
    #     dict_fc_yn = {
    #         'Main_SP1_P100a_Thich_nhat': ['Sản phẩm thích nhất', 'SA', {'1': 'Yes', '2': 'No'}, 'Main_P100a_Thich_nhat', 1],
    #         'Main_SP2_P100a_Thich_nhat': ['Sản phẩm thích nhất', 'SA', {'1': 'Yes', '2': 'No'}, 'Main_P100a_Thich_nhat', 2],
    #         'Main_SP3_P100a_Thich_nhat': ['Sản phẩm thích nhất', 'SA', {'1': 'Yes', '2': 'No'}, 'Main_P100a_Thich_nhat', 3],
    #         'Main_SP4_P100a_Thich_nhat': ['Sản phẩm thích nhất', 'SA', {'1': 'Yes', '2': 'No'}, 'Main_P100a_Thich_nhat', 4],
    #
    #         'Main_SP1_P100b_Thich_nhi': ['Sản phẩm thích nhì', 'SA', {'1': 'Yes', '2': 'No'}, 'Main_P100b_Thich_nhi', 1],
    #         'Main_SP2_P100b_Thich_nhi': ['Sản phẩm thích nhì', 'SA', {'1': 'Yes', '2': 'No'}, 'Main_P100b_Thich_nhi', 2],
    #         'Main_SP3_P100b_Thich_nhi': ['Sản phẩm thích nhì', 'SA', {'1': 'Yes', '2': 'No'}, 'Main_P100b_Thich_nhi', 3],
    #         'Main_SP4_P100b_Thich_nhi': ['Sản phẩm thích nhì', 'SA', {'1': 'Yes', '2': 'No'}, 'Main_P100b_Thich_nhi', 4],
    #     }
    #
    #     for key, val in dict_fc_yn.items():
    #         df_info_output = pd.concat([df_info_output,
    #                                     pd.DataFrame(columns=['var_name', 'var_lbl', 'var_type', 'val_lbl'],
    #                                                  data=[[key, val[0], val[1], val[2]]])], axis=0, ignore_index=True)
    #
    #         df_data_output = pd.concat(
    #             [df_data_output, pd.DataFrame(columns=[key], data=[2] * df_data_output.shape[0])], axis=1)
    #
    #         org_fc_name = val[-2]
    #
    #         df_data_output[key] = [1 if a == val[-1] else 2 for a in df_data_output[org_fc_name]]
    #
    #
    #     dict_fc_by_sp = {
    #         'Main_P100a_Thich_nhat_by_sp': ['P100a. Sản phẩm thích nhất', 'SA', {'1': '473', '2': '843', '3': '691', '4': '350'}, 'Main_P100a_Thich_nhat'],
    #         'Main_P100b_Thich_nhi_by_sp': ['P100a. Sản phẩm thích nhì', 'SA', {'1': '473', '2': '843', '3': '691', '4': '350'}, 'Main_P100b_Thich_nhi'],
    #     }
    #
    #     for key, val in dict_fc_by_sp.items():
    #         df_info_output = pd.concat([df_info_output,
    #                                     pd.DataFrame(columns=['var_name', 'var_lbl', 'var_type', 'val_lbl'],
    #                                                  data=[[key, val[0], val[1], val[2]]])], axis=0, ignore_index=True)
    #
    #         df_data_output = pd.concat(
    #             [df_data_output, pd.DataFrame(columns=[key], data=[-999] * df_data_output.shape[0])], axis=1)
    #
    #         for i in range(1, 5):
    #             df_filter = df_data_output.query(f"{val[-1]} == {i}").copy()
    #             if not df_filter.empty:
    #                 df_data_output.loc[df_filter.index, [key]] = df_data_output.loc[df_filter.index, [f'Main_SP{i}_0e_Ma_san_pham{i}']].values
    #
    #     del df_filter
    #
    #     # Define structure----------------------------------------------------------------------------------------------
    #     self.logger.info('Define structure')
    #
    #     id_col = 'Q0a_RespondentID'
    #     sp_col = 'Ma_SP'
    #
    #     lst_scr = [
    #         'Recruit_S0_Thanh_pho',
    #         'Recruit_S0_Thanh_pho_o2',
    #         'Recruit_S1_Nganh_cam_1',
    #         'Recruit_S1_Nganh_cam_2',
    #         'Recruit_S1_Nganh_cam_3',
    #         'Recruit_S1_Nganh_cam_4',
    #         'Recruit_S1_Nganh_cam_5',
    #         'Recruit_S1_Nganh_cam_6',
    #         'Recruit_S1_Nganh_cam_7',
    #         'Recruit_S2_Gioi_tinh',
    #         'Recruit_S3_Khoang_tuoi',
    #         'Recruit_S4_Nghe_nghiep_hien_tai',
    #         'Recruit_S4_Nghe_nghiep_hien_tai_o14',
    #         'Recruit_S5_Thu_nhap_HGD',
    #         'Recruit_S6_Nguoi_quyet_dinh_chinh',
    #         'Recruit_S7_Nhan_biet_1',
    #         'Recruit_S7_Nhan_biet_2',
    #         'Recruit_S7_Nhan_biet_3',
    #         'Recruit_S7_Nhan_biet_4',
    #         'Recruit_S7_Nhan_biet_5',
    #         'Recruit_S7_Nhan_biet_6',
    #         'Recruit_S7_Nhan_biet_7',
    #         'Recruit_S7_Nhan_biet_8',
    #         'Recruit_S7_Nhan_biet_9',
    #         'Recruit_S7_Nhan_biet_10',
    #         'Recruit_S7_Nhan_biet_11',
    #         'Recruit_S7_Nhan_biet_12',
    #         'Recruit_S7_Nhan_biet_13',
    #         'Recruit_S7_Nhan_biet_14',
    #         'Recruit_S7_Nhan_biet_15',
    #         'Recruit_S7_Nhan_biet_16',
    #         'Recruit_S7_Nhan_biet_17',
    #         'Recruit_S7_Nhan_biet_18',
    #         'Recruit_S7_Nhan_biet_19',
    #         'Recruit_S7_Nhan_biet_20',
    #         'Recruit_S7_Nhan_biet_21',
    #         'Recruit_S7_Nhan_biet_22',
    #         'Recruit_S7_Nhan_biet_23',
    #         'Recruit_S7_Nhan_biet_24',
    #         'Recruit_S7_Nhan_biet_25',
    #         'Recruit_S7_Nhan_biet_26',
    #         'Recruit_S7_Nhan_biet_27',
    #         'Recruit_S7_Nhan_biet_28',
    #         'Recruit_S7_Nhan_biet_29',
    #         'Recruit_S7_Nhan_biet_30',
    #         'Recruit_S7_Nhan_biet_31',
    #         'Recruit_S7_Nhan_biet_32',
    #         'Recruit_S7_Nhan_biet_33',
    #         'Recruit_S7_Nhan_biet_34',
    #         'Recruit_S7_Nhan_biet_35',
    #         'Recruit_S7_Nhan_biet_36',
    #         'Recruit_S7_Nhan_biet_37',
    #         'Recruit_S7_Nhan_biet_38',
    #         'Recruit_S7_Nhan_biet_39',
    #         'Recruit_S7_Nhan_biet_40',
    #         'Recruit_S7_Nhan_biet_41',
    #         'Recruit_S7_Nhan_biet_42',
    #         'Recruit_S7_Nhan_biet_43',
    #         'Recruit_S7_Nhan_biet_44',
    #         'Recruit_S7_Nhan_biet_45',
    #         'Recruit_S7_Nhan_biet_46',
    #         'Recruit_S7_Nhan_biet_47',
    #         'Recruit_S7_Nhan_biet_48',
    #         'Recruit_S7_Nhan_biet_49',
    #         'Recruit_S7_Nhan_biet_50',
    #         'Recruit_S7_Nhan_biet_51',
    #         'Recruit_S7_Nhan_biet_52',
    #         'Recruit_S7_Nhan_biet_53',
    #         'Recruit_S7_Nhan_biet_54',
    #         'Recruit_S7_Nhan_biet_55',
    #         'Recruit_S7_Nhan_biet_56',
    #         'Recruit_S7_Nhan_biet_57',
    #         'Recruit_S7_Nhan_biet_58',
    #         'Recruit_S7_Nhan_biet_59',
    #         'Recruit_S7_Nhan_biet_60',
    #         'Recruit_S7_Nhan_biet_61',
    #         'Recruit_S7_Nhan_biet_62',
    #         'Recruit_S7_Nhan_biet_63',
    #         'Recruit_S7_Nhan_biet_64',
    #         'Recruit_S7_Nhan_biet_65',
    #         'Recruit_S7_Nhan_biet_66',
    #         'Recruit_S7_Nhan_biet_67',
    #         'Recruit_S7_Nhan_biet_68',
    #         'Recruit_S7_Nhan_biet_69',
    #         'Recruit_S7_Nhan_biet_o69',
    #         'Recruit_S8_Su_dung_P3M_1',
    #         'Recruit_S8_Su_dung_P3M_2',
    #         'Recruit_S8_Su_dung_P3M_3',
    #         'Recruit_S8_Su_dung_P3M_4',
    #         'Recruit_S8_Su_dung_P3M_5',
    #         'Recruit_S8_Su_dung_P3M_6',
    #         'Recruit_S8_Su_dung_P3M_7',
    #         'Recruit_S8_Su_dung_P3M_8',
    #         'Recruit_S8_Su_dung_P3M_9',
    #         'Recruit_S8_Su_dung_P3M_10',
    #         'Recruit_S8_Su_dung_P3M_11',
    #         'Recruit_S8_Su_dung_P3M_12',
    #         'Recruit_S8_Su_dung_P3M_13',
    #         'Recruit_S8_Su_dung_P3M_14',
    #         'Recruit_S8_Su_dung_P3M_15',
    #         'Recruit_S8_Su_dung_P3M_16',
    #         'Recruit_S8_Su_dung_P3M_17',
    #         'Recruit_S8_Su_dung_P3M_18',
    #         'Recruit_S8_Su_dung_P3M_19',
    #         'Recruit_S8_Su_dung_P3M_20',
    #         'Recruit_S8_Su_dung_P3M_21',
    #         'Recruit_S8_Su_dung_P3M_22',
    #         'Recruit_S8_Su_dung_P3M_23',
    #         'Recruit_S8_Su_dung_P3M_24',
    #         'Recruit_S8_Su_dung_P3M_25',
    #         'Recruit_S8_Su_dung_P3M_26',
    #         'Recruit_S8_Su_dung_P3M_27',
    #         'Recruit_S8_Su_dung_P3M_28',
    #         'Recruit_S8_Su_dung_P3M_29',
    #         'Recruit_S8_Su_dung_P3M_30',
    #         'Recruit_S8_Su_dung_P3M_31',
    #         'Recruit_S8_Su_dung_P3M_32',
    #         'Recruit_S8_Su_dung_P3M_33',
    #         'Recruit_S8_Su_dung_P3M_34',
    #         'Recruit_S8_Su_dung_P3M_35',
    #         'Recruit_S8_Su_dung_P3M_36',
    #         'Recruit_S8_Su_dung_P3M_37',
    #         'Recruit_S8_Su_dung_P3M_38',
    #         'Recruit_S8_Su_dung_P3M_39',
    #         'Recruit_S8_Su_dung_P3M_40',
    #         'Recruit_S8_Su_dung_P3M_41',
    #         'Recruit_S8_Su_dung_P3M_42',
    #         'Recruit_S8_Su_dung_P3M_43',
    #         'Recruit_S8_Su_dung_P3M_44',
    #         'Recruit_S8_Su_dung_P3M_45',
    #         'Recruit_S8_Su_dung_P3M_46',
    #         'Recruit_S8_Su_dung_P3M_47',
    #         'Recruit_S8_Su_dung_P3M_48',
    #         'Recruit_S8_Su_dung_P3M_49',
    #         'Recruit_S8_Su_dung_P3M_50',
    #         'Recruit_S8_Su_dung_P3M_51',
    #         'Recruit_S8_Su_dung_P3M_52',
    #         'Recruit_S8_Su_dung_P3M_53',
    #         'Recruit_S8_Su_dung_P3M_54',
    #         'Recruit_S8_Su_dung_P3M_55',
    #         'Recruit_S8_Su_dung_P3M_56',
    #         'Recruit_S8_Su_dung_P3M_57',
    #         'Recruit_S8_Su_dung_P3M_58',
    #         'Recruit_S8_Su_dung_P3M_59',
    #         'Recruit_S8_Su_dung_P3M_60',
    #         'Recruit_S8_Su_dung_P3M_61',
    #         'Recruit_S8_Su_dung_P3M_62',
    #         'Recruit_S8_Su_dung_P3M_63',
    #         'Recruit_S8_Su_dung_P3M_64',
    #         'Recruit_S8_Su_dung_P3M_65',
    #         'Recruit_S8_Su_dung_P3M_66',
    #         'Recruit_S8_Su_dung_P3M_67',
    #         'Recruit_S8_Su_dung_P3M_68',
    #         'Recruit_S8_Su_dung_P3M_69',
    #         'Recruit_S9_BUMO_SKU',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_01',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_02',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_03',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_04',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_05',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_06',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_07',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_08',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_09',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_10',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_11',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_12',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_13',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_14',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_15',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_16',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_17',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_18',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_19',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_20',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_21',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_22',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_23',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_24',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_25',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_26',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_27',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_28',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_29',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_30',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_31',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_32',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_33',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_34',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_35',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_36',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_37',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_38',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_39',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_40',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_41',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_42',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_43',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_44',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_45',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_46',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_47',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_48',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_49',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_50',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_51',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_52',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_53',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_54',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_55',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_56',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_57',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_58',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_59',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_60',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_61',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_62',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_63',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_64',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_65',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_66',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_67',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_68',
    #         'Recruit_S10_Tan_suat_su_dung_SKU_69',
    #         'Recruit_S11_Tan_suat_mi_an_lien',
    #         'Recruit_S12_BUMO_thuong_hieu',
    #         'Recruit_S12_BUMO_thuong_hieu_o8',
    #         'Recruit_S13_Suc_khoe_1',
    #         'Recruit_S13_Suc_khoe_2',
    #         'Recruit_S13_Suc_khoe_3',
    #         'Recruit_S13_Suc_khoe_4',
    #         'Recruit_S13_Suc_khoe_5',
    #         'Recruit_S13_Suc_khoe_6',
    #         'Recruit_S13_Suc_khoe_7',
    #         'Recruit_S13_Suc_khoe_8',
    #     ]
    #
    #     lst_fc = [
    #         'Main_P100a_Thich_nhat',
    #         'Main_P100a_Thich_nhat_by_sp',
    #         'Main_P100b_Thich_nhi',
    #         'Main_P100b_Thich_nhi_by_sp',
    #         'Main_P100c_OE_Ly_do_FC',
    #     ]
    #
    #     dict_sp = {
    #         1: {
    #             'Main_SP1_0e_Ma_san_pham1': 'Ma_SP',
    #             'Main_SP1_1_OL_Noi_chung': 'Main_1_OL_Noi_chung',
    #             'Main_SP1_2_OL_Soi_mi': 'Main_2_OL_Soi_mi',
    #             'Main_SP1_3_OL_Nuoc_dung': 'Main_3_OL_Nuoc_dung',
    #             'Main_SP1_4_OE_Cam_nhan': 'Main_4_OE_Cam_nhan',
    #             'Main_SP1_P100a_Thich_nhat': 'Main_P100a_Thich_nhat_YN',
    #             'Main_SP1_P100b_Thich_nhi': 'Main_P100b_Thich_nhi_YN',
    #         },
    #         2: {
    #             'Main_SP2_0e_Ma_san_pham2': 'Ma_SP',
    #             'Main_SP2_1_OL_Noi_chung': 'Main_1_OL_Noi_chung',
    #             'Main_SP2_2_OL_Soi_mi': 'Main_2_OL_Soi_mi',
    #             'Main_SP2_3_OL_Nuoc_dung': 'Main_3_OL_Nuoc_dung',
    #             'Main_SP2_4_OE_Cam_nhan': 'Main_4_OE_Cam_nhan',
    #             'Main_SP2_P100a_Thich_nhat': 'Main_P100a_Thich_nhat_YN',
    #             'Main_SP2_P100b_Thich_nhi': 'Main_P100b_Thich_nhi_YN',
    #         },
    #         3: {
    #             'Main_SP3_0e_Ma_san_pham3': 'Ma_SP',
    #             'Main_SP3_1_OL_Noi_chung': 'Main_1_OL_Noi_chung',
    #             'Main_SP3_2_OL_Soi_mi': 'Main_2_OL_Soi_mi',
    #             'Main_SP3_3_OL_Nuoc_dung': 'Main_3_OL_Nuoc_dung',
    #             'Main_SP3_4_OE_Cam_nhan': 'Main_4_OE_Cam_nhan',
    #             'Main_SP3_P100a_Thich_nhat': 'Main_P100a_Thich_nhat_YN',
    #             'Main_SP3_P100b_Thich_nhi': 'Main_P100b_Thich_nhi_YN',
    #         },
    #         4: {
    #             'Main_SP4_0e_Ma_san_pham4': 'Ma_SP',
    #             'Main_SP4_1_OL_Noi_chung': 'Main_1_OL_Noi_chung',
    #             'Main_SP4_2_OL_Soi_mi': 'Main_2_OL_Soi_mi',
    #             'Main_SP4_3_OL_Nuoc_dung': 'Main_3_OL_Nuoc_dung',
    #             'Main_SP4_4_OE_Cam_nhan': 'Main_4_OE_Cam_nhan',
    #             'Main_SP4_P100a_Thich_nhat': 'Main_P100a_Thich_nhat_YN',
    #             'Main_SP4_P100b_Thich_nhi': 'Main_P100b_Thich_nhi_YN',
    #         },
    #     }
    #
    #     dict_qre_group_mean = {
    #
    #         'Main_1_OL_Noi_chung': {
    #             'range': [],
    #             'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
    #             'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
    #         },
    #         'Main_2_OL_Soi_mi': {
    #             'range': [],
    #             'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
    #             'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
    #         },
    #         'Main_3_OL_Nuoc_dung': {
    #             'range': [],
    #             'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
    #             'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
    #         },
    #
    #     }
    #
    #     # dict_qre_OE_info_org = {'Main_Q3_Cai_thien_New_OE|1-3': ['Q3. lbl', 'MA',  {'net_code': {'999': 'Không có/Không cần', '90001|SỢI PHỞ (NET)': {'101': 'Thêm nội dung về sợi phở/làm nổi bật/nhấn mạnh ý tưởng về sợi phở', '102': 'Được làm từ gạo nếp nương tạo cảm giác thiên nhiên/tự nhiên/sạch/không thuốc', '103': 'Được làm từ gạo nếp nương giúp sợi phở thanh lành (hơn)', '104': 'Được làm từ gạo nếp nương vùng tây bắc ', '105': 'Sợi phở dẻo bùi', '106': 'Sợi phở thơm', '107': 'Sợi phở ngon', '108': 'Đề cập đến độ dai', '109': 'Gạo nếp nương đặc sắc của vùng cao tạo cảm giác thoải mái', '110': 'Gạo nếp nương đặc sắc của vùng cao tạo cảm giác thanh nhẹ', '111': 'Gạo nếp nương đặc sắc của vùng cao tạo cảm giác món phở có vị đậm đà'}, '90002|NGUYÊN LIỆU (NET)': {'201': 'Đề cập chi tiết hơn về các nguyên liệu truyền thống', '202': 'Đề cập về thịt', '203': 'Đông trùng hạ thảo giúp cân bằng cảm xúc'}, '90003|NƯỚC DÙNG (NET)': {'301': 'Được nấu theo công thức của quán phở Thìn Bờ Hồ danh tiếng 70 năm làm cho vị sợi phở ngon hấp dẫn', '302': 'Được nấu từ công thức đặc biệt của quán phở trứ danh của Hà Nội tạo cảm giác hương vị ngon, sợi phở thanh lành', '303': 'Sẽ có/mang lại vị thực tế hơn', '304': 'Được nấu theo công thức quản phở Bờ Hồ lâu đời', '305': 'Thêm nội dung về nước dùng/làm nổi bật/nhấn mạnh ý tưởng về nước dùng', '306': 'Được nấu theo công thức quản phở nổi tiếng'}, '90004|SỢI PHỞ KẾT HỢP NƯỚC DÙNG (NET)': {'401': 'Tạo sự kết hợp ấn tượng giữa sợi phở và nước dùng'}, '90005|KHÁC (NET)': {'601': 'Bổ sung thêm lợi ích/chất dinh dưỡng mang lại cho cơ thể', '610': 'Bổ sung thêm công thức nước phở Thìn đã có ngay trong gói phở ăn liền giúp tiết kiệm thời gian hơn khi ăn ngoài hàng'}, '90006|ĐIỀU CHỈNH NỘI DUNG (NET)': {'602': 'Kết hợp độc đáo đổi thành kết hợp hài hòa sẽ hay hơn', '603': 'Gạo nếp nương của vùng cao đổi thành gạo nếp nương đặc sản của vùng cao', '604': 'Giúp cân bằng cảm xúc ngay cả lúc mệt mỏi, cho cơ thể nhẹ nhàng và tạo nên một lối sống lành mạnh, thư thái đổi thành sản phẩm mang đến sức khỏe tốt cho cơ thể, giúp cơ thể hồi phục sức nhanh'}}}],}
    #     dict_qre_OE_info_org = dict()
    #     if codelist_file.filename:
    #         dict_qre_OE_info_org = eval(codelist_file.file.read())
    #
    #     dict_qre_OE_info = dict()
    #     for k, v in dict_qre_OE_info_org.items():
    #         oe_name, oe_num_col = k.rsplit('|', 1)
    #         oe_num_col = oe_num_col.split('-')
    #         oe_num_col = range(int(oe_num_col[0]), int(oe_num_col[1]) + 1)
    #
    #         for i in oe_num_col:
    #             dict_qre_OE_info.update({
    #                 f'{oe_name}_{i}': [f'{oe_name}_{i}'] + v
    #             })
    #
    #     # lst_addin_OE_value = [['Q0a_RespondentID', 1056, 'Main_Ma_san_pham_Q3', 1, 'Main_Q5a_OE_Ly_do_thich_Y1_1', 101],]
    #     lst_addin_OE_value = list()
    #     if coding_file.filename:
    #         lst_addin_OE_value = eval(coding_file.file.read())
    #
    #     # net code for exist qres
    #     dict_qre_net_info = {
    #         # 'Main_MA_vi_ngot_Q12_[0-9]+': {
    #         #     '2': 'Đường mía',
    #         #     '3': 'Đường cát/ đường trắng',
    #         #     '4': 'Đường phèn', '5': 'Đường bột',
    #         #     'net_code': {
    #         #
    #         #         '90001|TRÁI CÂY (NET)': {
    #         #             '1': 'Trái cây',
    #         #             '13': 'Trái cây, vui lòng ghi rõ loại trái cây',
    #         #             '201': 'Chanh dây', '202': 'Cam/ chanh/ quýt',
    #         #         },
    #         #     }
    #         # }
    #     }
    #     # End Define structure------------------------------------------------------------------------------------------
    #
    #     # Data stack format---------------------------------------------------------------------------------------------
    #     self.logger.info('Data stack format')
    #
    #     # df_data_stack generate
    #     df_data_scr = df_data_output.loc[:, [id_col] + lst_scr].copy()
    #     df_data_fc = df_data_output.loc[:, [id_col] + lst_fc].copy()
    #
    #     lst_df_data_sp = [df_data_output.loc[:, [id_col] + list(val.keys())].copy() for val in dict_sp.values()]
    #
    #     for i, df in enumerate(lst_df_data_sp):
    #         df.rename(columns=dict_sp[i + 1], inplace=True)
    #
    #     df_data_stack = pd.concat(lst_df_data_sp, axis=0, ignore_index=True)
    #
    #     df_data_stack = df_data_scr.merge(df_data_stack, how='left', on=[id_col])
    #     df_data_stack.reset_index(drop=True, inplace=True)
    #
    #     df_data_stack = df_data_stack.merge(df_data_fc, how='left', on=[id_col])
    #
    #     df_data_stack.sort_values(by=[id_col, sp_col], inplace=True)
    #     df_data_stack.reset_index(drop=True, inplace=True)
    #
    #     df_info_stack = df_info_output.copy()
    #
    #     for key, val in dict_sp[1].items():
    #         df_info_stack.loc[df_info_stack['var_name'] == key, ['var_name']] = [val]
    #
    #     df_info_stack.drop_duplicates(subset=['var_name'], keep='first', inplace=True)
    #
    #     if dict_qre_OE_info:
    #
    #         # ADD OE to Data stack--------------------------------------------------------------------------------------
    #         lst_OE_col = list(dict_qre_OE_info.keys())
    #         df_data_stack[lst_OE_col] = pd.DataFrame([[np.nan] * len(lst_OE_col)], index=df_data_stack.index)
    #
    #         # Remember edit this
    #         for item in lst_addin_OE_value:
    #             df_data_stack.loc[
    #                 (df_data_stack[item[0]] == item[1]) & (df_data_stack[item[2]] == item[3]), [item[4]]] = [item[5]]
    #
    #         # END ADD OE to Data stack----------------------------------------------------------------------------------
    #
    #         # ADD OE to Info stack--------------------------------------------------------------------------------------
    #         df_info_stack = pd.concat([df_info_stack,
    #                                    pd.DataFrame(columns=['var_name', 'var_lbl', 'var_type', 'val_lbl'],
    #                                                 data=list(dict_qre_OE_info.values()))], axis=0)
    #         # END ADD OE to Info stack----------------------------------------------------------------------------------
    #
    #     if dict_qre_net_info:
    #         pass
    #         # # ADD MA OE to Data stack-----------------------------------------------------------------------------------
    #         # # Remember edit this
    #         # for item in lst_addin_MA_value:
    #         #
    #         #     idx_item = df_data_stack.loc[(df_data_stack[item[0]] == item[1]) & (df_data_stack[item[2]] == item[3]), item[0]].index[0]
    #         #     str_ma_oe_name = item[4].rsplit('_', 1)[0]
    #         #     int_ma_oe_code = int(item[4].rsplit('_', 1)[1].replace('o', ''))
    #         #
    #         #     lst_ma_oe_col = df_info_stack.loc[df_info_stack['var_name'].str.contains(f'{str_ma_oe_name}_[0-9]+'), 'var_name'].values.tolist()
    #         #
    #         #     is_found = False
    #         #     for col in lst_ma_oe_col:
    #         #         if df_data_stack.at[idx_item, col] == int_ma_oe_code:
    #         #             is_found = True
    #         #             df_data_stack.at[idx_item, col] = item[5]
    #         #             break
    #         #
    #         #     if not is_found:
    #         #         for col in lst_ma_oe_col:
    #         #             if pd.isnull(df_data_stack.at[idx_item, col]):
    #         #                 df_data_stack.at[idx_item, col] = item[5]
    #         #                 break
    #         # # END ADD MA OE to Data stack-------------------------------------------------------------------------------
    #
    #         # # ADD MA OE to Info stack--------------------------------------------------------------------------------------
    #         # for key, val in dict_qre_net_info.items():
    #         #     df_info_stack.loc[df_info_stack['var_name'].str.contains(key), 'val_lbl'] = [val] * df_info_stack.loc[df_info_stack['var_name'].str.contains(key), 'val_lbl'].shape[0]
    #         # # END ADD MA OE to Info stack----------------------------------------------------------------------------------
    #
    #
    #     # Reset df_info_stack index
    #     df_info_stack['idx_var_name'] = df_info_stack['var_name']
    #     df_info_stack.set_index('idx_var_name', inplace=True)
    #     df_info_stack = df_info_stack.loc[list(df_data_stack.columns), :]
    #     df_info_stack.reindex(list(df_data_stack.columns))
    #     df_info_stack.reset_index(drop=True, inplace=True)
    #
    #     # df_data_stack.to_excel('zzzz_df_data_stack.csv')
    #     # df_info_stack.to_excel('zzzz_df_info_stack.csv')
    #     # End Data stack format-----------------------------------------------------------------------------------------
    #
    #     # Data unstack format-------------------------------------------------------------------------------------------
    #     self.logger.info('Data unstack format')
    #
    #     lst_col_part_body = list(dict_sp[1].values())
    #     lst_col_part_body.remove(sp_col)
    #
    #     dict_stack_structure = {
    #         'id_col': id_col,
    #         'sp_col': sp_col,
    #         'lst_col_part_head': lst_scr,
    #         'lst_col_part_body': lst_col_part_body,
    #         'lst_col_part_tail': lst_fc,
    #     }
    #
    #     df_data_unstack, df_info_unstack = self.convert_to_unstack(df_data_stack, df_info_stack, dict_stack_structure)
    #
    #     # df_data_unstack.to_csv('zzzz_df_data_unstack.csv', encoding='utf-8-sig')
    #     # df_info_unstack.to_csv('zzzz_df_info_unstack.csv', encoding='utf-8-sig')
    #     # End Data unstack format---------------------------------------------------------------------------------------
    #
    #     # Export data tables--------------------------------------------------------------------------------------------
    #     if tables_format_file.filename:
    #
    #         # ADD MEAN & GROUP------------------------------------------------------------------------------------------
    #         self.logger.info('ADD MEAN & GROUP')
    #
    #         lst_qre_mean = list()
    #         lst_qre_group = list()
    #
    #         if dict_qre_group_mean:
    #             for key, val in dict_qre_group_mean.items():
    #
    #                 if val['range']:
    #                     for i in val['range']:
    #                         lst_qre_mean.append([f'{key}_{i}', val['mean']])
    #                         lst_qre_group.append([f'{key}_{i}', val['group']])
    #                 else:
    #                     lst_qre_mean.append([key, val['mean']])
    #                     lst_qre_group.append([key, val['group']])
    #
    #         # End ADD MEAN & GROUP--------------------------------------------------------------------------------------
    #
    #         str_topline_file_name = self.str_file_name.replace('.xlsx', '_Topline.xlsx')
    #         DataTableGenerator.__init__(self, df_data=df_data_stack, df_info=df_info_stack,
    #                                     xlsx_name=str_topline_file_name, logger=self.logger,
    #                                     lst_qre_group=lst_qre_group, lst_qre_mean=lst_qre_mean, is_md=False)
    #
    #         lst_func_to_run = eval(tables_format_file.file.read())
    #         self.run_tables_by_js_files(lst_func_to_run)
    #         self.format_sig_table()
    #     # End Export data tables----------------------------------------------------------------------------------------
    #
    #     # Generate SAV files--------------------------------------------------------------------------------------------
    #     self.logger.info('Generate SAV files')
    #
    #     # Remove net_code to export sav---------------------------------------------------------------------------------
    #     df_info_stack_without_net = df_info_stack.copy()
    #
    #     for idx in df_info_stack_without_net.index:
    #         val_lbl = df_info_stack_without_net.at[idx, 'val_lbl']
    #
    #         if 'net_code' in val_lbl.keys():
    #             df_info_stack_without_net.at[idx, 'val_lbl'] = self.unnetted_qre_val(val_lbl)
    #     # END Remove net_code to export sav-----------------------------------------------------------------------------
    #
    #     self.generate_sav_sps(df_data=df_data_stack, df_qres_info=df_info_stack_without_net,
    #                           df_data_2=df_data_unstack, df_qres_info_2=df_info_unstack,
    #                           is_md=False, is_export_xlsx=True)
    #     # END Generate SAV files----------------------------------------------------------------------------------------


