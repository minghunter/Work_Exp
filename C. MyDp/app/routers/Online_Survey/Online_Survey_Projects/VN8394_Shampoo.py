from app.classes.AP_DataConverter import APDataConverter
from ..export_online_survey_data_table import DataTableGenerator
from ..convert_unstack import ConvertUnstack
from ..convert_stack import ConvertStack
import pandas as pd
import numpy as np
import time
import traceback
import datetime


class VN8394Shampoo(APDataConverter, DataTableGenerator, ConvertUnstack, ConvertStack):

    def convert_vn8394_shampoo(self, py_script_file, tables_format_file, codelist_file, coding_file):
        """DO NOT EDIT THIS FUNCTION"""

        try:
            start_time = time.time()

            if py_script_file:
                exec(py_script_file.file.read())
                exit()

            self.processing_script_vn8394_shampoo(tables_format_file, codelist_file, coding_file)

            self.logger.warning('DATA PROCESSING COMPLETED in %d seconds' % (time.time() - start_time))

        except Exception:
            self.logger.error(traceback.format_exc())
            str_err_log_name = self.str_file_name.replace('.xlsx', '_Errors.txt')
            with open(str_err_log_name, 'w') as err_log_txt:
                err_log_txt.writelines(traceback.format_exc())


    def processing_script_vn8394_shampoo(self, tables_format_file, codelist_file, coding_file):
        """
        :param tables_format_file: txt uploaded table format file
        :param codelist_file: txt uploaded codelist file
        :param coding_file: txt uploaded coding file
        :return: zip file include files: .sav, .sps, .xlsx(rawdata, topline)

        EDIT FROM HERE
        """

        # Convert system rawdata to dataframes--------------------------------------------------------------------------
        self.logger.info('Convert system rawdata to dataframes')
        df_data, df_info = self.convert_df_mc()


        # Pre processing------------------------------------------------------------------------------------------------
        info_col_name = ['var_name', 'var_lbl', 'var_type', 'val_lbl']

        self.logger.info('Pre processing')

        dict_add_new_qres = {
            'CITY_Combined': ['CITY', 'SA', {'1': 'HCM', '2': 'HN'}, 2],
        }

        for key, val in dict_add_new_qres.items():
            df_info = pd.concat([df_info, pd.DataFrame(columns=info_col_name, data=[[key, val[0], val[1], val[2]]])], axis=0, ignore_index=True)
            df_data = pd.concat([df_data, pd.DataFrame(columns=[key], data=[val[-1]] * df_data.shape[0])], axis=1)

        df_fil = df_data.query('Recruit_S1_Thanh_pho.isin([1, 2])')
        df_data.loc[df_fil.index, ['CITY_Combined']] = [1]
        del df_fil

        df_data.reset_index(drop=True, inplace=True)
        df_info.reset_index(drop=True, inplace=True)

        # # ----------------------------------
        # df_data.to_excel('zzz_df_data.xlsx')
        # df_info.to_excel('zzz_df_info.xlsx')
        # # ----------------------------------


        # --------------------------------------------------------------------------------------------------------------
        df_data_unstack, df_info_unstack = df_data.copy(), df_info.copy()
        # --------------------------------------------------------------------------------------------------------------

        dict_add_new_qres = {


            'Ma_SP_A': ['Mã SP', 'SA', {'1': 'Ý tưởng A', '2': 'Ý tưởng B', '3': 'Ý tưởng C', '4': 'Ý tưởng D'}, 1],
            'Ma_SP_B': ['Mã SP', 'SA', {'1': 'Ý tưởng A', '2': 'Ý tưởng B', '3': 'Ý tưởng C', '4': 'Ý tưởng D'}, 2],
            'Ma_SP_C': ['Mã SP', 'SA', {'1': 'Ý tưởng A', '2': 'Ý tưởng B', '3': 'Ý tưởng C', '4': 'Ý tưởng D'}, 3],
            'Ma_SP_D': ['Mã SP', 'SA', {'1': 'Ý tưởng A', '2': 'Ý tưởng B', '3': 'Ý tưởng C', '4': 'Ý tưởng D'}, 4],

            'Main_Q8_FC_A': ['Q8. FORCE CHOICE', 'SA', {'1': 'Yes', '2': 'No'}, np.nan],
            'Main_Q8_FC_B': ['Q8. FORCE CHOICE', 'SA', {'1': 'Yes', '2': 'No'}, np.nan],
            'Main_Q8_FC_C': ['Q8. FORCE CHOICE', 'SA', {'1': 'Yes', '2': 'No'}, np.nan],
            'Main_Q8_FC_D': ['Q8. FORCE CHOICE', 'SA', {'1': 'Yes', '2': 'No'}, np.nan],

            'Main_Q9_OE_FC_A': ['Q9. OE FORCE CHOICE', 'FT', {}, np.nan],
            'Main_Q9_OE_FC_B': ['Q9. OE FORCE CHOICE', 'FT', {}, np.nan],
            'Main_Q9_OE_FC_C': ['Q9. OE FORCE CHOICE', 'FT', {}, np.nan],
            'Main_Q9_OE_FC_D': ['Q9. OE FORCE CHOICE', 'FT', {}, np.nan],

            'A1_Ma_ND_1': ['A1. Nhận định', 'SA', {'1': 'Mái tóc dày mượt giúp mình tự tin trong cuộc sống. Nhưng tóc mình dễ bị gãy rụng sau khi gội. Ước gì mái tóc luôn mượt mà chắc khoẻ và không gãy rụng để mình luôn tự tin trong mọi tình huống', '2': 'Sau một ngày làm việc mệt mỏi, tôi rất thích được gội đầu vì mỗi khi gội xong, tóc tôi sẽ mềm mượt và không bết dính, làm tôi cảm giác nhẹ tênh, thư thái như trút bỏ được mọi lo âu.', '3': 'Dầu gội thông thường giúp mềm mượt tóc nhưng lại hay bết dính. Tôi phải gội đầu rất thường xuyên. Ước gì có một loại dầu gội mềm mượt mà không bết dính để tôi không bận tâm đến việc gội đầu mỗi ngày', '4': 'Mình mong muốn có được mái tóc mềm mượt và chắc khỏe, nhưng những loại Dầu Gội thông thường chỉ giúp mượt tóc, tóc mình vẫn yếu và dễ đứt gãy. Ước gì tóc mình vừa chắc khỏe vừa mềm mượt', '5': 'Dưới tác động của thời tiết nắng nóng, gió và khói bụi, mái tóc trở nên xơ rối khiến mình ái ngại với khoảnh khắc xoã tóc của mình sau khi phải buộc tóc lên. Mình muốn tự tin bung xoã mái tóc mượt trong mọi khoảnh khắc để theo đuổi ước mơ xa', '6': 'Không có câu nào đúng với chị'}, 1],
            'A1_Ma_ND_2': ['A1. Nhận định', 'SA', {'1': 'Mái tóc dày mượt giúp mình tự tin trong cuộc sống. Nhưng tóc mình dễ bị gãy rụng sau khi gội. Ước gì mái tóc luôn mượt mà chắc khoẻ và không gãy rụng để mình luôn tự tin trong mọi tình huống', '2': 'Sau một ngày làm việc mệt mỏi, tôi rất thích được gội đầu vì mỗi khi gội xong, tóc tôi sẽ mềm mượt và không bết dính, làm tôi cảm giác nhẹ tênh, thư thái như trút bỏ được mọi lo âu.', '3': 'Dầu gội thông thường giúp mềm mượt tóc nhưng lại hay bết dính. Tôi phải gội đầu rất thường xuyên. Ước gì có một loại dầu gội mềm mượt mà không bết dính để tôi không bận tâm đến việc gội đầu mỗi ngày', '4': 'Mình mong muốn có được mái tóc mềm mượt và chắc khỏe, nhưng những loại Dầu Gội thông thường chỉ giúp mượt tóc, tóc mình vẫn yếu và dễ đứt gãy. Ước gì tóc mình vừa chắc khỏe vừa mềm mượt', '5': 'Dưới tác động của thời tiết nắng nóng, gió và khói bụi, mái tóc trở nên xơ rối khiến mình ái ngại với khoảnh khắc xoã tóc của mình sau khi phải buộc tóc lên. Mình muốn tự tin bung xoã mái tóc mượt trong mọi khoảnh khắc để theo đuổi ước mơ xa', '6': 'Không có câu nào đúng với chị'}, 2],
            'A1_Ma_ND_3': ['A1. Nhận định', 'SA', {'1': 'Mái tóc dày mượt giúp mình tự tin trong cuộc sống. Nhưng tóc mình dễ bị gãy rụng sau khi gội. Ước gì mái tóc luôn mượt mà chắc khoẻ và không gãy rụng để mình luôn tự tin trong mọi tình huống', '2': 'Sau một ngày làm việc mệt mỏi, tôi rất thích được gội đầu vì mỗi khi gội xong, tóc tôi sẽ mềm mượt và không bết dính, làm tôi cảm giác nhẹ tênh, thư thái như trút bỏ được mọi lo âu.', '3': 'Dầu gội thông thường giúp mềm mượt tóc nhưng lại hay bết dính. Tôi phải gội đầu rất thường xuyên. Ước gì có một loại dầu gội mềm mượt mà không bết dính để tôi không bận tâm đến việc gội đầu mỗi ngày', '4': 'Mình mong muốn có được mái tóc mềm mượt và chắc khỏe, nhưng những loại Dầu Gội thông thường chỉ giúp mượt tóc, tóc mình vẫn yếu và dễ đứt gãy. Ước gì tóc mình vừa chắc khỏe vừa mềm mượt', '5': 'Dưới tác động của thời tiết nắng nóng, gió và khói bụi, mái tóc trở nên xơ rối khiến mình ái ngại với khoảnh khắc xoã tóc của mình sau khi phải buộc tóc lên. Mình muốn tự tin bung xoã mái tóc mượt trong mọi khoảnh khắc để theo đuổi ước mơ xa', '6': 'Không có câu nào đúng với chị'}, 3],
            'A1_Ma_ND_4': ['A1. Nhận định', 'SA', {'1': 'Mái tóc dày mượt giúp mình tự tin trong cuộc sống. Nhưng tóc mình dễ bị gãy rụng sau khi gội. Ước gì mái tóc luôn mượt mà chắc khoẻ và không gãy rụng để mình luôn tự tin trong mọi tình huống', '2': 'Sau một ngày làm việc mệt mỏi, tôi rất thích được gội đầu vì mỗi khi gội xong, tóc tôi sẽ mềm mượt và không bết dính, làm tôi cảm giác nhẹ tênh, thư thái như trút bỏ được mọi lo âu.', '3': 'Dầu gội thông thường giúp mềm mượt tóc nhưng lại hay bết dính. Tôi phải gội đầu rất thường xuyên. Ước gì có một loại dầu gội mềm mượt mà không bết dính để tôi không bận tâm đến việc gội đầu mỗi ngày', '4': 'Mình mong muốn có được mái tóc mềm mượt và chắc khỏe, nhưng những loại Dầu Gội thông thường chỉ giúp mượt tóc, tóc mình vẫn yếu và dễ đứt gãy. Ước gì tóc mình vừa chắc khỏe vừa mềm mượt', '5': 'Dưới tác động của thời tiết nắng nóng, gió và khói bụi, mái tóc trở nên xơ rối khiến mình ái ngại với khoảnh khắc xoã tóc của mình sau khi phải buộc tóc lên. Mình muốn tự tin bung xoã mái tóc mượt trong mọi khoảnh khắc để theo đuổi ước mơ xa', '6': 'Không có câu nào đúng với chị'}, 4],
            'A1_Ma_ND_5': ['A1. Nhận định', 'SA', {'1': 'Mái tóc dày mượt giúp mình tự tin trong cuộc sống. Nhưng tóc mình dễ bị gãy rụng sau khi gội. Ước gì mái tóc luôn mượt mà chắc khoẻ và không gãy rụng để mình luôn tự tin trong mọi tình huống', '2': 'Sau một ngày làm việc mệt mỏi, tôi rất thích được gội đầu vì mỗi khi gội xong, tóc tôi sẽ mềm mượt và không bết dính, làm tôi cảm giác nhẹ tênh, thư thái như trút bỏ được mọi lo âu.', '3': 'Dầu gội thông thường giúp mềm mượt tóc nhưng lại hay bết dính. Tôi phải gội đầu rất thường xuyên. Ước gì có một loại dầu gội mềm mượt mà không bết dính để tôi không bận tâm đến việc gội đầu mỗi ngày', '4': 'Mình mong muốn có được mái tóc mềm mượt và chắc khỏe, nhưng những loại Dầu Gội thông thường chỉ giúp mượt tóc, tóc mình vẫn yếu và dễ đứt gãy. Ước gì tóc mình vừa chắc khỏe vừa mềm mượt', '5': 'Dưới tác động của thời tiết nắng nóng, gió và khói bụi, mái tóc trở nên xơ rối khiến mình ái ngại với khoảnh khắc xoã tóc của mình sau khi phải buộc tóc lên. Mình muốn tự tin bung xoã mái tóc mượt trong mọi khoảnh khắc để theo đuổi ước mơ xa', '6': 'Không có câu nào đúng với chị'}, 5],
            'A1_Ma_ND_6': ['A1. Nhận định', 'SA', {'1': 'Mái tóc dày mượt giúp mình tự tin trong cuộc sống. Nhưng tóc mình dễ bị gãy rụng sau khi gội. Ước gì mái tóc luôn mượt mà chắc khoẻ và không gãy rụng để mình luôn tự tin trong mọi tình huống', '2': 'Sau một ngày làm việc mệt mỏi, tôi rất thích được gội đầu vì mỗi khi gội xong, tóc tôi sẽ mềm mượt và không bết dính, làm tôi cảm giác nhẹ tênh, thư thái như trút bỏ được mọi lo âu.', '3': 'Dầu gội thông thường giúp mềm mượt tóc nhưng lại hay bết dính. Tôi phải gội đầu rất thường xuyên. Ước gì có một loại dầu gội mềm mượt mà không bết dính để tôi không bận tâm đến việc gội đầu mỗi ngày', '4': 'Mình mong muốn có được mái tóc mềm mượt và chắc khỏe, nhưng những loại Dầu Gội thông thường chỉ giúp mượt tóc, tóc mình vẫn yếu và dễ đứt gãy. Ước gì tóc mình vừa chắc khỏe vừa mềm mượt', '5': 'Dưới tác động của thời tiết nắng nóng, gió và khói bụi, mái tóc trở nên xơ rối khiến mình ái ngại với khoảnh khắc xoã tóc của mình sau khi phải buộc tóc lên. Mình muốn tự tin bung xoã mái tóc mượt trong mọi khoảnh khắc để theo đuổi ước mơ xa', '6': 'Không có câu nào đúng với chị'}, 6],

            'A1_ND_1_YN': ['A1. Nhận định', 'SA', {'1': 'Yes', '2': 'No'}, 2],
            'A1_ND_2_YN': ['A1. Nhận định', 'SA', {'1': 'Yes', '2': 'No'}, 2],
            'A1_ND_3_YN': ['A1. Nhận định', 'SA', {'1': 'Yes', '2': 'No'}, 2],
            'A1_ND_4_YN': ['A1. Nhận định', 'SA', {'1': 'Yes', '2': 'No'}, 2],
            'A1_ND_5_YN': ['A1. Nhận định', 'SA', {'1': 'Yes', '2': 'No'}, 2],
            'A1_ND_6_YN': ['A1. Nhận định', 'SA', {'1': 'Yes', '2': 'No'}, 2],
        }

        for key, val in dict_add_new_qres.items():
            df_info = pd.concat([df_info, pd.DataFrame(columns=info_col_name, data=[[key, val[0], val[1], val[2]]])], axis=0, ignore_index=True)
            df_data = pd.concat([df_data, pd.DataFrame(columns=[key], data=[val[-1]] * df_data.shape[0])], axis=1)


        for i, v in enumerate(['A', 'B', 'C', 'D']):

            df_data.loc[:, [f'Main_Q8_FC_{v}']] = [2]  # 2 = N0

            df_fil = df_data.query(f'Main_Q8_FC == {i + 1}')

            df_data.loc[df_fil.index, [f'Main_Q8_FC_{v}']] = [1]  # 1 = Yes
            df_data.loc[df_fil.index, f'Main_Q9_OE_FC_{v}'] = df_data.loc[df_fil.index, f'Main_Q9_OE_FC']

            del df_fil

        for i in range(1, 7):
            df_fil = df_data.query(f"Main_A1_Nhan_dinh == {i}")
            df_data.loc[df_fil.index, [f'A1_ND_{i}_YN']] = [1]
            del df_fil


        df_data.reset_index(drop=True, inplace=True)
        df_info.reset_index(drop=True, inplace=True)

        # # ----------------------------------
        # df_data.to_excel('zzz_df_data.xlsx')
        # df_info.to_excel('zzz_df_info.xlsx')
        # # ----------------------------------

        # Define structure----------------------------------------------------------------------------------------------
        self.logger.info('Define structure')

        id_col = 'Q0a_RespondentID'

        sp_col = 'Ma_SP'
        nd_col = 'A1_Ma_ND'

        lst_scr = [
            'Recruit_S1_Thanh_pho',
            'Recruit_S1_Thanh_pho_o5',
            'CITY_Combined',
            'Recruit_S2a_Tuoi',
            'Recruit_S2b_Do_tuoi',
            'Recruit_S3_Quyet_dinh',
            'Recruit_S4a_Dau_goi_1',
            'Recruit_S4a_Dau_goi_2',
            'Recruit_S4a_Dau_goi_3',
            'Recruit_S4a_Dau_goi_4',
            'Recruit_S4a_Dau_goi_5',
            'Recruit_S4a_Dau_goi_6',
            'Recruit_S4a_Dau_goi_7',
            'Recruit_S4a_Dau_goi_8',
            'Recruit_S4a_Dau_goi_9',
            'Recruit_S4a_Dau_goi_10',
            'Recruit_S4a_Dau_goi_11',
            'Recruit_S4a_Dau_goi_12',
            'Recruit_S4a_Dau_goi_13',
            'Recruit_S4a_Dau_goi_14',
            'Recruit_S4a_Dau_goi_15',
            'Recruit_S4a_Dau_goi_16',
            'Recruit_S4a_Dau_goi_17',
            'Recruit_S4a_Dau_goi_18',
            'Recruit_S4a_Dau_goi_19',
            'Recruit_S4a_Dau_goi_20',
            'Recruit_S4a_Dau_goi_21',
            'Recruit_S4a_Dau_goi_o20',
            'Recruit_S4a_Dau_goi_o21',
            'Recruit_S4b_BUMO',
            'Recruit_S5_Tan_suat',
            'Recruit_S6_Dau_xa_1',
            'Recruit_S6_Dau_xa_2',
            'Recruit_S6_Dau_xa_3',
            'Recruit_S6_Dau_xa_4',
            'Recruit_S6_Dau_xa_5',
            'Recruit_S6_Dau_xa_6',
            'Recruit_S6_Dau_xa_7',
            'Recruit_S6_Dau_xa_8',
            'Recruit_S6_Dau_xa_9',
            'Recruit_S6_Dau_xa_10',
            'Recruit_S6_Dau_xa_11',
            'Recruit_S6_Dau_xa_12',
            'Recruit_S6_Dau_xa_13',
            'Recruit_S6_Dau_xa_14',
            'Recruit_S6_Dau_xa_15',
            'Recruit_S6_Dau_xa_16',
            'Recruit_S6_Dau_xa_17',
            'Recruit_S6_Dau_xa_18',
            'Recruit_S6_Dau_xa_o18',
            'Recruit_S7_Dau_goi_dau_xa',
            'Recruit_S8_Tinh_trang_hon_nhan',
            'Recruit_S9_Thu_nhap_HGD',
            'Recruit_S10_Nganh_cam_1',
            'Recruit_S10_Nganh_cam_2',
            'Recruit_S10_Nganh_cam_3',
            'Recruit_S10_Nganh_cam_4',
            'Recruit_S10_Nganh_cam_5',
            'Recruit_S11_Nghe_nghiep_hien_tai',
            'Recruit_S11_Nghe_nghiep_hien_tai_o7',
            'Recruit_S11_Nghe_nghiep_hien_tai_o8',
            'Recruit_S12_Tham_gia_NCTT',
            'Recruit_S13_Suc_khoe_1',
            'Recruit_S13_Suc_khoe_2',
            'Recruit_S13_Suc_khoe_3',
            'Recruit_S13_Suc_khoe_4',
            'Recruit_T1',
            'Main_A1_Nhan_dinh',
            'Main_A2_So_cong_dung',
            'Main_A3a_Cong_dung_1',
            'Main_A3a_Cong_dung_1_o11',
            'Main_A3b_Cong_dung_2_1',
            'Main_A3b_Cong_dung_2_2',
            'Main_A3b_Cong_dung_2_3',
            'Main_A3b_Cong_dung_2_4',
            'Main_A3b_Cong_dung_2_5',
            'Main_A3b_Cong_dung_2_6',
            'Main_A3b_Cong_dung_2_7',
            'Main_A3b_Cong_dung_2_8',
            'Main_A3b_Cong_dung_2_9',
            'Main_A3b_Cong_dung_2_10',
            'Main_A3b_Cong_dung_2_11',
            'Main_A3b_Cong_dung_2_o11',
            'Main_A4a_Chong_rung_toc',
            'Main_A4b_Moc_toc',
            'Main_A4c_Mem_muot',
            'Main_A4d_Ngan_bet_dinh',
            'Main_A4e_Mui_huong',
            'Main_A5_Mui_huong',
            'Main_A5_Mui_huong_o7',
            'Main_A100_Tham_gia_khao_sat_01',
            'Main_A100_Tham_gia_khao_sat_02',
            'Main_A100_Tham_gia_khao_sat_03',
            'Main_A100_Tham_gia_khao_sat_04',
        ]

        dict_main = {
            1: {
                'Ma_SP_A': 'Ma_SP',
                'Main_Q1_OL_Concept_A': 'Main_Q1_OL',
                'Main_Q2a_OE_Like_Concept_A': 'Main_Q2a_OE_Like',
                'Main_Q2b_OE_DisLike_Concept_A': 'Main_Q2b_OE_DisLike',
                'Main_Q3_Moi_la_Concept_A': 'Main_Q3_Moi_la',
                'Main_Q3a_OE_Moi_la_Concept_A': 'Main_Q3a_OE_Moi_la',
                'Main_Q4_Dang_tin_Concept_A': 'Main_Q4_Dang_tin',
                'Main_Q5_Phu_hop_Concept_A': 'Main_Q5_Phu_hop',
                'Main_Q6_PI_Concept_A': 'Main_Q6_PI',
                'Main_Q7b_OE_Khong_mua_Concept_A': 'Main_Q7b_OE_Khong_mua',
                'Main_Q8_FC_A': 'Main_Q8_FC_YN',
                'Main_Q9_OE_FC_A': 'Main_Q9_OE_FC',
            },
            2: {
                'Ma_SP_B': 'Ma_SP',
                'Main_Q1_OL_Concept_B': 'Main_Q1_OL',
                'Main_Q2a_OE_Like_Concept_B': 'Main_Q2a_OE_Like',
                'Main_Q2b_OE_DisLike_Concept_B': 'Main_Q2b_OE_DisLike',
                'Main_Q3_Moi_la_Concept_B': 'Main_Q3_Moi_la',
                'Main_Q3a_OE_Moi_la_Concept_B': 'Main_Q3a_OE_Moi_la',
                'Main_Q4_Dang_tin_Concept_B': 'Main_Q4_Dang_tin',
                'Main_Q5_Phu_hop_Concept_B': 'Main_Q5_Phu_hop',
                'Main_Q6_PI_Concept_B': 'Main_Q6_PI',
                'Main_Q7b_OE_Khong_mua_Concept_B': 'Main_Q7b_OE_Khong_mua',
                'Main_Q8_FC_B': 'Main_Q8_FC_YN',
                'Main_Q9_OE_FC_B': 'Main_Q9_OE_FC',
            },
            3: {
                'Ma_SP_C': 'Ma_SP',
                'Main_Q1_OL_Concept_C': 'Main_Q1_OL',
                'Main_Q2a_OE_Like_Concept_C': 'Main_Q2a_OE_Like',
                'Main_Q2b_OE_DisLike_Concept_C': 'Main_Q2b_OE_DisLike',
                'Main_Q3_Moi_la_Concept_C': 'Main_Q3_Moi_la',
                'Main_Q3a_OE_Moi_la_Concept_C': 'Main_Q3a_OE_Moi_la',
                'Main_Q4_Dang_tin_Concept_C': 'Main_Q4_Dang_tin',
                'Main_Q5_Phu_hop_Concept_C': 'Main_Q5_Phu_hop',
                'Main_Q6_PI_Concept_C': 'Main_Q6_PI',
                'Main_Q7b_OE_Khong_mua_Concept_C': 'Main_Q7b_OE_Khong_mua',
                'Main_Q8_FC_C': 'Main_Q8_FC_YN',
                'Main_Q9_OE_FC_C': 'Main_Q9_OE_FC',
            },
            4: {
                'Ma_SP_D': 'Ma_SP',
                'Main_Q1_OL_Concept_D': 'Main_Q1_OL',
                'Main_Q2a_OE_Like_Concept_D': 'Main_Q2a_OE_Like',
                'Main_Q2b_OE_DisLike_Concept_D': 'Main_Q2b_OE_DisLike',
                'Main_Q3_Moi_la_Concept_D': 'Main_Q3_Moi_la',
                'Main_Q3a_OE_Moi_la_Concept_D': 'Main_Q3a_OE_Moi_la',
                'Main_Q4_Dang_tin_Concept_D': 'Main_Q4_Dang_tin',
                'Main_Q5_Phu_hop_Concept_D': 'Main_Q5_Phu_hop',
                'Main_Q6_PI_Concept_D': 'Main_Q6_PI',
                'Main_Q7b_OE_Khong_mua_Concept_D': 'Main_Q7b_OE_Khong_mua',
                'Main_Q8_FC_D': 'Main_Q8_FC_YN',
                'Main_Q9_OE_FC_D': 'Main_Q9_OE_FC',
            },
        }

        lst_fc = [
            'Main_Q2c_SA_Impressive_Concept_A_1',
            'Main_Q2c_SA_Impressive_Concept_A_2',
            'Main_Q2c_SA_Impressive_Concept_A_3',
            'Main_Q2c_SA_Impressive_Concept_A_4',
            'Main_Q2c_SA_Impressive_Concept_A_5',
            'Main_Q2c_SA_Impressive_Concept_A_6',
            'Main_Q2c_SA_Impressive_Concept_A_7',
            'Main_Q2c_SA_Impressive_Concept_A_8',
            'Main_Q2c_SA_Impressive_Concept_A_9',
            'Main_Q2c_SA_Impressive_Concept_A_10',
            'Main_Q2c_SA_Impressive_Concept_A_11',
            'Main_Q2c_SA_Impressive_Concept_A_12',
            'Main_Q2c_SA_Impressive_Concept_A_13',
            'Main_Q2c_SA_Impressive_Concept_A_14',
            'Main_Q2c_SA_Impressive_Concept_A_o14',

            'Main_Q2c_SA_Impressive_Concept_B_1',
            'Main_Q2c_SA_Impressive_Concept_B_2',
            'Main_Q2c_SA_Impressive_Concept_B_3',
            'Main_Q2c_SA_Impressive_Concept_B_4',
            'Main_Q2c_SA_Impressive_Concept_B_5',
            'Main_Q2c_SA_Impressive_Concept_B_6',
            'Main_Q2c_SA_Impressive_Concept_B_7',
            'Main_Q2c_SA_Impressive_Concept_B_8',
            'Main_Q2c_SA_Impressive_Concept_B_9',
            'Main_Q2c_SA_Impressive_Concept_B_10',
            'Main_Q2c_SA_Impressive_Concept_B_11',
            'Main_Q2c_SA_Impressive_Concept_B_12',
            'Main_Q2c_SA_Impressive_Concept_B_13',
            'Main_Q2c_SA_Impressive_Concept_B_o13',

            'Main_Q2c_SA_Impressive_Concept_C_1',
            'Main_Q2c_SA_Impressive_Concept_C_2',
            'Main_Q2c_SA_Impressive_Concept_C_3',
            'Main_Q2c_SA_Impressive_Concept_C_4',
            'Main_Q2c_SA_Impressive_Concept_C_5',
            'Main_Q2c_SA_Impressive_Concept_C_6',
            'Main_Q2c_SA_Impressive_Concept_C_7',
            'Main_Q2c_SA_Impressive_Concept_C_8',
            'Main_Q2c_SA_Impressive_Concept_C_9',
            'Main_Q2c_SA_Impressive_Concept_C_10',
            'Main_Q2c_SA_Impressive_Concept_C_11',
            'Main_Q2c_SA_Impressive_Concept_C_12',
            'Main_Q2c_SA_Impressive_Concept_C_13',
            'Main_Q2c_SA_Impressive_Concept_C_14',
            'Main_Q2c_SA_Impressive_Concept_C_o14',

            'Main_Q2c_SA_Impressive_Concept_D_1',
            'Main_Q2c_SA_Impressive_Concept_D_2',
            'Main_Q2c_SA_Impressive_Concept_D_3',
            'Main_Q2c_SA_Impressive_Concept_D_4',
            'Main_Q2c_SA_Impressive_Concept_D_5',
            'Main_Q2c_SA_Impressive_Concept_D_6',
            'Main_Q2c_SA_Impressive_Concept_D_7',
            'Main_Q2c_SA_Impressive_Concept_D_8',
            'Main_Q2c_SA_Impressive_Concept_D_9',
            'Main_Q2c_SA_Impressive_Concept_D_10',
            'Main_Q2c_SA_Impressive_Concept_D_11',
            'Main_Q2c_SA_Impressive_Concept_D_o11',

            'Main_10a_Improvement',
            'Main_10a_Improvement_o4',
            'Main_10b_Improvement',
            'Main_10b_Improvement_o5',
            'Main_10c_Improvement',
            'Main_10c_Improvement_o5',
        ]

        dict_nd = {
            1: {
                'A1_Ma_ND_1': 'A1_Ma_ND',
                'A1_ND_1_YN': 'A1_ND_YN',
            },
            2: {
                'A1_Ma_ND_2': 'A1_Ma_ND',
                'A1_ND_2_YN': 'A1_ND_YN',
            },
            3: {
                'A1_Ma_ND_3': 'A1_Ma_ND',
                'A1_ND_3_YN': 'A1_ND_YN',
            },
            4: {
                'A1_Ma_ND_4': 'A1_Ma_ND',
                'A1_ND_4_YN': 'A1_ND_YN',
            },
            5: {
                'A1_Ma_ND_5': 'A1_Ma_ND',
                'A1_ND_5_YN': 'A1_ND_YN',
            },
            6: {
                'A1_Ma_ND_6': 'A1_Ma_ND',
                'A1_ND_6_YN': 'A1_ND_YN',
            },

        }

        # -----------------
        dict_qre_group_mean = {
            'Main_Q1_OL': {
                'range': [],  # f'0{i}' for i in range(1, 4)
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Main_Q3_Moi_la': {
                'range': [],  # f'0{i}' for i in range(1, 4)
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Main_Q4_Dang_tin': {
                'range': [],  # f'0{i}' for i in range(1, 4)
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Main_Q5_Phu_hop': {
                'range': [],  # f'0{i}' for i in range(1, 4)
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Main_Q6_PI': {
                'range': [],  # f'0{i}' for i in range(1, 4)
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
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
            "Main_Q2c_SA_Impressive_Concept_A_[0-9]+": {
                '1': '(Chiết xuất) 100% Từ Vỏ Bưởi',
                '2': '100 % từ Bưởi',
                '3': 'Vitamin C',
                '4': 'Biotin',
                '5': 'Nước chưng cất hoa hồng/nước chưng cất hoa hồng thơm dịu',
                '6': 'Detox',
                '7': 'Detox/thanh lọc/loại bỏ/ giảm/ làm sạch dầu thừa trên da đầu/thân tóc',
                '8': 'Mềm mượt, Bồng Bềnh Nhẹ Tênh',
                '9': 'Bồng Bềnh Nhẹ Tênh',
                '10': 'Phù hợp với quy định/được chứng nhận bởi FDA và được Viện Da Liễu Trung Ương',
                '11': '(Chiết xuất/ làm) Từ Vỏ Bưởi',
                '12': '(làm từ) Bưởi',
                '13': 'Không ấn tượng gì',
                '15': "Hoa hồng/ tinh chất hoa hồng/ hương hoa hồng",
                '16': "Chiết xuất từ thiên nhiên",
                '17': "Chống nhờn da đầu",
                '18': "Chống gãy rụng",
                '19': "Giúp tóc thơm và mềm",
                '14': 'Khác (ghi rõ)'
            },
            "Main_Q2c_SA_Impressive_Concept_B_[0-9]+": {
                '1': '(Chiết xuất) 100% Từ Vỏ Bưởi',
                '2': '100 % từ Bưởi',
                '3': 'Dầu Hạt Hoa Trà/ Dầu hạt hoa trà thơm mát',
                '4': 'Vitamin C',
                '5': 'Biotin',
                '6': '(Tóc) Chắc khỏe từ (sâu) bên trong',
                '7': '(Mái tóc) mềm mượt bên ngoài',
                '8': '(Tóc) Khỏe trong, mượt ngoài',
                '9': 'Phù hợp với quy định/được chứng nhận bởi FDA và được Viện Da Liễu Trung Ương',
                '10': '(Chiết xuất/ làm) Từ Vỏ Bưởi',
                '11': '(làm từ) Bưởi',
                '12': 'Không ấn tượng gì',
                '15': "Vỏ bưởi kết hợp với dầu hạt hoa trà",
                '16': "Tinh dầu hoa trà",
                '17': "Chiết xuất từ thiên nhiên, không hoá chất",
                '13': 'Khác (ghi rõ)'
            },
            "Main_Q2c_SA_Impressive_Concept_C_[0-9]+": {
                '1': '(Chiết xuất) 100% Từ Vỏ Bưởi',
                '2': '100 % từ Bưởi',
                '3': 'Tinh dầu hương thảo/Hương thảo',
                '4': 'Vitamin C',
                '5': 'Biotin',
                '6': 'Thơm mát',
                '7': '(Dưỡng tóc) mềm mượt chắc khỏe',
                '8': 'Ngăn ngừa gãy rụng',
                '9': 'Tóc mượt tự nhiên, không lo gãy rụng',
                '10': 'Phù hợp với quy định/được chứng nhận bởi FDA và được Viện Da Liễu Trung Ương',
                '11': '(Chiết xuất/ làm) Từ Vỏ Bưởi',
                '12': '(làm tử) Bưởi',
                '13': 'Không ấn tượng gì',
                '15': "Mềm mượt và không rụng tóc",
                '16': "Mượt tóc, không xơ rối",
                '17': "Giảm gãy rụng",
                '18': "Sự kết hợp của bưởi và hương thảo",
                '14': 'Khác (ghi rõ)'
            },
            "Main_Q2c_SA_Impressive_Concept_D_[0-9]+": {
                '1': 'Hệ dưỡng chất kết hợp Activ-Infusion/Activ-Infusion',
                '2': 'Tinh dầu argan/dầu argan',
                '3': 'Protein tơ tằm',
                '4': 'Vitamin C',
                '5': 'Ngát hương và mềm mượt gấp 5 lần',
                '6': 'Mềm mượt gấp 5 lần',
                '7': 'Sẵn sàng bung xõa',
                '8': 'Tạm biệt mái tóc khô/ xơ rối',
                '9': 'Mềm mượt diệu kì',
                '10': 'Không ấn tượng gì',
                '16': "Ngát hương/ Mùi ngát hương",
                '17': "Giảm xơ rối /Tóc không bị xơ rối",
                '19': "Giúp tóc mềm mượt/ mềm mượt/ mềm mượt như tơ tằm",
                '20': "Tơ tằm",
                '11': 'Khác (ghi rõ)'
            },

            # 'Q1a_[0-9]+': {
            #     'net_code': {
            #         '900001|net|GIA VỊ': {
            #             '1': 'Tương ớt',
            #             '2': 'Nước mắm',
            #             '3': 'Nước tương',
            #             '4': 'Tương cà',
            #             '5': 'Hạt nêm',
            #         },
            #         '900002|net|DẦU GỘI/SỮA TẮM/BỘT GIẶT': {
            #             '6': 'Dầu gội',
            #             '7': 'Sữa tắm',
            #             '8': 'Bột giặt',
            #         },
            #         '900003|net|THỊT ĐÃ CHẾ BIẾN': {
            #             '9': 'Thịt hộp',
            #             '10': 'Xúc xích thanh trùng',
            #         },
            #         '900004|net|SỮA HẠT SÔ-CÔ-LA / SỮA SÔ-CÔ-LA CHO BÉ': {
            #             '11': 'Sữa hạt sô-cô-la / sữa sô-cô-la cho con',
            #         },
            #         '900005|net|THỰC PHẨM TIỆN LỢI': {
            #             '12': 'Mì ăn liền',
            #             '13': 'Phở ăn liền',
            #         },
            #     }
            # },
        }

        lst_clear_ma_oe_value = {
            "Main_Q2c_SA_Impressive_Concept_A": [14],
            "Main_Q2c_SA_Impressive_Concept_B": [13],
            "Main_Q2c_SA_Impressive_Concept_C": [14],
            "Main_Q2c_SA_Impressive_Concept_D": [11],
        }

        lst_addin_MA_value = [
            ['Q0a_RespondentID', 1004, 'Ma_SP', 1, 'Main_Q2c_SA_Impressive_Concept_A', 12],
            ['Q0a_RespondentID', 1009, 'Ma_SP', 1, 'Main_Q2c_SA_Impressive_Concept_A', 11],
            ['Q0a_RespondentID', 1009, 'Ma_SP', 1, 'Main_Q2c_SA_Impressive_Concept_A', 15],
            ['Q0a_RespondentID', 1011, 'Ma_SP', 1, 'Main_Q2c_SA_Impressive_Concept_A', 15],
            ['Q0a_RespondentID', 1011, 'Ma_SP', 1, 'Main_Q2c_SA_Impressive_Concept_A', 11],
            ['Q0a_RespondentID', 1015, 'Ma_SP', 1, 'Main_Q2c_SA_Impressive_Concept_A', 15],
            ['Q0a_RespondentID', 1018, 'Ma_SP', 1, 'Main_Q2c_SA_Impressive_Concept_A', 15],
            ['Q0a_RespondentID', 1020, 'Ma_SP', 1, 'Main_Q2c_SA_Impressive_Concept_A', 9],
            ['Q0a_RespondentID', 1033, 'Ma_SP', 1, 'Main_Q2c_SA_Impressive_Concept_A', 16],
            ['Q0a_RespondentID', 1034, 'Ma_SP', 1, 'Main_Q2c_SA_Impressive_Concept_A', 17],
            ['Q0a_RespondentID', 1035, 'Ma_SP', 1, 'Main_Q2c_SA_Impressive_Concept_A', 18],
            ['Q0a_RespondentID', 1039, 'Ma_SP', 1, 'Main_Q2c_SA_Impressive_Concept_A', 5],
            ['Q0a_RespondentID', 1046, 'Ma_SP', 1, 'Main_Q2c_SA_Impressive_Concept_A', 15],
            ['Q0a_RespondentID', 1055, 'Ma_SP', 1, 'Main_Q2c_SA_Impressive_Concept_A', 19],
            ['Q0a_RespondentID', 1004, 'Ma_SP', 2, 'Main_Q2c_SA_Impressive_Concept_B', 15],
            ['Q0a_RespondentID', 1020, 'Ma_SP', 2, 'Main_Q2c_SA_Impressive_Concept_B', 12],
            ['Q0a_RespondentID', 1033, 'Ma_SP', 2, 'Main_Q2c_SA_Impressive_Concept_B', 17],
            ['Q0a_RespondentID', 1035, 'Ma_SP', 2, 'Main_Q2c_SA_Impressive_Concept_B', 12],
            ['Q0a_RespondentID', 1039, 'Ma_SP', 2, 'Main_Q2c_SA_Impressive_Concept_B', 16],
            ['Q0a_RespondentID', 1051, 'Ma_SP', 2, 'Main_Q2c_SA_Impressive_Concept_B', 16],
            ['Q0a_RespondentID', 1003, 'Ma_SP', 3, 'Main_Q2c_SA_Impressive_Concept_C', 15],
            ['Q0a_RespondentID', 1004, 'Ma_SP', 3, 'Main_Q2c_SA_Impressive_Concept_C', 13],
            ['Q0a_RespondentID', 1018, 'Ma_SP', 3, 'Main_Q2c_SA_Impressive_Concept_C', 16],
            ['Q0a_RespondentID', 1020, 'Ma_SP', 3, 'Main_Q2c_SA_Impressive_Concept_C', 17],
            ['Q0a_RespondentID', 1047, 'Ma_SP', 3, 'Main_Q2c_SA_Impressive_Concept_C', 17],
            ['Q0a_RespondentID', 1051, 'Ma_SP', 3, 'Main_Q2c_SA_Impressive_Concept_C', 18],
            ['Q0a_RespondentID', 1007, 'Ma_SP', 4, 'Main_Q2c_SA_Impressive_Concept_D', 16],
            ['Q0a_RespondentID', 1007, 'Ma_SP', 4, 'Main_Q2c_SA_Impressive_Concept_D', 19],
            ['Q0a_RespondentID', 1011, 'Ma_SP', 4, 'Main_Q2c_SA_Impressive_Concept_D', 16],
            ['Q0a_RespondentID', 1014, 'Ma_SP', 4, 'Main_Q2c_SA_Impressive_Concept_D', 17],
            ['Q0a_RespondentID', 1014, 'Ma_SP', 4, 'Main_Q2c_SA_Impressive_Concept_D', 19],
            ['Q0a_RespondentID', 1020, 'Ma_SP', 4, 'Main_Q2c_SA_Impressive_Concept_D', 17],
            ['Q0a_RespondentID', 1022, 'Ma_SP', 4, 'Main_Q2c_SA_Impressive_Concept_D', 19],
            ['Q0a_RespondentID', 1034, 'Ma_SP', 4, 'Main_Q2c_SA_Impressive_Concept_D', 16],
            ['Q0a_RespondentID', 1043, 'Ma_SP', 4, 'Main_Q2c_SA_Impressive_Concept_D', 20],
            ['Q0a_RespondentID', 1060, 'Ma_SP', 4, 'Main_Q2c_SA_Impressive_Concept_D', 19],
        ]



        # End Define structure------------------------------------------------------------------------------------------


        # CONVERT TO STACK----------------------------------------------------------------------------------------------
        df_data_stack, df_info_stack = self.convert_to_stack(df_data, df_info, id_col, sp_col, lst_scr, dict_main, lst_fc)
        df_data_stack_nd, df_info_stack_nd = self.convert_to_stack(df_data, df_info, id_col, nd_col, lst_scr, dict_nd, [])

        # CLEAR DATA
        dict_clear_q2c = {
            1: [
                'Main_Q2c_SA_Impressive_Concept_A_1',
                'Main_Q2c_SA_Impressive_Concept_A_2',
                'Main_Q2c_SA_Impressive_Concept_A_3',
                'Main_Q2c_SA_Impressive_Concept_A_4',
                'Main_Q2c_SA_Impressive_Concept_A_5',
                'Main_Q2c_SA_Impressive_Concept_A_6',
                'Main_Q2c_SA_Impressive_Concept_A_7',
                'Main_Q2c_SA_Impressive_Concept_A_8',
                'Main_Q2c_SA_Impressive_Concept_A_9',
                'Main_Q2c_SA_Impressive_Concept_A_10',
                'Main_Q2c_SA_Impressive_Concept_A_11',
                'Main_Q2c_SA_Impressive_Concept_A_12',
                'Main_Q2c_SA_Impressive_Concept_A_13',
                'Main_Q2c_SA_Impressive_Concept_A_14',
                'Main_Q2c_SA_Impressive_Concept_A_o14',
            ],
            2: [
                'Main_Q2c_SA_Impressive_Concept_B_1',
                'Main_Q2c_SA_Impressive_Concept_B_2',
                'Main_Q2c_SA_Impressive_Concept_B_3',
                'Main_Q2c_SA_Impressive_Concept_B_4',
                'Main_Q2c_SA_Impressive_Concept_B_5',
                'Main_Q2c_SA_Impressive_Concept_B_6',
                'Main_Q2c_SA_Impressive_Concept_B_7',
                'Main_Q2c_SA_Impressive_Concept_B_8',
                'Main_Q2c_SA_Impressive_Concept_B_9',
                'Main_Q2c_SA_Impressive_Concept_B_10',
                'Main_Q2c_SA_Impressive_Concept_B_11',
                'Main_Q2c_SA_Impressive_Concept_B_12',
                'Main_Q2c_SA_Impressive_Concept_B_13',
                'Main_Q2c_SA_Impressive_Concept_B_o13',
            ],
            3: [
                'Main_Q2c_SA_Impressive_Concept_C_1',
                'Main_Q2c_SA_Impressive_Concept_C_2',
                'Main_Q2c_SA_Impressive_Concept_C_3',
                'Main_Q2c_SA_Impressive_Concept_C_4',
                'Main_Q2c_SA_Impressive_Concept_C_5',
                'Main_Q2c_SA_Impressive_Concept_C_6',
                'Main_Q2c_SA_Impressive_Concept_C_7',
                'Main_Q2c_SA_Impressive_Concept_C_8',
                'Main_Q2c_SA_Impressive_Concept_C_9',
                'Main_Q2c_SA_Impressive_Concept_C_10',
                'Main_Q2c_SA_Impressive_Concept_C_11',
                'Main_Q2c_SA_Impressive_Concept_C_12',
                'Main_Q2c_SA_Impressive_Concept_C_13',
                'Main_Q2c_SA_Impressive_Concept_C_14',
                'Main_Q2c_SA_Impressive_Concept_C_o14',
            ],
            4: [
                'Main_Q2c_SA_Impressive_Concept_D_1',
                'Main_Q2c_SA_Impressive_Concept_D_2',
                'Main_Q2c_SA_Impressive_Concept_D_3',
                'Main_Q2c_SA_Impressive_Concept_D_4',
                'Main_Q2c_SA_Impressive_Concept_D_5',
                'Main_Q2c_SA_Impressive_Concept_D_6',
                'Main_Q2c_SA_Impressive_Concept_D_7',
                'Main_Q2c_SA_Impressive_Concept_D_8',
                'Main_Q2c_SA_Impressive_Concept_D_9',
                'Main_Q2c_SA_Impressive_Concept_D_10',
                'Main_Q2c_SA_Impressive_Concept_D_11',
                'Main_Q2c_SA_Impressive_Concept_D_o11',
            ],
        }

        for k, v in dict_clear_q2c.items():
            df_fil = df_data_stack.query(f"Ma_SP != {k}")
            df_data_stack.loc[df_fil.index, v] = [np.nan] * len(v)
            del df_fil


        lst_qre_q10abc = [
            'Main_10a_Improvement',
            'Main_10a_Improvement_o4',
            'Main_10b_Improvement',
            'Main_10b_Improvement_o5',
            'Main_10c_Improvement',
            'Main_10c_Improvement_o5',
        ]

        df_fil = df_data_stack.query("Main_Q8_FC_YN == 2")
        df_data_stack.loc[df_fil.index, lst_qre_q10abc] = [np.nan] * len(lst_qre_q10abc)
        del df_fil





        # CONVERT TO STACK----------------------------------------------------------------------------------------------

        # OE RUNNING
        if dict_qre_OE_info:

            # ADD OE to Data stack--------------------------------------------------------------------------------------
            lst_OE_col = list(dict_qre_OE_info.keys())
            df_data_stack[lst_OE_col] = pd.DataFrame([[np.nan] * len(lst_OE_col)], index=df_data_stack.index)

            # Remember edit this
            for item in lst_addin_OE_value:
                df_data_stack.loc[(df_data_stack[item[0]] == item[1]) & (df_data_stack[item[2]] == item[3]), [item[4]]] = [item[5]]

            # END ADD OE to Data stack----------------------------------------------------------------------------------

            # ADD OE to Info stack--------------------------------------------------------------------------------------
            df_info_stack = pd.concat([df_info_stack, pd.DataFrame(columns=['var_name', 'var_lbl', 'var_type', 'val_lbl'], data=list(dict_qre_OE_info.values()))], axis=0)
            # END ADD OE to Info stack----------------------------------------------------------------------------------

        if lst_addin_MA_value:
            # ADD MA OE to Data stack-----------------------------------------------------------------------------------
            # Remember edit this
            for item in lst_addin_MA_value:

                # ['Q0a_RespondentID', 1004, 'Ma_SP', 1, 'Main_Q2c_SA_Impressive_Concept_A', 12],
                df_fil_info = df_info_stack.query(f"var_name.str.contains('{item[-2]}_[0-9]+')")
                df_fil_data = df_data_stack.query(f"{item[0]} == {item[1]} & {item[2]} == {item[3]}")

                for idx_row in df_fil_data.index:
                    for col_name in df_fil_info['var_name'].values.tolist():
                        if pd.isnull(df_fil_data.at[idx_row, col_name]):
                            df_data_stack.loc[idx_row, col_name] = item[-1]
                            break
                        else:
                            lst_clear_val = lst_clear_ma_oe_value[item[-2]]
                            aaa = df_fil_data.at[idx_row, col_name]

                            if df_fil_data.at[idx_row, col_name] in lst_clear_val:
                                df_data_stack.at[idx_row, col_name] = np.nan



            # END ADD MA OE to Data stack-------------------------------------------------------------------------------

        if dict_qre_net_info:
            # ADD MA NET CODE to df_info--------------------------------------------------------------------------------
            for key, val in dict_qre_net_info.items():
                df_info_stack.loc[df_info_stack['var_name'].str.contains(key), 'val_lbl'] = [val]
            # END ADD MA NET CODE to df_info----------------------------------------------------------------------------

        # REMEMBER RESET INDEX BEFORE RUN TABLES
        df_data_stack.reset_index(drop=True, inplace=True)
        df_info_stack.reset_index(drop=True, inplace=True)

        # Export data tables--------------------------------------------------------------------------------------------
        if tables_format_file.filename:

            df_data_tbl, df_info_tbl = df_data_stack.copy(), df_info_stack.copy()

            df_data_tbl = pd.concat([df_data_tbl, df_data_stack_nd], axis=0)
            df_info_tbl = pd.concat([df_info_tbl, df_info_stack_nd], axis=0)

            df_data_tbl.sort_values(by=[id_col], inplace=True)
            df_data_tbl.reset_index(drop=True, inplace=True)

            df_info_tbl.drop_duplicates(subset=['var_name'], keep='first', inplace=True)
            df_info_tbl.reset_index(drop=True, inplace=True)

            # # ----------------------------------
            # df_data_tbl.to_excel('zzz_df_data_tbl.xlsx')
            # df_info_tbl.to_excel('zzz_df_info_tbl.xlsx')
            # # ----------------------------------

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
            DataTableGenerator.__init__(self, df_data=df_data_tbl, df_info=df_info_tbl,
                                        xlsx_name=str_topline_file_name, logger=self.logger,
                                        lst_qre_group=lst_qre_group, lst_qre_mean=lst_qre_mean, is_md=False)

            # lst_func_to_run = eval(tables_format_file.file.read())

            local_dic = locals()
            exec(tables_format_file.file.read(), globals(), local_dic)
            lst_func_to_run = local_dic['lst_func_to_run']


            self.run_tables_by_js_files(lst_func_to_run)
            self.format_sig_table()
        # End Export data tables----------------------------------------------------------------------------------------

        # Generate SAV files--------------------------------------------------------------------------------------------
        self.logger.info('Generate SAV files')

        df_info_stack = self.remove_net_code(df_info_stack)

        dict_dfs = {
            1: {
                'data': df_data_unstack,
                'info': df_info_unstack,
                'tail_name': 'Unstack',
                'sheet_name': 'Unstack',
                'is_recode_to_lbl': True,
            },
            2: {
                'data': df_data_stack,
                'info': df_info_stack,
                'tail_name': 'Stack',
                'sheet_name': 'Stack',
                'is_recode_to_lbl': False,
            },
        }

        self.generate_multiple_sav_sps(dict_dfs=dict_dfs, is_md=False, is_export_xlsx=True)
        # END Generate SAV files----------------------------------------------------------------------------------------






    def remove_net_code(self, df_info: pd.DataFrame) -> pd.DataFrame:
        # Remove net_code to export sav---------------------------------------------------------------------------------
        df_info_without_net = df_info.copy()

        for idx in df_info_without_net.index:
            val_lbl = df_info_without_net.at[idx, 'val_lbl']

            if 'net_code' in val_lbl.keys():
                df_info_without_net.at[idx, 'val_lbl'] = self.unnetted_qre_val(val_lbl)

        return df_info_without_net
        # END Remove net_code to export sav-----------------------------------------------------------------------------