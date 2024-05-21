from app.classes.AP_DataConverter import APDataConverter
from ..export_online_survey_data_table import DataTableGenerator
from ..convert_unstack import ConvertUnstack
from ..convert_stack import ConvertStack
import pandas as pd
import numpy as np
import time
import traceback
import datetime


class VN8363Shampoo(APDataConverter, DataTableGenerator, ConvertUnstack, ConvertStack):

    def convert_vn8363_shampoo(self, py_script_file, tables_format_file, codelist_file, coding_file):
        """DO NOT EDIT THIS FUNCTION"""

        try:
            start_time = time.time()

            if py_script_file:
                exec(py_script_file.file.read())
                exit()

            self.processing_script_vn8363_shampoo(tables_format_file, codelist_file, coding_file)

            self.logger.warning('DATA PROCESSING COMPLETED in %d seconds' % (time.time() - start_time))

        except Exception:
            self.logger.error(traceback.format_exc())
            str_err_log_name = self.str_file_name.replace('.xlsx', '_Errors.txt')
            with open(str_err_log_name, 'w') as err_log_txt:
                err_log_txt.writelines(traceback.format_exc())


    def processing_script_vn8363_shampoo(self, tables_format_file, codelist_file, coding_file):
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
            'Main_Q12_Prefer': ['Q12. Giữa ý tưởng chị thích nhất lúc nãy Dầu gội đầu ngăn gãy rụng tóc với dưỡng chất 100% từ vỏ bưởi và ý tưởng sản phẩm này, chị thích cái nào hơn?',
                                'SA', {'1': 'Ý tưởng A', '2': 'Ý tưởng B', '3': 'Ý tưởng C', '4': 'Ý tưởng D'}, np.nan],

            'S4c_Mass': ['S4c_Mass', 'SA', {'1': 'Yes', '0': 'No'}, 0],
            'S4c_Natural': ['S4c_Natural', 'SA', {'1': 'Yes', '0': 'No'}, 0],

            'S4bc_S6_PhanLoai': ['S4bc & S6. Nhóm người dùng dầu gội & dầu xả', 'SA', {'1': 'Dùng dầu gội & dầu xả cùng nhãn hiệu', '2': 'Dùng dầu gội & dầu xả không cùng nhãn hiệu', '3': 'Không dùng dầu xả'}, 2]
        }

        for key, val in dict_add_new_qres.items():
            df_info = pd.concat([df_info, pd.DataFrame(columns=info_col_name, data=[[key, val[0], val[1], val[2]]])], axis=0, ignore_index=True)
            df_data = pd.concat([df_data, pd.DataFrame(columns=[key], data=[val[-1]] * df_data.shape[0])], axis=1)

        df_data.loc[df_data['Main_Q12a_Prefer'] == 1, ['Main_Q12_Prefer']] = [1]
        df_data.loc[df_data['Main_Q12b_Prefer'] == 1, ['Main_Q12_Prefer']] = [2]
        df_data.loc[df_data['Main_Q12c_Prefer'] == 1, ['Main_Q12_Prefer']] = [3]
        df_data.loc[(df_data['Main_Q12a_Prefer'] == 2) | (df_data['Main_Q12b_Prefer'] == 2) | (df_data['Main_Q12c_Prefer'] == 2), ['Main_Q12_Prefer']] = [4]

        df_info.loc[df_info['var_name'] == 'Main_A5_OE_nguyen_lieu', ['val_lbl']] = [{'2': 'Ko biết', '3': 'Nguyên liệu:'}]

        # S4c_Mass & S4c_Natural
        dict_s4c_brand = {
            'mass': {
                1: 'Clear',
                2: 'Sunsilk',
                3: 'Dove',
                4: 'Lifebuoy',
                5: 'Tresemme',
                6: 'Head&Shoulders',
                7: 'Pantene',
                8: 'Rejoice',
                9: 'Enchant',
                18: 'Khác (ghi rõ)',
            },
            'natural': {
                10: 'Love Beauty & Planet',
                11: 'Herbal Essences',
                12: 'Nguyên Xuân',
                13: 'Thái Dương',
                14: 'Palmolive',
                15: 'Purite',
                16: 'Cocoon',
                17: 'Dầu Gội Thiên Nhiên, Bồ Kết, Thảo Mộc, Thảo Dược Nói Chung Khác',
            },
            # 19: 'Tôi chỉ đang sử dụng 1 loại dầu gội đầu duy nhất'
        }

        # MASS
        str_query_0 = f"Recruit_S4b_BUMO.isin({list(dict_s4c_brand['mass'].keys())})"
        str_query_1 = ' | '.join([f"Recruit_S4c_Nhan_hieu_song_song_{i}.isin({list(dict_s4c_brand['mass'].keys())})" for i in range(1, 20)])
        str_query_2 = f"({' | '.join([f'Recruit_S4c_Nhan_hieu_song_song_{i}.isin([19])' for i in range(1, 20)])}) & (Recruit_S4b_BUMO.isin({list(dict_s4c_brand['mass'].keys())}))"

        df_fil = df_data.query(f"{str_query_0} | ({str_query_1}) | ({str_query_2})").copy()
        df_data.loc[df_fil.index, ['S4c_Mass']] = [1]
        del df_fil


        # NATURAL
        str_query_0 = f"Recruit_S4b_BUMO.isin({list(dict_s4c_brand['natural'].keys())})"
        str_query_1 = ' | '.join([f"Recruit_S4c_Nhan_hieu_song_song_{i}.isin({list(dict_s4c_brand['natural'].keys())})" for i in range(1, 20)])
        str_query_2 = f"({' | '.join([f'Recruit_S4c_Nhan_hieu_song_song_{i}.isin([19])' for i in range(1, 20)])}) & (Recruit_S4b_BUMO.isin({list(dict_s4c_brand['natural'].keys())}))"

        df_fil = df_data.query(f"{str_query_0} | ({str_query_1}) | ({str_query_2})").copy()
        df_data.loc[df_fil.index, ['S4c_Natural']] = [1]
        del df_fil

        # S4bc_S6_PhanLoai
        # '2': 'Dùng dầu gội & dầu xả không cùng nhãn hiệu'

        # '3': 'Không dùng dầu xả'
        df_fil = df_data.query("Recruit_S6_Dau_xa_1 == 12").copy()  # 12 = không có
        df_data.loc[df_fil.index, ['S4bc_S6_PhanLoai']] = [3]
        del df_fil

        # '1': 'Dùng dầu gội & dầu xả cùng nhãn hiệu'
        lst_pair = [
            [2, 1],
            [3, 2],
            [5, 3],
            [6, 8],
            [7, 5],
            [8, 9],
            [1, 11],
            [11, 10],
        ]

        lst_query = list()
        for pair in lst_pair:
            # str_query = f"((Recruit_S4b_BUMO == xxx | (Recruit_S4c_Nhan_hieu_song_song_1 == yyy)) & (Recruit_S6_Dau_xa_1 == zzz))"
            str_query = f"((Recruit_S4b_BUMO == {pair[0]} | ({' | '.join([f'Recruit_S4c_Nhan_hieu_song_song_{i} == {pair[0]}' for i in range(1, 20)])}  )) & ({' | '.join([f'Recruit_S6_Dau_xa_{j} == {pair[1]}' for j in range(1, 14)])}))"
            lst_query.append(str_query)

        df_fil = df_data.query(' | '.join(lst_query)).copy()
        df_data.loc[df_fil.index, ['S4bc_S6_PhanLoai']] = [1]
        del df_fil





        lst_col_drop = [
            'Main_Q0d_BUMO',
            'Main_Q0e_Respondent_PhoneNumber',
            'Main_Q12a_Prefer',
            'Main_Q12b_Prefer',
            'Main_Q12c_Prefer',
        ]

        df_data.drop(columns=lst_col_drop, inplace=True)

        df_info['idx_var_name'] = df_info['var_name']
        df_info.set_index('idx_var_name', inplace=True)
        df_info = df_info.loc[list(df_data.columns), :]
        df_info.reindex(list(df_data.columns))
        df_info.reset_index(drop=True, inplace=True)

        df_data.reset_index(drop=True, inplace=True)
        df_info.reset_index(drop=True, inplace=True)

        # --------------------------------------------------------------------------------------------------------------
        df_data_unstack, df_info_unstack = df_data.copy(), df_info.copy()
        # --------------------------------------------------------------------------------------------------------------

        dict_add_new_qres = {
            'Ma_SP_A': ['Mã SP', 'SA', {'1': 'Ý tưởng A', '2': 'Ý tưởng B', '3': 'Ý tưởng C', '4': 'Ý tưởng D'}, 1],
            'Ma_SP_B': ['Mã SP', 'SA', {'1': 'Ý tưởng A', '2': 'Ý tưởng B', '3': 'Ý tưởng C', '4': 'Ý tưởng D'}, 2],
            'Ma_SP_C': ['Mã SP', 'SA', {'1': 'Ý tưởng A', '2': 'Ý tưởng B', '3': 'Ý tưởng C', '4': 'Ý tưởng D'}, 3],
            'Ma_SP_D': ['Mã SP', 'SA', {'1': 'Ý tưởng A', '2': 'Ý tưởng B', '3': 'Ý tưởng C', '4': 'Ý tưởng D'}, 4],

            'Main_Q1_OL_Concept_D': ['Main_Q1_OL_Concept_D', 'SA', {'1': '1. Hoàn toàn không thích', '2': '2. Không thích', '3': '3. Không thích cũng không ghét', '4': '4. Thích', '5': '5. Rất thích'}, np.nan],
            'Main_Q2a_OE_Like_Concept_D': ['Main_Q2a_OE_Like_Concept_D', 'FT', {}, np.nan],
            'Main_Q2b_OE_DisLike_Concept_D': ['Main_Q2b_OE_DisLike_Concept_D', 'FT', {}, np.nan],
            'Main_Q3_Moi_la_Concept_D': ['Main_Q3_Moi_la_Concept_D', 'SA', {'1': '1. Hoàn toàn không mới lạ và khác biệt', '2': '2. Không mới lạ và khác biệt', '3': '3. Hơi không mới lạ và khác biệt', '4': '4. Mới lạ và khác biệt', '5': '5. Rất mới lạ và khác biệt'}, np.nan],
            'Main_Q4_Dang_tin_Concept_D': ['Main_Q4_Dang_tin_Concept_D', 'SA', {'1': '1. Hoàn toàn không đáng tin', '2': '2. Không đáng tin', '3': '3. Hơi không đáng tin', '4': '4. Đáng tin', '5': '5. Rất đáng tin'}, np.nan],
            'Main_Q5_Phu_hop_Concept_D': ['Main_Q5_Phu_hop_Concept_D', 'SA', {'1': '1. Hoàn toàn không phù hợp', '2': '2. Không phù hợp', '3': '3. Hơi không phù hợp', '4': '4. Phù hợp', '5': '5. Hoàn toàn phù hợp'}, np.nan],
            'Main_Q6_PI_Concept_D': ['Main_Q6_PI_Concept_D', 'SA', {'1': '1. Chắc chắn sẽ không mua', '2': '2. Không mua', '3': '3. Có thể sẽ mua hoặc không', '4': '4. Sẽ mua', '5': '5. Chắc chắn sẽ mua'}, np.nan],
            'Main_Q7a_OE_Mua_Concept_D': ['Main_Q7a_OE_Mua_Concept_D', 'FT', {}, np.nan],
            'Main_Q7b_OE_Khong_mua_Concept_D': ['Main_Q7b_OE_Khong_mua_Concept_D', 'FT', {}, np.nan],

            'Main_Q8_FC_A': ['Q8. FORCE CHOICE', 'SA', {'1': 'Yes', '2': 'No'}, np.nan],
            'Main_Q8_FC_B': ['Q8. FORCE CHOICE', 'SA', {'1': 'Yes', '2': 'No'}, np.nan],
            'Main_Q8_FC_C': ['Q8. FORCE CHOICE', 'SA', {'1': 'Yes', '2': 'No'}, np.nan],
            'Main_Q8_FC_D': ['Q8. FORCE CHOICE', 'SA', {'1': 'Yes', '2': 'No'}, np.nan],

            'Main_Q9_OE_FC_A': ['Q9. OE FORCE CHOICE', 'FT', {}, np.nan],
            'Main_Q9_OE_FC_B': ['Q9. OE FORCE CHOICE', 'FT', {}, np.nan],
            'Main_Q9_OE_FC_C': ['Q9. OE FORCE CHOICE', 'FT', {}, np.nan],
            'Main_Q9_OE_FC_D': ['Q9. OE FORCE CHOICE', 'FT', {}, np.nan],

            'Main_Q12_Prefer_A': ['Q12. Prefer', 'SA', {'1': 'Yes', '2': 'No'}, np.nan],
            'Main_Q12_Prefer_B': ['Q12. Prefer', 'SA', {'1': 'Yes', '2': 'No'}, np.nan],
            'Main_Q12_Prefer_C': ['Q12. Prefer', 'SA', {'1': 'Yes', '2': 'No'}, np.nan],
            'Main_Q12_Prefer_D': ['Q12. Prefer', 'SA', {'1': 'Yes', '2': 'No'}, np.nan],

            'Main_Q13_OE_Prefer_A': ['Q13. OE Prefer', 'FT', {}, np.nan],
            'Main_Q13_OE_Prefer_B': ['Q13. OE Prefer', 'FT', {}, np.nan],
            'Main_Q13_OE_Prefer_C': ['Q13. OE Prefer', 'FT', {}, np.nan],
            'Main_Q13_OE_Prefer_D': ['Q13. OE Prefer', 'FT', {}, np.nan],

            'A4_Ma_ND_1': ['A4. Nhận định', 'SA', {'1': 'Việc sử dụng hóa chất hoặc thiết bị nhiệt lên tóc như uốn, nhuộm, duỗi, sấy,… quá thường xuyên khiến tóc tôi hư tổn, chân tóc yếu, dễ gãy rụng.', '2': 'Với tôi, mái tóc đẹp được xem là ”trang sức” lộng lẫy của phụ nữ. Từ thời xa xưa, chị em phụ nữ đã có bí quyết để có mái tóc chắc khỏe, bồng  bềnh, ít gãy rụng đến từ thiên nhiên', '3': 'Khói bụi, độ ẩm cao, thường xuyên tiếp xúc với ánh nắng mặt trời và phương pháp chăm sóc tóc sai cách khiến da đầu tôi bị tổn thương, tóc xơ rối dễ gãy rụng.'}, 1],
            'A4_Ma_ND_2': ['A4. Nhận định', 'SA', {'1': 'Việc sử dụng hóa chất hoặc thiết bị nhiệt lên tóc như uốn, nhuộm, duỗi, sấy,… quá thường xuyên khiến tóc tôi hư tổn, chân tóc yếu, dễ gãy rụng.', '2': 'Với tôi, mái tóc đẹp được xem là ”trang sức” lộng lẫy của phụ nữ. Từ thời xa xưa, chị em phụ nữ đã có bí quyết để có mái tóc chắc khỏe, bồng  bềnh, ít gãy rụng đến từ thiên nhiên', '3': 'Khói bụi, độ ẩm cao, thường xuyên tiếp xúc với ánh nắng mặt trời và phương pháp chăm sóc tóc sai cách khiến da đầu tôi bị tổn thương, tóc xơ rối dễ gãy rụng.'}, 2],
            'A4_Ma_ND_3': ['A4. Nhận định', 'SA', {'1': 'Việc sử dụng hóa chất hoặc thiết bị nhiệt lên tóc như uốn, nhuộm, duỗi, sấy,… quá thường xuyên khiến tóc tôi hư tổn, chân tóc yếu, dễ gãy rụng.', '2': 'Với tôi, mái tóc đẹp được xem là ”trang sức” lộng lẫy của phụ nữ. Từ thời xa xưa, chị em phụ nữ đã có bí quyết để có mái tóc chắc khỏe, bồng  bềnh, ít gãy rụng đến từ thiên nhiên', '3': 'Khói bụi, độ ẩm cao, thường xuyên tiếp xúc với ánh nắng mặt trời và phương pháp chăm sóc tóc sai cách khiến da đầu tôi bị tổn thương, tóc xơ rối dễ gãy rụng.'}, 3],

            'A4_ND_1_YN': ['A4. Nhận định', 'SA', {'1': 'Yes', '2': 'No'}, 2],
            'A4_ND_2_YN': ['A4. Nhận định', 'SA', {'1': 'Yes', '2': 'No'}, 2],
            'A4_ND_3_YN': ['A4. Nhận định', 'SA', {'1': 'Yes', '2': 'No'}, 2],
        }

        for key, val in dict_add_new_qres.items():
            df_info = pd.concat([df_info, pd.DataFrame(columns=info_col_name, data=[[key, val[0], val[1], val[2]]])], axis=0, ignore_index=True)
            df_data = pd.concat([df_data, pd.DataFrame(columns=[key], data=[val[-1]] * df_data.shape[0])], axis=1)

        df_data['Main_Q1_OL_Concept_D'] = df_data['Main_Q10_OL']
        df_data['Main_Q6_PI_Concept_D'] = df_data['Main_Q11_PI']

        for i, v in enumerate(['A', 'B', 'C', 'D']):

            if v != 'D':
                df_data.loc[:, [f'Main_Q8_FC_{v}']] = [2]  # 2 = N0

                df_fil = df_data.query(f'Main_Q8_FC == {i + 1}')

                df_data.loc[df_fil.index, [f'Main_Q8_FC_{v}']] = [1]  # 1 = Yes

                df_data.loc[df_fil.index, f'Main_Q9_OE_FC_{v}'] = df_data.loc[df_fil.index, f'Main_Q9_OE_FC']

                df_data.loc[df_fil.index, [f'Main_Q12_Prefer_{v}']] = [2]  # 2 = N0

                del df_fil
            else:
                df_data.loc[:, [f'Main_Q12_Prefer_{v}']] = [2]

            df_fil = df_data.query(f'Main_Q12_Prefer == {i + 1}')
            df_data.loc[df_fil.index, [f'Main_Q12_Prefer_{v}']] = [1]  # 1 = Yes
            del df_fil

            if v != 'D':
                df_fil = df_data.query(f'Main_Q12_Prefer == {i + 1}')
                df_data.loc[df_fil.index, f'Main_Q13_OE_Prefer_{v}'] = df_data.loc[df_fil.index, f'Main_Q13a_OE_Prefer']
                del df_fil
            else:
                df_fil = df_data.query(f'Main_Q12_Prefer == {i + 1}')
                df_data.loc[df_fil.index, f'Main_Q13_OE_Prefer_{v}'] = df_data.loc[df_fil.index, f'Main_Q13b_OE_Prefer']
                del df_fil


        for i in range(1, 4):
            df_fil = df_data.query(f"Main_A4_nhan_dinh == {i}")
            df_data.loc[df_fil.index, [f'A4_ND_{i}_YN']] = [1]
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
        a4_col = 'A4_Ma_ND'


        lst_scr = [
            'RespondentWard',
            'RespondentDist',
            'Recruit_S1_Thanh_pho',
            'Recruit_S1_Thanh_pho_o5',
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
            'Recruit_S4a_Dau_goi_o18',
            'Recruit_S4b_BUMO',
            'Recruit_S4c_Nhan_hieu_song_song_1',
            'Recruit_S4c_Nhan_hieu_song_song_2',
            'Recruit_S4c_Nhan_hieu_song_song_3',
            'Recruit_S4c_Nhan_hieu_song_song_4',
            'Recruit_S4c_Nhan_hieu_song_song_5',
            'Recruit_S4c_Nhan_hieu_song_song_6',
            'Recruit_S4c_Nhan_hieu_song_song_7',
            'Recruit_S4c_Nhan_hieu_song_song_8',
            'Recruit_S4c_Nhan_hieu_song_song_9',
            'Recruit_S4c_Nhan_hieu_song_song_10',
            'Recruit_S4c_Nhan_hieu_song_song_11',
            'Recruit_S4c_Nhan_hieu_song_song_12',
            'Recruit_S4c_Nhan_hieu_song_song_13',
            'Recruit_S4c_Nhan_hieu_song_song_14',
            'Recruit_S4c_Nhan_hieu_song_song_15',
            'Recruit_S4c_Nhan_hieu_song_song_16',
            'Recruit_S4c_Nhan_hieu_song_song_17',
            'Recruit_S4c_Nhan_hieu_song_song_18',
            'Recruit_S4c_Nhan_hieu_song_song_19',
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
            'Recruit_S6_Dau_xa_o13',
            'Recruit_S7_Nhan_dinh_1',
            'Recruit_S7_Nhan_dinh_2',
            'Recruit_S7_Nhan_dinh_3',
            'Recruit_S7_Nhan_dinh_4',
            'Recruit_S7_Nhan_dinh_5',
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
            'Recruit_S13_Suc_khoe_5',
            'Recruit_S13_Suc_khoe_6',
            'Recruit_T1',
            'Main_A1',
            'Main_A2a_OE_Da_dung',
            'Main_A2a_OE_Chua_dung',
            'Main_A3_Hai_long_01',
            'Main_A3_Hai_long_02',
            'Main_A3_Hai_long_03',
            'Main_A3_Hai_long_04',
            'Main_A3_Hai_long_05',
            'Main_A3_Hai_long_06',
            'Main_A4_nhan_dinh',
            'Main_A5_OE_nguyen_lieu',
            'Main_A5_OE_nguyen_lieu_o3',
            'Main_A6_Nguyen_lieu_1',
            'Main_A6_Nguyen_lieu_2',
            'Main_A6_Nguyen_lieu_3',
            'Main_A6_Nguyen_lieu_4',
            'Main_A6_Nguyen_lieu_5',
            'Main_A6_Nguyen_lieu_6',
            'Main_A6_Nguyen_lieu_7',
            'Main_A6_Nguyen_lieu_8',
            'Main_A6_Nguyen_lieu_9',
            'Main_A6_Nguyen_lieu_10',
            'Main_A100_Tham_gia_khao_sat_01',
            'Main_A100_Tham_gia_khao_sat_02',
            'Main_A100_Tham_gia_khao_sat_03',
            'Main_A100_Tham_gia_khao_sat_04',
            'S4c_Mass',
            'S4c_Natural',
            'S4bc_S6_PhanLoai',
        ]

        dict_main = {
            1: {
                'Ma_SP_A': 'Ma_SP',
                'Main_Q1_OL_Concept_A': 'Main_Q1_OL',
                'Main_Q2a_OE_Like_Concept_A': 'Main_Q2a_OE_Like',
                'Main_Q2b_OE_DisLike_Concept_A': 'Main_Q2b_OE_DisLike',
                'Main_Q3_Moi_la_Concept_A': 'Main_Q3_Moi_la',
                'Main_Q4_Dang_tin_Concept_A': 'Main_Q4_Dang_tin',
                'Main_Q5_Phu_hop_Concept_A': 'Main_Q5_Phu_hop',
                'Main_Q6_PI_Concept_A': 'Main_Q6_PI',
                'Main_Q7a_OE_Mua_Concept_A': 'Main_Q7a_OE_Mua',
                'Main_Q7b_OE_Khong_mua_Concept_A': 'Main_Q7b_OE_Khong_mua',
                'Main_Q8_FC_A': 'Main_Q8_FC_New',
                'Main_Q9_OE_FC_A': 'Main_Q9_OE_FC_New',
                'Main_Q12_Prefer_A': 'Main_Q12_Prefer_New',
                'Main_Q13_OE_Prefer_A': 'Main_Q13_OE_Prefer_New',

            },
            2: {
                'Ma_SP_B': 'Ma_SP',
                'Main_Q1_OL_Concept_B': 'Main_Q1_OL',
                'Main_Q2a_OE_Like_Concept_B': 'Main_Q2a_OE_Like',
                'Main_Q2b_OE_DisLike_Concept_B': 'Main_Q2b_OE_DisLike',
                'Main_Q3_Moi_la_Concept_B': 'Main_Q3_Moi_la',
                'Main_Q4_Dang_tin_Concept_B': 'Main_Q4_Dang_tin',
                'Main_Q5_Phu_hop_Concept_B': 'Main_Q5_Phu_hop',
                'Main_Q6_PI_Concept_B': 'Main_Q6_PI',
                'Main_Q7a_OE_Mua_Concept_B': 'Main_Q7a_OE_Mua',
                'Main_Q7b_OE_Khong_mua_Concept_B': 'Main_Q7b_OE_Khong_mua',
                'Main_Q8_FC_B': 'Main_Q8_FC_New',
                'Main_Q9_OE_FC_B': 'Main_Q9_OE_FC_New',
                'Main_Q12_Prefer_B': 'Main_Q12_Prefer_New',
                'Main_Q13_OE_Prefer_B': 'Main_Q13_OE_Prefer_New',
            },
            3: {
                'Ma_SP_C': 'Ma_SP',
                'Main_Q1_OL_Concept_C': 'Main_Q1_OL',
                'Main_Q2a_OE_Like_Concept_C': 'Main_Q2a_OE_Like',
                'Main_Q2b_OE_DisLike_Concept_C': 'Main_Q2b_OE_DisLike',
                'Main_Q3_Moi_la_Concept_C': 'Main_Q3_Moi_la',
                'Main_Q4_Dang_tin_Concept_C': 'Main_Q4_Dang_tin',
                'Main_Q5_Phu_hop_Concept_C': 'Main_Q5_Phu_hop',
                'Main_Q6_PI_Concept_C': 'Main_Q6_PI',
                'Main_Q7a_OE_Mua_Concept_C': 'Main_Q7a_OE_Mua',
                'Main_Q7b_OE_Khong_mua_Concept_C': 'Main_Q7b_OE_Khong_mua',
                'Main_Q8_FC_C': 'Main_Q8_FC_New',
                'Main_Q9_OE_FC_C': 'Main_Q9_OE_FC_New',
                'Main_Q12_Prefer_C': 'Main_Q12_Prefer_New',
                'Main_Q13_OE_Prefer_C': 'Main_Q13_OE_Prefer_New',
            },
            4: {
                'Ma_SP_D': 'Ma_SP',
                'Main_Q1_OL_Concept_D': 'Main_Q1_OL',
                'Main_Q2a_OE_Like_Concept_D': 'Main_Q2a_OE_Like',
                'Main_Q2b_OE_DisLike_Concept_D': 'Main_Q2b_OE_DisLike',
                'Main_Q3_Moi_la_Concept_D': 'Main_Q3_Moi_la',
                'Main_Q4_Dang_tin_Concept_D': 'Main_Q4_Dang_tin',
                'Main_Q5_Phu_hop_Concept_D': 'Main_Q5_Phu_hop',
                'Main_Q6_PI_Concept_D': 'Main_Q6_PI',
                'Main_Q7a_OE_Mua_Concept_D': 'Main_Q7a_OE_Mua',
                'Main_Q7b_OE_Khong_mua_Concept_D': 'Main_Q7b_OE_Khong_mua',
                'Main_Q8_FC_D': 'Main_Q8_FC_New',
                'Main_Q9_OE_FC_D': 'Main_Q9_OE_FC_New',
                'Main_Q12_Prefer_D': 'Main_Q12_Prefer_New',
                'Main_Q13_OE_Prefer_D': 'Main_Q13_OE_Prefer_New',
            },
        }

        # lst_fc_kb = [
        #     'T3',
        #     'T3_Check',
        #     'T4',
        #     'T5',
        #     'T5_Check',
        #     'T6',
        # ]

        dict_a4_nd = {
            1: {
                'A4_Ma_ND_1': 'A4_Ma_ND',
                'A4_ND_1_YN': 'A4_ND_YN',
            },
            2: {
                'A4_Ma_ND_2': 'A4_Ma_ND',
                'A4_ND_2_YN': 'A4_ND_YN',
            },
            3: {
                'A4_Ma_ND_3': 'A4_Ma_ND',
                'A4_ND_3_YN': 'A4_ND_YN',
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
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Main_Q4_Dang_tin': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Main_Q5_Phu_hop': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Main_Q6_PI': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },

            'Main_A3_Hai_long': {
                'range': [f'0{i}' for i in range(1, 7)],
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
        # End Define structure------------------------------------------------------------------------------------------


        # CONVERT TO STACK----------------------------------------------------------------------------------------------
        df_data_stack, df_info_stack = self.convert_to_stack(df_data, df_info, id_col, sp_col, lst_scr, dict_main, [])
        df_data_stack_a4, df_info_stack_a4 = self.convert_to_stack(df_data, df_info, id_col, a4_col, lst_scr, dict_a4_nd, [])

        # RE-LABEL
        lst_relabel = [
            ['Main_Q1_OL', 'Q1. Mức độ yêu thích của chị đối với ý tưởng sản phẩm ...?'],
            ['Main_Q2a_OE_Like', 'Q2a. Vì sao chị THÍCH ý tưởng sản phẩm này?'],
            ['Main_Q2b_OE_DisLike', 'Q2b. Vì sao chị KHÔNG THÍCH ý tưởng sản phẩm này?'],
            ['Main_Q3_Moi_la', 'Q3. Theo chị mức độ mới lạ, khác biệt của sản phẩm ...?'],
            ['Main_Q4_Dang_tin', 'Q4. Chị thấy các thông tin về sản phẩm trong bảng ý tưởng này có đáng tin không?'],
            ['Main_Q5_Phu_hop', 'Q5. Chị thấy ý tưởng sản phẩm ... PHÙ HỢP với nhu cầu sử dụng dầu gội của chị như thế nào?'],
            ['Main_Q6_PI', 'Q6. Mức độ chị MUỐN MUA hay KHÔNG MUỐN MUA thử sản phẩm ...?'],
            ['Main_Q7a_OE_Mua', 'Q7a. Vì sao chị MUỐN MUA thử sản phẩm trong bảng ý tưởng này?'],
            ['Main_Q7b_OE_Khong_mua', 'Q7b. Vì sao chị KHÔNG MUỐN MUA thử sản phẩm trong bảng ý tưởng này?'],
        ]

        for arr in lst_relabel:
            df_info_stack.loc[df_info_stack['var_name'] == arr[0], ['var_lbl']] = [arr[1]]

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

        if dict_qre_net_info:
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

            df_data_tbl = pd.concat([df_data_tbl, df_data_stack_a4], axis=0)
            df_info_tbl = pd.concat([df_info_tbl, df_info_stack_a4], axis=0)

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

            lst_func_to_run = eval(tables_format_file.file.read())
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
                'is_recode_to_lbl': True,
            },
        }

        self.generate_multiple_sav_sps(dict_dfs=dict_dfs, is_md=False, is_export_xlsx=True)
        # END Generate SAV files----------------------------------------------------------------------------------------



    # @staticmethod
    # def convert_to_stack(df_data: pd.DataFrame, df_info: pd.DataFrame, id_col: str, sp_col: str, lst_scr: list, dict_sp: dict) -> (pd.DataFrame, pd.DataFrame):
    #
    #     # df_data_stack generate
    #     df_data_scr = df_data.loc[:, [id_col] + lst_scr].copy()
    #
    #     lst_df_data_sp = [df_data.loc[:, [id_col] + list(val.keys())].copy() for val in dict_sp.values()]
    #
    #     for i, df in enumerate(lst_df_data_sp):
    #         df.rename(columns=dict_sp[i + 1], inplace=True)
    #
    #     df_data_stack = pd.concat(lst_df_data_sp, axis=0, ignore_index=True)
    #
    #     df_data_stack = df_data_scr.merge(df_data_stack, how='left', on=[id_col])
    #     df_data_stack.reset_index(drop=True, inplace=True)
    #
    #     df_data_stack.sort_values(by=[id_col, sp_col], inplace=True)
    #     df_data_stack.reset_index(drop=True, inplace=True)
    #
    #     df_info_stack = df_info.copy()
    #
    #     for key, val in dict_sp[1].items():
    #         df_info_stack.loc[df_info_stack['var_name'] == key, ['var_name']] = [val]
    #
    #     df_info_stack.drop_duplicates(subset=['var_name'], keep='first', inplace=True)
    #
    #
    #     # Reset df_info_stack index
    #     df_info_stack['idx_var_name'] = df_info_stack['var_name']
    #     df_info_stack.set_index('idx_var_name', inplace=True)
    #     df_info_stack = df_info_stack.loc[list(df_data_stack.columns), :]
    #     df_info_stack.reindex(list(df_data_stack.columns))
    #     df_info_stack.reset_index(drop=True, inplace=True)
    #
    #     return df_data_stack, df_info_stack



    def remove_net_code(self, df_info: pd.DataFrame) -> pd.DataFrame:
        # Remove net_code to export sav---------------------------------------------------------------------------------
        df_info_without_net = df_info.copy()

        for idx in df_info_without_net.index:
            val_lbl = df_info_without_net.at[idx, 'val_lbl']

            if 'net_code' in val_lbl.keys():
                df_info_without_net.at[idx, 'val_lbl'] = self.unnetted_qre_val(val_lbl)

        return df_info_without_net
        # END Remove net_code to export sav-----------------------------------------------------------------------------