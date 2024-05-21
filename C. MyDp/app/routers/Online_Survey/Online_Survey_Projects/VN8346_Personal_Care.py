from app.classes.AP_DataConverter import APDataConverter
from ..export_online_survey_data_table import DataTableGenerator
from ..convert_unstack import ConvertUnstack
from ..convert_stack import ConvertStack
import pandas as pd
import numpy as np
import time
import traceback
import datetime


class VN8346PersonalCare(APDataConverter, DataTableGenerator, ConvertUnstack, ConvertStack):

    def convert_vn8346_personal_care(self, py_script_file, tables_format_file, codelist_file, coding_file):
        """DO NOT EDIT THIS FUNCTION"""

        try:
            start_time = time.time()

            if py_script_file:
                exec(py_script_file.file.read())
                exit()

            self.processing_script_vn8346_personal_care(tables_format_file, codelist_file, coding_file)

            self.logger.warning('DATA PROCESSING COMPLETED in %d seconds' % (time.time() - start_time))

        except Exception:
            self.logger.error(traceback.format_exc())
            str_err_log_name = self.str_file_name.replace('.xlsx', '_Errors.txt')
            with open(str_err_log_name, 'w') as err_log_txt:
                err_log_txt.writelines(traceback.format_exc())


    def processing_script_vn8346_personal_care(self, tables_format_file, codelist_file, coding_file):
        """
        :param tables_format_file: txt uploaded table format file
        :param codelist_file: txt uploaded codelist file
        :param coding_file: txt uploaded coding file
        :return: zip file include files: .sav, .sps, .xlsx(rawdata, topline)

        EDIT FROM HERE
        """

        # Convert system rawdata to dataframes--------------------------------------------------------------------------
        self.logger.info('Convert system rawdata to dataframes')

        self.lstDrop.remove('InterviewerName')

        df_data, df_info = self.convert_df_mc()

        # Pre processing------------------------------------------------------------------------------------------------
        self.logger.info('Pre processing')

        for i in ['A', 'B', 'C']:
            df_fil = df_data.query(f'Q2_OL_Com_{i}1_Before.isnull() & ~Q2_OL_Com_{i}1_After.isnull()').copy()
            df_data.loc[df_fil.index, f'Q2_OL_Com_{i}1_Before'] = df_data.loc[df_fil.index, f'Q2_OL_Com_{i}1_After']

            df_data.rename(columns={f'Q2_OL_Com_{i}1_Before': f'Q2_OL_Com_{i}1'}, inplace=True)
            df_info.loc[df_info['var_name'].str.contains(f'Q2_OL_Com_{i}1_Before'), ['var_name']] = [f'Q2_OL_Com_{i}1']

        lst_col_drop = [
            'Part2a_Rotation',
            'Q2_OL_Com_A1_After',
            'Q2_OL_Com_B1_After',
            'Q2_OL_Com_C1_After',
            'PVV_Thaotac_A_1',
            'PVV_Thaotac_A_2',
            'PVV_Thaotac_A_3',
            'PVV_Thaotac_B_1',
            'PVV_Thaotac_B_2',
            'PVV_Thaotac_B_3',
            'PVV_Thaotac_C_1',
            'PVV_Thaotac_C_2',
            'PVV_Thaotac_C_3',
            'Q2_LikeMost_Pack_A',
            'Q2_LikeMost_Pack_B',
            'Q2_LikeMost_Pack_C',
            'Q3_MOI_OL_A',
            'Q3_MOI_OL_B',
            'Q3_MOI_OL_C',
            'Q2_ConceptFit_Pack_A',
            'Q2_ConceptFit_Pack_B',
            'Q2_ConceptFit_Pack_C',
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

        info_col_name = ['var_name', 'var_lbl', 'var_type', 'val_lbl']

        dict_add_new_qres = {
            'Ma_Part_1_A': ['Mã part 1', 'SA', {'1': 'Concept A', '2': 'Concept B', '3': 'Concept C', '4': 'Concept D'}, 1],
            'Ma_Part_1_B': ['Mã part 1', 'SA', {'1': 'Concept A', '2': 'Concept B', '3': 'Concept C', '4': 'Concept D'}, 2],
            'Ma_Part_1_C': ['Mã part 1', 'SA', {'1': 'Concept A', '2': 'Concept B', '3': 'Concept C', '4': 'Concept D'}, 3],
            'Ma_Part_1_D': ['Mã part 1', 'SA', {'1': 'Concept A', '2': 'Concept B', '3': 'Concept C', '4': 'Concept D'}, 4],

            'Q1_LikeMost_Rank_A': ['Q1_LikeMost.Trong 4 ý tưởng Sữa rửa mặt chị đã xem vừa rồi, chị thích ý tưởng nào hơn?Vui lòng xếp theo thứ tự từ thích nhất đến ít thích hơn (1 - thích nhất; 4 - ít thích nhất)', 'SA', {'1': '1st', '2': '2nd', '3': '3rd', '4': '4th'}, np.nan],
            'Q1_LikeOverall_A': ['Q1_LikeOverall.Vì sao chị THÍCH ý tưởng này?Ý tưởng:', 'FT', {}, np.nan],
            'Q1_Dislike_Overall_A': ['Q1_Dislike_Overall.Vui lòng cho biết có điểm nào chị CHƯA THÍCH ở mô tả này không?Ý tưởng:', 'FT', {}, np.nan],
            'Q1_FC_PI_Benefit_A': ['Q1_FC_PI_Benefit.Vui lòng cho biết ý định mua đối với ý tưởng SRM:  ?', 'SA', {'1': '1.Chắc chắn sẽ không mua', '2': '2. Không mua', '3': '3. Có thể sẽ mua hoặc không', '4': '4. Sẽ mua', '5': '5. Chắc chắn sẽ mua'}, np.nan],

            'Q1_LikeMost_Rank_B': ['Q1_LikeMost.Trong 4 ý tưởng Sữa rửa mặt chị đã xem vừa rồi, chị thích ý tưởng nào hơn?Vui lòng xếp theo thứ tự từ thích nhất đến ít thích hơn (1 - thích nhất; 4 - ít thích nhất)', 'SA', {'1': '1st', '2': '2nd', '3': '3rd', '4': '4th'}, np.nan],
            'Q1_LikeOverall_B': ['Q1_LikeOverall.Vì sao chị THÍCH ý tưởng này?Ý tưởng:', 'FT', {}, np.nan],
            'Q1_Dislike_Overall_B': ['Q1_Dislike_Overall.Vui lòng cho biết có điểm nào chị CHƯA THÍCH ở mô tả này không?Ý tưởng:', 'FT', {}, np.nan],
            'Q1_FC_PI_Benefit_B': ['Q1_FC_PI_Benefit.Vui lòng cho biết ý định mua đối với ý tưởng SRM:  ?', 'SA', {'1': '1.Chắc chắn sẽ không mua', '2': '2. Không mua', '3': '3. Có thể sẽ mua hoặc không', '4': '4. Sẽ mua', '5': '5. Chắc chắn sẽ mua'}, np.nan],

            'Q1_LikeMost_Rank_C': ['Q1_LikeMost.Trong 4 ý tưởng Sữa rửa mặt chị đã xem vừa rồi, chị thích ý tưởng nào hơn?Vui lòng xếp theo thứ tự từ thích nhất đến ít thích hơn (1 - thích nhất; 4 - ít thích nhất)', 'SA', {'1': '1st', '2': '2nd', '3': '3rd', '4': '4th'}, np.nan],
            'Q1_LikeOverall_C': ['Q1_LikeOverall.Vì sao chị THÍCH ý tưởng này?Ý tưởng:', 'FT', {}, np.nan],
            'Q1_Dislike_Overall_C': ['Q1_Dislike_Overall.Vui lòng cho biết có điểm nào chị CHƯA THÍCH ở mô tả này không?Ý tưởng:', 'FT', {}, np.nan],
            'Q1_FC_PI_Benefit_C': ['Q1_FC_PI_Benefit.Vui lòng cho biết ý định mua đối với ý tưởng SRM:  ?', 'SA', {'1': '1.Chắc chắn sẽ không mua', '2': '2. Không mua', '3': '3. Có thể sẽ mua hoặc không', '4': '4. Sẽ mua', '5': '5. Chắc chắn sẽ mua'}, np.nan],

            'Q1_LikeMost_Rank_D': ['Q1_LikeMost.Trong 4 ý tưởng Sữa rửa mặt chị đã xem vừa rồi, chị thích ý tưởng nào hơn?Vui lòng xếp theo thứ tự từ thích nhất đến ít thích hơn (1 - thích nhất; 4 - ít thích nhất)', 'SA', {'1': '1st', '2': '2nd', '3': '3rd', '4': '4th'}, np.nan],
            'Q1_LikeOverall_D': ['Q1_LikeOverall.Vì sao chị THÍCH ý tưởng này?Ý tưởng:', 'FT', {}, np.nan],
            'Q1_Dislike_Overall_D': ['Q1_Dislike_Overall.Vui lòng cho biết có điểm nào chị CHƯA THÍCH ở mô tả này không?Ý tưởng:', 'FT', {}, np.nan],
            'Q1_FC_PI_Benefit_D': ['Q1_FC_PI_Benefit.Vui lòng cho biết ý định mua đối với ý tưởng SRM:  ?', 'SA', {'1': '1.Chắc chắn sẽ không mua', '2': '2. Không mua', '3': '3. Có thể sẽ mua hoặc không', '4': '4. Sẽ mua', '5': '5. Chắc chắn sẽ mua'}, np.nan],

            'Ma_Part_2a_A1': ['Mã part 2a', 'SA', {'1': 'ComA1', '2': 'ComA2', '3': 'ComB1', '4': 'ComB2', '5': 'ComC1', '6': 'ComC2'}, np.nan],
            'Ma_Part_2a_A2': ['Mã part 2a', 'SA', {'1': 'ComA1', '2': 'ComA2', '3': 'ComB1', '4': 'ComB2', '5': 'ComC1', '6': 'ComC2'}, np.nan],
            'Ma_Part_2a_B1': ['Mã part 2a', 'SA', {'1': 'ComA1', '2': 'ComA2', '3': 'ComB1', '4': 'ComB2', '5': 'ComC1', '6': 'ComC2'}, np.nan],
            'Ma_Part_2a_B2': ['Mã part 2a', 'SA', {'1': 'ComA1', '2': 'ComA2', '3': 'ComB1', '4': 'ComB2', '5': 'ComC1', '6': 'ComC2'}, np.nan],
            'Ma_Part_2a_C1': ['Mã part 2a', 'SA', {'1': 'ComA1', '2': 'ComA2', '3': 'ComB1', '4': 'ComB2', '5': 'ComC1', '6': 'ComC2'}, np.nan],
            'Ma_Part_2a_C2': ['Mã part 2a', 'SA', {'1': 'ComA1', '2': 'ComA2', '3': 'ComB1', '4': 'ComB2', '5': 'ComC1', '6': 'ComC2'}, np.nan],

            'Q2_LikeMost_Com_A1': ['Q2_LikeMost_Com.Trong 2 bảng mô tả Sữa rửa mặt trên chị thích bảng nào hơn?', 'SA', {'1': '1st', '2': '2nd'}, np.nan],
            'Q2_LikeMost_Com_A2': ['Q2_LikeMost_Com.Trong 2 bảng mô tả Sữa rửa mặt trên chị thích bảng nào hơn?', 'SA', {'1': '1st', '2': '2nd'}, np.nan],
            'Q2_LikeMost_Com_B1': ['Q2_LikeMost_Com.Trong 2 bảng mô tả Sữa rửa mặt trên chị thích bảng nào hơn?', 'SA', {'1': '1st', '2': '2nd'}, np.nan],
            'Q2_LikeMost_Com_B2': ['Q2_LikeMost_Com.Trong 2 bảng mô tả Sữa rửa mặt trên chị thích bảng nào hơn?', 'SA', {'1': '1st', '2': '2nd'}, np.nan],
            'Q2_LikeMost_Com_C1': ['Q2_LikeMost_Com.Trong 2 bảng mô tả Sữa rửa mặt trên chị thích bảng nào hơn?', 'SA', {'1': '1st', '2': '2nd'}, np.nan],
            'Q2_LikeMost_Com_C2': ['Q2_LikeMost_Com.Trong 2 bảng mô tả Sữa rửa mặt trên chị thích bảng nào hơn?', 'SA', {'1': '1st', '2': '2nd'}, np.nan],

            'Ma_Part_2b_A1': ['Mã part 2b', 'SA', {'1': 'PacA1', '2': 'PacA2', '3': 'PacA3', '4': 'PacB1', '5': 'PacB2', '6': 'PacB3', '7': 'PacC1', '8': 'PacC2', '9': 'PacC3'}, np.nan],
            'Ma_Part_2b_A2': ['Mã part 2b', 'SA', {'1': 'PacA1', '2': 'PacA2', '3': 'PacA3', '4': 'PacB1', '5': 'PacB2', '6': 'PacB3', '7': 'PacC1', '8': 'PacC2', '9': 'PacC3'}, np.nan],
            'Ma_Part_2b_A3': ['Mã part 2b', 'SA', {'1': 'PacA1', '2': 'PacA2', '3': 'PacA3', '4': 'PacB1', '5': 'PacB2', '6': 'PacB3', '7': 'PacC1', '8': 'PacC2', '9': 'PacC3'}, np.nan],
            'Ma_Part_2b_B1': ['Mã part 2b', 'SA', {'1': 'PacA1', '2': 'PacA2', '3': 'PacA3', '4': 'PacB1', '5': 'PacB2', '6': 'PacB3', '7': 'PacC1', '8': 'PacC2', '9': 'PacC3'}, np.nan],
            'Ma_Part_2b_B2': ['Mã part 2b', 'SA', {'1': 'PacA1', '2': 'PacA2', '3': 'PacA3', '4': 'PacB1', '5': 'PacB2', '6': 'PacB3', '7': 'PacC1', '8': 'PacC2', '9': 'PacC3'}, np.nan],
            'Ma_Part_2b_B3': ['Mã part 2b', 'SA', {'1': 'PacA1', '2': 'PacA2', '3': 'PacA3', '4': 'PacB1', '5': 'PacB2', '6': 'PacB3', '7': 'PacC1', '8': 'PacC2', '9': 'PacC3'}, np.nan],
            'Ma_Part_2b_C1': ['Mã part 2b', 'SA', {'1': 'PacA1', '2': 'PacA2', '3': 'PacA3', '4': 'PacB1', '5': 'PacB2', '6': 'PacB3', '7': 'PacC1', '8': 'PacC2', '9': 'PacC3'}, np.nan],
            'Ma_Part_2b_C2': ['Mã part 2b', 'SA', {'1': 'PacA1', '2': 'PacA2', '3': 'PacA3', '4': 'PacB1', '5': 'PacB2', '6': 'PacB3', '7': 'PacC1', '8': 'PacC2', '9': 'PacC3'}, np.nan],
            'Ma_Part_2b_C3': ['Mã part 2b', 'SA', {'1': 'PacA1', '2': 'PacA2', '3': 'PacA3', '4': 'PacB1', '5': 'PacB2', '6': 'PacB3', '7': 'PacC1', '8': 'PacC2', '9': 'PacC3'}, np.nan],

            'Q2_LikeMost_Pack_A1_Ranking': ['Q2_LikeMost_Pack_Ranking.Sau khi quan sát 3 thiết kế bao bì này, chị hãy xếp hạng theo thứ tự yêu thích nhất đến ít thích hơn', 'SA', {'1': '1st', '2': '2nd', '3': '3rd'}, np.nan],
            'Q2_ConceptFit_Pack_A1_Ranking': ['Q2_ConceptFit_Pack_Ranking.Vui lòng xếp hạng các thiết kế bao bì này theo mức độ phù hợp với ý tưởng sản phẩm trên', 'SA', {'1': '1st', '2': '2nd', '3': '3rd'}, np.nan],

            'Q2_LikeMost_Pack_A2_Ranking': ['Q2_LikeMost_Pack_Ranking.Sau khi quan sát 3 thiết kế bao bì này, chị hãy xếp hạng theo thứ tự yêu thích nhất đến ít thích hơn', 'SA', {'1': '1st', '2': '2nd', '3': '3rd'}, np.nan],
            'Q2_ConceptFit_Pack_A2_Ranking': ['Q2_ConceptFit_Pack_Ranking.Vui lòng xếp hạng các thiết kế bao bì này theo mức độ phù hợp với ý tưởng sản phẩm trên', 'SA', {'1': '1st', '2': '2nd', '3': '3rd'}, np.nan],

            'Q2_LikeMost_Pack_A3_Ranking': ['Q2_LikeMost_Pack_Ranking.Sau khi quan sát 3 thiết kế bao bì này, chị hãy xếp hạng theo thứ tự yêu thích nhất đến ít thích hơn', 'SA', {'1': '1st', '2': '2nd', '3': '3rd'}, np.nan],
            'Q2_ConceptFit_Pack_A3_Ranking': ['Q2_ConceptFit_Pack_Ranking.Vui lòng xếp hạng các thiết kế bao bì này theo mức độ phù hợp với ý tưởng sản phẩm trên', 'SA', {'1': '1st', '2': '2nd', '3': '3rd'}, np.nan],

            'Q2_LikeMost_Pack_B1_Ranking': ['Q2_LikeMost_Pack_Ranking.Sau khi quan sát 3 thiết kế bao bì này, chị hãy xếp hạng theo thứ tự yêu thích nhất đến ít thích hơn', 'SA', {'1': '1st', '2': '2nd', '3': '3rd'}, np.nan],
            'Q2_ConceptFit_Pack_B1_Ranking': ['Q2_ConceptFit_Pack_Ranking.Vui lòng xếp hạng các thiết kế bao bì này theo mức độ phù hợp với ý tưởng sản phẩm trên', 'SA', {'1': '1st', '2': '2nd', '3': '3rd'}, np.nan],

            'Q2_LikeMost_Pack_B2_Ranking': ['Q2_LikeMost_Pack_Ranking.Sau khi quan sát 3 thiết kế bao bì này, chị hãy xếp hạng theo thứ tự yêu thích nhất đến ít thích hơn', 'SA', {'1': '1st', '2': '2nd', '3': '3rd'}, np.nan],
            'Q2_ConceptFit_Pack_B2_Ranking': ['Q2_ConceptFit_Pack_Ranking.Vui lòng xếp hạng các thiết kế bao bì này theo mức độ phù hợp với ý tưởng sản phẩm trên', 'SA', {'1': '1st', '2': '2nd', '3': '3rd'}, np.nan],

            'Q2_LikeMost_Pack_B3_Ranking': ['Q2_LikeMost_Pack_Ranking.Sau khi quan sát 3 thiết kế bao bì này, chị hãy xếp hạng theo thứ tự yêu thích nhất đến ít thích hơn', 'SA', {'1': '1st', '2': '2nd', '3': '3rd'}, np.nan],
            'Q2_ConceptFit_Pack_B3_Ranking': ['Q2_ConceptFit_Pack_Ranking.Vui lòng xếp hạng các thiết kế bao bì này theo mức độ phù hợp với ý tưởng sản phẩm trên', 'SA', {'1': '1st', '2': '2nd', '3': '3rd'}, np.nan],

            'Q2_LikeMost_Pack_C1_Ranking': ['Q2_LikeMost_Pack_Ranking.Sau khi quan sát 3 thiết kế bao bì này, chị hãy xếp hạng theo thứ tự yêu thích nhất đến ít thích hơn', 'SA', {'1': '1st', '2': '2nd', '3': '3rd'}, np.nan],
            'Q2_ConceptFit_Pack_C1_Ranking': ['Q2_ConceptFit_Pack_Ranking.Vui lòng xếp hạng các thiết kế bao bì này theo mức độ phù hợp với ý tưởng sản phẩm trên', 'SA', {'1': '1st', '2': '2nd', '3': '3rd'}, np.nan],

            'Q2_LikeMost_Pack_C2_Ranking': ['Q2_LikeMost_Pack_Ranking.Sau khi quan sát 3 thiết kế bao bì này, chị hãy xếp hạng theo thứ tự yêu thích nhất đến ít thích hơn', 'SA', {'1': '1st', '2': '2nd', '3': '3rd'}, np.nan],
            'Q2_ConceptFit_Pack_C2_Ranking': ['Q2_ConceptFit_Pack_Ranking.Vui lòng xếp hạng các thiết kế bao bì này theo mức độ phù hợp với ý tưởng sản phẩm trên', 'SA', {'1': '1st', '2': '2nd', '3': '3rd'}, np.nan],

            'Q2_LikeMost_Pack_C3_Ranking': ['Q2_LikeMost_Pack_Ranking.Sau khi quan sát 3 thiết kế bao bì này, chị hãy xếp hạng theo thứ tự yêu thích nhất đến ít thích hơn', 'SA', {'1': '1st', '2': '2nd', '3': '3rd'}, np.nan],
            'Q2_ConceptFit_Pack_C3_Ranking': ['Q2_ConceptFit_Pack_Ranking.Vui lòng xếp hạng các thiết kế bao bì này theo mức độ phù hợp với ý tưởng sản phẩm trên', 'SA', {'1': '1st', '2': '2nd', '3': '3rd'}, np.nan],

            'Ma_Part_3_A': ['Mã part 3', 'SA', {'1': 'YT A', '2': 'YT B', '3': 'YT C'}, 1],
            'Ma_Part_3_B': ['Mã part 3', 'SA', {'1': 'YT A', '2': 'YT B', '3': 'YT C'}, 2],
            'Ma_Part_3_C': ['Mã part 3', 'SA', {'1': 'YT A', '2': 'YT B', '3': 'YT C'}, 3],

            'Q3_MOI_LikeMost_A_Rank': ['Q3_MOI_LikeMost. Trong 3 Kem dưỡng da trên, vui lòng xếp hạng theo thứ tự thích nhất đến ít thích nhất (Với 1 - thích nhất; 3 - ít thích nhất)', 'SA', {'1': '1st', '2': '2nd', '3': '3rd'}, np.nan],
            'Q3_MOI_Com_A_Like': ['Q3_MOI_Com_Like.Chị THÍCH Điều gì ở ý tưởng Kem dưỡng da này?Kem dưỡng thích nhất:', 'FT', {}, np.nan],
            'Q3_MOI_Com_A_DisLike': ['Q3_MOI_Com_DisLike. Đối với ý tưởng ÍT THÍCH NHẤT, vui lòng cho biết có điểm nào chị CHƯA THÍCH ở ý tưởngKem dưỡng da này không?Kem dưỡng ít thích nhất:', 'FT', {}, np.nan],

            'Q3_MOI_LikeMost_B_Rank': ['Q3_MOI_LikeMost. Trong 3 Kem dưỡng da trên, vui lòng xếp hạng theo thứ tự thích nhất đến ít thích nhất (Với 1 - thích nhất; 3 - ít thích nhất)', 'SA', {'1': '1st', '2': '2nd', '3': '3rd'}, np.nan],
            'Q3_MOI_Com_B_Like': ['Q3_MOI_Com_Like.Chị THÍCH Điều gì ở ý tưởng Kem dưỡng da này?Kem dưỡng thích nhất:', 'FT', {}, np.nan],
            'Q3_MOI_Com_B_DisLike': ['Q3_MOI_Com_DisLike. Đối với ý tưởng ÍT THÍCH NHẤT, vui lòng cho biết có điểm nào chị CHƯA THÍCH ở ý tưởngKem dưỡng da này không?Kem dưỡng ít thích nhất:', 'FT', {}, np.nan],

            'Q3_MOI_LikeMost_C_Rank': ['Q3_MOI_LikeMost. Trong 3 Kem dưỡng da trên, vui lòng xếp hạng theo thứ tự thích nhất đến ít thích nhất (Với 1 - thích nhất; 3 - ít thích nhất)', 'SA', {'1': '1st', '2': '2nd', '3': '3rd'}, np.nan],
            'Q3_MOI_Com_C_Like': ['Q3_MOI_Com_Like.Chị THÍCH Điều gì ở ý tưởng Kem dưỡng da này?Kem dưỡng thích nhất:', 'FT', {}, np.nan],
            'Q3_MOI_Com_C_DisLike': ['Q3_MOI_Com_DisLike. Đối với ý tưởng ÍT THÍCH NHẤT, vui lòng cho biết có điểm nào chị CHƯA THÍCH ở ý tưởngKem dưỡng da này không?Kem dưỡng ít thích nhất:', 'FT', {}, np.nan],

        }

        for key, val in dict_add_new_qres.items():
            df_info = pd.concat([df_info, pd.DataFrame(columns=info_col_name, data=[[key, val[0], val[1], val[2]]])], axis=0, ignore_index=True)
            df_data = pd.concat([df_data, pd.DataFrame(columns=[key], data=[val[-1]] * df_data.shape[0])], axis=1)

        # PART 1
        for i, v in enumerate(['A', 'B', 'C', 'D']):
            for r in range(1, 5):
                df_fil = df_data.query(f'Q1_LikeMost_Rank{r} == {i + 1}')
                df_data.loc[df_fil.index, [f'Q1_LikeMost_Rank_{v}']] = [r]
                # df_data[f'Q1_LikeMost_Rank{r}_{v}'] = [1 if a == i + 1 else 2 for a in df_data[f'Q1_LikeMost_Rank{r}']]
                del df_fil

            if v != 'D':
                df_fil = df_data.query(f'Q1_LikeMost_Rank1 == {i + 1}').copy()
                if not df_fil.empty:
                    df_data.loc[df_fil.index, f'Q1_LikeOverall_{v}'] = df_data.loc[df_fil.index, 'Q1_LikeOverall_I']
                    df_data.loc[df_fil.index, f'Q1_Dislike_Overall_{v}'] = df_data.loc[df_fil.index, 'Q1_Dislike_Overall_I']
                    df_data.loc[df_fil.index, f'Q1_FC_PI_Benefit_{v}'] = df_data.loc[df_fil.index, 'Q1_FC_PI_Benefit_I']

                df_fil = df_data.query(f'Q1_LikeMost_Rank1 == 4 & Q1_LikeMost_Rank2 == {i + 1}').copy()
                if not df_fil.empty:
                    df_data.loc[df_fil.index, f'Q1_LikeOverall_{v}'] = df_data.loc[df_fil.index, 'Q1_LikeOverall_II']
                    df_data.loc[df_fil.index, f'Q1_Dislike_Overall_{v}'] = df_data.loc[df_fil.index, 'Q1_Dislike_Overall_II']
                    df_data.loc[df_fil.index, f'Q1_FC_PI_Benefit_{v}'] = df_data.loc[df_fil.index, 'Q1_FC_PI_Benefit_II']

                del df_fil

            # # NEW UPDATE - DELETE
            # df_fil = df_data.query(f'Q1_LikeMost_Rank1 == {i + 1}').copy()
            # df_data.loc[df_fil.index, f'Q1_FC_PI_Benefit_{v}'] = df_data.loc[df_fil.index, 'Q1_FC_PI_Benefit_I']


        # PART 2a
        for i, v in enumerate(['A', 'B', 'C']):
            df_fil = df_data.query(f'Q2_LikeMost_Com_{v} > 0').copy()
            if not df_fil.empty:
                df_data.loc[df_fil.index, [f'Ma_Part_2a_{v}1']] = [(i * 2) + 1]
                df_data.loc[df_fil.index, [f'Ma_Part_2a_{v}2']] = [(i * 2) + 2]

                df_data.loc[df_fil.index, [f'Q2_LikeMost_Com_{v}1']] = [2]
                df_data.loc[df_fil.index, [f'Q2_LikeMost_Com_{v}2']] = [2]

                df_fil2 = df_data.query(f'Q2_LikeMost_Com_{v} == 1').copy()
                if not df_fil2.empty:
                    df_data.loc[df_fil2.index, [f'Q2_LikeMost_Com_{v}1']] = [1]

                df_fil2 = df_data.query(f'Q2_LikeMost_Com_{v} == 2').copy()
                if not df_fil2.empty:
                    df_data.loc[df_fil2.index, [f'Q2_LikeMost_Com_{v}2']] = [1]

                del df_fil2

            del df_fil

        # PART 2b
        for i, v in enumerate(['A', 'B', 'C']):
            df_fil = df_data.query(f'Q2_LikeMost_Pack_{v}_Ranking_Rank1 > 0').copy()
            if not df_fil.empty:
                df_data.loc[df_fil.index, [f'Ma_Part_2b_{v}1']] = [(i * 3) + 1]
                df_data.loc[df_fil.index, [f'Ma_Part_2b_{v}2']] = [(i * 3) + 2]
                df_data.loc[df_fil.index, [f'Ma_Part_2b_{v}3']] = [(i * 3) + 3]

                for r in range(1, 4):
                    for sp in range(1, 4):
                        df_fil2 = df_data.query(f'Q2_LikeMost_Pack_{v}_Ranking_Rank{r} == {sp}').copy()
                        if not df_fil2.empty:
                            df_data.loc[df_fil2.index, [f'Q2_LikeMost_Pack_{v}{sp}_Ranking']] = [r]

                        del df_fil2

                        df_fil2 = df_data.query(f'Q2_ConceptFit_Pack_{v}_Ranking_Rank{r} == {sp}').copy()
                        if not df_fil2.empty:
                            df_data.loc[df_fil2.index, [f'Q2_ConceptFit_Pack_{v}{sp}_Ranking']] = [r]

                        del df_fil2

        # PART 4
        for i, v in enumerate(['A', 'B', 'C']):
            for r in range(1, 4):
                df_fil2 = df_data.query(f'Q3_MOI_LikeMost_Rank{r} == {i + 1}').copy()
                if not df_fil2.empty:
                    df_data.loc[df_fil2.index, [f'Q3_MOI_LikeMost_{v}_Rank']] = [r]
                del df_fil2

            df_fil2 = df_data.query(f'Q3_MOI_LikeMost_Rank1 == {i + 1}').copy()
            if not df_fil2.empty:
                df_data.loc[df_fil2.index, f'Q3_MOI_Com_{v}_Like'] = df_data.loc[df_fil2.index, 'Q3_MOI_Com_Like']
            del df_fil2

            df_fil2 = df_data.query(f'Q3_MOI_LikeMost_Rank3 == {i + 1}').copy()
            if not df_fil2.empty:
                df_data.loc[df_fil2.index, f'Q3_MOI_Com_{v}_DisLike'] = df_data.loc[df_fil2.index, 'Q3_MOI_Com_DisLike']
            del df_fil2

        # # ----------------------------------
        # df_data.to_excel('zzz_df_data.xlsx')
        # df_info.to_excel('zzz_df_info.xlsx')
        # # ----------------------------------

        # Define structure----------------------------------------------------------------------------------------------
        self.logger.info('Define structure')

        id_col = 'Q0a_RespondentID'

        sp_part_1 = 'Ma_Part_1'
        sp_part_2a = 'Ma_Part_2a'
        sp_part_2b = 'Ma_Part_2b'
        sp_part_3 = 'Ma_Part_3'

        lst_scr = [
            'InterviewerName',
            'LOI',
            'RespondentWard',
            'RespondentDist',
            'RespondentCity',
            'Recruit_S1_Thanh_pho',
            'Recruit_S1_Thanh_pho_o5',
            'Recruit_S2_Nam_sinh',
            'Recruit_S2_Do_tuoi',
            'Recruit_S3_Quyet_dinh',
            'Recruit_S4_Brands_1',
            'Recruit_S4_Brands_2',
            'Recruit_S4_Brands_3',
            'Recruit_S4_Brands_4',
            'Recruit_S4_Brands_5',
            'Recruit_S4_Brands_o1',
            'Recruit_S4_Brands_o2',
            'Recruit_S4_Brands_o3',
            'Recruit_S4_Brands_o4',
            'Recruit_S4_Brands_o5',
            'Recruit_S5_BUMO',
            'Recruit_S5_BUMO_o16',
            'Recruit_S6o_Extra_1',
            'Recruit_S6o_Extra_2',
            'Recruit_S6_Gia',
            'Recruit_S7_BUMO_Ponds',
            'Recruit_S8_Tan_suat',
            'Recruit_S9_Tinh_trang_hon_nhan',
            'Recruit_S10_Thu_nhap_HGD',
            'Recruit_S11_Nganh_cam_1',
            'Recruit_S11_Nganh_cam_2',
            'Recruit_S11_Nganh_cam_3',
            'Recruit_S11_Nganh_cam_4',
            'Recruit_S11_Nganh_cam_5',
            'Recruit_S12_Nghe_nghiep_hien_tai',
            'Recruit_S12_Nghe_nghiep_hien_tai_o8',
            'Recruit_S12_Nghe_nghiep_hien_tai_o9',
            'Recruit_S13_Tham_gia_NCTT',
            'Recruit_S14_Suc_khoe_1',
            'Recruit_S14_Suc_khoe_2',
            'Recruit_S14_Suc_khoe_3',
            'Recruit_S14_Suc_khoe_4',
            'Recruit_S14_Suc_khoe_5',
            'Recruit_S14_Suc_khoe_6',
            'Recruit_T1',
            'Q1_LikeMost_Dummy1',
            'Q1_LikeMost_Dummy2',
            'Q1_LikeMost_Dummy3',
            'Q1_LikeMost_Dummy4',
        ]

        dict_part_1 = {
            1: {
                'Ma_Part_1_A': 'Ma_Part_1',
                'Q1_OL_A': 'Q1_OL',
                'Q1_Uniqueness_A': 'Q1_Uniqueness',
                'Q1_Relevancy_A': 'Q1_Relevancy',
                'Q1_LikeMost_Rank_A': 'Q1_LikeMost_Rank',
                'Q1_LikeOverall_A': 'Q1_LikeOverall',
                'Q1_Dislike_Overall_A': 'Q1_Dislike_Overall',
                'Q1_FC_PI_Benefit_A': 'Q1_FC_PI_Benefit',
            },
            2: {
                'Ma_Part_1_B': 'Ma_Part_1',
                'Q1_OL_B': 'Q1_OL',
                'Q1_Uniqueness_B': 'Q1_Uniqueness',
                'Q1_Relevancy_B': 'Q1_Relevancy',
                'Q1_LikeMost_Rank_B': 'Q1_LikeMost_Rank',
                'Q1_LikeOverall_B': 'Q1_LikeOverall',
                'Q1_Dislike_Overall_B': 'Q1_Dislike_Overall',
                'Q1_FC_PI_Benefit_B': 'Q1_FC_PI_Benefit',
            },
            3: {
                'Ma_Part_1_C': 'Ma_Part_1',
                'Q1_OL_C': 'Q1_OL',
                'Q1_Uniqueness_C': 'Q1_Uniqueness',
                'Q1_Relevancy_C': 'Q1_Relevancy',
                'Q1_LikeMost_Rank_C': 'Q1_LikeMost_Rank',
                'Q1_LikeOverall_C': 'Q1_LikeOverall',
                'Q1_Dislike_Overall_C': 'Q1_Dislike_Overall',
                'Q1_FC_PI_Benefit_C': 'Q1_FC_PI_Benefit',
            },
            4: {
                'Ma_Part_1_D': 'Ma_Part_1',
                'Q1_OL_D': 'Q1_OL',
                'Q1_Uniqueness_D': 'Q1_Uniqueness',
                'Q1_Relevancy_D': 'Q1_Relevancy',
                'Q1_LikeMost_Rank_D': 'Q1_LikeMost_Rank',
                'Q1_LikeOverall_D': 'Q1_LikeOverall',
                'Q1_Dislike_Overall_D': 'Q1_Dislike_Overall',
                'Q1_FC_PI_Benefit_D': 'Q1_FC_PI_Benefit',
            },
        }

        dict_part_2a = {
            1: {
                'Ma_Part_2a_A1': 'Ma_Part_2a',
                'Q2_OL_Com_A1': 'Q2_OL_Com',
                'Q2_LikeMost_Com_A1': 'Q2_LikeMost_Com',
                'Q2_FC_Com_Like_A1': 'Q2_FC_Com_Like',
                'Q2_FC_Com_DisLike_A1': 'Q2_FC_Com_DisLike',
                'Q2_FC_PI_Com_A1': 'Q2_FC_PI_Com',
            },
            2: {
                'Ma_Part_2a_A2': 'Ma_Part_2a',
                'Q2_OL_Com_A2': 'Q2_OL_Com',
                'Q2_LikeMost_Com_A2': 'Q2_LikeMost_Com',
                'Q2_FC_Com_Like_A2': 'Q2_FC_Com_Like',
                'Q2_FC_Com_DisLike_A2': 'Q2_FC_Com_DisLike',
                'Q2_FC_PI_Com_A2': 'Q2_FC_PI_Com',
            },
            3: {
                'Ma_Part_2a_B1': 'Ma_Part_2a',
                'Q2_OL_Com_B1': 'Q2_OL_Com',
                'Q2_LikeMost_Com_B1': 'Q2_LikeMost_Com',
                'Q2_FC_Com_Like_B1': 'Q2_FC_Com_Like',
                'Q2_FC_Com_DisLike_B1': 'Q2_FC_Com_DisLike',
                'Q2_FC_PI_Com_B1': 'Q2_FC_PI_Com',
            },
            4: {
                'Ma_Part_2a_B2': 'Ma_Part_2a',
                'Q2_OL_Com_B2': 'Q2_OL_Com',
                'Q2_LikeMost_Com_B2': 'Q2_LikeMost_Com',
                'Q2_FC_Com_Like_B2': 'Q2_FC_Com_Like',
                'Q2_FC_Com_DisLike_B2': 'Q2_FC_Com_DisLike',
                'Q2_FC_PI_Com_B2': 'Q2_FC_PI_Com',
            },
            5: {
                'Ma_Part_2a_C1': 'Ma_Part_2a',
                'Q2_OL_Com_C1': 'Q2_OL_Com',
                'Q2_LikeMost_Com_C1': 'Q2_LikeMost_Com',
                'Q2_FC_Com_Like_C1': 'Q2_FC_Com_Like',
                'Q2_FC_Com_DisLike_C1': 'Q2_FC_Com_DisLike',
                'Q2_FC_PI_Com_C1': 'Q2_FC_PI_Com',
            },
            6: {
                'Ma_Part_2a_C2': 'Ma_Part_2a',
                'Q2_OL_Com_C2': 'Q2_OL_Com',
                'Q2_LikeMost_Com_C2': 'Q2_LikeMost_Com',
                'Q2_FC_Com_Like_C2': 'Q2_FC_Com_Like',
                'Q2_FC_Com_DisLike_C2': 'Q2_FC_Com_DisLike',
                'Q2_FC_PI_Com_C2': 'Q2_FC_PI_Com',
            },

        }

        dict_part_2b = {
            1: {
                'Ma_Part_2b_A1': 'Ma_Part_2b',
                'Q2_LikeMost_Pack_A1_Ranking': 'Q2_LikeMost_Pack_Ranking',
                'Q2_ConceptFit_Pack_A1_Ranking': 'Q2_ConceptFit_Pack_Ranking',
                'Q2_FC_Pack_Like_A1': 'Q2_FC_Pack_Like',
                'Q2_FC_Pack_DisLike_A1': 'Q2_FC_Pack_DisLike',
            },
            2: {
                'Ma_Part_2b_A2': 'Ma_Part_2b',
                'Q2_LikeMost_Pack_A2_Ranking': 'Q2_LikeMost_Pack_Ranking',
                'Q2_ConceptFit_Pack_A2_Ranking': 'Q2_ConceptFit_Pack_Ranking',
                'Q2_FC_Pack_Like_A2': 'Q2_FC_Pack_Like',
                'Q2_FC_Pack_DisLike_A2': 'Q2_FC_Pack_DisLike',
            },
            3: {
                'Ma_Part_2b_A3': 'Ma_Part_2b',
                'Q2_LikeMost_Pack_A3_Ranking': 'Q2_LikeMost_Pack_Ranking',
                'Q2_ConceptFit_Pack_A3_Ranking': 'Q2_ConceptFit_Pack_Ranking',
                'Q2_FC_Pack_Like_A3': 'Q2_FC_Pack_Like',
                'Q2_FC_Pack_DisLike_A3': 'Q2_FC_Pack_DisLike',
            },
            4: {
                'Ma_Part_2b_B1': 'Ma_Part_2b',
                'Q2_LikeMost_Pack_B1_Ranking': 'Q2_LikeMost_Pack_Ranking',
                'Q2_ConceptFit_Pack_B1_Ranking': 'Q2_ConceptFit_Pack_Ranking',
                'Q2_FC_Pack_Like_B1': 'Q2_FC_Pack_Like',
                'Q2_FC_Pack_DisLike_B1': 'Q2_FC_Pack_DisLike',
            },
            5: {
                'Ma_Part_2b_B2': 'Ma_Part_2b',
                'Q2_LikeMost_Pack_B2_Ranking': 'Q2_LikeMost_Pack_Ranking',
                'Q2_ConceptFit_Pack_B2_Ranking': 'Q2_ConceptFit_Pack_Ranking',
                'Q2_FC_Pack_Like_B2': 'Q2_FC_Pack_Like',
                'Q2_FC_Pack_DisLike_B2': 'Q2_FC_Pack_DisLike',
            },
            6: {
                'Ma_Part_2b_B3': 'Ma_Part_2b',
                'Q2_LikeMost_Pack_B3_Ranking': 'Q2_LikeMost_Pack_Ranking',
                'Q2_ConceptFit_Pack_B3_Ranking': 'Q2_ConceptFit_Pack_Ranking',
                'Q2_FC_Pack_Like_B3': 'Q2_FC_Pack_Like',
                'Q2_FC_Pack_DisLike_B3': 'Q2_FC_Pack_DisLike',
            },
            7: {
                'Ma_Part_2b_C1': 'Ma_Part_2b',
                'Q2_LikeMost_Pack_C1_Ranking': 'Q2_LikeMost_Pack_Ranking',
                'Q2_ConceptFit_Pack_C1_Ranking': 'Q2_ConceptFit_Pack_Ranking',
                'Q2_FC_Pack_Like_C1': 'Q2_FC_Pack_Like',
                'Q2_FC_Pack_DisLike_C1': 'Q2_FC_Pack_DisLike',
            },
            8: {
                'Ma_Part_2b_C2': 'Ma_Part_2b',
                'Q2_LikeMost_Pack_C2_Ranking': 'Q2_LikeMost_Pack_Ranking',
                'Q2_ConceptFit_Pack_C2_Ranking': 'Q2_ConceptFit_Pack_Ranking',
                'Q2_FC_Pack_Like_C2': 'Q2_FC_Pack_Like',
                'Q2_FC_Pack_DisLike_C2': 'Q2_FC_Pack_DisLike',
            },
            9: {
                'Ma_Part_2b_C3': 'Ma_Part_2b',
                'Q2_LikeMost_Pack_C3_Ranking': 'Q2_LikeMost_Pack_Ranking',
                'Q2_ConceptFit_Pack_C3_Ranking': 'Q2_ConceptFit_Pack_Ranking',
                'Q2_FC_Pack_Like_C3': 'Q2_FC_Pack_Like',
                'Q2_FC_Pack_DisLike_C3': 'Q2_FC_Pack_DisLike',
            },
        }

        dict_part_3 = {
            1: {
                'Ma_Part_3_A': 'Ma_Part_3',
                'Q3_MOI_OL_01': 'Q3_MOI_OL',
                'Q3_MOI_LikeMost_A_Rank': 'Q3_MOI_LikeMost_Rank',
                'Q3_MOI_Com_A_Like': 'Q3_MOI_Com_Like',
                'Q3_MOI_Com_A_DisLike': 'Q3_MOI_Com_DisLike',
            },
            2: {
                'Ma_Part_3_B': 'Ma_Part_3',
                'Q3_MOI_OL_02': 'Q3_MOI_OL',
                'Q3_MOI_LikeMost_B_Rank': 'Q3_MOI_LikeMost_Rank',
                'Q3_MOI_Com_B_Like': 'Q3_MOI_Com_Like',
                'Q3_MOI_Com_B_DisLike': 'Q3_MOI_Com_DisLike',
            },
            3: {
                'Ma_Part_3_C': 'Ma_Part_3',
                'Q3_MOI_OL_03': 'Q3_MOI_OL',
                'Q3_MOI_LikeMost_C_Rank': 'Q3_MOI_LikeMost_Rank',
                'Q3_MOI_Com_C_Like': 'Q3_MOI_Com_Like',
                'Q3_MOI_Com_C_DisLike': 'Q3_MOI_Com_DisLike',
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

        # -----------------
        dict_qre_group_mean = {
            'Q1_OL': {
                'range': [],  # f'0{i}' for i in range(1, 4)
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Q1_Uniqueness': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Q1_Relevancy': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Q1_FC_PI_Benefit': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },


            'Q2_OL_Com': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Q2_FC_PI_Com': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },

            'Q3_MOI_OL': {
                'range': [],
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
        df_data_part_1, df_info_part_1 = self.convert_to_stack(df_data, df_info, id_col, sp_part_1, lst_scr, dict_part_1, [])
        df_data_part_2a, df_info_part_2a = self.convert_to_stack(df_data, df_info, id_col, sp_part_2a, lst_scr, dict_part_2a, [])
        df_data_part_2b, df_info_part_2b = self.convert_to_stack(df_data, df_info, id_col, sp_part_2b, lst_scr, dict_part_2b, [])
        df_data_part_3, df_info_part_3 = self.convert_to_stack(df_data, df_info, id_col, sp_part_3, lst_scr, dict_part_3, [])

        df_data_part_2a = df_data_part_2a.query("Ma_Part_2a > 0")
        df_data_part_2b = df_data_part_2b.query("Ma_Part_2b > 0")
        # CONVERT TO STACK----------------------------------------------------------------------------------------------

        # OE RUNNING
        if dict_qre_OE_info:
            df_data_part_1, df_info_part_1 = self.add_oe_to_df(df_data_part_1, df_info_part_1, lst_addin_OE_value, dict_qre_OE_info, ['Q1_LikeOverall_OE', 'Q1_Dislike_Overall_OE'])
            df_data_part_2a, df_info_part_2a = self.add_oe_to_df(df_data_part_2a, df_info_part_2a, lst_addin_OE_value, dict_qre_OE_info, ['Q2_FC_Com_Like_OE', 'Q2_FC_Com_DisLike_OE'])
            df_data_part_2b, df_info_part_2b = self.add_oe_to_df(df_data_part_2b, df_info_part_2b, lst_addin_OE_value, dict_qre_OE_info, ['Q2_FC_Pack_Like_OE', 'Q2_FC_Pack_DisLike_OE'])
            df_data_part_3, df_info_part_3 = self.add_oe_to_df(df_data_part_3, df_info_part_3, lst_addin_OE_value, dict_qre_OE_info, ['Q3_MOI_Com_Like_OE', 'Q3_MOI_Com_DisLike_OE'])

            df_info_part_1.reset_index(drop=True, inplace=True)
            df_info_part_2a.reset_index(drop=True, inplace=True)
            df_info_part_2b.reset_index(drop=True, inplace=True)
            df_info_part_3.reset_index(drop=True, inplace=True)

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
                df_info.loc[df_info['var_name'].str.contains(key), 'val_lbl'] = [val]
            # END ADD MA NET CODE to df_info----------------------------------------------------------------------------

        # REMEMBER RESET INDEX BEFORE RUN TABLES
        df_data.reset_index(drop=True, inplace=True)
        df_info.reset_index(drop=True, inplace=True)

        # Export data tables--------------------------------------------------------------------------------------------
        if tables_format_file.filename:

            df_data_tbl = pd.concat([df_data_part_1, df_data_part_2a, df_data_part_2b, df_data_part_3], axis=0)
            df_info_tbl = pd.concat([df_info_part_1, df_info_part_2a, df_info_part_2b, df_info_part_3], axis=0)

            df_data_tbl.sort_values(by=[id_col], inplace=True)
            df_data_tbl.reset_index(drop=True, inplace=True)

            df_info_tbl.drop_duplicates(subset=['var_name'], keep='first', inplace=True)
            df_info_tbl.reset_index(drop=True, inplace=True)

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

        # Remove net_code to export sav---------------------------------------------------------------------------------
        df_info_part_1 = self.remove_net_code(df_info_part_1)
        df_info_part_2a = self.remove_net_code(df_info_part_2a)
        df_info_part_2b = self.remove_net_code(df_info_part_2b)
        df_info_part_3 = self.remove_net_code(df_info_part_3)
        # END Remove net_code to export sav-----------------------------------------------------------------------------

        dict_dfs = {
            1: {
                'data': df_data_unstack,
                'info': df_info_unstack,
                'tail_name': 'Unstack',
                'sheet_name': 'Unstack',
                'is_recode_to_lbl': True,
            },
            2: {
                'data': df_data_part_1,
                'info': df_info_part_1,
                'tail_name': 'Part_1_Stack',
                'sheet_name': 'Part_1_Stack',
                'is_recode_to_lbl': True,
            },
            3: {
                'data': df_data_part_2a,
                'info': df_info_part_2a,
                'tail_name': 'Part_2a_Stack',
                'sheet_name': 'Part_2a_Stack',
                'is_recode_to_lbl': True,
            },
            4: {
                'data': df_data_part_2b,
                'info': df_info_part_2b,
                'tail_name': 'Part_2b_Stack',
                'sheet_name': 'Part_2b_Stack',
                'is_recode_to_lbl': True,
            },
            5: {
                'data': df_data_part_3,
                'info': df_info_part_3,
                'tail_name': 'Part_3_Stack',
                'sheet_name': 'Part_3_Stack',
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



    @staticmethod
    def add_oe_to_df(df_data: pd.DataFrame, df_info: pd.DataFrame, lst_coding: list, dict_codelist: dict, lst_running_qre: list) -> (pd.DataFrame, pd.DataFrame):

        dict_codelist_new = dict()
        for key, val in dict_codelist.items():
            str_key = str(key).rsplit('_', 1)[0]
            if str_key in lst_running_qre:
                dict_codelist_new[key] = val

        lst_OE_col = list(dict_codelist_new.keys())
        df_data[lst_OE_col] = pd.DataFrame([[np.nan] * len(lst_OE_col)], index=df_data.index)

        # Remember edit this
        for item in lst_coding:
            str_qre_oe = item[4].rsplit('_', 1)[0]
            if str_qre_oe in lst_running_qre:
                df_data.loc[(df_data[item[0]] == item[1]) & (df_data[item[2]] == item[3]), [item[4]]] = [item[5]]

        # ADD OE to Info--------------------------------------------------------------------------------------
        df_info = pd.concat([df_info, pd.DataFrame(columns=['var_name', 'var_lbl', 'var_type', 'val_lbl'],
                                                   data=list(dict_codelist_new.values()))], axis=0)
        # END ADD OE to Info----------------------------------------------------------------------------------

        return df_data, df_info



    def remove_net_code(self, df_info: pd.DataFrame) -> pd.DataFrame:
        # Remove net_code to export sav---------------------------------------------------------------------------------
        df_info_without_net = df_info.copy()

        for idx in df_info_without_net.index:
            val_lbl = df_info_without_net.at[idx, 'val_lbl']

            if 'net_code' in val_lbl.keys():
                df_info_without_net.at[idx, 'val_lbl'] = self.unnetted_qre_val(val_lbl)

        return df_info_without_net
        # END Remove net_code to export sav-----------------------------------------------------------------------------