from app.classes.AP_DataConverter import APDataConverter
from ..export_online_survey_data_table import DataTableGenerator
from ..convert_unstack import ConvertUnstack
from ..convert_stack import ConvertStack
import pandas as pd
import numpy as np
import time
import traceback
import datetime


class VN8387ChiliSauce(APDataConverter, DataTableGenerator, ConvertUnstack, ConvertStack):

    def convert_vn8387_chili_sauce(self, py_script_file, tables_format_file, codelist_file, coding_file):
        """DO NOT EDIT THIS FUNCTION"""

        try:
            start_time = time.time()

            if py_script_file:
                exec(py_script_file.file.read())
                exit()

            self.processing_script_vn8387_chili_sauce(tables_format_file, codelist_file, coding_file)

            self.logger.warning('DATA PROCESSING COMPLETED in %d seconds' % (time.time() - start_time))

        except Exception:
            self.logger.error(traceback.format_exc())
            str_err_log_name = self.str_file_name.replace('.xlsx', '_Errors.txt')
            with open(str_err_log_name, 'w') as err_log_txt:
                err_log_txt.writelines(traceback.format_exc())


    def processing_script_vn8387_chili_sauce(self, tables_format_file, codelist_file, coding_file):
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
        info_col_name = ['var_name', 'var_lbl', 'var_type', 'val_lbl']  # DO NOT EDIT THIS

        self.logger.info('Pre processing')

        lst_update = [
            ['714857_2395117', 'S1_Area', 2],
            ['714857_2395121', 'S1_Area', 2],
            ['714857_2395214', 'S1_Area', 2],
            ['714857_2395278', 'S1_Area', 2],
            ['714858_2395116', 'S1_Area', 2],
            ['714858_2395129', 'S1_Area', 2],
            ['714858_2395194', 'S1_Area', 2],
            ['714858_2395198', 'S1_Area', 2],
            ['714858_2395203', 'S1_Area', 2],
            ['714858_2395207', 'S1_Area', 2],
            ['714858_2395226', 'S1_Area', 2],
            ['714858_2395248', 'S1_Area', 2],
            ['714858_2395258', 'S1_Area', 2],
            ['714858_2395295', 'S1_Area', 2],
            ['714858_2395304', 'S1_Area', 2],
            ['714840_2395109', 'S1_Area', 2],
            ['714840_2395111', 'S1_Area', 2],
            ['714840_2395131', 'S1_Area', 2],
            ['714840_2395234', 'S1_Area', 2],
            ['714840_2395265', 'S1_Area', 2],
            ['714840_2395267', 'S1_Area', 2],
            ['714840_2395277', 'S1_Area', 2],
            ['714840_2395289', 'S1_Area', 2],
            ['714840_2395302', 'S1_Area', 2],
            ['714840_2395305', 'S1_Area', 2],
            ['714840_2395124', 'S1_Area', 1],
            ['714840_2395139', 'S1_Area', 1],
            ['714840_2395159', 'S1_Area', 1],
            ['714840_2395165', 'S1_Area', 1],
            ['714840_2395170', 'S1_Area', 1],
            ['714840_2395218', 'S1_Area', 1],
            ['714840_2395219', 'S1_Area', 1],
            ['714840_2395228', 'S1_Area', 1],
            ['714840_2395231', 'S1_Area', 1],
            ['714840_2395243', 'S1_Area', 1],

            ['714840_2395178', 'S1_Area', 2],
            ['714840_2395197', 'S1_Area', 2],

        ]

        for row in lst_update:
            df_data.loc[df_data['ID'] == row[0], [row[1]]] = [row[-1]]



        # --------------------------------------------------------------------------------------------------------------
        df_data_unstack, df_info_unstack = df_data.copy(), df_info.copy()
        # --------------------------------------------------------------------------------------------------------------

        dict_add_new_qres = {
            'Ma_CC_1': ['Mã Concept', 'SA', {'1': 'Concept 1', '2': 'Concept 2', '3': 'Concept 3'}, 1],
            'Ma_CC_2': ['Mã Concept', 'SA', {'1': 'Concept 1', '2': 'Concept 2', '3': 'Concept 3'}, 2],
            'Ma_CC_3': ['Mã Concept', 'SA', {'1': 'Concept 1', '2': 'Concept 2', '3': 'Concept 3'}, 3],

            'Q9_CC_1_YN': ['Q9. Force choice', 'SA', {'1': 'Yes', '2': 'No'}, 2],
            'Q9_CC_2_YN': ['Q9. Force choice', 'SA', {'1': 'Yes', '2': 'No'}, 2],
            'Q9_CC_3_YN': ['Q9. Force choice', 'SA', {'1': 'Yes', '2': 'No'}, 2],

            'Q11_CC_1_YN': ['Q11. Force choice PI', 'SA', {'1': 'Yes', '2': 'No'}, 2],
            'Q11_CC_2_YN': ['Q11. Force choice PI', 'SA', {'1': 'Yes', '2': 'No'}, 2],
            'Q11_CC_3_YN': ['Q11. Force choice PI', 'SA', {'1': 'Yes', '2': 'No'}, 2],

            'Q12_CC1': ['Q12. Nếu chọn giữa tương ớt Chinsu và ..., anh/chị sẽ chọn mua loại nào?', 'SA', {'1': 'Tương ớt trong ý tưởng CC1', '4': 'Tương ớt Chinsu'}, np.nan],
            'Q12_CC2': ['Q12. Nếu chọn giữa tương ớt Chinsu và ..., anh/chị sẽ chọn mua loại nào?', 'SA', {'2': 'Tương ớt trong ý tưởng CC2', '4': 'Tương ớt Chinsu'}, np.nan],
            'Q12_CC3': ['Q12. Nếu chọn giữa tương ớt Chinsu và ..., anh/chị sẽ chọn mua loại nào?', 'SA', {'3': 'Tương ớt trong ý tưởng CC3', '4': 'Tương ớt Chinsu'}, np.nan],
        }

        for key, val in dict_add_new_qres.items():
            df_info = pd.concat([df_info, pd.DataFrame(columns=info_col_name, data=[[key, val[0], val[1], val[2]]])], axis=0, ignore_index=True)
            df_data = pd.concat([df_data, pd.DataFrame(columns=[key], data=[val[-1]] * df_data.shape[0])], axis=1)

        for i in range(1, 4):
            df_fil = df_data.query(f"Q9 == {i}")
            df_data.loc[df_fil.index, [f'Q9_CC_{i}_YN']] = [1]
            del df_fil

            df_fil = df_data.query(f"Q11_1 == {i}")
            df_data.loc[df_fil.index, [f'Q11_CC_{i}_YN']] = [1]
            del df_fil

            df_fil = df_data.query(f"Q11_1 == {i}")
            df_data.loc[df_fil.index, f'Q12_CC{i}'] = df_data.loc[df_fil.index, 'Q12']
            del df_fil

        df_data.reset_index(drop=True, inplace=True)
        df_info.reset_index(drop=True, inplace=True)

        # # ----------------------------------
        # df_data.to_excel('zzz_df_data.xlsx')
        # df_info.to_excel('zzz_df_info.xlsx')
        # # ----------------------------------

        # Define structure----------------------------------------------------------------------------------------------
        self.logger.info('Define structure')

        id_col = 'ID'
        sp_col = 'Ma_CC'

        lst_scr = [
            'S0',
            'S1',
            'S1a',
            'S1_Area',
            'S2',
            'S3_1',
            'S3_2',
            'S3_3',
            'S3_4',
            'S3_5',
            'S3_6',
            'S3_7',
            'S3_8',
            'S4_1',
            'S4_2',
            'S4_3',
            'S4_4',
            'S4_5',
            'S4_6',
            'S4_7',
            'S4_8',
            'S4_o8',
            'S5',
            'S6',
            'S7',
            'S8_1',
            'S8_2',
            'S8_3',
            'S8_4',
            'S8_5',
            'S8_6',
            'S8_7',
            'S8_8',
            'S8_o8',
            'S9',
            'S10',
            'S10_o22',
            'S11',
            'S12',
        ]

        dict_main = {
            1: {
                'Ma_CC_1': 'Ma_CC',
                'Q2': 'Q2_R2_P2',
                'Q3': 'Q3_R3_P3',
                'Q4': 'Q4_R4_P4',
                'Q5': 'Q5_R5_P5',
                'Q6': 'Q6_R6_P6',
                'Q7': 'Q7_R7_P7',
                'Q8': 'Q8_R8_P8',
                'Q9_CC_1_YN': 'Q9_R9_P9',
                'Q11_CC_1_YN': 'Q11_R11_P11',
            },
            2: {
                'Ma_CC_2': 'Ma_CC',
                'R2': 'Q2_R2_P2',
                'R3': 'Q3_R3_P3',
                'R4': 'Q4_R4_P4',
                'R5': 'Q5_R5_P5',
                'R6': 'Q6_R6_P6',
                'R7': 'Q7_R7_P7',
                'R8': 'Q8_R8_P8',
                'Q9_CC_2_YN': 'Q9_R9_P9',
                'Q11_CC_2_YN': 'Q11_R11_P11',
            },
            3: {
                'Ma_CC_3': 'Ma_CC',
                'P2': 'Q2_R2_P2',
                'P3': 'Q3_R3_P3',
                'P4': 'Q4_R4_P4',
                'P5': 'Q5_R5_P5',
                'P6': 'Q6_R6_P6',
                'P7': 'Q7_R7_P7',
                'P8': 'Q8_R8_P8',
                'Q9_CC_3_YN': 'Q9_R9_P9',
                'Q11_CC_3_YN': 'Q11_R11_P11',
            }
        }

        lst_fc = [
            'Q9',
            'Q10_1',
            'Q10_2',
            'Q10_3',
            'Q10_4',
            'Q10_5',
            'Q10_6',
            'Q10_o6',
            'Q11_1',
            'Q11_2',
            'Q11_3',
            'Q11_a',
            'Q12',
            'Q12_CC1',
            'Q12_CC2',
            'Q12_CC3',
            'Q13',
            'Q13_PI',
            'Q14_1st',
            'Q14_2nd',
            'Q14_3rd',
        ]


        # -----------------
        dict_qre_group_mean = {
            'Q2_R2_P2': {
                'range': [],  # f'0{i}' for i in range(1, 4)
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Q5_R5_P5': {
                'range': [],  # f'0{i}' for i in range(1, 4)
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Q6_R6_P6': {
                'range': [],  # f'0{i}' for i in range(1, 4)
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Q7_R7_P7': {
                'range': [],  # f'0{i}' for i in range(1, 4)
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Q8_R8_P8': {
                'range': [],  # f'0{i}' for i in range(1, 4)
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Q13_PI': {
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
        df_data_stack, df_info_stack = self.convert_to_stack(df_data, df_info, id_col, sp_col, lst_scr, dict_main, lst_fc)
        # END CONVERT TO STACK------------------------------------------------------------------------------------------

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

            # df_data_tbl = pd.concat([df_data_tbl, df_data_stack_a4], axis=0)
            # df_info_tbl = pd.concat([df_info_tbl, df_info_stack_a4], axis=0)

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
                'is_recode_to_lbl': True,
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