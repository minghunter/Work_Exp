from app.classes.AP_DataConverter import APDataConverter
from app.routers.Online_Survey.export_online_survey_data_table import DataTableGenerator
from app.routers.Online_Survey.convert_unstack import ConvertUnstack
import pandas as pd
import numpy as np
import time
import traceback


class VN8280NoodleSTBTest(APDataConverter, DataTableGenerator, ConvertUnstack):

    def convert_vn8280_noodle_stb_test(self, py_script_file, tables_format_file, codelist_file, coding_file):
        """DO NOT EDIT THIS FUNCTION"""

        try:
            start_time = time.time()

            if py_script_file:
                exec(py_script_file.file.read())
                exit()

            self.processing_script_vn8280_noodle_stb_test(tables_format_file, codelist_file, coding_file)

            self.logger.warning('DATA PROCESSING COMPLETED in %d seconds' % (time.time() - start_time))

        except Exception:
            self.logger.error(traceback.format_exc())
            str_err_log_name = self.str_file_name.replace('.xlsx', '_Errors.txt')
            with open(str_err_log_name, 'w') as err_log_txt:
                err_log_txt.writelines(traceback.format_exc())


    def processing_script_vn8280_noodle_stb_test(self, tables_format_file, codelist_file, coding_file):
        """
        :param tables_format_file: txt uploaded table format file
        :param codelist_file: txt uploaded codelist file
        :param coding_file: txt uploaded coding file
        :return: zip file include files: .sav, .sps, .xlsx(rawdata, topline)

        EDIT FROM HERE
        """

        # Convert system rawdata to dataframes--------------------------------------------------------------------------
        df_data_output, df_info_output = self.convert_df_mc()

        # Pre processing------------------------------------------------------------------------------------------------
        self.logger.info('Pre processing')

        dict_fc_yn = {
            'Q25_1st_YN': ['Kịch bản thích nhất', 'SA', {'1': 'Yes', '2': 'No'}, 1],
            'Q25_2nd_YN': ['Kịch bản thích nhất', 'SA', {'1': 'Yes', '2': 'No'}, 2],
            'Q25_3rd_YN': ['Kịch bản thích nhất', 'SA', {'1': 'Yes', '2': 'No'}, 3],

            'Q28_1st_YN': ['Kịch bản muốn mua nhất', 'SA', {'1': 'Yes', '2': 'No'}, 1],
            'Q28_2nd_YN': ['Kịch bản muốn mua nhất', 'SA', {'1': 'Yes', '2': 'No'}, 2],
            'Q28_3rd_YN': ['Kịch bản muốn mua nhất', 'SA', {'1': 'Yes', '2': 'No'}, 3],

        }

        for key, val in dict_fc_yn.items():
            df_info_output = pd.concat([df_info_output,
                                        pd.DataFrame(columns=['var_name', 'var_lbl', 'var_type', 'val_lbl'],
                                                     data=[[key, val[0], val[1], val[2]]])], axis=0, ignore_index=True)

            df_data_output = pd.concat(
                [df_data_output, pd.DataFrame(columns=[key], data=[2] * df_data_output.shape[0])], axis=1)
            df_data_output[key] = [1 if a == val[-1] else 2 for a in df_data_output[key[:3]]]

        df_info_output.loc[df_info_output['var_name'].isin(['Q26', 'Q29']), ['var_lbl']] = df_info_output.loc[
            df_info_output['var_name'].isin(['Q25', 'Q28']), ['var_lbl']].values

        # Define structure----------------------------------------------------------------------------------------------
        self.logger.info('Define structure')

        id_col = 'ID'
        sp_col = 'Ma_Kich_Ban'

        lst_scr = [
            'S3',
            'S4',
            'S5',
            'S6_1',
            'S6_2',
            'S6_3',
            'S6_4',
            'S6_5',
            'S7',
            'S8_1',
            'S8_2',
            'S8_3',
            'S8_4',
            'S8_5',
            'S8_6',
            'S8_7',
            'S8_8',
            'S8_9',
            'S8_10',
            'S8_11',
            'S8_o11',
            'Q0a_Xoay_vong',
        ]

        lst_fc = [
            'Q25',
            'Q26',
            'Q27',
            'Q28',
            'Q29',
            'Q30',
            'B1',
            'B2a',
            'B2b',
            'Q32a',
            'Q32b_01',
            'Q32b_02',
            'Q32b_03',
            'Q32b_04',
            'Q32b_05',
        ]

        dict_sp = {
            1: {
                'Q0b': 'Ma_Kich_Ban',
                'Q1': 'Q1',
                'Q2': 'Q2',
                'Q3a': 'Q3a',
                'Q3b': 'Q3b',
                'Q4': 'Q4',
                'Q5': 'Q5',
                'Q6': 'Q6',
                'Q7a': 'Q7a',
                'Q7b': 'Q7b',
                'Q8': 'Q8',
                'Q25_1st_YN': 'Q25_YN',
                'Q28_1st_YN': 'Q28_YN',
            },
            2: {
                'P0b': 'Ma_Kich_Ban',
                'P1': 'Q1',
                'P2': 'Q2',
                'P3a': 'Q3a',
                'P3b': 'Q3b',
                'P4': 'Q4',
                'P5': 'Q5',
                'P6': 'Q6',
                'P7a': 'Q7a',
                'P7b': 'Q7b',
                'P8': 'Q8',
                'Q25_2nd_YN': 'Q25_YN',
                'Q28_2nd_YN': 'Q28_YN',
            },
            3: {
                'R0b': 'Ma_Kich_Ban',
                'R1': 'Q1',
                'R2': 'Q2',
                'R3a': 'Q3a',
                'R3b': 'Q3b',
                'R4': 'Q4',
                'R5': 'Q5',
                'R6': 'Q6',
                'R7a': 'Q7a',
                'R7b': 'Q7b',
                'R8': 'Q8',
                'Q25_3rd_YN': 'Q25_YN',
                'Q28_3rd_YN': 'Q28_YN',
            },
        }

        dict_qre_group_mean = {

            'Q2': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Q6': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'B1': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },

        }

        # dict_qre_OE_info_org = {'Main_Q3_Cai_thien_New_OE|1-3': ['Q3. Vậy có điểm nào anh/chị muốn cải thiện, sửa đổi để anh/chị thích/hài lòng hơn nữa? ', 'MA',  {'net_code': {'999': 'Không có/Không cần', '90001|SỢI PHỞ (NET)': {'101': 'Thêm nội dung về sợi phở/làm nổi bật/nhấn mạnh ý tưởng về sợi phở', '102': 'Được làm từ gạo nếp nương tạo cảm giác thiên nhiên/tự nhiên/sạch/không thuốc', '103': 'Được làm từ gạo nếp nương giúp sợi phở thanh lành (hơn)', '104': 'Được làm từ gạo nếp nương vùng tây bắc ', '105': 'Sợi phở dẻo bùi', '106': 'Sợi phở thơm', '107': 'Sợi phở ngon', '108': 'Đề cập đến độ dai', '109': 'Gạo nếp nương đặc sắc của vùng cao tạo cảm giác thoải mái', '110': 'Gạo nếp nương đặc sắc của vùng cao tạo cảm giác thanh nhẹ', '111': 'Gạo nếp nương đặc sắc của vùng cao tạo cảm giác món phở có vị đậm đà'}, '90002|NGUYÊN LIỆU (NET)': {'201': 'Đề cập chi tiết hơn về các nguyên liệu truyền thống', '202': 'Đề cập về thịt', '203': 'Đông trùng hạ thảo giúp cân bằng cảm xúc'}, '90003|NƯỚC DÙNG (NET)': {'301': 'Được nấu theo công thức của quán phở Thìn Bờ Hồ danh tiếng 70 năm làm cho vị sợi phở ngon hấp dẫn', '302': 'Được nấu từ công thức đặc biệt của quán phở trứ danh của Hà Nội tạo cảm giác hương vị ngon, sợi phở thanh lành', '303': 'Sẽ có/mang lại vị thực tế hơn', '304': 'Được nấu theo công thức quản phở Bờ Hồ lâu đời', '305': 'Thêm nội dung về nước dùng/làm nổi bật/nhấn mạnh ý tưởng về nước dùng', '306': 'Được nấu theo công thức quản phở nổi tiếng'}, '90004|SỢI PHỞ KẾT HỢP NƯỚC DÙNG (NET)': {'401': 'Tạo sự kết hợp ấn tượng giữa sợi phở và nước dùng'}, '90005|KHÁC (NET)': {'601': 'Bổ sung thêm lợi ích/chất dinh dưỡng mang lại cho cơ thể', '610': 'Bổ sung thêm công thức nước phở Thìn đã có ngay trong gói phở ăn liền giúp tiết kiệm thời gian hơn khi ăn ngoài hàng'}, '90006|ĐIỀU CHỈNH NỘI DUNG (NET)': {'602': 'Kết hợp độc đáo đổi thành kết hợp hài hòa sẽ hay hơn', '603': 'Gạo nếp nương của vùng cao đổi thành gạo nếp nương đặc sản của vùng cao', '604': 'Giúp cân bằng cảm xúc ngay cả lúc mệt mỏi, cho cơ thể nhẹ nhàng và tạo nên một lối sống lành mạnh, thư thái đổi thành sản phẩm mang đến sức khỏe tốt cho cơ thể, giúp cơ thể hồi phục sức nhanh'}}}],}
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

        # Data stack format---------------------------------------------------------------------------------------------
        self.logger.info('Data stack format')

        # df_data_stack generate
        df_data_scr = df_data_output.loc[:, [id_col] + lst_scr].copy()
        df_data_fc = df_data_output.loc[:, [id_col] + lst_fc].copy()

        lst_df_data_sp = [df_data_output.loc[:, [id_col] + list(val.keys())].copy() for val in dict_sp.values()]

        for i, df in enumerate(lst_df_data_sp):
            df.rename(columns=dict_sp[i + 1], inplace=True)

        df_data_stack = pd.concat(lst_df_data_sp, axis=0, ignore_index=True)

        df_data_stack = df_data_scr.merge(df_data_stack, how='left', on=[id_col])
        df_data_stack.reset_index(drop=True, inplace=True)

        df_data_stack = df_data_stack.merge(df_data_fc, how='left', on=[id_col])

        df_data_stack.sort_values(by=[id_col, sp_col], inplace=True)
        df_data_stack.reset_index(drop=True, inplace=True)

        df_info_stack = df_info_output.copy()

        for key, val in dict_sp[1].items():
            df_info_stack.loc[df_info_stack['var_name'] == key, ['var_name']] = [val]

        df_info_stack.drop_duplicates(subset=['var_name'], keep='first', inplace=True)

        df_info_stack.loc[df_info_stack['var_name'] == 'Ma_Kich_Ban', ['var_lbl', 'val_lbl']] = ["Mã kịch bản", {'1': 'Kich ban A', '2': 'Kich ban B', '3': 'Kich ban C'}]

        if dict_qre_OE_info:

            # ADD OE to Data stack--------------------------------------------------------------------------------------
            lst_OE_col = list(dict_qre_OE_info.keys())
            df_data_stack[lst_OE_col] = pd.DataFrame([[np.nan] * len(lst_OE_col)], index=df_data_stack.index)

            # Remember edit this
            for item in lst_addin_OE_value:
                df_data_stack.loc[
                    (df_data_stack[item[0]] == item[1]) & (df_data_stack[item[2]] == item[3]), [item[4]]] = [item[5]]

            # END ADD OE to Data stack----------------------------------------------------------------------------------

            # ADD OE to Info stack--------------------------------------------------------------------------------------
            df_info_stack = pd.concat([df_info_stack,
                                       pd.DataFrame(columns=['var_name', 'var_lbl', 'var_type', 'val_lbl'],
                                                    data=list(dict_qre_OE_info.values()))], axis=0)
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


        # Reset df_info_stack index
        df_info_stack['idx_var_name'] = df_info_stack['var_name']
        df_info_stack.set_index('idx_var_name', inplace=True)
        df_info_stack = df_info_stack.loc[list(df_data_stack.columns), :]
        df_info_stack.reindex(list(df_data_stack.columns))
        df_info_stack.reset_index(drop=True, inplace=True)

        # df_data_stack.to_excel('zzzz_df_data_stack.csv')
        # df_info_stack.to_excel('zzzz_df_info_stack.csv')
        # End Data stack format-----------------------------------------------------------------------------------------

        # Data unstack format-------------------------------------------------------------------------------------------
        self.logger.info('Data unstack format')

        lst_col_part_body = list(dict_sp[1].values())
        lst_col_part_body.remove(sp_col)

        dict_stack_structure = {
            'id_col': id_col,
            'sp_col': sp_col,
            'lst_col_part_head': lst_scr,
            'lst_col_part_body': lst_col_part_body,
            'lst_col_part_tail': lst_fc,
        }

        df_data_unstack, df_info_unstack = self.convert_to_unstack(df_data_stack, df_info_stack, dict_stack_structure)

        # df_data_unstack.to_csv('zzzz_df_data_unstack.csv', encoding='utf-8-sig')
        # df_info_unstack.to_csv('zzzz_df_info_unstack.csv', encoding='utf-8-sig')
        # End Data unstack format---------------------------------------------------------------------------------------

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

        self.generate_sav_sps(df_data=df_data_stack, df_qres_info=df_info_stack_without_net,
                              df_data_2=df_data_unstack, df_qres_info_2=df_info_unstack,
                              is_md=False, is_export_xlsx=True)