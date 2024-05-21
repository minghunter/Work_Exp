from app.classes.AP_DataConverter import APDataConverter
from ..export_online_survey_data_table import DataTableGenerator
from ..calculate_lsm import LSMCalculation
from ..convert_unstack import ConvertUnstack
from ..convert_stack import ConvertStack
import pandas as pd
import numpy as np
import time
import traceback



class VN8384Battery(APDataConverter, DataTableGenerator, ConvertUnstack, LSMCalculation, ConvertStack):

    def convert_vn8384_battery(self, py_script_file, tables_format_file, codelist_file, coding_file):
        """DO NOT EDIT THIS FUNCTION"""

        try:
            start_time = time.time()

            if py_script_file:
                exec(py_script_file.file.read())
                exit()

            self.processing_script_vn8384_battery(tables_format_file, codelist_file, coding_file)

            self.logger.warning('DATA PROCESSING COMPLETED in %d seconds' % (time.time() - start_time))

        except Exception:
            self.logger.error(traceback.format_exc())
            str_err_log_name = self.str_file_name.replace('.xlsx', '_Errors.txt')
            with open(str_err_log_name, 'w') as err_log_txt:
                err_log_txt.writelines(traceback.format_exc())


    def processing_script_vn8384_battery(self, tables_format_file, codelist_file, coding_file):
        """
        :param tables_format_file: txt uploaded table format file
        :param codelist_file: txt uploaded codelist file
        :param coding_file: txt uploaded coding file
        :return: zip file include files: .sav, .sps, .xlsx(rawdata, topline)

        EDIT FROM HERE
        """

        # Convert system rawdata to dataframes--------------------------------------------------------------------------
        df_data, df_info = self.convert_df_mc()

        # df_data, df_info = self.cal_lsm_6(df_data, df_info)

        info_col_name = ['var_name', 'var_lbl', 'var_type', 'val_lbl']

        # Pre processing------------------------------------------------------------------------------------------------
        self.logger.info('Pre processing')

        # df_data_stack, df_info_stack = df_data.copy(), df_info.copy()
        #
        # dict_add_new_qres = {
        #     'Ma_Concept_1': ['Concept', 'SA', {'1': 'Thưởng Tết', '2': 'Tết Không sao'}, 1],
        #     'Ma_Concept_2': ['Concept', 'SA', {'1': 'Thưởng Tết', '2': 'Tết Không sao'}, 2],
        # }
        #
        # for key, val in dict_add_new_qres.items():
        #     df_info_stack = pd.concat([df_info_stack, pd.DataFrame(columns=info_col_name, data=[[key, val[0], val[1], val[2]]])], axis=0, ignore_index=True)
        #     df_data_stack = pd.concat([df_data_stack, pd.DataFrame(columns=[key], data=[val[-1]] * df_data_stack.shape[0])], axis=1)
        #
        # df_data_stack.reset_index(drop=True, inplace=True)
        # df_info_stack.reset_index(drop=True, inplace=True)

        # Define structure----------------------------------------------------------------------------------------------
        self.logger.info('Define structure')

        # id_col = 'ID'
        # sp_col = 'Ma_Concept'
        #
        # lst_scr = [
        #     'City',
        #     'Gender',
        #     'Age',
        #     'Marriage',
        #     'BannedIndustry',
        #     'Prohibited',
        #     'CC1',
        #     'CC2_1',
        #     'CC2_2',
        #     'CC2_3',
        #     'CC2_4',
        #     'CC2_5',
        #     'CC2_6',
        #     'CC2_7',
        #     'CC2_8',
        #     'CC2_9',
        #     'CC2_10',
        #     'CC3_1',
        #     'CC3_2',
        #     'CC3_3',
        #     'CC3_4',
        #     'CC3_5',
        #     'CC3_6',
        #     'CC3_7',
        #     'CC4_1',
        #     'CC4_2',
        #     'CC4_3',
        #     'CC4_4',
        #     'CC4_5',
        #     'CC4_6',
        #     'CC4_7',
        #     'CC4_8',
        #     'CC4_9',
        #     'CC4_10',
        #     'CC4_11',
        #     'CC4_12',
        #     'CC4_13',
        #     'CC4_14',
        #     'CC4_15',
        #     'CC4_16',
        #     'CC4_17',
        #     'CC4_18',
        #     'CC4_19',
        #     'CC4_20',
        #     'CC4_21',
        #     'CC4_22',
        #     'CC4_23',
        #     'CC4_24',
        #     'CC4_25',
        #     'CC5_1',
        #     'CC5_2',
        #     'CC5_3',
        #     'CC5_4',
        #     'CC5_5',
        #     'CC5_6',
        #     'CC5_7',
        #     'CC5_8',
        #     'CC5_9',
        #     'CC5_10',
        #     'CC5_11',
        #     'CC5_12',
        #     'CC5_13',
        #     'CC5_14',
        #     'CC5_15',
        #     'CC5_16',
        #     'CC5_17',
        #     'CC5_18',
        #     'CC6_1',
        #     'CC6_2',
        #     'CC6_3',
        #     'CC6_4',
        #     'CC6_5',
        #     'CC6_6',
        #     'CC6_7',
        #     'CC6_8',
        #     'CC6_9',
        #     'CC6_10',
        #     'CC7_1',
        #     'CC7_2',
        #     'CC7_3',
        #     'CC8',
        #     'CC9',
        #     'CC10_1',
        #     'CC10_2',
        #     'CC10_3',
        #     'CC10_4',
        #     'CC10_5',
        #     'CC10_6',
        #     'CC10_7',
        #     'CC10_8',
        #     'CC10_9',
        #     'CC10_10',
        #     'CC10_11',
        #     'CC10_12',
        #     'CC10_13',
        #     'CC10_14',
        #     'CC1_Score',
        #     'CC2_Score',
        #     'CC3_Score',
        #     'CC4_Score',
        #     'CC5_Score',
        #     'CC6_Score',
        #     'LSM_Score',
        #     'LSM',
        # ]
        #
        # dict_main = {
        #     1: {
        #         'Ma_Concept_1': 'Ma_Concept',
        #         'C1_Q4': 'C1_Q4_x_C2_Q3',
        #         'C1_Q5_1': 'C1_Q5_x_C2_Q4_1',
        #         'C1_Q5_2': 'C1_Q5_x_C2_Q4_2',
        #         'C1_Q5_3': 'C1_Q5_x_C2_Q4_3',
        #         'C1_Q5_4': 'C1_Q5_x_C2_Q4_4',
        #         'C1_Q5_5': 'C1_Q5_x_C2_Q4_5',
        #         'C1_Q6': 'C1_Q6_x_C2_Q5',
        #     },
        #     2: {
        #         'Ma_Concept_2': 'Ma_Concept',
        #         'C2_Q3': 'C1_Q4_x_C2_Q3',
        #         'C2_Q4_1': 'C1_Q5_x_C2_Q4_1',
        #         'C2_Q4_2': 'C1_Q5_x_C2_Q4_2',
        #         'C2_Q4_3': 'C1_Q5_x_C2_Q4_3',
        #         'C2_Q4_4': 'C1_Q5_x_C2_Q4_4',
        #         'C2_Q4_5': 'C1_Q5_x_C2_Q4_5',
        #         'C2_Q5': 'C1_Q6_x_C2_Q5',
        #     },
        # }
        #
        # lst_fc = []


        # # CONVERT TO STACK----------------------------------------------------------------------------------------------
        # df_data_stack, df_info_stack = self.convert_to_stack(df_data_stack, df_info_stack, id_col, sp_col, lst_scr, dict_main, lst_fc)
        #
        # df_info_stack.loc[df_info_stack['var_name'] == 'C1_Q4_x_C2_Q3', ['var_lbl']] = ['On a scale of 1-5, how sympathetic do you feel with the story ...']
        # df_info_stack.loc[df_info_stack['var_name'].str.contains('C1_Q5_x_C2_Q4_[0-9]'), ['var_lbl']] = ['After listening to the story of ..., which brand below do you feel is suitable with this story?']
        # df_info_stack.loc[df_info_stack['var_name'] == 'C1_Q6_x_C2_Q5', ['var_lbl']] = ['On a scale of 1-5, please rate how much you like the story ...']
        #
        # df_data_stack.reset_index(drop=True, inplace=True)
        # df_info_stack.reset_index(drop=True, inplace=True)
        # # CONVERT TO STACK----------------------------------------------------------------------------------------------


        dict_qre_group_mean = {
            # 'Promotionevaluate': {
            #     'range': [f'0{i}' for i in range(1, 4)],
            #     'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
            #     'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            # },
            # 'C1_Q4': {
            #     'range': [],
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
            df_info = pd.concat([df_info, pd.DataFrame(columns=info_col_name, data=list(dict_qre_OE_info.values()))], axis=0)
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
                df_info.loc[df_info['var_name'].str.contains(key), 'val_lbl'] = [val]
            # END ADD MA NET CODE to df_info----------------------------------------------------------------------------

        # REMEMBER RESET INDEX BEFORE RUN TABLES
        df_data.reset_index(drop=True, inplace=True)
        df_info.reset_index(drop=True, inplace=True)


        # Export data tables--------------------------------------------------------------------------------------------
        if tables_format_file.filename:

            df_data_tbl, df_info_tbl = df_data.copy(), df_info.copy()

            # df_data_tbl = pd.concat([df_data_tbl, df_data_stack], axis=0)
            # df_info_tbl = pd.concat([df_info_tbl, df_info_stack], axis=0)
            #
            # df_data_tbl.sort_values(by=[id_col], inplace=True)
            # df_data_tbl.reset_index(drop=True, inplace=True)
            #
            # df_info_tbl.drop_duplicates(subset=['var_name'], keep='first', inplace=True)
            # df_info_tbl.reset_index(drop=True, inplace=True)


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

        # Remove net_code to export sav---------------------------------------------------------------------------------
        df_info = self.remove_net_code(df_info)
        # END Remove net_code to export sav-----------------------------------------------------------------------------

        dict_dfs = {
            1: {
                'data': df_data,
                'info': df_info,
                'tail_name': '',
                'sheet_name': '',
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