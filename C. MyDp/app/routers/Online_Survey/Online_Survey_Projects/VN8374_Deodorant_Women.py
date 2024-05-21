from app.classes.AP_DataConverter import APDataConverter
from ..export_online_survey_data_table import DataTableGenerator
from ..calculate_lsm import LSMCalculation
from ..convert_unstack import ConvertUnstack
import pandas as pd
import numpy as np
import time
import traceback



class VN8374DeodorantWomen(APDataConverter, DataTableGenerator, ConvertUnstack, LSMCalculation):

    def convert_vn8374_deodorant_women(self, py_script_file, tables_format_file, codelist_file, coding_file):
        """DO NOT EDIT THIS FUNCTION"""

        try:
            start_time = time.time()

            if py_script_file:
                exec(py_script_file.file.read())
                exit()

            self.processing_script_vn8374_deodorant_women(tables_format_file, codelist_file, coding_file)

            self.logger.warning('DATA PROCESSING COMPLETED in %d seconds' % (time.time() - start_time))

        except Exception:
            self.logger.error(traceback.format_exc())
            str_err_log_name = self.str_file_name.replace('.xlsx', '_Errors.txt')
            with open(str_err_log_name, 'w') as err_log_txt:
                err_log_txt.writelines(traceback.format_exc())


    def processing_script_vn8374_deodorant_women(self, tables_format_file, codelist_file, coding_file):
        """
        :param tables_format_file: txt uploaded table format file
        :param codelist_file: txt uploaded codelist file
        :param coding_file: txt uploaded coding file
        :return: zip file include files: .sav, .sps, .xlsx(rawdata, topline)

        EDIT FROM HERE
        """

        # Convert system rawdata to dataframes--------------------------------------------------------------------------
        df_data, df_info = self.convert_df_mc()

        df_data, df_info = self.cal_lsm_6(df_data, df_info)

        # Pre processing------------------------------------------------------------------------------------------------
        self.logger.info('Pre processing')

        df_fil = df_data.query("FirstFormat > 0 & ProductInUse_05 != 2").copy()
        df_data.loc[df_fil.index, ['FirstFormat']] = [np.nan]

        for i in range(1, 7):
            df_fil = df_data.query(f"CurrentFormat_{i} > 0 & ProductInUse_05 != 2").copy()
            df_data.loc[df_fil.index, [f'CurrentFormat_{i}']] = [np.nan]

        # NEW---------------------------------------------------------
        info_col_name = ['var_name', 'var_lbl', 'var_type', 'val_lbl']

        dict_add_new_qres = {
            'IssueRecode_1': ['IssueRecode', 'MA', {'1': 'Chỉ chọn 1 hoặc 4', '2': 'Chỉ chọn 2 hoặc 3', '3': 'Chọn (1 hoặc 4) và (2 hoặc 3)'}, np.nan],
            'IssueRecode_2': ['IssueRecode', 'MA', {'1': 'Chỉ chọn 1 hoặc 4', '2': 'Chỉ chọn 2 hoặc 3', '3': 'Chọn (1 hoặc 4) và (2 hoặc 3)'}, np.nan],
            'IssueRecode_3': ['IssueRecode', 'MA', {'1': 'Chỉ chọn 1 hoặc 4', '2': 'Chỉ chọn 2 hoặc 3', '3': 'Chọn (1 hoặc 4) và (2 hoặc 3)'}, np.nan],

            'Expectation3_Recode_1': ['Expectation3_Recode', 'MA', {
                '1': 'Chỉ chọn code 2 hoặc 3 hoặc 5 (có thể cùng lúc chọn 1, 2 hoặc 3 code trong 3 code này)',
                '2': 'Chọn code (1 hoặc 4) và (2 hoặc 3 hoặc 5)'
            }, np.nan],
            'Expectation3_Recode_2': ['Expectation3_Recode', 'MA', {
                '1': 'Chỉ chọn code 2 hoặc 3 hoặc 5 (có thể cùng lúc chọn 1, 2 hoặc 3 code trong 3 code này)',
                '2': 'Chọn code (1 hoặc 4) và (2 hoặc 3 hoặc 5)'
            }, np.nan],

            'ImportantFactor_ExpectFactor_1': ['ImportantFactor/ExpectFactor', 'MA', {'1': 'Beauty', '2': 'Efficacy', '3': 'Other'}, np.nan],
            'ImportantFactor_ExpectFactor_2': ['ImportantFactor/ExpectFactor', 'MA', {'1': 'Beauty', '2': 'Efficacy', '3': 'Other'}, np.nan],
            'ImportantFactor_ExpectFactor_3': ['ImportantFactor/ExpectFactor', 'MA', {'1': 'Beauty', '2': 'Efficacy', '3': 'Other'}, np.nan],

            'ImportantFactor_ExpectFactor_Recode': ['ImportantFactor/ExpectFactor Recode', 'MA', {'1': 'Nhóm Beauty = Không chọn Efficacy', '2': 'Nhóm Efficacy = Không chọn Beauty', '3': 'Nhóm Mix = Chọn cả Beauty + Efficacy'}, np.nan],
        }



        for key, val in dict_add_new_qres.items():
            df_info = pd.concat([df_info, pd.DataFrame(columns=info_col_name, data=[[key, val[0], val[1], val[2]]])], axis=0, ignore_index=True)
            df_data = pd.concat([df_data, pd.DataFrame(columns=[key], data=[val[-1]] * df_data.shape[0])], axis=1)

        for idx in df_data.index:
            # ----------------------------------------------------------------------------------------------------------
            lst_issue_col = [
                'Issue_1',
                'Issue_2',
                'Issue_3',
                'Issue_4',
                'Issue_5',
                'Issue_6',
                'Issue_7',
            ]

            set_value = set(df_data.loc[idx, lst_issue_col].values.tolist())
            set_value = {x for x in set_value if x == x}

            lst_value_recode = list()
            if len(set_value.difference({1, 4})) == 0:
                lst_value_recode.append(1)

            if len(set_value.difference({2, 3})) == 0:
                lst_value_recode.append(2)

            if (set_value.issuperset({1}) or set_value.issuperset({4})) and (set_value.issuperset({2}) or set_value.issuperset({3})):
                lst_value_recode.append(3)

            if len(lst_value_recode):
                df_data.loc[idx, [f"IssueRecode_{i + 1}" for i in range(len(lst_value_recode))]] = lst_value_recode


        for idx in df_data.index:
            # ----------------------------------------------------------------------------------------------------------
            lst_expectation3_col = [
                'Expectation3_1',
                'Expectation3_2',
                'Expectation3_3',
                'Expectation3_4',
                'Expectation3_5',
                'Expectation3_6',
            ]

            set_value = set(df_data.loc[idx, lst_expectation3_col].values.tolist())
            set_value = {x for x in set_value if x == x}

            lst_value_recode = list()

            # '1': 'Chỉ chọn code 2 hoặc 3 hoặc 5 (có thể cùng lúc chọn 1, 2 hoặc 3 code trong 3 code này)',
            if len(set_value.difference({2, 3, 5})) == 0:
                lst_value_recode.append(1)

            # '2': 'Chọn code (1 hoặc 4) và (2 hoặc 3 hoặc 5)'
            if len(set_value.intersection({1, 4})) > 0 and len(set_value.intersection({2, 3, 5})) > 0:
                lst_value_recode.append(2)

            if len(lst_value_recode):
                df_data.loc[idx, [f"Expectation3_Recode_{i + 1}" for i in range(len(lst_value_recode))]] = lst_value_recode


        for idx in df_data.index:
            # ----------------------------------------------------------------------------------------------------------
            lst_factor_col = [
                'ImportantFactor_1',
                'ImportantFactor_2',
                'ImportantFactor_3',
                'ImportantFactor_4',
                'ImportantFactor_5',
                'ImportantFactor_6',
                'ImportantFactor_7',
                'ImportantFactor_8',
                'ImportantFactor_9',
                'ImportantFactor_10',
                'ImportantFactor_11',
                'ImportantFactor_12',
                'ImportantFactor_13',
                'ImportantFactor_14',
                'ImportantFactor_15',
                'ExpectFactor_1',
                'ExpectFactor_2',
                'ExpectFactor_3',
                'ExpectFactor_4',
                'ExpectFactor_5',
                'ExpectFactor_6',
                'ExpectFactor_7',
                'ExpectFactor_8',
                'ExpectFactor_9',
                'ExpectFactor_10',
                'ExpectFactor_11',
                'ExpectFactor_12',
                'ExpectFactor_13',
                'ExpectFactor_14',
                'ExpectFactor_15',
            ]

            set_value = set(df_data.loc[idx, lst_factor_col].values.tolist())
            set_value = {x for x in set_value if x == x}

            lst_value_recode = list()

            # Beauty = 8, 12, 14
            if len(set_value.intersection({8, 12, 14})) > 0:
                lst_value_recode.append(1)

            # Efficacy = 1, 6, 7
            if len(set_value.intersection({1, 6, 7})) > 0:
                lst_value_recode.append(2)

            # Other = còn lại
            if len(set_value.intersection({2, 3, 4, 5, 9, 10, 11, 13})) > 0:
                lst_value_recode.append(3)

            if len(lst_value_recode):
                df_data.loc[idx, [f"ImportantFactor_ExpectFactor_{i + 1}" for i in range(len(lst_value_recode))]] = lst_value_recode

            if 2 not in lst_value_recode:
                df_data.loc[idx, ['ImportantFactor_ExpectFactor_Recode']] = 1
            elif 1 not in lst_value_recode:
                df_data.loc[idx, ['ImportantFactor_ExpectFactor_Recode']] = 2
            elif 1 in lst_value_recode and 2 in lst_value_recode:
                df_data.loc[idx, ['ImportantFactor_ExpectFactor_Recode']] = 3





        df_data.reset_index(drop=True, inplace=True)
        df_info.reset_index(drop=True, inplace=True)

        # Define structure----------------------------------------------------------------------------------------------
        self.logger.info('Define structure')

        dict_qre_group_mean = {
            # 'Promotionevaluate': {
            #     'range': [f'0{i}' for i in range(1, 4)],
            #     'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
            #     'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            # },
            # 'Evaluate1': {
            #     'range': [f'0{i}' for i in range(1, 6)],
            #     'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
            #     'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            # },
            # 'Evaluate2': {
            #     'range': [f'0{i}' for i in range(1, 6)],
            #     'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
            #     'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            # },
            # 'Evaluate3': {
            #     'range': [f'0{i}' for i in range(1, 6)],
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
            df_info = pd.concat([df_info, pd.DataFrame(columns=['var_name', 'var_lbl', 'var_type', 'val_lbl'], data=list(dict_qre_OE_info.values()))], axis=0)
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
            # 1: {
            #     'data': df_data,
            #     'info': df_info_without_net,
            #     'tail_name': 'code',
            #     'sheet_name': 'code',
            #     'is_recode_to_lbl': False,
            # },
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