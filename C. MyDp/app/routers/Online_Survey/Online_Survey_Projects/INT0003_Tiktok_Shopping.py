from app.classes.AP_DataConverter import APDataConverter
from ..export_online_survey_data_table import DataTableGenerator
from ..convert_unstack import ConvertUnstack
from ..convert_stack import ConvertStack
import pandas as pd
import numpy as np
import time
import traceback
import datetime


class INT0003TiktokShopping(APDataConverter, DataTableGenerator, ConvertUnstack, ConvertStack):

    def convert_int0003_tiktok_shopping(self, py_script_file, tables_format_file, codelist_file, coding_file):
        """DO NOT EDIT THIS FUNCTION"""

        try:
            start_time = time.time()

            if py_script_file:
                exec(py_script_file.file.read())
                exit()

            self.processing_script_int0003_tiktok_shopping(tables_format_file, codelist_file, coding_file)

            self.logger.warning('DATA PROCESSING COMPLETED in %d seconds' % (time.time() - start_time))

        except Exception:
            self.logger.error(traceback.format_exc())
            str_err_log_name = self.str_file_name.replace('.xlsx', '_Errors.txt')
            with open(str_err_log_name, 'w') as err_log_txt:
                err_log_txt.writelines(traceback.format_exc())


    def processing_script_int0003_tiktok_shopping(self, tables_format_file, codelist_file, coding_file):
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

        info_col_name = ['var_name', 'var_lbl', 'var_type', 'val_lbl']

        # Pre processing------------------------------------------------------------------------------------------------

        # --------------------------------------------------------------------------------------------------------------
        df_data_unstack, df_info_unstack = df_data.copy(), df_info.copy()
        # --------------------------------------------------------------------------------------------------------------

        # Define structure----------------------------------------------------------------------------------------------
        self.logger.info('Define structure')

        id_col = 'ID'

        # -----------------
        dict_qre_group_mean = {
            # 'Q1': {
            #     'range': [],  # f'0{i}' for i in range(1, 4)
            #     'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
            #     'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            # },
            # 'Q4': {
            #     'range': [],
            #     'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
            #     'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            # },
            # 'Q5': {
            #     'range': [],
            #     'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
            #     'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            # },
            # 'Q9': {
            #     'range': [],
            #     'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
            #     'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            # },
            # 'Q6': {
            #     'range': [],
            #     'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
            #     'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            # },
            # 'Q10': {
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
            # "S8": {
            #     'net_code': {
            #         '1': 'Bia 333',
            #         '900001|net|TIGER (NET)': {
            #             '2': 'Tiger nâu/ Tiger thường',
            #             '3': 'Tiger Crystal/ Tiger bạc',
            #             '4': 'Tiger Soju',
            #             '5': 'Tiger Platinum',
            #         },
            #         '900002|net|SAI GON (NET)': {
            #             '6': 'Sài Gòn Special/ Sai Gòn lùn',
            #             '7': 'Sài Gòn Xanh/ Sài Gòn Lager',
            #             '8': 'Sài Gòn Export/ Sài Gòn Đỏ',
            #             '9': 'Sài Gòn Chill',
            #         },
            #         '900003|net|HEINEKEN (NET)': {
            #             '10': 'Heineken thường',
            #             '11': 'Heineken Silver/ Heineken bạc',
            #         },
            #         '900004|net|LARUE (NET)': {
            #             '12': 'Larue thường',
            #             '13': 'Larue Special/ Larue xanh',
            #             '14': 'Larue Smooth',
            #         },
            #         '15': 'Bia Việt',
            #         '16': 'Budweiser',
            #         '17': 'Nhãn hiệu khác',
            #     }
            # },

        }

        lst_clear_ma_oe_value = {
            # "Main_Q2c_SA_Impressive_Concept_A": [14],
            # "Main_Q2c_SA_Impressive_Concept_B": [13],
            # "Main_Q2c_SA_Impressive_Concept_C": [14],
            # "Main_Q2c_SA_Impressive_Concept_D": [11],
        }

        lst_addin_MA_value = [
            # ['Q0a_RespondentID', 1004, 'Ma_SP', 1, 'Main_Q2c_SA_Impressive_Concept_A', 12],
            # ['Q0a_RespondentID', 1009, 'Ma_SP', 1, 'Main_Q2c_SA_Impressive_Concept_A', 11],
            # ['Q0a_RespondentID', 1009, 'Ma_SP', 1, 'Main_Q2c_SA_Impressive_Concept_A', 15],
        ]



        # End Define structure------------------------------------------------------------------------------------------


        # # CONVERT TO STACK----------------------------------------------------------------------------------------------
        # df_data_stack, df_info_stack = self.convert_to_stack(df_data, df_info, id_col, sp_col, lst_scr, dict_main, lst_fc)
        # # CONVERT TO STACK----------------------------------------------------------------------------------------------

        # # OE RUNNING
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
        #     # END ADD OE to Data stack----------------------------------------------------------------------------------
        #
        #     # ADD OE to Info stack--------------------------------------------------------------------------------------
        #     df_info_stack = pd.concat([df_info_stack, pd.DataFrame(columns=['var_name', 'var_lbl', 'var_type', 'val_lbl'], data=list(dict_qre_OE_info.values()))], axis=0)
        #     # END ADD OE to Info stack----------------------------------------------------------------------------------
        #
        # if lst_addin_MA_value:
        #     # ADD MA OE to Data stack-----------------------------------------------------------------------------------
        #     # Remember edit this
        #     for item in lst_addin_MA_value:
        #
        #         # ['Q0a_RespondentID', 1004, 'Ma_SP', 1, 'Main_Q2c_SA_Impressive_Concept_A', 12],
        #         df_fil_info = df_info_stack.query(f"var_name.str.contains('{item[-2]}_[0-9]+')")
        #         df_fil_data = df_data_stack.query(f"{item[0]} == {item[1]} & {item[2]} == {item[3]}")
        #
        #         for idx_row in df_fil_data.index:
        #             for col_name in df_fil_info['var_name'].values.tolist():
        #                 if pd.isnull(df_fil_data.at[idx_row, col_name]):
        #                     df_data_stack.loc[idx_row, col_name] = item[-1]
        #                     break
        #                 else:
        #                     lst_clear_val = lst_clear_ma_oe_value[item[-2]]
        #                     aaa = df_fil_data.at[idx_row, col_name]
        #
        #                     if df_fil_data.at[idx_row, col_name] in lst_clear_val:
        #                         df_data_stack.at[idx_row, col_name] = np.nan
        #
        #
        #
        #     # END ADD MA OE to Data stack-------------------------------------------------------------------------------
        #
        # if dict_qre_net_info:
        #     # ADD MA NET CODE to df_info--------------------------------------------------------------------------------
        #     for key, val in dict_qre_net_info.items():
        #         df_info_stack.loc[df_info_stack['var_name'].str.contains(key), 'val_lbl'] = [val]
        #     # END ADD MA NET CODE to df_info----------------------------------------------------------------------------
        #
        # # REMEMBER RESET INDEX BEFORE RUN TABLES
        # df_data_stack.reset_index(drop=True, inplace=True)
        # df_info_stack.reset_index(drop=True, inplace=True)

        # Export data tables--------------------------------------------------------------------------------------------
        if tables_format_file.filename:

            df_data_tbl, df_info_tbl = df_data_unstack.copy(), df_info_unstack.copy()

            df_data_tbl = pd.concat([df_data_tbl], axis=0)
            df_info_tbl = pd.concat([df_info_tbl], axis=0)

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

        # df_info_stack = self.remove_net_code(df_info_stack)

        dict_dfs = {
            1: {
                'data': df_data_unstack,
                'info': df_info_unstack,
                'tail_name': '_',
                'sheet_name': '_',
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