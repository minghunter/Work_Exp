from app.classes.AP_DataConverter import APDataConverter
from app.routers.Online_Survey.export_online_survey_data_table import DataTableGenerator
from app.routers.Online_Survey.convert_unstack import ConvertUnstack
import pandas as pd
import numpy as np
import time
import traceback


class VN8314MKTActivitiesMiR2(APDataConverter, DataTableGenerator, ConvertUnstack):

    def convert_vn8314_mkt_activities_mi_r2(self, py_script_file, tables_format_file, codelist_file, coding_file):
        """DO NOT EDIT THIS FUNCTION"""

        try:
            start_time = time.time()

            if py_script_file:
                exec(py_script_file.file.read())
                exit()

            self.processing_script_vn8314_mkt_activities_mi_r2(tables_format_file, codelist_file, coding_file)

            self.logger.warning('DATA PROCESSING COMPLETED in %d seconds' % (time.time() - start_time))

        except Exception:
            self.logger.error(traceback.format_exc())
            str_err_log_name = self.str_file_name.replace('.xlsx', '_Errors.txt')
            with open(str_err_log_name, 'w') as err_log_txt:
                err_log_txt.writelines(traceback.format_exc())



    def processing_script_vn8314_mkt_activities_mi_r2(self, tables_format_file, codelist_file, coding_file):
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

        dict_qres_add_new = {
            'Ma_Act_1': ['Hoạt động', 'SA', {'1': 'Act 1', '2': 'Act 2', '3': 'Act 3', '4': 'Act 4'}, 1],
            'Ma_Act_2': ['Hoạt động', 'SA', {'1': 'Act 1', '2': 'Act 2', '3': 'Act 3', '4': 'Act 4'}, 2],
            'Ma_Act_3': ['Hoạt động', 'SA', {'1': 'Act 1', '2': 'Act 2', '3': 'Act 3', '4': 'Act 4'}, 3],
            'Ma_Act_4': ['Hoạt động', 'SA', {'1': 'Act 1', '2': 'Act 2', '3': 'Act 3', '4': 'Act 4'}, 4],
        }

        for key, val in dict_qres_add_new.items():
            df_info_output = pd.concat([df_info_output, pd.DataFrame(columns=['var_name', 'var_lbl', 'var_type', 'val_lbl'], data=[[key, val[0], val[1], val[2]]])], axis=0, ignore_index=True)
            df_data_output = pd.concat([df_data_output, pd.DataFrame(columns=[key], data=[val[-1]] * df_data_output.shape[0])], axis=1)

        for col in ['Q3A_3', 'Q3B_3', 'Q3C_3', 'Q3D_3']:
            df_data_output[col].replace({6: np.nan}, inplace=True)

        # Define structure----------------------------------------------------------------------------------------------
        self.logger.info('Define structure')

        id_col = 'RespondentID'
        sp_col = 'Ma_Act'

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
            'S9',
        ]

        # This project don't have FC Qres
        lst_fc = [
            # 'Main_P100a_Thich_nhat',
        ]

        dict_sp = {
            1: {
                'Ma_Act_1': 'Ma_Act',
                'Q3A_01': 'Q3_01',
                'Q3A_02': 'Q3_02',
                'Q3A_3': 'Q3_3',
                'Q4A': 'Q4',
                'Q5A': 'Q5',
                'Q6A': 'Q6',
            },
            2: {
                'Ma_Act_2': 'Ma_Act',
                'Q3B_01': 'Q3_01',
                'Q3B_02': 'Q3_02',
                'Q3B_3': 'Q3_3',
                'Q4B': 'Q4',
                'Q5B': 'Q5',
                'Q6B': 'Q6',
            },
            3: {
                'Ma_Act_3': 'Ma_Act',
                'Q3C_01': 'Q3_01',
                'Q3C_02': 'Q3_02',
                'Q3C_3': 'Q3_3',
                'Q4C': 'Q4',
                'Q5C': 'Q5',
                'Q6C': 'Q6',
            },
            4: {
                'Ma_Act_4': 'Ma_Act',
                'Q3D_01': 'Q3_01',
                'Q3D_02': 'Q3_02',
                'Q3D_3': 'Q3_3',
                'Q4D': 'Q4',
                'Q5D': 'Q5',
                'Q6D': 'Q6',
            },
        }

        dict_qre_group_mean = {

            'Q3': {
                'range': [f'0{i}' for i in range(1, 3)],
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Q3_3': {
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
        # df_data_fc = df_data_output.loc[:, [id_col] + lst_fc].copy()

        lst_df_data_sp = [df_data_output.loc[:, [id_col] + list(val.keys())].copy() for val in dict_sp.values()]

        for i, df in enumerate(lst_df_data_sp):
            df.rename(columns=dict_sp[i + 1], inplace=True)

        df_data_stack = pd.concat(lst_df_data_sp, axis=0, ignore_index=True)

        df_data_stack = df_data_scr.merge(df_data_stack, how='left', on=[id_col])
        df_data_stack.reset_index(drop=True, inplace=True)

        # df_data_stack = df_data_stack.merge(df_data_fc, how='left', on=[id_col])

        df_data_stack.sort_values(by=[id_col, sp_col], inplace=True)
        df_data_stack.reset_index(drop=True, inplace=True)

        df_info_stack = df_info_output.copy()

        for key, val in dict_sp[1].items():
            df_info_stack.loc[df_info_stack['var_name'] == key, ['var_name']] = [val]

        df_info_stack.drop_duplicates(subset=['var_name'], keep='first', inplace=True)

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
            DataTableGenerator.__init__(self, df_data=df_data_stack, df_info=df_info_stack,
                                        xlsx_name=str_topline_file_name, logger=self.logger,
                                        lst_qre_group=lst_qre_group, lst_qre_mean=lst_qre_mean, is_md=False)

            lst_func_to_run = eval(tables_format_file.file.read())
            self.run_tables_by_js_files(lst_func_to_run)
            self.format_sig_table()
        # End Export data tables----------------------------------------------------------------------------------------

        # Generate SAV files--------------------------------------------------------------------------------------------
        self.logger.info('Generate SAV files')

        # Remove net_code to export sav---------------------------------------------------------------------------------
        df_info_stack_without_net = df_info_stack.copy()

        for idx in df_info_stack_without_net.index:
            val_lbl = df_info_stack_without_net.at[idx, 'val_lbl']

            if 'net_code' in val_lbl.keys():
                df_info_stack_without_net.at[idx, 'val_lbl'] = self.unnetted_qre_val(val_lbl)
        # END Remove net_code to export sav-----------------------------------------------------------------------------

        dict_dfs = {
            1: {
                'data': df_data_stack,
                'info': df_info_stack_without_net,
                'tail_name': 'Stack',
                'sheet_name': 'Stack',
            },
            2: {
                'data': df_data_unstack,
                'info': df_info_unstack,
                'tail_name': 'Unstack',
                'sheet_name': 'Unstack',
            },
        }

        self.generate_multiple_sav_sps(dict_dfs=dict_dfs, is_md=False, is_export_xlsx=True, is_recode_to_lbl=True)
        # END Generate SAV files----------------------------------------------------------------------------------------


