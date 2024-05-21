from app.classes.AP_DataConverter import APDataConverter
from app.routers.Online_Survey.export_online_survey_data_table import DataTableGenerator
from app.routers.Online_Survey.convert_unstack import ConvertUnstack
import pandas as pd
import numpy as np
import time
import traceback


class VN8307RakanClt(APDataConverter, DataTableGenerator, ConvertUnstack):

    def convert_vn8307_rakan_clt(self, py_script_file, tables_format_file, codelist_file, coding_file):
        """DO NOT EDIT THIS FUNCTION"""

        try:
            start_time = time.time()

            if py_script_file:
                exec(py_script_file.file.read())
                exit()

            self.processing_script_vn8307_rakan_clt(tables_format_file, codelist_file, coding_file)

            self.logger.warning('DATA PROCESSING COMPLETED in %d seconds' % (time.time() - start_time))

        except Exception:
            self.logger.error(traceback.format_exc())
            str_err_log_name = self.str_file_name.replace('.xlsx', '_Errors.txt')
            with open(str_err_log_name, 'w') as err_log_txt:
                err_log_txt.writelines(traceback.format_exc())



    def processing_script_vn8307_rakan_clt(self, tables_format_file, codelist_file, coding_file):
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

        lst_restr_oe = [
            ['Main_P6a1_OE_Ly_do_thich_soi_mi_o2', 'Main_P6a2_OE_Ly_do_thich_soi_mi_o2'],
            ['Main_P6a1_OE_Ly_do_thich_soi_mi_o3', 'Main_P6a2_OE_Ly_do_thich_soi_mi_o3'],
            ['Main_P6a1_OE_Ly_do_thich_soi_mi_o4', 'Main_P6a2_OE_Ly_do_thich_soi_mi_o4'],
            ['Main_P6a1_OE_Ly_do_thich_soi_mi_o5', 'Main_P6a2_OE_Ly_do_thich_soi_mi_o5'],
            ['Main_P6a1_OE_Ly_do_thich_soi_mi_o6', 'Main_P6a2_OE_Ly_do_thich_soi_mi_o6'],
            ['Main_P6a1_OE_Ly_do_thich_soi_mi_o7', 'Main_P6a2_OE_Ly_do_thich_soi_mi_o7'],
        ]

        for pair in lst_restr_oe:
            df_data_output[pair[0]] = [b if pd.isnull(a) else a for a, b in zip(df_data_output[pair[0]], df_data_output[pair[1]])]

        df_data_output.drop(columns=[
            'QC_S0b_Respondent_name',
            'QC_S0d_PhoneNumber',
            'B2B',
            'Main_P6a1_OE_Ly_do_thich_soi_mi_1',
            'Main_P6a1_OE_Ly_do_thich_soi_mi_2',
            'Main_P6a1_OE_Ly_do_thich_soi_mi_3',
            'Main_P6a1_OE_Ly_do_thich_soi_mi_4',
            'Main_P6a1_OE_Ly_do_thich_soi_mi_5',
            'Main_P6a1_OE_Ly_do_thich_soi_mi_6',
            'Main_P6a1_OE_Ly_do_thich_soi_mi_7',
            'Main_P6b_OE_Ly_do_KHONG_thich_soi_mi_1',
            'Main_P6b_OE_Ly_do_KHONG_thich_soi_mi_2',
            'Main_P6b_OE_Ly_do_KHONG_thich_soi_mi_3',
            'Main_P6b_OE_Ly_do_KHONG_thich_soi_mi_4',
            'Main_P6b_OE_Ly_do_KHONG_thich_soi_mi_5',
            'Main_P6b_OE_Ly_do_KHONG_thich_soi_mi_6',
            'Main_P6b_OE_Ly_do_KHONG_thich_soi_mi_7',
            'Main_P6a2_OE_Ly_do_thich_soi_mi_1',
            'Main_P6a2_OE_Ly_do_thich_soi_mi_2',
            'Main_P6a2_OE_Ly_do_thich_soi_mi_3',
            'Main_P6a2_OE_Ly_do_thich_soi_mi_4',
            'Main_P6a2_OE_Ly_do_thich_soi_mi_5',
            'Main_P6a2_OE_Ly_do_thich_soi_mi_6',
            'Main_P6a2_OE_Ly_do_thich_soi_mi_7',
            'Main_P6a2_OE_Ly_do_thich_soi_mi_o2',
            'Main_P6a2_OE_Ly_do_thich_soi_mi_o3',
            'Main_P6a2_OE_Ly_do_thich_soi_mi_o4',
            'Main_P6a2_OE_Ly_do_thich_soi_mi_o5',
            'Main_P6a2_OE_Ly_do_thich_soi_mi_o6',
            'Main_P6a2_OE_Ly_do_thich_soi_mi_o7',

        ], inplace=True)

        lst_rename_oe = [
            ['Main_P6a1_OE_Ly_do_thich_soi_mi_o2', 'Main_P6a1_OE_Ly_do_thich_soi_mi_Y_dau_tien', 'Ý đầu tiên'],
            ['Main_P6a1_OE_Ly_do_thich_soi_mi_o3', 'Main_P6a1_OE_Ly_do_thich_soi_mi_Mau_soi_mi', 'Màu sợi mì'],
            ['Main_P6a1_OE_Ly_do_thich_soi_mi_o4', 'Main_P6a1_OE_Ly_do_thich_soi_mi_Do_mem_soi_mi', 'Độ mềm sợi mì'],
            ['Main_P6a1_OE_Ly_do_thich_soi_mi_o5', 'Main_P6a1_OE_Ly_do_thich_soi_mi_Do_dai_soi_mi', 'Độ dai sợi mì'],
            ['Main_P6a1_OE_Ly_do_thich_soi_mi_o6', 'Main_P6a1_OE_Ly_do_thich_soi_mi_Do_muot_soi_mi', 'Độ mướt sợi mì'],
            ['Main_P6a1_OE_Ly_do_thich_soi_mi_o7', 'Main_P6a1_OE_Ly_do_thich_soi_mi_Khac', 'Khác (ghi rõ)'],

            ['Main_P6b_OE_Ly_do_KHONG_thich_soi_mi_o2', 'Main_P6b_OE_Ly_do_KHONG_thich_soi_mi_Y_dau_tien', 'Ý đầu tiên'],
            ['Main_P6b_OE_Ly_do_KHONG_thich_soi_mi_o3', 'Main_P6b_OE_Ly_do_KHONG_thich_soi_mi_Mau_soi_mi', 'Màu sợi mì'],
            ['Main_P6b_OE_Ly_do_KHONG_thich_soi_mi_o4', 'Main_P6b_OE_Ly_do_KHONG_thich_soi_mi_Do_mem_soi_mi', 'Độ mềm sợi mì'],
            ['Main_P6b_OE_Ly_do_KHONG_thich_soi_mi_o5', 'Main_P6b_OE_Ly_do_KHONG_thich_soi_mi_Do_dai_soi_mi', 'Độ dai sợi mì'],
            ['Main_P6b_OE_Ly_do_KHONG_thich_soi_mi_o6', 'Main_P6b_OE_Ly_do_KHONG_thich_soi_mi_Do_muot_soi_mi', 'Độ mướt sợi mì'],
            ['Main_P6b_OE_Ly_do_KHONG_thich_soi_mi_o7', 'Main_P6b_OE_Ly_do_KHONG_thich_soi_mi_Khac', 'Khác (ghi rõ)'],
        ]

        for row in lst_rename_oe:
            df_data_output.rename(columns={row[0]: row[1]}, inplace=True)
            df_info_output.loc[df_info_output['var_name'] == row[0], ['var_name', 'var_lbl']] = [row[1], row[2]]


        # Define structure----------------------------------------------------------------------------------------------
        self.logger.info('Define structure')

        dict_qre_group_mean = {
            'Main_P1_OL_Chung': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Main_P2_OL_Ngoai_quan': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Main_P3_OL_Soi_mi': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Main_P4_OL_Mau_soi_mi': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Main_P5_OL_Cam_giac_nhai': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Main_P7_OL_Nuoc_dung': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Main_P8_OL_Mui_nuoc_dung': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Main_P9_OL_Vi': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Main_P11_PI_Y_dinh_mua': {
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

        # Reset df_info_stack index
        df_info_output['idx_var_name'] = df_info_output['var_name']
        df_info_output.set_index('idx_var_name', inplace=True)
        df_info_output = df_info_output.loc[list(df_data_output.columns), :]
        df_info_output.reindex(list(df_data_output.columns))
        df_info_output.reset_index(drop=True, inplace=True)




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
            DataTableGenerator.__init__(self, df_data=df_data_output, df_info=df_info_output,
                                        xlsx_name=str_topline_file_name, logger=self.logger,
                                        lst_qre_group=lst_qre_group, lst_qre_mean=lst_qre_mean, is_md=False)

            lst_func_to_run = eval(tables_format_file.file.read())
            self.run_tables_by_js_files(lst_func_to_run)
            self.format_sig_table()

            # df_data_tbl.to_excel('zzz_df_data_tbl.xlsx')
            # df_info_tbl.to_excel('zzz_df_info_tbl.xlsx')
        # End Export data tables----------------------------------------------------------------------------------------




        # Generate SAV files--------------------------------------------------------------------------------------------
        self.logger.info('Generate SAV files')

        # Remove net_code to export sav---------------------------------------------------------------------------------

        df_info_output_without_net = df_info_output.copy()

        for idx in df_info_output_without_net.index:
            val_lbl = df_info_output_without_net.at[idx, 'val_lbl']

            if 'net_code' in val_lbl.keys():
                df_info_output_without_net.at[idx, 'val_lbl'] = self.unnetted_qre_val(val_lbl)

        # END Remove net_code to export sav-----------------------------------------------------------------------------

        dict_dfs = {
            1: {
                'data': df_data_output,
                'info': df_info_output_without_net,
                'tail_name': 'Data',
                'sheet_name': 'Data',
            },
        }

        self.generate_multiple_sav_sps(dict_dfs=dict_dfs, is_md=False, is_export_xlsx=True, is_recode_to_lbl=False)

        # END Generate SAV files----------------------------------------------------------------------------------------


