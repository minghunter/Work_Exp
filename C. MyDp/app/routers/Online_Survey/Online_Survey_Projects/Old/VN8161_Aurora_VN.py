from app.classes.Logging_Custom_Formatter import Logger
from app.classes.AP_DataConverter import APDataConverter
from app.routers.Online_Survey.export_online_survey_data_table import DataTableGenerator
from app.routers.Online_Survey.convert_unstack import ConvertUnstack
import pandas as pd
import numpy as np
import time


logger = Logger.logger('my-dp')


class VN8161AuroraVN(APDataConverter, DataTableGenerator, ConvertUnstack):

    def convert_vn8161_aurora_vn(self, py_script_file, tables_format_file, codelist_file, coding_file):

        start_time = time.time()

        df_data_output, df_qres_info_output = self.convert_df_mc()

        # Define structure----------------------------------------------------------------------------------------------
        logger.info('Define structure')

        dict_qre_group_mean = {
            # 'Main_R1_OL': {
            #     'range': [],
            #     'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
            #     'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            # },

        }

        # dict_qre_new_vars_info = {
        #     # 'Main_Q1_Y_tuong_nho_nhat_New': ['Main_Q1_Y_tuong_nho_nhat_New', 'Q1. Sau khi đã nghe qua các ý tưởng phở ăn liền mới này, hiện tại Anh/Chị NHỚ NHẤT VÀ THÍCH NHẤT ý tưởng nào?', 'SA', {'1': 'Yes', '2': 'No'}],
        #     # 'Main_Q2_Y_tuong_phu_hop_nhat_New': ['Main_Q2_Y_tuong_phu_hop_nhat_New', 'Q2. Ý tưởng sản phẩm phở ăn liền nào phù hợp nhất với lợi ích ‘Một loại phở ăn liền có hương vị thơm ngon và sợi phở thanh lành, giúp cân bằng cảm xúc ngay cả lúc mệt mỏi, cho cơ thể nhẹ nhàng và tạo nên một lối sống lành mạnh, thư thái’?', 'SA', {'1': 'Yes', '2': 'No'}],
        #     # 'Main_Q3_Cai_thien_New': ['Main_Q3_Cai_thien_New', 'Q3. Vậy có điểm nào anh/chị muốn cải thiện, sửa đổi để anh/chị thích/hài lòng hơn nữa?', 'FT', {}],
        #     # 'Main_Q4_Ten_SP_New': ['Main_Q4_Ten_SP_New', 'Q4. Sau đây là một số tên sản phẩm phở ăn liền mới, theo bạn tên nào là phù hợp nhất với ý tưởng sản phẩm _?', 'SA', {'1': 'Phở Bờ Hồ', '2': 'Phở Gánh Phố Cổ', '3': 'Phở Ngõ Nhỏ', '4': 'Phở An Nhiên', '5': 'Phở Quốc Hương'}],
        #
        # }

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

        if dict_qre_OE_info:

            # ADD OE to Data stack--------------------------------------------------------------------------------------
            lst_OE_col = list(dict_qre_OE_info.keys())
            df_data_output[lst_OE_col] = pd.DataFrame([[np.nan] * len(lst_OE_col)], index=df_data_output.index)

            # Remember edit this
            for item in lst_addin_OE_value:
                df_data_output.loc[(df_data_output[item[0]] == item[1]) & (df_data_output[item[2]] == item[3]), [item[4]]] = [item[5]]

            df_data_output.loc[df_data_output['S13'] == 1, ['S13_OE_1']] = [999]

            # END ADD OE to Data stack----------------------------------------------------------------------------------

            # ADD OE to Info stack--------------------------------------------------------------------------------------
            df_qres_info_output = pd.concat([df_qres_info_output,
                                             pd.DataFrame(columns=['var_name', 'var_lbl', 'var_type', 'val_lbl'],
                                                          data=list(dict_qre_OE_info.values()))], axis=0)

            df_qres_info_output.reset_index(drop=True, inplace=True)

            # END ADD OE to Info stack----------------------------------------------------------------------------------



        # if dict_qre_net_info:
        #
        #     # ADD MA OE to Data stack-----------------------------------------------------------------------------------
        #     # Remember edit this
        #     for item in lst_addin_MA_value:
        #
        #         idx_item = df_data_stack.loc[(df_data_stack[item[0]] == item[1]) & (df_data_stack[item[2]] == item[3]), item[0]].index[0]
        #         str_ma_oe_name = item[4].rsplit('_', 1)[0]
        #         int_ma_oe_code = int(item[4].rsplit('_', 1)[1].replace('o', ''))
        #
        #         lst_ma_oe_col = df_info_stack.loc[df_info_stack['var_name'].str.contains(f'{str_ma_oe_name}_[0-9]+'), 'var_name'].values.tolist()
        #
        #         is_found = False
        #         for col in lst_ma_oe_col:
        #             if df_data_stack.at[idx_item, col] == int_ma_oe_code:
        #                 is_found = True
        #                 df_data_stack.at[idx_item, col] = item[5]
        #                 break
        #
        #         if not is_found:
        #             for col in lst_ma_oe_col:
        #                 if pd.isnull(df_data_stack.at[idx_item, col]):
        #                     df_data_stack.at[idx_item, col] = item[5]
        #                     break
        #     # END ADD MA OE to Data stack-------------------------------------------------------------------------------
        #
        #     # ADD MA OE to Info stack--------------------------------------------------------------------------------------
        #     for key, val in dict_qre_net_info.items():
        #         df_info_stack.loc[df_info_stack['var_name'].str.contains(key), 'val_lbl'] = [val] * df_info_stack.loc[df_info_stack['var_name'].str.contains(key), 'val_lbl'].shape[0]
        #
        #     # END ADD MA OE to Info stack----------------------------------------------------------------------------------



        # ADD MEAN & GROUP----------------------------------------------------------------------------------------------
        logger.info('ADD MEAN & GROUP')

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

        DataTableGenerator.__init__(self, df_data=df_data_output, df_info=df_qres_info_output,
                                    xlsx_name=str_topline_file_name,
                                    lst_qre_group=lst_qre_group, lst_qre_mean=lst_qre_mean)

        lst_func_to_run = list()
        if tables_format_file.filename:
            lst_func_to_run = eval(tables_format_file.file.read())

        self.run_tables_by_js_files(lst_func_to_run)

        self.format_sig_table()

        # End Export data tables----------------------------------------------------------------------------------------

        logger.info('Generate SAV files')

        # Remove net_code to export sav---------------------------------------------------------------------------------
        df_qres_info_output_without_net = df_qres_info_output.copy()

        for idx in df_qres_info_output_without_net.index:
            val_lbl = df_qres_info_output_without_net.at[idx, 'val_lbl']

            if 'net_code' in val_lbl.keys():
                df_qres_info_output_without_net.at[idx, 'val_lbl'] = self.unnetted_qre_val(val_lbl)
        # END Remove net_code to export sav-----------------------------------------------------------------------------

        self.generate_sav_sps(df_data=df_data_output, df_qres_info=df_qres_info_output_without_net, is_md=False, is_export_xlsx=True)

        logger.warning('DATA PROCESSING COMPLETED in %d seconds' % (time.time() - start_time))
