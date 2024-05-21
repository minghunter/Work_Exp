from app.classes.Logging_Custom_Formatter import Logger
from app.classes.AP_DataConverter import APDataConverter
from app.routers.Online_Survey.export_online_survey_data_table import DataTableGenerator
from app.routers.Online_Survey.convert_unstack import ConvertUnstack
import pandas as pd
import numpy as np
import time


logger = Logger.logger('my-dp')


class INT0001OnlineShoppingBehavior(APDataConverter, DataTableGenerator, ConvertUnstack):

    def convert_int0001_online_shopping_behavior(self, py_script_file, tables_format_file, codelist_file, coding_file):

        start_time = time.time()

        df_data_output, df_qres_info_output = self.convert_df_mc()

        # FILTER DATA---------------------------------------------------------------------------------------------------

        # Filter Task_duration >= 2 minutes
        df_data_output['Task_duration'] = pd.to_timedelta(df_data_output['Task_duration'])
        df_data_output = df_data_output.loc[df_data_output['Task_duration'] >= pd.to_timedelta(2, 'm')]

        # df_data_output = df_data_output.query("~(Q6 < Q14_Fashion & Q6 < Q14_Beauty & Q6 < Q14_IT & Q6 < Q14_Home & Q6 < Q14_Mombaby & Q6 < Q14_Food)")
        # df_data_output = df_data_output.query("~(Q5 <= 4 & ((Q12_Fashion == 1 & Q6 < Q14_Fashion) & (Q12_Beauty == 1 & Q6 < Q14_Beauty) & (Q12_IT == 1 & Q6 < Q14_IT) & (Q12_Home == 1 & Q6 < Q14_Home) & (Q12_Mombaby == 1 & Q6 < Q14_Mombaby) & (Q12_Food == 1 & Q6 < Q14_Food)))")

        df_data_output = df_data_output.query("ID.isin(['712495_2333729', '712495_2333730', '712495_2333735', '712495_2333736', '712495_2333737', '712495_2333739', '712495_2333740', '712495_2333741', '712495_2333743', '712495_2333744', '712495_2333747', '712495_2333748', '712495_2333749', '712495_2333750', '712495_2333751', '712495_2333752', '712495_2333753', '712495_2333757', '712495_2333758', '712495_2333760', '712495_2333761', '712495_2333763', '712495_2333766', '712495_2333767', '712495_2333768', '712495_2333770', '712495_2333771', '712495_2333773', '712495_2333775', '712495_2333776', '712495_2333777', '712495_2333778', '712495_2333779', '712495_2333780', '712495_2333781', '712495_2333782', '712495_2333783', '712495_2333784', '712495_2333785', '712495_2333786', '712495_2333787', '712495_2333788', '712495_2333789', '712495_2333790', '712495_2333792', '712495_2333796', '712495_2333797', '712495_2333802', '712495_2333803', '712495_2333805', '712495_2333806', '712495_2333807', '712495_2333812', '712495_2333813', '712495_2333814', '712495_2333815', '712495_2333818', '712495_2333819', '712495_2333821', '712495_2333826', '712495_2333827', '712495_2333829', '712495_2333832', '712495_2333834', '712495_2333835', '712495_2333837', '712495_2333839', '712495_2333845', '712495_2333846', '712495_2333853', '712495_2333854', '712495_2333857', '712495_2333859', '712495_2333861', '712495_2333863', '712495_2333864', '712495_2333869', '712495_2333870', '712495_2333871', '712495_2333872', '712495_2333874', '712495_2333876', '712495_2333878', '712495_2333884', '712495_2333887', '712495_2333890', '712495_2333891', '712495_2333893', '712495_2333896', '712495_2333897', '712495_2333899', '712495_2333900', '712495_2333901', '712495_2333902', '712495_2333909', '712495_2333912', '712495_2333916', '712495_2333920', '712495_2333922', '712495_2333923', '712495_2333924', '712495_2333927', '712495_2333929', '712495_2333933', '712495_2333935', '712495_2333936', '712495_2333938', '712495_2333939', '712495_2333940', '712495_2333941', '712495_2333942', '712495_2333944', '712495_2333945', '712495_2333949', '712495_2333951', '712495_2333952', '712495_2333953', '712495_2333956', '712495_2333958', '712495_2333959', '712495_2333961', '712495_2333962', '712495_2333964', '712495_2333967', '712495_2333970', '712495_2333971', '712495_2333973', '712495_2333974', '712495_2333975', '712495_2333980', '712495_2333981', '712495_2333983', '712495_2333985', '712495_2333992', '712495_2333993', '712495_2333994', '712495_2333995', '712495_2333999', '712495_2334003', '712495_2334004', '712495_2334005', '712495_2334007', '712495_2334008', '712495_2334009', '712495_2334011', '712495_2334013', '712495_2334014', '712495_2334015', '712495_2334016', '712495_2334018', '712495_2334020', '712495_2334021', '712495_2334022', '712495_2334028', '712495_2334031', '712495_2334035', '712495_2334037', '712495_2334038', '712495_2334039', '712495_2334042', '712495_2334046', '712495_2334047', '712495_2334049', '712495_2334052', '712495_2334055', '712495_2334056', '712495_2334057', '712495_2334058', '712495_2334062', '712495_2334064', '712495_2334066', '712495_2334068', '712495_2334074', '712495_2334076', '712495_2334077', '712495_2334078', '712495_2334079', '712495_2334081', '712495_2334082', '712495_2334084', '712495_2334086', '712495_2334087', '712495_2334088', '712495_2334089', '712495_2334092', '712495_2334096', '712495_2334097', '712495_2334098', '712495_2334102', '712495_2334104', '712495_2334105', '712495_2334114', '712495_2334115', '712495_2334116', '712495_2334118', '712495_2334119', '712495_2334120', '712495_2334122', '712495_2334125', '712495_2334134', '712495_2334135', '712495_2334137', '712495_2334141', '712495_2334145', '712495_2334147', '712495_2334151', '712495_2334152', '712495_2334156', '712495_2334157', '712495_2334158', '712495_2334159', '712495_2334170', '712495_2334177', '712495_2334180', '712495_2334182', '712495_2334185', '712495_2334215', '712495_2334218', '712495_2334219', '712495_2334232', '712495_2334247', '712495_2334254', '712495_2334255', '712495_2334258', '712495_2334270', '712495_2334279', '712495_2334280', '712495_2334283', '712495_2334289', '712495_2334296', '712495_2334301', '712495_2334304', '712495_2334314', '712495_2334316', '712495_2334319', '712495_2334324', '712495_2334328', '712495_2334330', '712495_2334333', '712495_2334341', '712495_2334347', '712495_2334356', '712495_2334367', '712495_2334380', '712495_2334395', '712495_2334402', '712495_2334414', '712495_2334422', '712495_2334430', '712495_2334603', '712495_2334652', '712495_2334653', '712495_2334660', '712495_2334680', '712495_2334755', '712495_2334767', '712495_2334774', '712495_2334849', '712505_2333711', '712505_2333712', '712505_2333718', '712505_2333719', '712505_2333721', '712505_2333731', '712505_2333759', '712505_2333762', '712505_2333774', '712505_2333824', '712505_2333894', '712505_2333907', '712505_2333921', '712505_2333931', '712505_2333979', '712505_2334069', '712505_2334184', '712505_2334213', '712505_2334237', '712505_2334277', '712505_2334650', '712505_2334663', '712505_2334692', '712505_2334701', '712505_2334795', '712505_2334888', '712505_2334994', '712505_2335027', '712505_2335149', '712505_2335344', '712505_2335356', '712505_2335840', '712505_2335844', '712505_2335974', '712505_2336358', '712505_2336504', '712505_2336626', '712505_2336813', '712505_2336912', '712505_2337062', '712505_2337231', '712505_2337339', '712505_2337350', '712505_2337614', '712505_2337626', '712505_2337674', '712505_2337706', '712505_2337815', '712505_2337860'])")

        # Define structure----------------------------------------------------------------------------------------------
        logger.info('Define structure')

        dict_qre_group_mean = {
            'Q6': {
                'range': [],
                'group': {'cats': {}, 'recode': {}},
                'mean': {1: 100000, 2: 150000, 3: 250000, 4: 400000, 5: 600000, 6: 850000, 7: 1500000, 8: 2000000}
            },

            'Q14_Fashion': {
                'range': [],
                'group': {'cats': {}, 'recode': {}},
                'mean': {1: 100000, 2: 150000, 3: 250000, 4: 400000, 5: 600000, 6: 850000, 7: 1500000, 8: 2000000}
            },

            'Q14_Beauty': {
                'range': [],
                'group': {'cats': {}, 'recode': {}},
                'mean': {1: 100000, 2: 150000, 3: 250000, 4: 400000, 5: 600000, 6: 850000, 7: 1500000, 8: 2000000}
            },

            'Q14_IT': {
                'range': [],
                'group': {'cats': {}, 'recode': {}},
                'mean': {1: 100000, 2: 150000, 3: 250000, 4: 400000, 5: 600000, 6: 850000, 7: 1500000, 8: 2000000}
            },

            'Q14_Home': {
                'range': [],
                'group': {'cats': {}, 'recode': {}},
                'mean': {1: 100000, 2: 150000, 3: 250000, 4: 400000, 5: 600000, 6: 850000, 7: 1500000, 8: 2000000}
            },

            'Q14_Mombaby': {
                'range': [],
                'group': {'cats': {}, 'recode': {}},
                'mean': {1: 100000, 2: 150000, 3: 250000, 4: 400000, 5: 600000, 6: 850000, 7: 1500000, 8: 2000000}
            },

            'Q14_Food': {
                'range': [],
                'group': {'cats': {}, 'recode': {}},
                'mean': {1: 100000, 2: 150000, 3: 250000, 4: 400000, 5: 600000, 6: 850000, 7: 1500000, 8: 2000000}
            },

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

        DataTableGenerator.__init__(self, df_data=df_data_output, df_info=df_qres_info_output, is_md=False,
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
