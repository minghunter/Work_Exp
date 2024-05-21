from app.classes.AP_DataConverter import APDataConverter
from ..export_online_survey_data_table import DataTableGenerator
from ..convert_unstack import ConvertUnstack
import pandas as pd
import numpy as np
import time
import traceback
import datetime


class VN8313CustomerUnderstanding(APDataConverter, DataTableGenerator, ConvertUnstack):

    def convert_vn8313_customer_understanding(self, py_script_file, tables_format_file, codelist_file, coding_file):
        """DO NOT EDIT THIS FUNCTION"""

        try:
            start_time = time.time()

            if py_script_file:
                exec(py_script_file.file.read())
                exit()

            self.processing_script_vn8313_customer_understanding(tables_format_file, codelist_file, coding_file)

            self.logger.warning('DATA PROCESSING COMPLETED in %d seconds' % (time.time() - start_time))

        except Exception:
            self.logger.error(traceback.format_exc())
            str_err_log_name = self.str_file_name.replace('.xlsx', '_Errors.txt')
            with open(str_err_log_name, 'w') as err_log_txt:
                err_log_txt.writelines(traceback.format_exc())


    def processing_script_vn8313_customer_understanding(self, tables_format_file, codelist_file, coding_file):
        """
        :param tables_format_file: txt uploaded table format file
        :param codelist_file: txt uploaded codelist file
        :param coding_file: txt uploaded coding file
        :return: zip file include files: .sav, .sps, .xlsx(rawdata, topline)

        EDIT FROM HERE
        """

        # Convert system rawdata to dataframes--------------------------------------------------------------------------
        df_data, df_info = self.convert_df_mc()

        df_fil_info = df_info.query("var_name.str.contains('Q2_3a|Q2_3b|Q2_3c')")
        df_info.loc[df_fil_info.index, ['var_type']] = ['MA']

        # df_fil = df_data.query("S10 >= 4").copy()
        # drop_index = list(df_fil.index)
        #
        # df_fil = df_data.query("S4.isin([6, 7, 8, 9, 10, 11, 12])").copy()
        # drop_index.extend(list(df_fil.index)[-2:])
        #
        # df_fil = df_data.query("S4 == 2").copy()
        # drop_index.extend(list(df_fil.index)[-42:])
        #
        # df_data.drop(index=drop_index, inplace=True)
        #
        # df_data.reset_index(drop=True, inplace=True)
        # df_info.reset_index(drop=True, inplace=True)

        # Pre processing------------------------------------------------------------------------------------------------
        self.logger.info('Pre processing')

        # ADD Q1abc_Group
        dict_new_vars = {
            # Q1a
            'Q1a_GIA_VI': ['Q1a', 'SA', {'1': 'GIA VỊ'}, [1, 2, 3, 4, 5]],
            'Q1a_DAU_GOI': ['Q1a', 'SA', {'1': 'DẦU GỘI/SỮA TẮM/BỘT GIẶT'}, [6, 7, 8]],
            'Q1a_THIT_DA_CHE_BIEN': ['Q1a', 'SA', {'1': 'THỊT ĐÃ CHẾ BIẾN'}, [9, 10]],
            'Q1a_SO_CO_LA': ['Q1a', 'SA', {'1': 'SỮA HẠT SÔ-CÔ-LA / SỮA SÔ-CÔ-LA CHO BÉ'}, [11]],
            'Q1a_THUC_PHAM_TIEN_LOI': ['Q1a', 'SA', {'1': 'THỰC PHẨM TIỆN LỢI'}, [12, 13]],

            # Q1b
            'Q1b_THIT_DA_CHE_BIEN': ['Q1b', 'SA', {'1': 'THỊT ĐÃ CHẾ BIẾN'}, [1, 2]],
            'Q1b_MON_AN_VAT': ['Q1b', 'SA', {'1': 'MÓN ĂN VẶT'}, [3, 4]],
            'Q1b_THUC_UONG_DONG_CHAI': ['Q1b', 'SA', {'1': 'THỨC UỐNG ĐÓNG CHAI/LON'}, [5, 6, 7]],

            # Q1c
            'Q1c_GIA_VI': ['Q1c', 'SA', {'1': 'GIA VỊ'}, [1, 2, 3, 4, 5]],
            'Q1c_DAU_GOI': ['Q1c', 'SA', {'1': 'DẦU GỘI/SỮA TẮM/BỘT GIẶT'}, [6, 7, 8]],
            'Q1c_THUC_UONG_DONG_CHAI': ['Q1c', 'SA', {'1': 'THỨC UỐNG ĐÓNG CHAI/LON'}, [9, 10, 11]],
            'Q1c_BIA': ['Q1c', 'SA', {'1': 'BIA LON/CHAI'}, [12]],
            'Q1c_THUC_PHAM_TIEN_LOI': ['Q1c', 'SA', {'1': 'THỰC PHẨM TIỆN LỢI'}, [13, 14]],
            'Q1c_CA_PHE_HOA_TAN': ['Q1c', 'SA', {'1': 'CÀ PHÊ HÒA TAN'}, [15]],
        }

        df_data = pd.concat([df_data, pd.DataFrame(columns=list(dict_new_vars.keys()), data=[])], axis=1)

        lst_new_vars_info = list()
        for key, val in dict_new_vars.items():
            lst_new_vars_info.append([key] + val[:-1])

            qre_name = key.split('_', 1)[0]
            qre_col = df_info.loc[df_info['var_name'].str.contains(f'{qre_name}_[0-9]+'), 'var_name'].values.tolist()
            str_query = ' | '.join([f"{q}.isin({val[-1]})" for q in qre_col])
            df_data_fil = df_data.query(str_query).copy()

            df_data.loc[df_data_fil.index, [key]] = [1]

        df_info = pd.concat([df_info, pd.DataFrame(columns=['var_name', 'var_lbl', 'var_type', 'val_lbl'], data=lst_new_vars_info)], axis=0)

        df_data.reset_index(drop=True, inplace=True)
        df_info.reset_index(drop=True, inplace=True)


        # CHECK DATA Q2_1 -> Q2_7---------------------------------------------------------------------------------------
        df_data = pd.concat([df_data, pd.DataFrame(columns=['Check_Q2'], data=[])], axis=1)
        df_info = pd.concat([df_info, pd.DataFrame(columns=['var_name', 'var_lbl', 'var_type', 'val_lbl'],
                                                   data=[['Check_Q2', 'Check_Q2', 'FT', {}]])], axis=0)

        df_data['Check_Q2'] = df_data['Check_Q2'].astype(str)
        df_data['Check_Q2'].replace({'nan': ''}, inplace=True)

        dict_query_q2 = {
            'Q2_1_1': 'Q1a_GIA_VI == 1 | Q1c_GIA_VI == 1',
            'Q2_2_1': 'Q1a_DAU_GOI == 1 | Q1c_DAU_GOI == 1',

            'Q2_3a_1': 'Q1a_THIT_DA_CHE_BIEN == 1 | Q1b_THIT_DA_CHE_BIEN == 1',
            'Q2_3b_1': 'Q1a_THUC_PHAM_TIEN_LOI == 1 | Q1c_THUC_PHAM_TIEN_LOI == 1',
            'Q2_3c_1': 'Q1b_MON_AN_VAT == 1',

            'Q2_4_1': 'Q1a_SO_CO_LA == 1',
            'Q2_5_1': 'Q1b_THUC_UONG_DONG_CHAI == 1 | Q1c_THUC_UONG_DONG_CHAI == 1',
            'Q2_6_1': 'Q1c_BIA == 1',
            'Q2_7_1': 'Q1c_CA_PHE_HOA_TAN == 1',
        }

        for key, val in dict_query_q2.items():
            df_data_fil = df_data.query(f"{key}.isnull() & ({val})").copy()
            if df_data_fil.empty:
                continue

            df_data.loc[df_data_fil.index, ['Check_Q2']] = df_data.loc[df_data_fil.index, ['Check_Q2']] + f"|{key.rsplit('_', 1)[0]}"

        df_data.reset_index(drop=True, inplace=True)
        df_info.reset_index(drop=True, inplace=True)
        # END CHECK DATA Q2_1 -> Q2_7-----------------------------------------------------------------------------------


        # Define structure----------------------------------------------------------------------------------------------
        self.logger.info('Define structure')

        dict_qre_group_mean = {
            # 'Q17': {
            #     'range': [f'0{i}' for i in range(1, 4)],
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
            'Q1a_[0-9]+': {
                'net_code': {
                    '900001|net|GIA VỊ': {
                        '1': 'Tương ớt',
                        '2': 'Nước mắm',
                        '3': 'Nước tương',
                        '4': 'Tương cà',
                        '5': 'Hạt nêm',
                    },
                    '900002|net|DẦU GỘI/SỮA TẮM/BỘT GIẶT': {
                        '6': 'Dầu gội',
                        '7': 'Sữa tắm',
                        '8': 'Bột giặt',
                    },
                    '900003|net|THỊT ĐÃ CHẾ BIẾN': {
                        '9': 'Thịt hộp',
                        '10': 'Xúc xích thanh trùng',
                    },
                    '900004|net|SỮA HẠT SÔ-CÔ-LA / SỮA SÔ-CÔ-LA CHO BÉ': {
                        '11': 'Sữa hạt sô-cô-la / sữa sô-cô-la cho con',
                    },
                    '900005|net|THỰC PHẨM TIỆN LỢI': {
                        '12': 'Mì ăn liền',
                        '13': 'Phở ăn liền',
                    },
                }
            },
            'Q1b_[0-9]+': {
                'net_code': {
                    '900001|net|THỊT ĐÃ CHẾ BIẾN': {
                        '1': 'Thịt hộp',
                        '2': 'Xúc xích thanh trùng',
                    },
                    '900002|net|MÓN ĂN VẶT': {
                        '3': 'Xúc xích tiệt trùng',
                        '4': 'Khô gà',
                    },
                    '900003|net|THỨC UỐNG ĐÓNG CHAI/LON': {
                        '5': 'Nước ngọt có gas',
                        '6': 'Nước tăng lực',
                        '7': 'Nước đóng chai (nước tinh khiết, nước khoáng)',
                    },
                }
            },
            'Q1c_[0-9]+': {
                'net_code': {
                    '900001|net|GIA VỊ': {
                        '1': 'Tương ớt',
                        '2': 'Nước mắm',
                        '3': 'Nước tương',
                        '4': 'Tương cà',
                        '5': 'Hạt nêm',
                    },
                    '900002|net|DẦU GỘI/SỮA TẮM/BỘT GIẶT': {
                        '6': 'Dầu gội',
                        '7': 'Sữa tắm',
                        '8': 'Bột giặt',
                    },
                    '900003|net|THỨC UỐNG ĐÓNG CHAI/LON': {
                        '9': 'Nước ngọt có gas',
                        '10': 'Nước tăng lực',
                        '11': 'Nước đóng chai (nước tinh khiết, nước khoáng)',
                    },
                    '900004|net|BIA LON/CHAI': {
                        '12': 'Bia lon/ chai',
                    },
                    '900005|net|THỰC PHẨM TIỆN LỢI': {
                        '13': 'Mì ăn liền',
                        '14': 'Phở ăn liền',
                    },
                    '900006|net|CÀ PHÊ HÒA TAN': {
                        '15': 'Cà phê hòa tan'
                    },
                },
            },
            'Q2_1_[0-9]+': {
                'net_code': {
                    '900001|net|PREMIUM': {
                        '1': 'Sản phẩm có thiết kế sang trọng, cao cấp',
                        '2': 'Sản phẩm làm từ nguyên liệu cao cấp, chất lượng cao',
                        '3': 'Được sản xuất từ công nghệ hiện đại, tiên tiến',
                    },
                    '900002|net|CONVENIENCE': {
                        '4': 'Cách sử dụng đơn giản, dễ dàng',
                        '5': 'Sản phẩm giúp tiết kiệm thời gian khi sử dụng',
                        '6': 'Gia vị được kết hợp hoàn chỉnh, không cần nêm nếm thêm',
                        '7': 'Phù hợp với đa dạng món ăn',
                    },
                    '900003|net|HEALTH': {
                        '8': 'Sản phẩm được làm từ các thành phần tự nhiên, tốt/ an toàn cho sức khỏe',
                        '9': 'Có các chứng nhận về tiêu chuẩn chất lượng như Vietgap, FDA, …',
                        '10': 'Đủ tiêu chuẩn xuất khẩu qua các nước tiên tiến như Nhật, Mỹ, Châu Âu',
                        '11': 'Bổ sung chất dinh dưỡng (Vitamin A, sắt, canxi,…) tốt cho sức khỏe',
                    },
                    '900004|net|TRENDY LIFE': {
                        '12': 'Sản phẩm có sự sáng tạo, độc đáo về mặt hương vị',
                        '13': 'Cho tôi cảm giác là người phụ nữ hiện đại, bắt kịp xu hướng',
                        '14': 'Có cách quảng cáo thú vị, phù hợp xu hướng',
                        '15': 'Phù hợp cho các dịp ăn mừng, tụ họp gia đình, bạn bè',
                    },
                    '900005|net|VALUE FOR MONEY': {
                        '16': 'Sản phẩm có chất lượng vượt trội hơn so với mức giá bỏ ra',
                    },
                },
                '17': 'Không điều nào ở trên'
            },
            'Q2_2_[0-9]+': {
                'net_code': {
                    '900001|net|PREMIUM': {
                        '1': 'Sản phẩm có thiết kế sang trọng, cao cấp',
                        '2': 'Sản phẩm làm từ nguyên liệu cao cấp, chất lượng cao',
                        '3': 'Sản phẩm ngoại nhập',
                        '4': 'Sản phẩm có chức năng chăm sóc chuyên sâu/phục vụ nhu cầu chuyên biệt',
                        '5': 'Sản phẩm được người nổi tiếng, nhân vật có ảnh hưởng lớn sử dụng',
                    },
                    '900002|net|CONVENIENCE': {
                        '6': 'Sản phẩm phù hợp với nhiều lứa tuổi, đối tượng',
                        '7': 'Cách sử dụng đơn giản, dễ dàng',
                        '8': 'Sản phẩm giúp tiết kiệm thời gian khi sử dụng',
                        '9': 'Sản phẩm bao gồm nhiều công dụng trong 1',
                        '10': 'Sản phẩm có nhiều kích thước phù hợp với mục đích sử dụng',
                    },
                    '900003|net|HEALTH': {
                        '11': 'Sản phẩm được làm từ các thành phần tự nhiên, tốt/ an toàn cho sức khỏe',
                        '12': 'Có chứng nhận an toàn trên bao bì (vd: Clinical test, bác sĩ khuyên dùng)',
                        '13': 'Sản phẩm có thể sử dụng được cho trẻ nhỏ',
                        '14': 'Sản phẩm phù hợp với cả da nhạy cảm',
                        '15': 'Sản phẩm giúp kháng khuẩn, virus',
                    },
                    '900004|net|TRENDY LIFE': {
                        '16': 'Sản phẩm được làm từ các thành phần mới lạ',
                        '17': 'Sản phẩm có hương thơm thời thượng',
                        '18': 'Sản phẩm có sự sáng tạo, độc đáo về mặt công nghệ',
                        '19': 'Sản phẩm có hình thức, cách sử dụng mới lạ, độc đáo',
                        '20': 'Sản phẩm được bàn tán nhiều trên mạng xã hội',
                    },
                    '900005|net|VALUE FOR MONEY': {
                        '21': 'Sản phẩm có chất lượng vượt trội hơn so với mức giá bỏ ra',
                        '22': 'Đi kèm các dịch vụ, tiện ích khác (giao tận nhà, tư vấn, bảo hành, lắp đặt tận nơi,...)',
                    },
                },
                '23': 'Không điều nào ở trên'
            },
            'Q2_3a_[0-9]+': {
                'net_code': {
                    '900001|net|PREMIUM': {
                        '1': 'Sản phẩm có thiết kế sang trọng, cao cấp',
                        '2': 'Sản phẩm làm từ nguyên liệu cao cấp, chất lượng cao',
                        '3': 'Sản phẩm ngoại nhập',
                    },
                    '900002|net|CONVENIENCE': {
                        '4': 'Sản phẩm phù hợp với nhiều lứa tuổi, đối tượng',
                        '5': 'Cách sử dụng đơn giản, dễ dàng',
                        '6': 'Sản phẩm giúp tiết kiệm thời gian khi sử dụng',
                        '7': 'Sản phẩm có nhiều kích thước phù hợp với mục đích sử dụng',
                    },
                    '900003|net|HEALTH': {
                        '8': 'Sản phẩm được làm từ các thành phần tự nhiên, tốt/ an toàn cho sức khỏe',
                        '9': 'Sản phẩm có hàm lượng dinh dưỡng (chất béo, đạm, calories,...) cân bằng',
                        '10': 'Sản phẩm được phát triển theo các xu hướng dinh dưỡng mới (có nguồn gốc từ thực vật, kiêng lactose/ gluten, …)',
                        '11': 'Sản phẩm hạn chế có chứa các chất gây hại',
                    },
                    '900004|net|TRENDY LIFE': {
                        '12': 'Sản phẩm được làm từ các thành phần mới lạ',
                        '13': 'Sản phẩm có hương vị sáng tạo, độc đáo',
                    },
                    '900005|net|VALUE FOR MONEY': {
                        '14': 'Sản phẩm có chất lượng vượt trội hơn so với mức giá bỏ ra',
                        '15': 'Đi kèm các dịch vụ, tiện ích khác (giao tận nhà, tư vấn, bảo hành, lắp đặt tận nơi,...)',
                    },
                },
                '16': 'Không điều nào ở trên'
            },
            'Q2_3b_[0-9]+': {
                'net_code': {
                    '900001|net|PREMIUM': {
                        '1': 'Sản phẩm có thiết kế sang trọng, cao cấp',
                        '2': 'Sản phẩm làm từ nguyên liệu cao cấp, chất lượng cao',
                        '3': 'Sản phẩm ngoại nhập',
                    },
                    '900002|net|CONVENIENCE': {
                        '4': 'Sản phẩm phù hợp với nhiều lứa tuổi, đối tượng',
                        '5': 'Cách sử dụng đơn giản, dễ dàng',
                        '6': 'Sản phẩm giúp tiết kiệm thời gian khi sử dụng',
                        '7': 'Sản phẩm có nhiều kích thước phù hợp với mục đích sử dụng',
                    },
                    '900003|net|HEALTH': {
                        '8': 'Sản phẩm được làm từ các thành phần tự nhiên, tốt/ an toàn cho sức khỏe',
                        '9': 'Sản phẩm có hàm lượng dinh dưỡng (chất béo, đạm, calories,...) cân bằng',
                        '10': 'Sản phẩm được phát triển theo các xu hướng dinh dưỡng mới (có nguồn gốc từ thực vật, kiêng lactose/ gluten, …)',
                        '11': 'Sản phẩm hạn chế có chứa các chất gây hại',
                    },
                    '900004|net|TRENDY LIFE': {
                        '12': 'Sản phẩm được làm từ các thành phần mới lạ',
                        '13': 'Sản phẩm có hương vị sáng tạo, độc đáo',
                    },
                    '900005|net|VALUE FOR MONEY': {
                        '14': 'Sản phẩm có chất lượng vượt trội hơn so với mức giá bỏ ra',
                        '15': 'Đi kèm các dịch vụ, tiện ích khác (giao tận nhà, tư vấn, bảo hành, lắp đặt tận nơi,...)',
                    },
                },
                '16': 'Không điều nào ở trên'
            },
            'Q2_3c_[0-9]+': {
                'net_code': {
                    '900001|net|PREMIUM': {
                        '1': 'Sản phẩm có thiết kế sang trọng, cao cấp',
                        '2': 'Sản phẩm làm từ nguyên liệu cao cấp, chất lượng cao',
                        '3': 'Sản phẩm ngoại nhập',
                    },
                    '900002|net|CONVENIENCE': {
                        '4': 'Sản phẩm phù hợp với nhiều lứa tuổi, đối tượng',
                        '5': 'Cách sử dụng đơn giản, dễ dàng',
                        '6': 'Sản phẩm giúp tiết kiệm thời gian khi sử dụng',
                        '7': 'Sản phẩm có nhiều kích thước phù hợp với mục đích sử dụng',
                    },
                    '900003|net|HEALTH': {
                        '8': 'Sản phẩm được làm từ các thành phần tự nhiên, tốt/ an toàn cho sức khỏe',
                        '9': 'Sản phẩm có hàm lượng dinh dưỡng (chất béo, đạm, calories,...) cân bằng',
                        '10': 'Sản phẩm được phát triển theo các xu hướng dinh dưỡng mới (có nguồn gốc từ thực vật, kiêng lactose/ gluten, …)',
                        '11': 'Sản phẩm hạn chế có chứa các chất gây hại',
                    },
                    '900004|net|TRENDY LIFE': {
                        '12': 'Sản phẩm được làm từ các thành phần mới lạ',
                        '13': 'Sản phẩm có hương vị sáng tạo, độc đáo',
                    },
                    '900005|net|VALUE FOR MONEY': {
                        '14': 'Sản phẩm có chất lượng vượt trội hơn so với mức giá bỏ ra',
                        '15': 'Đi kèm các dịch vụ, tiện ích khác (giao tận nhà, tư vấn, bảo hành, lắp đặt tận nơi,...)',
                    },
                },
                '16': 'Không điều nào ở trên'
            },
            'Q2_4_[0-9]+': {
                'net_code': {
                    '900001|net|PREMIUM': {
                        '1': 'Sản phẩm có nguyên liệu nhập khẩu',
                        '2': 'Sản phẩm ngoại nhập',
                        '3': 'Sản phẩm làm từ các loại hạt',
                        '4': 'Sản phẩm có bao bì tối giản',
                        '5': 'Sản phẩm có bao bì nổi bật',
                    },
                    '900002|net|CONVENIENCE': {
                        '6': 'Cách sử dụng đơn giản, dễ dàng',
                        '7': 'Sản phẩm có nhiều kích thước phù hợp với mục đích sử dụng',
                        '8': 'Sản phẩm có cách dùng trực tiếp, không chế biến phức tạp',
                    },
                    '900003|net|HEALTH': {
                        '9': 'Sản phẩm được làm từ các thành phần tự nhiên, tốt/ an toàn cho sức khỏe',
                        '10': 'Sản phẩm có hàm lượng dinh dưỡng (chất béo, đạm, calories,...) cân bằng',
                        '11': 'Sản phẩm được phát triển theo các xu hướng dinh dưỡng mới (có nguồn gốc từ thực vật, kiêng lactose/ gluten, …)',
                        '12': 'Sản phẩm có ít đường/ ít calories so với các sản phẩm cùng loại',
                        '13': 'Sản phẩm được bảo trợ bởi một tổ chức, cá nhân độc lập có uy tín (viện dinh dưỡng, bác sĩ, người nổi tiếng)',
                        '14': 'Sản phẩm được làm từ các thành phần mới lạ',
                    },
                    '900004|net|TRENDY LIFE': {
                        '15': 'Sản phẩm có hương vị sáng tạo, độc đáo',
                        '16': 'Sản phẩm có hình thức và cách sử dụng mới lạ, độc đáo',
                        '17': 'Sản phẩm có bao bì đẹp, bắt mắt, có thể sưu tầm',
                    },
                    '900005|net|VALUE FOR MONEY': {
                        '18': 'Sản phẩm có chất lượng vượt trội hơn so với mức giá bỏ ra',
                    },
                },
                '19': 'Không điều nào ở trên'
            },
            'Q2_5_[0-9]+': {
                'net_code': {
                    '900001|net|PREMIUM': {
                        '1': 'Sản phẩm sang trọng, đẳng cấp',
                        '2': 'Sản phẩm làm từ nguyên liệu cao cấp, chất lượng cao',
                        '3': 'Sản phẩm ngoại nhập',
                        '4': 'Sản phẩm có bao bì trẻ trung, gần gũi',
                    },
                    '900002|net|CONVENIENCE': {
                        '5': 'Sản phẩm giúp tiết kiệm thời gian khi sử dụng',
                        '6': 'Sản phẩm có nhiều kích thước phù hợp với mục đích sử dụng',
                        '7': 'Các loại đồ uống truyền thống nhưng được đóng lon, chai có nhãn hiệu',
                    },
                    '900003|net|HEALTH': {
                        '8': 'Sản phẩm được làm từ các thành phần tự nhiên, tốt/ an toàn cho sức khỏe',
                        '9': 'Sản phẩm có ít đường/ ít calories so với các sản phẩm cùng loại',
                        '10': 'Sản phẩm giúp tăng cường sức đề kháng, bổ sung vi chất ....',
                    },
                    '900004|net|TRENDY LIFE': {
                        '11': 'Sản phẩm có sự sáng tạo, độc đáo về mặt hương vị',
                    },
                    '900005|net|VALUE FOR MONEY': {
                        '12': 'Sản phẩm có chất lượng vượt trội hơn so với mức giá bỏ ra',
                    },
                },
                '13': 'Không điều nào ở trên'
            },
            'Q2_6_[0-9]+': {
                'net_code': {
                    '900001|net|PREMIUM': {
                        '1': 'Sản phẩm có thiết kế sang trọng, cao cấp',
                        '2': 'Sản phẩm làm từ nguyên liệu cao cấp, chất lượng cao',
                        '3': 'Sản phẩm ngoại nhập',
                        '4': 'Sản phẩm được người nổi tiếng, nhân vật có ảnh hưởng lớn sử dụng',
                    },
                    '900002|net|CONVENIENCE': {
                        '5': 'Sản phẩm phù hợp với nhiều lứa tuổi, đối tượng',
                        '6': 'Sản phẩm có nhiều kích thước phù hợp với mục đích sử dụng',
                        '7': 'Sản phẩm có bao bì dạng bom/ keg, cho trải nghiệm như ở nhà hàng, quán nhậu',
                    },
                    '900003|net|HEALTH': {
                        '8': 'Sản phẩm được làm từ các thành phần tự nhiên, tốt/ an toàn cho sức khỏe',
                        '9': 'Sản phẩm có hàm lượng dinh dưỡng (chất béo, đạm, calories,...) cân bằng',
                        '10': 'Sản phẩm hạn chế có chứa các chất gây hại',
                    },
                    '900004|net|TRENDY LIFE': {
                        '11': 'Sản phẩm được làm từ các thành phần mới lạ',
                        '12': 'Sản phẩm có sự sáng tạo, độc đáo về mặt hương vị',
                        '13': 'Sản phẩm luôn có những phiên bản độc đáo nhờ kết hợp với các nhãn hiệu khác',
                    },
                    '900005|net|VALUE FOR MONEY': {
                        '14': 'Sản phẩm có chất lượng vượt trội hơn so với mức giá bỏ ra',
                        '15': 'Đi kèm các dịch vụ, tiện ích khác (giao tận nhà, tư vấn, bảo hành, lắp đặt tận nơi,...)',
                    },
                },
                '16': 'Không điều nào ở trên'
            },
            'Q2_7_[0-9]+': {
                'net_code': {
                    '900001|net|PREMIUM': {
                        '1': 'Sản phẩm có bao bì tối giản',
                        '2': 'Sản phẩm có thiết kế và cách sử dụng mới lạ, độc đáo',
                        '3': 'Sản phẩm có nhiều thành phần cà phê thật hơn',
                        '4': 'Sản phẩm có bao bì trẻ trung, gần gũi',
                    },
                    '900002|net|CONVENIENCE': {
                        '5': 'Cách sử dụng đơn giản, dễ dàng',
                        '6': 'Sản phẩm giúp tiết kiệm thời gian khi sử dụng',
                        '7': 'Sản phẩm có nhiều kích thước phù hợp với mục đích sử dụng',
                        '8': 'Sản phẩm cho phép tùy chỉnh theo ý thích, khẩu vị một cách dễ dàng',
                    },
                    '900003|net|HEALTH': {
                        '9': 'Sản phẩm được làm từ các thành phần tự nhiên, tốt/ an toàn cho sức khỏe',
                        '10': 'Sản phẩm có hàm lượng dinh dưỡng (chất béo, đạm, calories,...) cân bằng',
                        '11': 'Sản phẩm được phát triển theo các xu hướng dinh dưỡng mới (có nguồn gốc từ thực vật, kiêng lactose/ gluten, …)',
                        '12': 'Sản phẩm giúp tăng cường sức đề kháng, bổ sung vi chất ....',
                        '13': 'Sản phẩm có tính chất tăng cường sự tập trung',
                    },
                    '900004|net|TRENDY LIFE': {
                        '14': 'Sản phẩm được làm từ các thành phần mới lạ',
                        '15': 'Sản phẩm có sự sáng tạo, độc đáo về mặt hương vị',
                        '16': 'Sản phẩm có bao bì đẹp, bắt mắt, có thể sưu tầm',
                    },
                    '900005|net|VALUE FOR MONEY': {
                        '17': 'Sản phẩm có chất lượng vượt trội hơn so với mức giá bỏ ra',
                    },
                },
                '18': 'Không điều nào ở trên'
            },
            'Q3_[0-9]+': {
                'net_code': {
                    '900001|net|ĐƯỢC GIỚI THIỆU': {
                        '1': 'Bạn bè/ người thân/ đồng nghiệp giới thiệu',
                        '2': 'Người bán tạp hóa giới thiệu',
                        '3': 'Được đề xuất/ đánh giá bởi những người nổi tiếng',
                    },
                    '900002|net|SẢN PHẨM BÀY BÁN': {
                        '4': 'Sản phẩm trưng bày ở siêu thị/ cửa hàng tiện lợi',
                        '5': 'Sản phẩm trưng bày ở tạp hóa',
                    },
                    '900003|net|VẬT DỤNG QUẢNG CÁO': {
                        '6': 'Bảng quảng cáo ngoài trời',
                        '7': 'POSM/ Banner ở cửa hàng tiện lợi',
                        '8': 'POSM/ Banner ở tạp hóa',
                        '9': 'Quảng cáo tại thang máy của các trung tâm thương mại (Cresent Mall, Vạn Hạnh Mall, ...)',
                    },
                    '900004|net|DIGITAL': {
                        '10': 'Quảng cáo trên các ứng dụng xem phim trực tuyến (FPT play, Vieon, Netflix, …)',
                        '11': 'Clip quảng cáo trên mạng xã hội (Facebook, Zalo, Youtube, Tiktok)',
                        '12': 'Bài đăng/ clip review trên mạng xã hội (Facebook, Zalo, Youtube, Tiktok)',
                    },
                    '900005|net|Quảng cáo trên các trang thương mại điện tử (Lazada, Tiki, Shopee, ...)': {
                        '13': 'Quảng cáo trên các trang thương mại điện tử (Lazada, Tiki, Shopee, ...)',
                    },
                    '900006|net|Quảng cáo trên kênh truyền hình': {
                        '14': 'Quảng cáo trên kênh truyền hình',
                    },
                    '900007|net|Quảng cáo tại rạp chiếu phim': {
                        '15': 'Quảng cáo tại rạp chiếu phim',
                    },
                },
                '16': 'Không điều nào ở trên'
            },
            'Q4': {
                'net_code': {
                    '900001|net|ĐƯỢC GIỚI THIỆU': {
                        '1': 'Bạn bè/ người thân/ đồng nghiệp giới thiệu',
                        '2': 'Người bán tạp hóa giới thiệu',
                        '3': 'Được đề xuất/ đánh giá bởi những người nổi tiếng',
                    },
                    '900002|net|SẢN PHẨM BÀY BÁN': {
                        '4': 'Sản phẩm trưng bày ở siêu thị/ cửa hàng tiện lợi',
                        '5': 'Sản phẩm trưng bày ở tạp hóa',
                    },
                    '900003|net|VẬT DỤNG QUẢNG CÁO': {
                        '6': 'Bảng quảng cáo ngoài trời',
                        '7': 'POSM/ Banner ở cửa hàng tiện lợi',
                        '8': 'POSM/ Banner ở tạp hóa',
                        '9': 'Quảng cáo tại thang máy của các trung tâm thương mại (Cresent Mall, Vạn Hạnh Mall, ...)',
                    },
                    '900004|net|DIGITAL': {
                        '10': 'Quảng cáo trên các ứng dụng xem phim trực tuyến (FPT play, Vieon, Netflix, …)',
                        '11': 'Clip quảng cáo trên mạng xã hội (Facebook, Zalo, Youtube, Tiktok)',
                        '12': 'Bài đăng/ clip review trên mạng xã hội (Facebook, Zalo, Youtube, Tiktok)',
                    },
                    '900005|net|Quảng cáo trên các trang thương mại điện tử (Lazada, Tiki, Shopee, ...)': {
                        '13': 'Quảng cáo trên các trang thương mại điện tử (Lazada, Tiki, Shopee, ...)',
                    },
                    '900006|net|Quảng cáo trên kênh truyền hình': {
                        '14': 'Quảng cáo trên kênh truyền hình',
                    },
                    '900007|net|Quảng cáo tại rạp chiếu phim': {
                        '15': 'Quảng cáo tại rạp chiếu phim',
                    },
                },
                '16': 'Không điều nào ở trên'
            }

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

            lst_func_to_run = eval(tables_format_file.file.read())
            self.run_tables_by_js_files(lst_func_to_run)
            self.format_sig_table()
        # End Export data tables----------------------------------------------------------------------------------------

        # Generate SAV files--------------------------------------------------------------------------------------------
        self.logger.info('Generate SAV files')

        # Remove net_code to export sav---------------------------------------------------------------------------------
        df_info_without_net = df_info.copy()

        for idx in df_info_without_net.index:
            val_lbl = df_info_without_net.at[idx, 'val_lbl']

            if 'net_code' in val_lbl.keys():
                df_info_without_net.at[idx, 'val_lbl'] = self.unnetted_qre_val(val_lbl)
        # END Remove net_code to export sav-----------------------------------------------------------------------------

        dict_dfs = {
            1: {
                'data': df_data,
                'info': df_info_without_net,
                'tail_name': '',
                'sheet_name': '',
                'is_recode_to_lbl': True,
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


