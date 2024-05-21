from app.classes.AP_DataConverter import APDataConverter
from app.routers.Online_Survey.export_online_survey_data_table import DataTableGenerator
from app.routers.Online_Survey.convert_unstack import ConvertUnstack
import pandas as pd
import numpy as np
import time



class VN8274McKinseyVNSME(APDataConverter, DataTableGenerator, ConvertUnstack):

    def convert_vn8274_mckinsey_vn_sme(self, py_script_file, tables_format_file, codelist_file, coding_file):

        start_time = time.time()

        df_data_output, df_info_output = self.convert_df_md()

        df_data_output['Payment_share_1'] = [float(a.replace('%', '')) for a in df_data_output['Payment_share_1']]
        df_data_output['Payment_share_2'] = [float(a.replace('%', '')) for a in df_data_output['Payment_share_2']]

        df_data_output['ResAge'] = df_data_output['ResAge'].astype(int)
        df_data_output['Payment_share_1'] = df_data_output['Payment_share_1'].astype(float)
        df_data_output['Payment_share_2'] = df_data_output['Payment_share_2'].astype(float)

        df_info_output.loc[df_info_output['var_name'].isin(['ResAge', 'Payment_share_1', 'Payment_share_2']), 'var_type'] = ['NUM']

        dict_rename = {
            'QPREMIUM_1': 'QPREMIUM_Old_1',
            'QPREMIUM_2': 'QPREMIUM_Old_2',
            'QPREMIUM_3': 'QPREMIUM_Old_3',
            'QPREMIUM_4': 'QPREMIUM_Old_4',
            'QPREMIUM_5': 'QPREMIUM_Old_5',
            'QPREMIUM_6': 'QPREMIUM_Old_6',
            'QPREMIUM_7': 'QPREMIUM_Old_7',
            'QPREMIUM_8': 'QPREMIUM_Old_8',
            'QPREMIUM_9': 'QPREMIUM_Old_9',
            'QPREMIUM_10': 'QPREMIUM_Old_10',
            'QPREMIUM_11': 'QPREMIUM_Old_11',
            'QPREMIUM_12': 'QPREMIUM_Old_12',
            'QPREMIUM_13': 'QPREMIUM_Old_13',
        }

        df_data_output.rename(columns=dict_rename, inplace=True)
        df_info_output['var_name'].replace(dict_rename, inplace=True)

        # Define structure----------------------------------------------------------------------------------------------
        self.logger.info('Define structure')

        dict_qre_group_mean = {
            'ATTQ': {
                'range': [f'0{i}' if i <= 9 else i for i in range(1, 36)],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {1: 1, 2: 1, 5: 2, 6: 2}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5, 6: 6}
            },

            'QWTP_Solution': {
                'range': [f'{i}_0{j}' for i in range(1, 17) for j in range(1, 4)],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {1: 1, 2: 1, 5: 2, 6: 2}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5, 6: 6}
            },

            'QBASIC1': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {4: 1, 5: 1, 1: 2, 2: 2}},
                'mean': {1: 5, 2: 4, 3: 3, 4: 2, 5: 1}
            },
            'QBASIC2': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {4: 1, 5: 1, 1: 2, 2: 2}},
                'mean': {1: 5, 2: 4, 3: 3, 4: 2, 5: 1}
            },
            'QBASIC3': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {4: 1, 5: 1, 1: 2, 2: 2}},
                'mean': {1: 5, 2: 4, 3: 3, 4: 2, 5: 1}
            },
            'QBASIC4': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {4: 1, 5: 1, 1: 2, 2: 2}},
                'mean': {1: 5, 2: 4, 3: 3, 4: 2, 5: 1}
            },
            'QBASIC5': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {4: 1, 5: 1, 1: 2, 2: 2}},
                'mean': {1: 5, 2: 4, 3: 3, 4: 2, 5: 1}
            },

            'QADVANCED1': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {4: 1, 5: 1, 1: 2, 2: 2}},
                'mean': {1: 5, 2: 4, 3: 3, 4: 2, 5: 1}
            },
            'QADVANCED2': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {4: 1, 5: 1, 1: 2, 2: 2}},
                'mean': {1: 5, 2: 4, 3: 3, 4: 2, 5: 1}
            },
            'QADVANCED3': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {4: 1, 5: 1, 1: 2, 2: 2}},
                'mean': {1: 5, 2: 4, 3: 3, 4: 2, 5: 1}
            },
            'QADVANCED4': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {4: 1, 5: 1, 1: 2, 2: 2}},
                'mean': {1: 5, 2: 4, 3: 3, 4: 2, 5: 1}
            },
            'QADVANCED5': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {4: 1, 5: 1, 1: 2, 2: 2}},
                'mean': {1: 5, 2: 4, 3: 3, 4: 2, 5: 1}
            },

            'QPREMIUM1': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {4: 1, 5: 1, 1: 2, 2: 2}},
                'mean': {1: 5, 2: 4, 3: 3, 4: 2, 5: 1}
            },
            'QPREMIUM2': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {4: 1, 5: 1, 1: 2, 2: 2}},
                'mean': {1: 5, 2: 4, 3: 3, 4: 2, 5: 1}
            },
            'QPREMIUM3': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {4: 1, 5: 1, 1: 2, 2: 2}},
                'mean': {1: 5, 2: 4, 3: 3, 4: 2, 5: 1}
            },
            'QPREMIUM4': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {4: 1, 5: 1, 1: 2, 2: 2}},
                'mean': {1: 5, 2: 4, 3: 3, 4: 2, 5: 1}
            },
            'QPREMIUM5': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {4: 1, 5: 1, 1: 2, 2: 2}},
                'mean': {1: 5, 2: 4, 3: 3, 4: 2, 5: 1}
            },

            'QCLOSING1': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'T2B'}, 'recode': {4: 1, 5: 1, 1: 2, 2: 2}},
                'mean': {1: 5, 2: 4, 3: 3, 4: 2, 5: 1}
            },


        }

        dict_qre_new_vars_info = {
            'QBASIC_1': ['You will get a basic POS, MOA and business solution package. Please select one growth solution.', 'MA', {'1': 'Basic mobile-based POS app (for phone and tablet) that can accept contactless card. Replace EDC with 0 upfront cost'}],
            'QBASIC_2': ['You will get a basic POS, MOA and business solution package. Please select one growth solution.', 'MA', {'1': 'Advanced mobile-based POS app (for phone and tablet) that can accept contactless cards, bank transfer, VietQR on mobile banking, dynamic e-wallet QR, integrated digital invoice. Replace traditional POS & EDC'}],
            'QBASIC_3': ['You will get a basic POS, MOA and business solution package. Please select one growth solution.', 'MA', {'1': 'Physical smart POS terminal that have all payment acceptance schemes and integrated barcode scanner and receipt printer'}],
            'QBASIC_4': ['You will get a basic POS, MOA and business solution package. Please select one growth solution.', 'MA', {'1': 'Basic operating account to manage cashflow, receivables and payables'}],
            'QBASIC_5': ['You will get a basic POS, MOA and business solution package. Please select one growth solution.', 'MA', {'1': 'Advanced operating account with automated payables and automated reconciliation for taxes and insurances'}],
            'QBASIC_6': ['You will get a basic POS, MOA and business solution package. Please select one growth solution.', 'MA', {'1': 'Master merchant operating account with automated workflow and aggregation for multiple accounts'}],
            'QBASIC_7': ['You will get a basic POS, MOA and business solution package. Please select one growth solution.', 'MA', {'1': 'Basic business solutions integrated to POS (product catalog, order management, accounting integration)'}],
            'QBASIC_8': ['You will get a basic POS, MOA and business solution package. Please select one growth solution.', 'MA', {'1': 'Industry specific solutions'}],
            'QBASIC_9': ['You will get a basic POS, MOA and business solution package. Please select one growth solution.', 'MA', {'1': 'Advanced solutions for business with multiple employees and complex operations'}],
            'QBASIC_10': ['You will get a basic POS, MOA and business solution package. Please select one growth solution.', 'MA', {'1': 'Business integrations'}],
            'QBASIC_11': ['You will get a basic POS, MOA and business solution package. Please select one growth solution.', 'MA', {'1': 'Single loyalty program and rewards for customers'}],
            'QBASIC_12': ['You will get a basic POS, MOA and business solution package. Please select one growth solution.', 'MA', {'1': 'Cross-merchant loyalty program for customers'}],
            'QBASIC_13': ['You will get a basic POS, MOA and business solution package. Please select one growth solution.', 'MA', {'1': 'Digital marketing integration (Facebook, Instagram, Tiktok, Google)'}],
            'QBASIC_14': ['You will get a basic POS, MOA and business solution package. Please select one growth solution.', 'MA', {'1': 'Expanded digital marketing channel (e.g., ads on a 3rd party platform or consumer app)'}],
            'QBASIC_15': ['You will get a basic POS, MOA and business solution package. Please select one growth solution.', 'MA', {'1': 'Instant approval and 24hour disbursement merchant cash advance'}],
            'QBASIC_16': ['You will get a basic POS, MOA and business solution package. Please select one growth solution.', 'MA', {'1': 'Working capital financing with high limit and attractive rate'}],
            'QADVANCED_1': ['You will get an advanced MOA and early accessibility to financing products. Please select up to 2 different POS options, 2 business solutions, 2 growth solution', 'MA', {'1': 'Basic mobile-based POS app (for phone and tablet) that can accept contactless card. Replace EDC with 0 upfront cost'}],
            'QADVANCED_2': ['You will get an advanced MOA and early accessibility to financing products. Please select up to 2 different POS options, 2 business solutions, 2 growth solution', 'MA', {'1': 'Advanced mobile-based POS app (for phone and tablet) that can accept contactless cards, bank transfer, VietQR on mobile banking, dynamic e-wallet QR, integrated digital invoice. Replace traditional POS & EDC'}],
            'QADVANCED_3': ['You will get an advanced MOA and early accessibility to financing products. Please select up to 2 different POS options, 2 business solutions, 2 growth solution', 'MA', {'1': 'Physical smart POS terminal that have all payment acceptance schemes and integrated barcode scanner and receipt printer'}],
            'QADVANCED_4': ['You will get an advanced MOA and early accessibility to financing products. Please select up to 2 different POS options, 2 business solutions, 2 growth solution', 'MA', {'1': 'Basic operating account to manage cashflow, receivables and payables'}],
            'QADVANCED_5': ['You will get an advanced MOA and early accessibility to financing products. Please select up to 2 different POS options, 2 business solutions, 2 growth solution', 'MA', {'1': 'Advanced operating account with automated payables and automated reconciliation for taxes and insurances'}],
            'QADVANCED_6': ['You will get an advanced MOA and early accessibility to financing products. Please select up to 2 different POS options, 2 business solutions, 2 growth solution', 'MA', {'1': 'Master merchant operating account with automated workflow and aggregation for multiple accounts'}],
            'QADVANCED_7': ['You will get an advanced MOA and early accessibility to financing products. Please select up to 2 different POS options, 2 business solutions, 2 growth solution', 'MA', {'1': 'Basic business solutions integrated to POS (product catalog, order management, accounting integration)'}],
            'QADVANCED_8': ['You will get an advanced MOA and early accessibility to financing products. Please select up to 2 different POS options, 2 business solutions, 2 growth solution', 'MA', {'1': 'Industry specific solutions'}],
            'QADVANCED_9': ['You will get an advanced MOA and early accessibility to financing products. Please select up to 2 different POS options, 2 business solutions, 2 growth solution', 'MA', {'1': 'Advanced solutions for business with multiple employees and complex operations'}],
            'QADVANCED_10': ['You will get an advanced MOA and early accessibility to financing products. Please select up to 2 different POS options, 2 business solutions, 2 growth solution', 'MA', {'1': 'Business integrations'}],
            'QADVANCED_11': ['You will get an advanced MOA and early accessibility to financing products. Please select up to 2 different POS options, 2 business solutions, 2 growth solution', 'MA', {'1': 'Single loyalty program and rewards for customers'}],
            'QADVANCED_12': ['You will get an advanced MOA and early accessibility to financing products. Please select up to 2 different POS options, 2 business solutions, 2 growth solution', 'MA', {'1': 'Cross-merchant loyalty program for customers'}],
            'QADVANCED_13': ['You will get an advanced MOA and early accessibility to financing products. Please select up to 2 different POS options, 2 business solutions, 2 growth solution', 'MA', {'1': 'Digital marketing integration (Facebook, Instagram, Tiktok, Google)'}],
            'QADVANCED_14': ['You will get an advanced MOA and early accessibility to financing products. Please select up to 2 different POS options, 2 business solutions, 2 growth solution', 'MA', {'1': 'Expanded digital marketing channel (e.g., ads on a 3rd party platform or consumer app)'}],
            'QADVANCED_15': ['You will get an advanced MOA and early accessibility to financing products. Please select up to 2 different POS options, 2 business solutions, 2 growth solution', 'MA', {'1': 'Instant approval and 24hour disbursement merchant cash advance'}],
            'QADVANCED_16': ['You will get an advanced MOA and early accessibility to financing products. Please select up to 2 different POS options, 2 business solutions, 2 growth solution', 'MA', {'1': 'Working capital financing with high limit and attractive rate'}],
            'QPREMIUM_1': ['You will get a master merchant account, perks for financing products, up to 10 POS devices, unlimited business and growth solutions.', 'MA', {'1': 'Basic mobile-based POS app (for phone and tablet) that can accept contactless card. Replace EDC with 0 upfront cost'}],
            'QPREMIUM_2': ['You will get a master merchant account, perks for financing products, up to 10 POS devices, unlimited business and growth solutions.', 'MA', {'1': 'Advanced mobile-based POS app (for phone and tablet) that can accept contactless cards, bank transfer, VietQR on mobile banking, dynamic e-wallet QR, integrated digital invoice. Replace traditional POS & EDC'}],
            'QPREMIUM_3': ['You will get a master merchant account, perks for financing products, up to 10 POS devices, unlimited business and growth solutions.', 'MA', {'1': 'Physical smart POS terminal that have all payment acceptance schemes and integrated barcode scanner and receipt printer'}],
            'QPREMIUM_4': ['You will get a master merchant account, perks for financing products, up to 10 POS devices, unlimited business and growth solutions.', 'MA', {'1': 'Basic operating account to manage cashflow, receivables and payables'}],
            'QPREMIUM_5': ['You will get a master merchant account, perks for financing products, up to 10 POS devices, unlimited business and growth solutions.', 'MA', {'1': 'Advanced operating account with automated payables and automated reconciliation for taxes and insurances'}],
            'QPREMIUM_6': ['You will get a master merchant account, perks for financing products, up to 10 POS devices, unlimited business and growth solutions.', 'MA', {'1': 'Master merchant operating account with automated workflow and aggregation for multiple accounts'}],
            'QPREMIUM_7': ['You will get a master merchant account, perks for financing products, up to 10 POS devices, unlimited business and growth solutions.', 'MA', {'1': 'Basic business solutions integrated to POS (product catalog, order management, accounting integration)'}],
            'QPREMIUM_8': ['You will get a master merchant account, perks for financing products, up to 10 POS devices, unlimited business and growth solutions.', 'MA', {'1': 'Industry specific solutions'}],
            'QPREMIUM_9': ['You will get a master merchant account, perks for financing products, up to 10 POS devices, unlimited business and growth solutions.', 'MA', {'1': 'Advanced solutions for business with multiple employees and complex operations'}],
            'QPREMIUM_10': ['You will get a master merchant account, perks for financing products, up to 10 POS devices, unlimited business and growth solutions.', 'MA', {'1': 'Business integrations'}],
            'QPREMIUM_11': ['You will get a master merchant account, perks for financing products, up to 10 POS devices, unlimited business and growth solutions.', 'MA', {'1': 'Single loyalty program and rewards for customers'}],
            'QPREMIUM_12': ['You will get a master merchant account, perks for financing products, up to 10 POS devices, unlimited business and growth solutions.', 'MA', {'1': 'Cross-merchant loyalty program for customers'}],
            'QPREMIUM_13': ['You will get a master merchant account, perks for financing products, up to 10 POS devices, unlimited business and growth solutions.', 'MA', {'1': 'Digital marketing integration (Facebook, Instagram, Tiktok, Google)'}],
            'QPREMIUM_14': ['You will get a master merchant account, perks for financing products, up to 10 POS devices, unlimited business and growth solutions.', 'MA', {'1': 'Expanded digital marketing channel (e.g., ads on a 3rd party platform or consumer app)'}],
            'QPREMIUM_15': ['You will get a master merchant account, perks for financing products, up to 10 POS devices, unlimited business and growth solutions.', 'MA', {'1': 'Instant approval and 24hour disbursement merchant cash advance'}],
            'QPREMIUM_16': ['You will get a master merchant account, perks for financing products, up to 10 POS devices, unlimited business and growth solutions.', 'MA', {'1': 'Working capital financing with high limit and attractive rate'}],
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

        # if dict_qre_OE_info:
        #
        #     # ADD OE to Data stack--------------------------------------------------------------------------------------
        #     lst_OE_col = list(dict_qre_OE_info.keys())
        #     df_data_output[lst_OE_col] = pd.DataFrame([[np.nan] * len(lst_OE_col)], index=df_data_output.index)
        #
        #     # Remember edit this
        #     for item in lst_addin_OE_value:
        #         df_data_output.loc[(df_data_output[item[0]] == item[1]) & (df_data_output[item[2]] == item[3]), [item[4]]] = [item[5]]
        #
        #     df_data_output.loc[df_data_output['S13'] == 1, ['S13_OE_1']] = [999]
        #
        #     # END ADD OE to Data stack----------------------------------------------------------------------------------
        #
        #     # ADD OE to Info stack--------------------------------------------------------------------------------------
        #     df_qres_info_output = pd.concat([df_qres_info_output,
        #                                      pd.DataFrame(columns=['var_name', 'var_lbl', 'var_type', 'val_lbl'],
        #                                                   data=list(dict_qre_OE_info.values()))], axis=0)
        #
        #     df_qres_info_output.reset_index(drop=True, inplace=True)
        #
        #     # END ADD OE to Info stack----------------------------------------------------------------------------------

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

        # ADD NEW VARS--------------------------------------------------------------------------------------------------
        lst_qre_info_addin = list()
        for key, val in dict_qre_new_vars_info.items():
            lst_qre_info_addin.append([key, val[0], val[1], val[2]])

        df_info_output = pd.concat([df_info_output, pd.DataFrame(columns=['var_name', 'var_lbl', 'var_type', 'val_lbl'], data=lst_qre_info_addin)], axis=0)
        df_data_output = pd.concat([df_data_output, pd.DataFrame(columns=list(dict_qre_new_vars_info.keys()), data=[])], axis=1)

        df_info_output.reset_index(drop=True, inplace=True)
        df_data_output.reset_index(drop=True, inplace=True)

        lst_query = [
            # QBASIC
            ['Location > 0', 'QBASIC_1', 1],
            ['Location > 0', 'QBASIC_4', 1],
            ['Location > 0', 'QBASIC_7', 1],
            ['QBASIC == 1', 'QBASIC_11', 1],
            ['QBASIC == 2', 'QBASIC_12', 1],
            ['QBASIC == 3', 'QBASIC_13', 1],
            ['QBASIC == 4', 'QBASIC_14', 1],

            # QADVANCED
            ['Location > 0', 'QADVANCED_5', 1],
            ['QADVANCED_POS_1 == 1', 'QADVANCED_1', 1],
            ['QADVANCED_POS_2 == 1', 'QADVANCED_2', 1],
            ['QADVANCED_POS_3 == 1', 'QADVANCED_3', 1],
            ['QADVANCED_Business_1 == 1', 'QADVANCED_7', 1],
            ['QADVANCED_Business_2 == 1', 'QADVANCED_8', 1],
            ['QADVANCED_Business_3 == 1', 'QADVANCED_9', 1],
            ['QADVANCED_Business_4 == 1', 'QADVANCED_10', 1],
            ['QADVANCED_Growth_1 == 1', 'QADVANCED_11', 1],
            ['QADVANCED_Growth_2 == 1', 'QADVANCED_12', 1],
            ['QADVANCED_Growth_3 == 1', 'QADVANCED_13', 1],
            ['QADVANCED_Growth_4 == 1', 'QADVANCED_14', 1],

            # QPREMIUM
            ['Location > 0', 'QPREMIUM_6', 1],
            ['QPREMIUM_Old_1 == 1', 'QPREMIUM_1', 1],
            ['QPREMIUM_Old_2 == 1', 'QPREMIUM_2', 1],
            ['QPREMIUM_Old_3 == 1', 'QPREMIUM_3', 1],
            ['QPREMIUM_Old_4 == 1', 'QPREMIUM_7', 1],
            ['QPREMIUM_Old_5 == 1', 'QPREMIUM_8', 1],
            ['QPREMIUM_Old_6 == 1', 'QPREMIUM_9', 1],
            ['QPREMIUM_Old_7 == 1', 'QPREMIUM_10', 1],
            ['QPREMIUM_Old_8 == 1', 'QPREMIUM_11', 1],
            ['QPREMIUM_Old_9 == 1', 'QPREMIUM_12', 1],
            ['QPREMIUM_Old_10 == 1', 'QPREMIUM_13', 1],
            ['QPREMIUM_Old_11 == 1', 'QPREMIUM_14', 1],
            ['QPREMIUM_Old_12 == 1', 'QPREMIUM_15', 1],
            ['QPREMIUM_Old_13 == 1', 'QPREMIUM_16', 1],
        ]
        for row in lst_query:
            df_query = df_data_output.query(row[0]).copy()
            if not df_query.empty:
                df_data_output.loc[df_query.index, [row[1]]] = [row[2]]

        lst_qre_drop = [
            'QBASIC',
            'QADVANCED_POS_1',
            'QADVANCED_POS_2',
            'QADVANCED_POS_3',
            'QADVANCED_Business_1',
            'QADVANCED_Business_2',
            'QADVANCED_Business_3',
            'QADVANCED_Business_4',
            'QADVANCED_Growth_1',
            'QADVANCED_Growth_2',
            'QADVANCED_Growth_3',
            'QADVANCED_Growth_4',
            'QPREMIUM_Old_1',
            'QPREMIUM_Old_2',
            'QPREMIUM_Old_3',
            'QPREMIUM_Old_4',
            'QPREMIUM_Old_5',
            'QPREMIUM_Old_6',
            'QPREMIUM_Old_7',
            'QPREMIUM_Old_8',
            'QPREMIUM_Old_9',
            'QPREMIUM_Old_10',
            'QPREMIUM_Old_11',
            'QPREMIUM_Old_12',
            'QPREMIUM_Old_13',
        ]

        df_info_output['idx_var'] = df_info_output['var_name']
        df_info_output.set_index('idx_var', inplace=True)
        df_info_output.drop(index=lst_qre_drop, inplace=True)

        df_part_1 = df_info_output.loc['ID': 'QWTP_Solution_16_03', :]

        lst2 = [f'QBASIC_{i}' for i in range(1, 17)] + [f'QBASIC{i}' for i in range(1, 7)]
        df_part_2 = df_info_output.loc[lst2, :]

        lst3 = [f'QADVANCED_{i}' for i in range(1, 17)] + [f'QADVANCED{i}' for i in range(1, 7)]
        df_part_3 = df_info_output.loc[lst3, :]

        lst4 = [f'QPREMIUM_{i}' for i in range(1, 17)] + [f'QPREMIUM{i}' for i in range(1, 7)]
        df_part_4 = df_info_output.loc[lst4, :]

        lst5 = ['QCLOSING1']
        df_part_5 = df_info_output.loc[lst5, :]

        df_info_output = pd.concat([df_part_1, df_part_2, df_part_3, df_part_4, df_part_5], axis=0, ignore_index=True)

        df_data_output.drop(columns=lst_qre_drop, inplace=True)
        lst_qres = df_info_output['var_name'].values.tolist()
        df_data_output = df_data_output.loc[:, lst_qres].reindex(columns=lst_qres)

        df_info_output.reset_index(drop=True, inplace=True)
        df_data_output.reset_index(drop=True, inplace=True)
        # END ADD NEW VARS----------------------------------------------------------------------------------------------

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

        DataTableGenerator.__init__(self, logger=self.logger, df_data=df_data_output, df_info=df_info_output, is_md=True,
                                    xlsx_name=str_topline_file_name, lst_qre_group=lst_qre_group, lst_qre_mean=lst_qre_mean)

        lst_func_to_run = list()
        if tables_format_file.filename:
            lst_func_to_run = eval(tables_format_file.file.read())

        self.run_tables_by_js_files(lst_func_to_run)

        self.format_sig_table()

        # End Export data tables----------------------------------------------------------------------------------------

        self.logger.info('Generate SAV files')

        # Remove net_code to export sav---------------------------------------------------------------------------------
        df_info_to_sav = df_info_output.copy()
        df_data_to_sav = df_data_output.copy()

        for idx in df_info_to_sav.index:
            var_name = df_info_to_sav.at[idx, 'var_name']
            var_lbl = df_info_to_sav.at[idx, 'var_lbl']
            var_type = df_info_to_sav.at[idx, 'var_type']
            val_lbl = df_info_to_sav.at[idx, 'val_lbl']

            if 'net_code' in val_lbl.keys():
                df_info_to_sav.at[idx, 'val_lbl'] = self.unnetted_qre_val(val_lbl)

            # McKinsey only - formatting MA question---------------------------------------------------------------------
            if var_type in ['MA']:
                df_info_to_sav.at[idx, 'var_lbl'] = f"{val_lbl['1']} - {var_lbl}"
                df_info_to_sav.at[idx, 'val_lbl'] = {'1': 'Yes', '0': 'No'}

                df_data_to_sav[var_name].replace({np.nan: 0}, inplace=True)

            # END McKinsey only - formatting MA question----------------------------------------------------------------

        # END Remove net_code to export sav-----------------------------------------------------------------------------

        lst_qres = df_info_to_sav['var_name'].values.tolist()
        df_data_to_sav = df_data_to_sav.loc[:, lst_qres].reindex(columns=lst_qres)

        # df_info_output.to_csv('zzz_df_info_output_vn8274.csv')
        # df_data_output.to_csv('zzz_df_data_output_vn8274.csv')
        # df_info_to_sav.to_csv('zzz_df_info_to_sav_vn8274.csv')
        # df_data_to_sav.to_csv('zzz_df_data_to_sav_vn8274.csv')

        self.generate_sav_sps(df_data=df_data_to_sav, df_qres_info=df_info_to_sav, is_md=True, is_export_xlsx=True)

        self.logger.warning('DATA PROCESSING COMPLETED in %d seconds' % (time.time() - start_time))

