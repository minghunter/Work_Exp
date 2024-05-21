from app.classes.Logging_Custom_Formatter import Logger
from app.classes.AP_DataConverter import APDataConverter
from app.routers.Online_Survey.export_online_survey_data_table import DataTableGenerator
from app.routers.Online_Survey.convert_unstack import ConvertUnstack
import pandas as pd
import numpy as np


logger = Logger.logger('my-dp')


class VN8228Wakeup(APDataConverter, DataTableGenerator, ConvertUnstack):

    def convert_vn8228_wakeup(self, coding_file):

        df_data_output, df_qres_info_output = self.convert_df_mc()

        logger.info('Combine OE a->b->a to a->b')

        lst_pair = [
            ['Q6a1_1st', 'Q6a2_1st'],
            ['Q6a1_2nd', 'Q6a2_2nd'],
            ['Q6a1_3rd', 'Q6a2_3rd'],
        ]

        for pair in lst_pair:
            l_name = pair[0]
            r_name = pair[1]
            df_data_output[l_name] = [b if pd.isnull(a) else a for a, b in zip(df_data_output[l_name], df_data_output[r_name])]

        logger.info('Recode FC in df_data_output')

        dict_qre_info_addin = {

            'Q11_1st': ['Q11_1st', 'Trong các ý tưởng sản phẩm cà phê bạn vừa đọc, bạn thích sản phẩm cà phê nào nhất? [SA]', 'SA', {'1': 'Yes', '2': 'No'}],
            'Q11_2nd': ['Q11_2nd', 'Trong các ý tưởng sản phẩm cà phê bạn vừa đọc, bạn thích sản phẩm cà phê nào nhất? [SA]', 'SA', {'1': 'Yes', '2': 'No'}],
            'Q11_3rd': ['Q11_3rd', 'Trong các ý tưởng sản phẩm cà phê bạn vừa đọc, bạn thích sản phẩm cà phê nào nhất? [SA]', 'SA', {'1': 'Yes', '2': 'No'}],
            'Q12_1st': ['Q12_1st', 'Vì sao bạn thích ý tưởng _ hơn các ý tưởng sản phẩm khác? [OE]', 'FT', {}],
            'Q12_2nd': ['Q12_2nd', 'Vì sao bạn thích ý tưởng _ hơn các ý tưởng sản phẩm khác? [OE]', 'FT', {}],
            'Q12_3rd': ['Q12_3rd', 'Vì sao bạn thích ý tưởng _ hơn các ý tưởng sản phẩm khác? [OE]', 'FT', {}],
            'Q13_1st_1': ['Q13_1st_1', 'Anh/ chị sẽ muốn uống cà phê của ý tưởng _ ở đâu? [MA]', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q13_1st_2': ['Q13_1st_2', 'Anh/ chị sẽ muốn uống cà phê của ý tưởng _ ở đâu? [MA]', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q13_1st_3': ['Q13_1st_3', 'Anh/ chị sẽ muốn uống cà phê của ý tưởng _ ở đâu? [MA]', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q13_1st_4': ['Q13_1st_4', 'Anh/ chị sẽ muốn uống cà phê của ý tưởng _ ở đâu? [MA]', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q13_1st_5': ['Q13_1st_5', 'Anh/ chị sẽ muốn uống cà phê của ý tưởng _ ở đâu? [MA]', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q13_2nd_1': ['Q13_2nd_1', 'Anh/ chị sẽ muốn uống cà phê của ý tưởng _ ở đâu? [MA]', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q13_2nd_2': ['Q13_2nd_2', 'Anh/ chị sẽ muốn uống cà phê của ý tưởng _ ở đâu? [MA]', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q13_2nd_3': ['Q13_2nd_3', 'Anh/ chị sẽ muốn uống cà phê của ý tưởng _ ở đâu? [MA]', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q13_2nd_4': ['Q13_2nd_4', 'Anh/ chị sẽ muốn uống cà phê của ý tưởng _ ở đâu? [MA]', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q13_2nd_5': ['Q13_2nd_5', 'Anh/ chị sẽ muốn uống cà phê của ý tưởng _ ở đâu? [MA]', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q13_3rd_1': ['Q13_3rd_1', 'Anh/ chị sẽ muốn uống cà phê của ý tưởng _ ở đâu? [MA]', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q13_3rd_2': ['Q13_3rd_2', 'Anh/ chị sẽ muốn uống cà phê của ý tưởng _ ở đâu? [MA]', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q13_3rd_3': ['Q13_3rd_3', 'Anh/ chị sẽ muốn uống cà phê của ý tưởng _ ở đâu? [MA]', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q13_3rd_4': ['Q13_3rd_4', 'Anh/ chị sẽ muốn uống cà phê của ý tưởng _ ở đâu? [MA]', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q13_3rd_5': ['Q13_3rd_5', 'Anh/ chị sẽ muốn uống cà phê của ý tưởng _ ở đâu? [MA]', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q13_1st_o5': ['Q13_1st_o5', 'Anh/ chị sẽ muốn uống cà phê của ý tưởng _ ở đâu? [MA]', 'FT', {}],
            'Q13_2nd_o5': ['Q13_2nd_o5', 'Anh/ chị sẽ muốn uống cà phê của ý tưởng _ ở đâu? [MA]', 'FT', {}],
            'Q13_3rd_o5': ['Q13_3rd_o5', 'Anh/ chị sẽ muốn uống cà phê của ý tưởng _ ở đâu? [MA]', 'FT', {}],
            'Q14_1st': ['Q14_1st', 'Trong các ý tưởng sản phẩm cà phê bạn vừa đọc, bạn muốn mua sản phẩm nào nhất? [SA]', 'SA', {'1': 'Yes', '2': 'No'}],
            'Q14_2nd': ['Q14_2nd', 'Trong các ý tưởng sản phẩm cà phê bạn vừa đọc, bạn muốn mua sản phẩm nào nhất? [SA]', 'SA', {'1': 'Yes', '2': 'No'}],
            'Q14_3rd': ['Q14_3rd', 'Trong các ý tưởng sản phẩm cà phê bạn vừa đọc, bạn muốn mua sản phẩm nào nhất? [SA]', 'SA', {'1': 'Yes', '2': 'No'}],
            'Q15_1st': ['Q15_1st', 'Trong các ý tưởng sản phẩm cà phê bạn vừa đọc, bạn muốn dùng sản phẩm cà phê nào thường xuyên nhất? [SA]', 'SA', {'1': 'Yes', '2': 'No'}],
            'Q15_2nd': ['Q15_2nd', 'Trong các ý tưởng sản phẩm cà phê bạn vừa đọc, bạn muốn dùng sản phẩm cà phê nào thường xuyên nhất? [SA]', 'SA', {'1': 'Yes', '2': 'No'}],
            'Q15_3rd': ['Q15_3rd', 'Trong các ý tưởng sản phẩm cà phê bạn vừa đọc, bạn muốn dùng sản phẩm cà phê nào thường xuyên nhất? [SA]', 'SA', {'1': 'Yes', '2': 'No'}],

            # Add new

            'S8_Mean': ['S8_Mean', 'S8. Mean', 'NUM', {}],

            'Q13_Concept_1_1': ['Q13_Concept_1_1', 'Anh/ chị sẽ muốn uống cà phê của ý tưởng Concept 1 ở đâu? [MA]', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q13_Concept_1_2': ['Q13_Concept_1_2', 'Anh/ chị sẽ muốn uống cà phê của ý tưởng Concept 1 ở đâu? [MA]', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q13_Concept_1_3': ['Q13_Concept_1_3', 'Anh/ chị sẽ muốn uống cà phê của ý tưởng Concept 1 ở đâu? [MA]', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q13_Concept_1_4': ['Q13_Concept_1_4', 'Anh/ chị sẽ muốn uống cà phê của ý tưởng Concept 1 ở đâu? [MA]', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q13_Concept_1_5': ['Q13_Concept_1_5', 'Anh/ chị sẽ muốn uống cà phê của ý tưởng Concept 1 ở đâu? [MA]', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q13_Concept_2_1': ['Q13_Concept_2_1', 'Anh/ chị sẽ muốn uống cà phê của ý tưởng Concept 2 ở đâu? [MA]', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q13_Concept_2_2': ['Q13_Concept_2_2', 'Anh/ chị sẽ muốn uống cà phê của ý tưởng Concept 2 ở đâu? [MA]', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q13_Concept_2_3': ['Q13_Concept_2_3', 'Anh/ chị sẽ muốn uống cà phê của ý tưởng Concept 2 ở đâu? [MA]', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q13_Concept_2_4': ['Q13_Concept_2_4', 'Anh/ chị sẽ muốn uống cà phê của ý tưởng Concept 2 ở đâu? [MA]', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q13_Concept_2_5': ['Q13_Concept_2_5', 'Anh/ chị sẽ muốn uống cà phê của ý tưởng Concept 2 ở đâu? [MA]', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q13_Concept_3_1': ['Q13_Concept_3_1', 'Anh/ chị sẽ muốn uống cà phê của ý tưởng Concept 3 ở đâu? [MA]', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q13_Concept_3_2': ['Q13_Concept_3_2', 'Anh/ chị sẽ muốn uống cà phê của ý tưởng Concept 3 ở đâu? [MA]', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q13_Concept_3_3': ['Q13_Concept_3_3', 'Anh/ chị sẽ muốn uống cà phê của ý tưởng Concept 3 ở đâu? [MA]', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q13_Concept_3_4': ['Q13_Concept_3_4', 'Anh/ chị sẽ muốn uống cà phê của ý tưởng Concept 3 ở đâu? [MA]', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q13_Concept_3_5': ['Q13_Concept_3_5', 'Anh/ chị sẽ muốn uống cà phê của ý tưởng Concept 3 ở đâu? [MA]', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],

            'Q2_Net_CFHT_1': ['Q2_Net_CFHT_1', 'Q2. Cà phê hòa tan (NET)', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q2_Net_CFHT_2': ['Q2_Net_CFHT_2', 'Q2. Cà phê hòa tan (NET)', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q2_Net_CFHT_3': ['Q2_Net_CFHT_3', 'Q2. Cà phê hòa tan (NET)', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q2_Net_CFHT_4': ['Q2_Net_CFHT_4', 'Q2. Cà phê hòa tan (NET)', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q2_Net_CFHT_5': ['Q2_Net_CFHT_5', 'Q2. Cà phê hòa tan (NET)', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],

            'Q2_Net_CFRX_1': ['Q2_Net_CFRX_1', 'Q2. Cà phê rang xay (NET)', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q2_Net_CFRX_2': ['Q2_Net_CFRX_2', 'Q2. Cà phê rang xay (NET)', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q2_Net_CFRX_3': ['Q2_Net_CFRX_3', 'Q2. Cà phê rang xay (NET)', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q2_Net_CFRX_4': ['Q2_Net_CFRX_4', 'Q2. Cà phê rang xay (NET)', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q2_Net_CFRX_5': ['Q2_Net_CFRX_5', 'Q2. Cà phê rang xay (NET)', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],

            'Q3_Net_CFHT': ['Q3_Net_CFHT', 'Q3. Cà phê hòa tan (NET)', 'NUM', {}],
            'Q3_Net_CFRX': ['Q3_Net_CFRX', 'Q3. Cà phê rang xay (NET)', 'NUM', {}],

        }

        df_data_output = pd.concat([df_data_output, pd.DataFrame(columns=dict_qre_info_addin.keys(), data=[[np.nan] * len(dict_qre_info_addin.keys())] * df_data_output.shape[0])], axis=1)

        df_data_output.loc[:, ['Q11_1st', 'Q11_2nd', 'Q11_3rd', 'Q14_1st', 'Q14_2nd', 'Q14_3rd', 'Q15_1st', 'Q15_2nd', 'Q15_3rd']] = [[2] * 9] * df_data_output.shape[0]

        for fc_qre in ['Q11', 'Q14', 'Q15']:
            for stt in ['1st', '2nd', '3rd']:
                df_filter = df_data_output.query(f"{fc_qre} == MaConcept_{stt}").copy()
                df_data_output.loc[df_filter.index, f"{fc_qre}_{stt}"] = [1] * df_filter.shape[0]

                if fc_qre == 'Q11':
                    df_data_output.loc[df_filter.index, f"Q12_{stt}"] = df_data_output.loc[df_filter.index, "Q12"]
                    df_data_output.loc[df_filter.index, f"Q13_{stt}_o5"] = df_data_output.loc[df_filter.index, "Q13_o5"]

                    for i in range(1, 6):
                        df_data_output.loc[df_filter.index, f"Q13_{stt}_{i}"] = df_data_output.loc[df_filter.index, f"Q13_{i}"]

        df_data_output['S8_Mean'] = df_data_output['S8']
        df_data_output['S8_Mean'].replace({1: 0, 2: 1, 3: 2, 4: 3, 5: 4, 6: 5, 7: 6, 8: 99}, inplace=True)
        df_data_output['S8_Mean'] = [int(b) if a == 99 else a for a, b in zip(df_data_output['S8_Mean'], df_data_output['S8_o8'])]

        lst_q13_org_col = [f'Q13_{j}' for j in range(1, 6)]
        for i in range(1, 4):
            lst_q13_new_col = [f'Q13_Concept_{i}_{j}' for j in range(1, 6)]

            df_data_output.loc[df_data_output['Q11'] == i, lst_q13_new_col] = df_data_output.loc[df_data_output['Q11'] == i, lst_q13_org_col].values


        for idx in df_data_output.index:

            lst_val = df_data_output.loc[idx, ['Q2_03', 'Q2_04', 'Q2_05']].values.tolist()
            lst_val = list(dict.fromkeys(lst_val))
            lst_val = [x for x in lst_val if ~np.isnan(x)]

            for i in range(len(lst_val)):
                df_data_output.at[idx, f'Q2_Net_CFHT_{i + 1}'] = lst_val[i]

            lst_val = df_data_output.loc[idx, ['Q2_01', 'Q2_02', 'Q2_07', 'Q2_08', 'Q2_09']].values.tolist()
            lst_val = list(dict.fromkeys(lst_val))
            lst_val = [x for x in lst_val if ~np.isnan(x)]

            for i in range(len(lst_val)):
                df_data_output.at[idx, f'Q2_Net_CFRX_{i + 1}'] = lst_val[i]

        df_data_output['Q3_Net_CFHT'] = df_data_output.loc[:, ['Q3_03_o2', 'Q3_04_o2', 'Q3_05_o2']].sum(skipna=True, numeric_only=True, axis=1)
        df_data_output['Q3_Net_CFRX'] = df_data_output.loc[:, ['Q3_01_o2', 'Q3_02_o2', 'Q3_07_o2', 'Q3_08_o2', 'Q3_09_o2']].sum(skipna=True, numeric_only=True, axis=1)

        logger.info('Add new fc val_lbl in df_qres_info_output')

        df_qres_info_output = pd.concat([df_qres_info_output,
                                         pd.DataFrame(columns=['var_name', 'var_lbl', 'var_type', 'val_lbl'],
                                                      data=list(dict_qre_info_addin.values()))], axis=0)
        df_qres_info_output.reset_index(drop=True, inplace=True)

        # df_data_output.to_csv('zzzz_df_data_output.csv', encoding='utf-8-sig')
        # df_qres_info_output.to_csv('zzzz_df_qres_info_output.csv', encoding='utf-8-sig')

        # Define structure----------------------------------------------------------------------------------------------
        logger.info('Define structure')

        id_col = 'ID'
        sp_col = 'MaConcept'

        lst_scr = [
            'S1',
            'S2',
            'S3',
            'S3_Recode',
            'S4',
            'S5',
            'S5_o15',
            'S6',
            'S7_1',
            'S7_2',
            'S7_3',
            'S7_4',
            'S7_5',
            'S7_6',
            'S7_7',
            'S8',
            'S8_Mean',
            'S8_o8',
            'S9',
            'S10',
            'Q1_1',
            'Q1_2',
            'Q1_3',
            'Q1_4',
            'Q1_5',
            'Q1_6',
            'Q1_7',
            'Q1_8',
            'Q1_9',
            'Q1_o9',

            'Q2_Net_CFHT_1',
            'Q2_Net_CFHT_2',
            'Q2_Net_CFHT_3',
            'Q2_Net_CFHT_4',
            'Q2_Net_CFHT_5',

            'Q2_Net_CFRX_1',
            'Q2_Net_CFRX_2',
            'Q2_Net_CFRX_3',
            'Q2_Net_CFRX_4',
            'Q2_Net_CFRX_5',

            'Q2_01',
            'Q2_01_o5',
            'Q2_02',
            'Q2_02_o5',
            'Q2_03',
            'Q2_03_o5',
            'Q2_04',
            'Q2_04_o5',
            'Q2_05',
            'Q2_05_o5',
            'Q2_06',
            'Q2_06_o5',
            'Q2_07',
            'Q2_07_o5',
            'Q2_08',
            'Q2_08_o5',
            'Q2_09',
            'Q2_09_o5',

            'Q3_Net_CFHT',
            'Q3_Net_CFRX',

            'Q3_01_o2',
            'Q3_02_o2',
            'Q3_03_o2',
            'Q3_04_o2',
            'Q3_05_o2',
            'Q3_06_o2',
            'Q3_07_o2',
            'Q3_08_o2',
            'Q3_09_o2',
            'Q4',
            'Rotation',
        ]

        lst_fc = [
            'Q11',
            'Q12',
            'Q13_Concept_1_1',
            'Q13_Concept_1_2',
            'Q13_Concept_1_3',
            'Q13_Concept_1_4',
            'Q13_Concept_1_5',
            'Q13_Concept_2_1',
            'Q13_Concept_2_2',
            'Q13_Concept_2_3',
            'Q13_Concept_2_4',
            'Q13_Concept_2_5',
            'Q13_Concept_3_1',
            'Q13_Concept_3_2',
            'Q13_Concept_3_3',
            'Q13_Concept_3_4',
            'Q13_Concept_3_5',
            'Q14',
            'Q15',
        ]

        dict_sp1 = {
            'MaConcept_1st': 'MaConcept',
            'Q5_1st': 'Q5',
            'Q6a1_1st': 'Q6a1',
            'Q6b_1st': 'Q6b',
            'Q7_1st': 'Q7',
            'Q8_1st': 'Q8',
            'Q9_1st': 'Q9',
            'Q10_1st': 'Q10',
            'Q11_1st': 'Q11_New',
            'Q12_1st': 'Q12_New',
            'Q13_1st_1': 'Q13_New_1',
            'Q13_1st_2': 'Q13_New_2',
            'Q13_1st_3': 'Q13_New_3',
            'Q13_1st_4': 'Q13_New_4',
            'Q13_1st_5': 'Q13_New_5',
            'Q13_1st_o5': 'Q13_New_o5',
            'Q14_1st': 'Q14_New',
            'Q15_1st': 'Q15_New',
        }

        dict_sp2 = {
            'MaConcept_2nd': 'MaConcept',
            'Q5_2nd': 'Q5',
            'Q6a1_2nd': 'Q6a1',
            'Q6b_2nd': 'Q6b',
            'Q7_2nd': 'Q7',
            'Q8_2nd': 'Q8',
            'Q9_2nd': 'Q9',
            'Q10_2nd': 'Q10',
            'Q11_2nd': 'Q11_New',
            'Q12_2nd': 'Q12_New',
            'Q13_2nd_1': 'Q13_New_1',
            'Q13_2nd_2': 'Q13_New_2',
            'Q13_2nd_3': 'Q13_New_3',
            'Q13_2nd_4': 'Q13_New_4',
            'Q13_2nd_5': 'Q13_New_5',
            'Q13_2nd_o5': 'Q13_New_o5',
            'Q14_2nd': 'Q14_New',
            'Q15_2nd': 'Q15_New',
        }

        dict_sp3 = {
            'MaConcept_3rd': 'MaConcept',
            'Q5_3rd': 'Q5',
            'Q6a1_3rd': 'Q6a1',
            'Q6b_3rd': 'Q6b',
            'Q7_3rd': 'Q7',
            'Q8_3rd': 'Q8',
            'Q9_3rd': 'Q9',
            'Q10_3rd': 'Q10',
            'Q11_3rd': 'Q11_New',
            'Q12_3rd': 'Q12_New',
            'Q13_3rd_1': 'Q13_New_1',
            'Q13_3rd_2': 'Q13_New_2',
            'Q13_3rd_3': 'Q13_New_3',
            'Q13_3rd_4': 'Q13_New_4',
            'Q13_3rd_5': 'Q13_New_5',
            'Q13_3rd_o5': 'Q13_New_o5',
            'Q14_3rd': 'Q14_New',
            'Q15_3rd': 'Q15_New',
        }

        dict_sp_code = {'1': 'Concept 1', '2': 'Concept 2', '3': 'Concept 3'}

        dict_qre_group_mean = {
            'Q5': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Q7': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Q8': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Q9': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Q10': {
                'range': [],
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
        }

        dict_qre_new_vars_info = {
            'Q11_New': ['Q11_New', 'Trong các ý tưởng sản phẩm cà phê bạn vừa đọc, bạn thích sản phẩm cà phê nào nhất? [SA]', 'SA', {'1': 'Yes', '2': 'No'}],
            'Q12_New': ['Q12_New', 'Vì sao bạn thích ý tưởng _ hơn các ý tưởng sản phẩm khác? [OE]', 'FT', {}],
            'Q13_New_1': ['Q13_New_1', 'Anh/ chị sẽ muốn uống cà phê của ý tưởng _ ở đâu? [MA]', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q13_New_2': ['Q13_New_2', 'Anh/ chị sẽ muốn uống cà phê của ý tưởng _ ở đâu? [MA]', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q13_New_3': ['Q13_New_3', 'Anh/ chị sẽ muốn uống cà phê của ý tưởng _ ở đâu? [MA]', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q13_New_4': ['Q13_New_4', 'Anh/ chị sẽ muốn uống cà phê của ý tưởng _ ở đâu? [MA]', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q13_New_5': ['Q13_New_5', 'Anh/ chị sẽ muốn uống cà phê của ý tưởng _ ở đâu? [MA]', 'MA', {'1': 'Tại nhà', '2': 'Tại trường/ công ty/ nơi làm việc', '3': 'Tại quán', '4': 'Vừa đi đường vừa uống/ uống tại nơi công cộng (công viên/ khu mua sắm…)', '5': 'Khác, vui lòng ghi rõ'}],
            'Q13_New_o5': ['Q13_New_o5', 'Anh/ chị sẽ muốn uống cà phê của ý tưởng _ ở đâu? [MA]', 'FT', {}],
            'Q14_New': ['Q14_New', 'Trong các ý tưởng sản phẩm cà phê bạn vừa đọc, bạn muốn mua sản phẩm nào nhất? [SA]', 'SA', {'1': 'Yes', '2': 'No'}],
            'Q15_New': ['Q15_New', 'Trong các ý tưởng sản phẩm cà phê bạn vừa đọc, bạn muốn dùng sản phẩm cà phê nào thường xuyên nhất? [SA]', 'SA', {'1': 'Yes', '2': 'No'}],
        }

        dict_qre_OE_info_org = {


            'Q6a1_OE|1-6': ['Q6a. Bạn thích ý tưởng sản phẩm cà phê này ở điểm nào?', 'MA',  {'net_code': {'999': 'Không có điểm gì thích', '90001|CONCEPT 1 (NET)': {'1': 'Pha được cả nước nóng hoặc nước lạnh', '3': 'Tiện lợi', '4': 'Pha nhanh chóng (công nghệ hòa tan nhanh chóng)', '5': 'Dễ dàng mang theo', '6': 'Thiết kế nhỏ gọn, mới lạ, sáng tạo', '7': 'Không mất nhiều thời gian', '8': 'Đa dạng lựa chọn hương vị', '9': 'Tính độc đáo, mới lạ', '10': 'Dễ tiếp cận với sinh viên'}, '90002|CONCEPT 2 (NET)': {'11': 'Lớp bọt đẹp mắt, hấp dẫn', '12': 'Nhìn ly cà phê hấp dẫn', '13': 'Thích ý tưởng tự lắc, tự pha tay', '14': 'Tiện dùng khi cần thiết, pha tại nhà', '15': 'Pha chế nhanh chóng, dễ dàng', '16': 'Cho cảm giác muốn uống liền', '17': 'Cảm giác lớp kem béo', '18': 'Ý tưởng cuốn hút nhờ lớp kem béo', '19': 'Pha chế như ở quán', '20': 'Hướng dẫn rõ ràng từ ý tưởng'}, '90003|CONCEPT 3 (NET)': {'21': 'Hộp có nhiều màu sắc đẹp mắt', '22': 'Tiện lợi', '23': 'Dễ dàng mang theo', '24': 'Dễ dàng pha chế', '25': 'Có nhiều hương vị', '26': 'Ý tưởng mới lạ, tính sáng tạo từ topping', '27': 'Phù hợp với giới trẻ', '28': 'Thiết kế đẹp', '29': 'Không mất nhiều thời gian', '30': 'Giống với trà sữa', '31': 'Pha hoặc sử dụng ở mọi nơi, mọi lúc', '32': 'Có nhiều topping nhai vui miệng, kết hợp với cà phê'}}}],
            'Q6b_OE|1-3': ['Q6b. Bạn không thích ý tưởng sản phẩm cà phê này ở điểm nào?', 'MA',  {'net_code': {'999': 'Không có điểm gì không thích', '90001|CONCEPT 1 (NET)': {'1': 'Cách pha không khác biệt với hòa tan', '2': 'Không bật mùi thơm do pha bằng nước lạnh', '3': 'Không đậm đà màu cà phê, nhạt như nước trà', '4': 'Không đậm đà cà phê', '5': 'Bao bì không thuận tiện, không gọn gàng như dạng túi', '6': 'Không biết rõ thành phần cà phê, nguồn gốc cà phê', '7': 'Cần có thêm dụng cụ pha chế', '8': 'Cách pha chế phức tạp', '9': 'Không ấn tượng với ý tưởng', '10': 'Sản phẩm bán nhiều trên thị trường, không ấn tượng', '11': 'Lượng cà phê quá ít', '12': 'Cảm giác dành cho giới trẻ, màu sắc trẻ con'}, '90002|CONCEPT 2 (NET)': {'13': 'Cần có thêm dụng cụ pha chế', '14': 'Cách pha chế phức tạp, bất tiện', '15': 'Cảm giác quá béo', '16': 'Mất nhiều thời gian chuẩn bị, pha chế', '17': 'Không tiện khi mang đi (chỉ sử dụng tại nhà)', '18': 'Không còn vị cà phê do béo', '19': 'Ý tưởng không ấn tượng'}, '90003|CONCEPT 3 (NET)': {'21': 'Nhìn như trà sữa có topping', '22': 'Mất đi hương vị cà phê', '23': 'Mất thời gian pha chế', '24': 'Không tiện khi mang đi (chỉ sử dụng tại nhà)', '25': 'Không tiện lợi', '26': 'Ý tưởng không ấn tượng', '27': 'Thích hợp cho chị em phụ nữ'}}}],
            'Q12_OE|1-5': ['Q12. Vì sao bạn thích ý tưởng _ hơn các ý tưởng sản phẩm khác? [OE]', 'MA',  {'1': 'Tiện lợi', '2': 'Hòa tan nhanh chóng', '3': 'Pha được cả nước nóng hoặc nước lạnh', '4': 'Có sẵn cốc để pha chế', '5': 'Hướng dẫn pha chế (ký hiệu) rõ ràng', '6': 'Dễ dàng mang theo', '7': 'Thiết kế nhỏ gọn', '8': 'Pha đậm nhạt (tùy chỉnh) theo ý thích', '9': 'Tự tay "lắc" ra lớp kem béo ngậy', '10': 'Có nhiều hương vị để lựa chọn', '11': 'Có thêm topping, khá thú vị', '12': 'Ý tưởng mới lạ, tò mò vị giác', '13': 'Không mất nhiều thời gian pha chế, đơn giản', '14': 'Màu sắc nhãn mác, bao bì hấp dẫn', '15': 'Phù hợp cho giới trẻ', '16': 'Màu sắc cà phê bắt mắt', '17': 'Vì có lớp kem béo ngậy', '18': 'Giống như ly cà phê được pha ở tiệm (hàng cà phê)'}],

            'Q6a1_OE_Concept_1|1-6': ['Q6a. Bạn thích ý tưởng sản phẩm cà phê này ở điểm nào? - Concept 1', 'MA',  {'net_code': {'999': 'Không có điểm gì thích', '90001|CONCEPT 1 (NET)': {'1': 'Pha được cả nước nóng hoặc nước lạnh', '3': 'Tiện lợi', '4': 'Pha nhanh chóng (công nghệ hòa tan nhanh chóng)', '5': 'Dễ dàng mang theo', '6': 'Thiết kế nhỏ gọn, mới lạ, sáng tạo', '7': 'Không mất nhiều thời gian', '8': 'Đa dạng lựa chọn hương vị', '9': 'Tính độc đáo, mới lạ', '10': 'Dễ tiếp cận với sinh viên'}, '90002|CONCEPT 2 (NET)': {'11': 'Lớp bọt đẹp mắt, hấp dẫn', '12': 'Nhìn ly cà phê hấp dẫn', '13': 'Thích ý tưởng tự lắc, tự pha tay', '14': 'Tiện dùng khi cần thiết, pha tại nhà', '15': 'Pha chế nhanh chóng, dễ dàng', '16': 'Cho cảm giác muốn uống liền', '17': 'Cảm giác lớp kem béo', '18': 'Ý tưởng cuốn hút nhờ lớp kem béo', '19': 'Pha chế như ở quán', '20': 'Hướng dẫn rõ ràng từ ý tưởng'}, '90003|CONCEPT 3 (NET)': {'21': 'Hộp có nhiều màu sắc đẹp mắt', '22': 'Tiện lợi', '23': 'Dễ dàng mang theo', '24': 'Dễ dàng pha chế', '25': 'Có nhiều hương vị', '26': 'Ý tưởng mới lạ, tính sáng tạo từ topping', '27': 'Phù hợp với giới trẻ', '28': 'Thiết kế đẹp', '29': 'Không mất nhiều thời gian', '30': 'Giống với trà sữa', '31': 'Pha hoặc sử dụng ở mọi nơi, mọi lúc', '32': 'Có nhiều topping nhai vui miệng, kết hợp với cà phê'}}}],
            'Q6b_OE_Concept_1|1-3': ['Q6b. Bạn không thích ý tưởng sản phẩm cà phê này ở điểm nào? - Concept 1', 'MA',  {'net_code': {'999': 'Không có điểm gì không thích', '90001|CONCEPT 1 (NET)': {'1': 'Cách pha không khác biệt với hòa tan', '2': 'Không bật mùi thơm do pha bằng nước lạnh', '3': 'Không đậm đà màu cà phê, nhạt như nước trà', '4': 'Không đậm đà cà phê', '5': 'Bao bì không thuận tiện, không gọn gàng như dạng túi', '6': 'Không biết rõ thành phần cà phê, nguồn gốc cà phê', '7': 'Cần có thêm dụng cụ pha chế', '8': 'Cách pha chế phức tạp', '9': 'Không ấn tượng với ý tưởng', '10': 'Sản phẩm bán nhiều trên thị trường, không ấn tượng', '11': 'Lượng cà phê quá ít', '12': 'Cảm giác dành cho giới trẻ, màu sắc trẻ con'}, '90002|CONCEPT 2 (NET)': {'13': 'Cần có thêm dụng cụ pha chế', '14': 'Cách pha chế phức tạp, bất tiện', '15': 'Cảm giác quá béo', '16': 'Mất nhiều thời gian chuẩn bị, pha chế', '17': 'Không tiện khi mang đi (chỉ sử dụng tại nhà)', '18': 'Không còn vị cà phê do béo', '19': 'Ý tưởng không ấn tượng'}, '90003|CONCEPT 3 (NET)': {'21': 'Nhìn như trà sữa có topping', '22': 'Mất đi hương vị cà phê', '23': 'Mất thời gian pha chế', '24': 'Không tiện khi mang đi (chỉ sử dụng tại nhà)', '25': 'Không tiện lợi', '26': 'Ý tưởng không ấn tượng', '27': 'Thích hợp cho chị em phụ nữ'}}}],
            'Q12_OE_Concept_1|1-5': ['Q12. Vì sao bạn thích ý tưởng _ hơn các ý tưởng sản phẩm khác? - Concept 1', 'MA',  {'1': 'Tiện lợi', '2': 'Hòa tan nhanh chóng', '3': 'Pha được cả nước nóng hoặc nước lạnh', '4': 'Có sẵn cốc để pha chế', '5': 'Hướng dẫn pha chế (ký hiệu) rõ ràng', '6': 'Dễ dàng mang theo', '7': 'Thiết kế nhỏ gọn', '8': 'Pha đậm nhạt (tùy chỉnh) theo ý thích', '9': 'Tự tay "lắc" ra lớp kem béo ngậy', '10': 'Có nhiều hương vị để lựa chọn', '11': 'Có thêm topping, khá thú vị', '12': 'Ý tưởng mới lạ, tò mò vị giác', '13': 'Không mất nhiều thời gian pha chế, đơn giản', '14': 'Màu sắc nhãn mác, bao bì hấp dẫn', '15': 'Phù hợp cho giới trẻ', '16': 'Màu sắc cà phê bắt mắt', '17': 'Vì có lớp kem béo ngậy', '18': 'Giống như ly cà phê được pha ở tiệm (hàng cà phê)'}],

            'Q6a1_OE_Concept_2|1-6': ['Q6a. Bạn thích ý tưởng sản phẩm cà phê này ở điểm nào? - Concept 2', 'MA',  {'net_code': {'999': 'Không có điểm gì thích', '90001|CONCEPT 1 (NET)': {'1': 'Pha được cả nước nóng hoặc nước lạnh', '3': 'Tiện lợi', '4': 'Pha nhanh chóng (công nghệ hòa tan nhanh chóng)', '5': 'Dễ dàng mang theo', '6': 'Thiết kế nhỏ gọn, mới lạ, sáng tạo', '7': 'Không mất nhiều thời gian', '8': 'Đa dạng lựa chọn hương vị', '9': 'Tính độc đáo, mới lạ', '10': 'Dễ tiếp cận với sinh viên'}, '90002|CONCEPT 2 (NET)': {'11': 'Lớp bọt đẹp mắt, hấp dẫn', '12': 'Nhìn ly cà phê hấp dẫn', '13': 'Thích ý tưởng tự lắc, tự pha tay', '14': 'Tiện dùng khi cần thiết, pha tại nhà', '15': 'Pha chế nhanh chóng, dễ dàng', '16': 'Cho cảm giác muốn uống liền', '17': 'Cảm giác lớp kem béo', '18': 'Ý tưởng cuốn hút nhờ lớp kem béo', '19': 'Pha chế như ở quán', '20': 'Hướng dẫn rõ ràng từ ý tưởng'}, '90003|CONCEPT 3 (NET)': {'21': 'Hộp có nhiều màu sắc đẹp mắt', '22': 'Tiện lợi', '23': 'Dễ dàng mang theo', '24': 'Dễ dàng pha chế', '25': 'Có nhiều hương vị', '26': 'Ý tưởng mới lạ, tính sáng tạo từ topping', '27': 'Phù hợp với giới trẻ', '28': 'Thiết kế đẹp', '29': 'Không mất nhiều thời gian', '30': 'Giống với trà sữa', '31': 'Pha hoặc sử dụng ở mọi nơi, mọi lúc', '32': 'Có nhiều topping nhai vui miệng, kết hợp với cà phê'}}}],
            'Q6b_OE_Concept_2|1-3': ['Q6b. Bạn không thích ý tưởng sản phẩm cà phê này ở điểm nào? - Concept 2', 'MA',  {'net_code': {'999': 'Không có điểm gì không thích', '90001|CONCEPT 1 (NET)': {'1': 'Cách pha không khác biệt với hòa tan', '2': 'Không bật mùi thơm do pha bằng nước lạnh', '3': 'Không đậm đà màu cà phê, nhạt như nước trà', '4': 'Không đậm đà cà phê', '5': 'Bao bì không thuận tiện, không gọn gàng như dạng túi', '6': 'Không biết rõ thành phần cà phê, nguồn gốc cà phê', '7': 'Cần có thêm dụng cụ pha chế', '8': 'Cách pha chế phức tạp', '9': 'Không ấn tượng với ý tưởng', '10': 'Sản phẩm bán nhiều trên thị trường, không ấn tượng', '11': 'Lượng cà phê quá ít', '12': 'Cảm giác dành cho giới trẻ, màu sắc trẻ con'}, '90002|CONCEPT 2 (NET)': {'13': 'Cần có thêm dụng cụ pha chế', '14': 'Cách pha chế phức tạp, bất tiện', '15': 'Cảm giác quá béo', '16': 'Mất nhiều thời gian chuẩn bị, pha chế', '17': 'Không tiện khi mang đi (chỉ sử dụng tại nhà)', '18': 'Không còn vị cà phê do béo', '19': 'Ý tưởng không ấn tượng'}, '90003|CONCEPT 3 (NET)': {'21': 'Nhìn như trà sữa có topping', '22': 'Mất đi hương vị cà phê', '23': 'Mất thời gian pha chế', '24': 'Không tiện khi mang đi (chỉ sử dụng tại nhà)', '25': 'Không tiện lợi', '26': 'Ý tưởng không ấn tượng', '27': 'Thích hợp cho chị em phụ nữ'}}}],
            'Q12_OE_Concept_2|1-5': ['Q12. Vì sao bạn thích ý tưởng _ hơn các ý tưởng sản phẩm khác? - Concept 2', 'MA',  {'1': 'Tiện lợi', '2': 'Hòa tan nhanh chóng', '3': 'Pha được cả nước nóng hoặc nước lạnh', '4': 'Có sẵn cốc để pha chế', '5': 'Hướng dẫn pha chế (ký hiệu) rõ ràng', '6': 'Dễ dàng mang theo', '7': 'Thiết kế nhỏ gọn', '8': 'Pha đậm nhạt (tùy chỉnh) theo ý thích', '9': 'Tự tay "lắc" ra lớp kem béo ngậy', '10': 'Có nhiều hương vị để lựa chọn', '11': 'Có thêm topping, khá thú vị', '12': 'Ý tưởng mới lạ, tò mò vị giác', '13': 'Không mất nhiều thời gian pha chế, đơn giản', '14': 'Màu sắc nhãn mác, bao bì hấp dẫn', '15': 'Phù hợp cho giới trẻ', '16': 'Màu sắc cà phê bắt mắt', '17': 'Vì có lớp kem béo ngậy', '18': 'Giống như ly cà phê được pha ở tiệm (hàng cà phê)'}],

            'Q6a1_OE_Concept_3|1-6': ['Q6a. Bạn thích ý tưởng sản phẩm cà phê này ở điểm nào? - Concept 3', 'MA',  {'net_code': {'999': 'Không có điểm gì thích', '90001|CONCEPT 1 (NET)': {'1': 'Pha được cả nước nóng hoặc nước lạnh', '3': 'Tiện lợi', '4': 'Pha nhanh chóng (công nghệ hòa tan nhanh chóng)', '5': 'Dễ dàng mang theo', '6': 'Thiết kế nhỏ gọn, mới lạ, sáng tạo', '7': 'Không mất nhiều thời gian', '8': 'Đa dạng lựa chọn hương vị', '9': 'Tính độc đáo, mới lạ', '10': 'Dễ tiếp cận với sinh viên'}, '90002|CONCEPT 2 (NET)': {'11': 'Lớp bọt đẹp mắt, hấp dẫn', '12': 'Nhìn ly cà phê hấp dẫn', '13': 'Thích ý tưởng tự lắc, tự pha tay', '14': 'Tiện dùng khi cần thiết, pha tại nhà', '15': 'Pha chế nhanh chóng, dễ dàng', '16': 'Cho cảm giác muốn uống liền', '17': 'Cảm giác lớp kem béo', '18': 'Ý tưởng cuốn hút nhờ lớp kem béo', '19': 'Pha chế như ở quán', '20': 'Hướng dẫn rõ ràng từ ý tưởng'}, '90003|CONCEPT 3 (NET)': {'21': 'Hộp có nhiều màu sắc đẹp mắt', '22': 'Tiện lợi', '23': 'Dễ dàng mang theo', '24': 'Dễ dàng pha chế', '25': 'Có nhiều hương vị', '26': 'Ý tưởng mới lạ, tính sáng tạo từ topping', '27': 'Phù hợp với giới trẻ', '28': 'Thiết kế đẹp', '29': 'Không mất nhiều thời gian', '30': 'Giống với trà sữa', '31': 'Pha hoặc sử dụng ở mọi nơi, mọi lúc', '32': 'Có nhiều topping nhai vui miệng, kết hợp với cà phê'}}}],
            'Q6b_OE_Concept_3|1-3': ['Q6b. Bạn không thích ý tưởng sản phẩm cà phê này ở điểm nào? - Concept 3', 'MA',  {'net_code': {'999': 'Không có điểm gì không thích', '90001|CONCEPT 1 (NET)': {'1': 'Cách pha không khác biệt với hòa tan', '2': 'Không bật mùi thơm do pha bằng nước lạnh', '3': 'Không đậm đà màu cà phê, nhạt như nước trà', '4': 'Không đậm đà cà phê', '5': 'Bao bì không thuận tiện, không gọn gàng như dạng túi', '6': 'Không biết rõ thành phần cà phê, nguồn gốc cà phê', '7': 'Cần có thêm dụng cụ pha chế', '8': 'Cách pha chế phức tạp', '9': 'Không ấn tượng với ý tưởng', '10': 'Sản phẩm bán nhiều trên thị trường, không ấn tượng', '11': 'Lượng cà phê quá ít', '12': 'Cảm giác dành cho giới trẻ, màu sắc trẻ con'}, '90002|CONCEPT 2 (NET)': {'13': 'Cần có thêm dụng cụ pha chế', '14': 'Cách pha chế phức tạp, bất tiện', '15': 'Cảm giác quá béo', '16': 'Mất nhiều thời gian chuẩn bị, pha chế', '17': 'Không tiện khi mang đi (chỉ sử dụng tại nhà)', '18': 'Không còn vị cà phê do béo', '19': 'Ý tưởng không ấn tượng'}, '90003|CONCEPT 3 (NET)': {'21': 'Nhìn như trà sữa có topping', '22': 'Mất đi hương vị cà phê', '23': 'Mất thời gian pha chế', '24': 'Không tiện khi mang đi (chỉ sử dụng tại nhà)', '25': 'Không tiện lợi', '26': 'Ý tưởng không ấn tượng', '27': 'Thích hợp cho chị em phụ nữ'}}}],
            'Q12_OE_Concept_3|1-5': ['Q12. Vì sao bạn thích ý tưởng _ hơn các ý tưởng sản phẩm khác? - Concept 3', 'MA',  {'1': 'Tiện lợi', '2': 'Hòa tan nhanh chóng', '3': 'Pha được cả nước nóng hoặc nước lạnh', '4': 'Có sẵn cốc để pha chế', '5': 'Hướng dẫn pha chế (ký hiệu) rõ ràng', '6': 'Dễ dàng mang theo', '7': 'Thiết kế nhỏ gọn', '8': 'Pha đậm nhạt (tùy chỉnh) theo ý thích', '9': 'Tự tay "lắc" ra lớp kem béo ngậy', '10': 'Có nhiều hương vị để lựa chọn', '11': 'Có thêm topping, khá thú vị', '12': 'Ý tưởng mới lạ, tò mò vị giác', '13': 'Không mất nhiều thời gian pha chế, đơn giản', '14': 'Màu sắc nhãn mác, bao bì hấp dẫn', '15': 'Phù hợp cho giới trẻ', '16': 'Màu sắc cà phê bắt mắt', '17': 'Vì có lớp kem béo ngậy', '18': 'Giống như ly cà phê được pha ở tiệm (hàng cà phê)'}],


        }

        dict_qre_OE_info = dict()
        for k, v in dict_qre_OE_info_org.items():
            oe_name, oe_num_col = k.rsplit('|', 1)
            oe_num_col = oe_num_col.split('-')
            oe_num_col = range(int(oe_num_col[0]), int(oe_num_col[1]) + 1)

            for i in oe_num_col:
                dict_qre_OE_info.update({
                    f'{oe_name}_{i}': [f'{oe_name}_{i}'] + v
                })

        # ['Q0a_RespondentID', 1056, 'Main_Ma_san_pham_Q3', 1, 'Main_Q5a_OE_Ly_do_thich_Y1_1', 101],
        lst_addin_OE_value = list()

        if coding_file.filename:
            lst_addin_OE_value = eval(coding_file.file.read())


        dict_qre_net_info = {
            'S5': {
                'net_code': {
                    '90001|House wife (NET)': {
                        '2': 'Nội trợ',
                    },
                    '90002|University student (NET)': {
                        '4': 'Sinh viên',
                    },
                    '90003|White (NET)': {
                        '7': 'Chủ cửa hàng lớn (Có nhân viên)',
                        '8': 'Quản lý cấp cao (Tổng giám đốc/ Phó tổng giám đốc/ Giám đốc/ Phó giám đốc doanh nghiệp)',
                        '9': 'Quản lý cấp trung (Trưởng phòng/ Phó phòng)',
                        '10': 'Chuyên viên (luật sư, bác sĩ, kỹ sư, giáo viên…)',
                        '11': 'Nhân viên văn phòng',
                    },
                    '90004|Blue (NET)': {
                        '6': 'Buôn bán nhỏ: buôn bán tại nhà hoặc ngoài chợ',
                        '12': 'Có đi làm (bán hàng, thâu ngân…)',
                        '13': 'Lao động chân tay (công nhân, tài xế, thợ sửa máy, ...)',
                    },
                },
                '14': 'Không trả lời/Từ chối',
                '15': 'Khác, vui lòng ghi rõ',
            },

            'S8': {
                'net_code': {
                    '90001|Light (NET)': {
                        '4': '3 lần uống/tuần',
                    },
                    '90002|Medium (NET)': {
                        '5': '4 lần uống/tuần',
                        '6': '5 lần uống/tuần',
                    },
                    '90003|Heavy (NET)': {
                        '7': '6 lần uống/tuần',
                        '8': 'Trên 6 lần uống/tuần (Ghi rõ)',
                    },
                },
            },


            'Q1_[0-9]+': {
                'net_code': {
                    '90001|Cà phê hòa tan (NET)': {
                        '3': 'Cà phê hòa tan 2 in 1',
                        '4': 'Cà phê hòa tan 3 in 1',
                        '5': 'Cà phê hòa tan 4 in 1',
                    },
                    '90002|Cà phê uống liền (NET)': {
                        '6': 'Cà phê uống liền đóng chai / lon'
                    },
                    '90003|Cà phê rang xay (NET)': {
                        '1': 'Cà phê (pha phin hoặc pha máy) pha tại nhà',
                        '2': 'Cà phê được pha ở cửa hàng (Highlands, The coffee house, Milano, …)',
                        '7': 'Cà phê viên nén/ Capsule',
                        '8': 'Cà phê pha ở các tiệm, xe đẩy, ki-ốt ven đường',
                        '9': 'Khác, vui lòng ghi rõ',
                    },
                },
            },
            'Q4': {
                'net_code': {
                    '90001|Cà phê hòa tan (NET)': {
                        '3': 'Cà phê hòa tan 2 in 1',
                        '4': 'Cà phê hòa tan 3 in 1',
                        '5': 'Cà phê hòa tan 4 in 1',
                    },
                    '90002|Cà phê uống liền (NET)': {
                        '6': 'Cà phê uống liền đóng chai / lon'
                    },
                    '90003|Cà phê rang xay (NET)': {
                        '1': 'Cà phê (pha phin hoặc pha máy) pha tại nhà',
                        '2': 'Cà phê được pha ở cửa hàng (Highlands, The coffee house, Milano, …)',
                        '7': 'Cà phê viên nén/ Capsule',
                        '8': 'Cà phê pha ở các tiệm, xe đẩy, ki-ốt ven đường',
                        '9': 'Khác, vui lòng ghi rõ',
                    },
                },
            },


        }
        # End Define structure------------------------------------------------------------------------------------------

        # Data stack format---------------------------------------------------------------------------------------------
        logger.info('Data stack format')

        # df_data_stack generate
        df_data_scr = df_data_output.loc[:, [id_col] + lst_scr].copy()
        df_data_fc = df_data_output.loc[:, [id_col] + lst_fc].copy()

        df_data_sp1 = df_data_output.loc[:, [id_col] + list(dict_sp1.keys())].copy()
        df_data_sp2 = df_data_output.loc[:, [id_col] + list(dict_sp2.keys())].copy()
        df_data_sp3 = df_data_output.loc[:, [id_col] + list(dict_sp3.keys())].copy()

        df_data_sp1.rename(columns=dict_sp1, inplace=True)
        df_data_sp2.rename(columns=dict_sp2, inplace=True)
        df_data_sp3.rename(columns=dict_sp3, inplace=True)

        df_data_stack = pd.concat([df_data_sp1, df_data_sp2, df_data_sp3], axis=0, ignore_index=True)

        df_data_stack = df_data_scr.merge(df_data_stack, how='left', on=[id_col])
        df_data_stack.reset_index(drop=True, inplace=True)

        df_data_stack = df_data_stack.merge(df_data_fc, how='left', on=[id_col])

        df_data_stack.sort_values(by=[id_col, sp_col], inplace=True)
        df_data_stack.reset_index(drop=True, inplace=True)

        # df_info_stack generate
        df_info_stack = pd.concat([df_qres_info_output,
                                   pd.DataFrame(columns=['var_name', 'var_lbl', 'var_type', 'val_lbl'],
                                                data=list(dict_qre_new_vars_info.values()))], axis=0)

        for key, val in dict_sp1.items():
            df_info_stack.loc[df_info_stack['var_name'] == key, ['var_name']] = [val]

        df_info_stack.loc[df_info_stack['var_name'] == sp_col, 'val_lbl'] = [dict_sp_code]

        df_filter = df_info_stack.loc[df_info_stack['var_name'].str.contains('Q3_0[0-9]+_o2'), ['var_type']].copy()
        df_info_stack.loc[df_filter.index, 'var_type'] = ['NUM'] * df_filter.shape[0]

        df_info_stack.drop_duplicates(subset=['var_name'], keep='first', inplace=True)

        if dict_qre_OE_info:

            # ADD OE to Data stack--------------------------------------------------------------------------------------
            lst_OE_col = list(dict_qre_OE_info.keys())

            df_data_stack[lst_OE_col] = pd.DataFrame([[np.nan] * len(lst_OE_col)], index=df_data_stack.index)

            # Remember edit this
            for item in lst_addin_OE_value:
                df_data_stack.loc[(df_data_stack[item[0]] == item[1]) & (df_data_stack[item[2]] == item[3]), [item[4]]] = [item[5]]

                str_qre, str_col = item[4].rsplit('_', 1)

                if 'Q12' in str_qre:
                    df_filter = df_data_stack.query(f"({item[0]} == '{item[1]}') & (Q11 == {item[3]})")
                else:
                    df_filter = df_data_stack.query(f"{item[0]} == '{item[1]}'")

                df_data_stack.loc[df_filter.index, [f'{str_qre}_Concept_{item[3]}_{str_col}']] = [item[5]] * df_filter.shape[0]




            # END ADD OE to Data stack----------------------------------------------------------------------------------

            # ADD OE to Info stack--------------------------------------------------------------------------------------
            df_info_stack = pd.concat([df_info_stack,
                                       pd.DataFrame(columns=['var_name', 'var_lbl', 'var_type', 'val_lbl'],
                                                    data=list(dict_qre_OE_info.values()))], axis=0)
            # END ADD OE to Info stack----------------------------------------------------------------------------------

        if dict_qre_net_info:

            # ADD Net code to Info stack--------------------------------------------------------------------------------
            for key, val in dict_qre_net_info.items():
                df_info_stack.loc[df_info_stack['var_name'].str.contains(key), 'val_lbl'] = [val] * df_info_stack.loc[df_info_stack['var_name'].str.contains(key), 'val_lbl'].shape[0]
            # END ADD Net code to Info stack----------------------------------------------------------------------------

        # Reset df_info_stack index
        df_info_stack['idx_var_name'] = df_info_stack['var_name']
        df_info_stack.set_index('idx_var_name', inplace=True)
        df_info_stack = df_info_stack.loc[df_data_stack.columns, :]
        df_info_stack.reindex(list(df_data_stack.columns))
        df_info_stack.reset_index(drop=True, inplace=True)

        # df_data_stack.to_csv('zzzz_df_data_stack.csv', encoding='utf-8-sig')
        # df_info_stack.to_csv('zzzz_df_info_stack.csv', encoding='utf-8-sig')
        # End Data stack format-----------------------------------------------------------------------------------------


        # Data unstack format-------------------------------------------------------------------------------------------
        logger.info('Data unstack format')

        lst_col_part_body = list(dict_sp1.values())
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

        # Addin data for this prj only----------------------------------------------------------------------------------
        dict_qre_group_mean.update({
            'Q5_Concept': {
                'range': [i for i in range(1, 4)],
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Q7_Concept': {
                'range': [i for i in range(1, 4)],
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Q8_Concept': {
                'range': [i for i in range(1, 4)],
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Q9_Concept': {
                'range': [i for i in range(1, 4)],
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
            'Q10_Concept': {
                'range': [i for i in range(1, 4)],
                'group': {'cats': {1: 'B2B', 2: 'Medium', 3: 'T2B'}, 'recode': {1: 1, 2: 1, 3: 2, 4: 3, 5: 3}},
                'mean': {1: 1, 2: 2, 3: 3, 4: 4, 5: 5}
            },
        })

        df_data_stack_addin = df_data_stack.copy()
        df_info_stack_addin = df_info_stack.copy()

        lst_addin_col = list()
        lst_org_col = list()
        lst_info_addin = list()
        if dict_qre_group_mean:
            for key, val in dict_qre_group_mean.items():
                if val['range']:
                    for i in val['range']:
                        lst_addin_col.append(f'{key}_{i}')

                        lst_info_addin_by_qre = df_info_stack_addin.loc[df_info_stack_addin['var_name'] == key.replace('_Concept', ''), :].values.tolist()[0]
                        lst_info_addin_by_qre[0] = f'{key}_{i}'
                        lst_info_addin_by_qre[1] = f'{lst_info_addin_by_qre[1]}_Concept {i}'

                        lst_info_addin.append(lst_info_addin_by_qre)
                else:
                    lst_org_col.append(key)

        df_data_stack_addin = pd.concat([df_data_stack_addin,
                                         pd.DataFrame(
                                             columns=lst_addin_col,
                                             data=[[np.nan] * len(lst_addin_col)] * df_data_stack_addin.shape[0])],
                                        axis=1)

        df_info_stack_addin = pd.concat([df_info_stack_addin,
                                         pd.DataFrame(
                                             columns=['var_name', 'var_lbl', 'var_type', 'val_lbl'],
                                             data=lst_info_addin)],
                                        axis=0)


        for idx in df_data_stack_addin.index:
            str_id = df_data_stack_addin.at[idx, 'ID']
            for qre in lst_org_col:
                for i in range(1, 4):
                    new_val = df_data_stack_addin.loc[((df_data_stack_addin['ID'] == str_id) & (df_data_stack_addin['MaConcept'] == i)), [qre]].values[0]
                    df_data_stack_addin.loc[df_data_stack_addin['ID'] == str_id, [f'{qre}_Concept_{i}']] = [new_val] * 3

        df_data_stack_addin.reset_index(drop=True, inplace=True)
        df_info_stack_addin.reset_index(drop=True, inplace=True)

        # End Addin data for this prj only------------------------------------------------------------------------------


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

        DataTableGenerator.__init__(self, df_data=df_data_stack_addin, df_info=df_info_stack_addin,
                                    xlsx_name=str_topline_file_name,
                                    lst_qre_group=lst_qre_group, lst_qre_mean=lst_qre_mean)

        lst_func_to_run = [
            {
                'json_file': './app/routers/Online_Survey/tables_standard_sig.json',
                'func_name': 'run_standard_table_sig',
                'tables_to_run': ['VN8228_Scr', 'VN8228_Main', 'VN8228_KPI', 'VN8228_OE', 'VN8228_Extra'],
                # 'tables_to_run': ['VN8228_OE'],
                # 'tables_to_run': ['VN8228_Extra'],

            },
        ]


        self.run_tables_by_js_files(lst_func_to_run)

        self.format_sig_table()

        # End Export data tables----------------------------------------------------------------------------------------

        logger.info('Generate SAV files')

        # Remove net_code to export sav---------------------------------------------------------------------------------
        df_info_stack_without_net = df_info_stack.copy()
        df_info_unstack_without_net = df_info_unstack.copy()

        for idx in df_info_stack_without_net.index:
            val_lbl = df_info_stack_without_net.at[idx, 'val_lbl']

            if 'net_code' in val_lbl.keys():
                df_info_stack_without_net.at[idx, 'val_lbl'] = self.unnetted_qre_val(val_lbl)

        for idx in df_info_unstack_without_net.index:
            val_lbl = df_info_unstack_without_net.at[idx, 'val_lbl']

            if 'net_code' in val_lbl.keys():
                df_info_unstack_without_net.at[idx, 'val_lbl'] = self.unnetted_qre_val(val_lbl)
        # END Remove net_code to export sav-----------------------------------------------------------------------------

        self.generate_sav_sps(df_data=df_data_stack, df_qres_info=df_info_stack_without_net, is_md=False, is_export_xlsx=True,
                              df_data_2=df_data_unstack, df_qres_info_2=df_info_unstack_without_net)

        logger.info('COMPLETED')
