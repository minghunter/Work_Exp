from app.classes.AP_DataConverter import APDataConverter
from app.routers.Online_Survey.export_online_survey_data_table import DataTableGenerator
from app.routers.Online_Survey.convert_unstack import ConvertUnstack
import pandas as pd
import numpy as np
import time
import traceback
import datetime


class INT0002PricingChange(APDataConverter, DataTableGenerator, ConvertUnstack):

    def convert_int0002_pricing_change(self, py_script_file, tables_format_file, codelist_file, coding_file):
        """DO NOT EDIT THIS FUNCTION"""

        try:
            start_time = time.time()

            if py_script_file:
                exec(py_script_file.file.read())
                exit()

            self.processing_script_int0002_pricing_change(tables_format_file, codelist_file, coding_file)

            self.logger.warning('DATA PROCESSING COMPLETED in %d seconds' % (time.time() - start_time))

        except Exception:
            self.logger.error(traceback.format_exc())
            str_err_log_name = self.str_file_name.replace('.xlsx', '_Errors.txt')
            with open(str_err_log_name, 'w') as err_log_txt:
                err_log_txt.writelines(traceback.format_exc())


    def processing_script_int0002_pricing_change(self, tables_format_file, codelist_file, coding_file):
        """
        :param tables_format_file: txt uploaded table format file
        :param codelist_file: txt uploaded codelist file
        :param coding_file: txt uploaded coding file
        :return: zip file include files: .sav, .sps, .xlsx(rawdata, topline)

        EDIT FROM HERE
        """

        # Convert system rawdata to dataframes--------------------------------------------------------------------------
        df_data, df_info = self.convert_df_mc()

        dict_q9_comb = {
            'Q9_Combine_1': ['Q9. Combine', 'MA', {'1': 'Good food selection', '2': 'Unique / special food', '3': 'Good accommodation', '4': 'Good / unique culture', '5': 'Good nature (beach, mountain)', '6': 'Good entertainment (e.g. amusement park)', '7': 'Unique activities (e.g. trekking, marine sport)', '8': 'Relaxed atmosphere', '9': 'Good place for shopping', '10': 'Good night entertainment', '11': 'Good cost', '12': 'Convenient location / transportation', '13': 'Good historical area', '14': 'As the location is famous in drama / TV'}],
            'Q9_Combine_2': ['Q9. Combine', 'MA', {'1': 'Good food selection', '2': 'Unique / special food', '3': 'Good accommodation', '4': 'Good / unique culture', '5': 'Good nature (beach, mountain)', '6': 'Good entertainment (e.g. amusement park)', '7': 'Unique activities (e.g. trekking, marine sport)', '8': 'Relaxed atmosphere', '9': 'Good place for shopping', '10': 'Good night entertainment', '11': 'Good cost', '12': 'Convenient location / transportation', '13': 'Good historical area', '14': 'As the location is famous in drama / TV'}],
            'Q9_Combine_3': ['Q9. Combine', 'MA', {'1': 'Good food selection', '2': 'Unique / special food', '3': 'Good accommodation', '4': 'Good / unique culture', '5': 'Good nature (beach, mountain)', '6': 'Good entertainment (e.g. amusement park)', '7': 'Unique activities (e.g. trekking, marine sport)', '8': 'Relaxed atmosphere', '9': 'Good place for shopping', '10': 'Good night entertainment', '11': 'Good cost', '12': 'Convenient location / transportation', '13': 'Good historical area', '14': 'As the location is famous in drama / TV'}],
            'Q9_Combine_4': ['Q9. Combine', 'MA', {'1': 'Good food selection', '2': 'Unique / special food', '3': 'Good accommodation', '4': 'Good / unique culture', '5': 'Good nature (beach, mountain)', '6': 'Good entertainment (e.g. amusement park)', '7': 'Unique activities (e.g. trekking, marine sport)', '8': 'Relaxed atmosphere', '9': 'Good place for shopping', '10': 'Good night entertainment', '11': 'Good cost', '12': 'Convenient location / transportation', '13': 'Good historical area', '14': 'As the location is famous in drama / TV'}],
            'Q9_Combine_5': ['Q9. Combine', 'MA', {'1': 'Good food selection', '2': 'Unique / special food', '3': 'Good accommodation', '4': 'Good / unique culture', '5': 'Good nature (beach, mountain)', '6': 'Good entertainment (e.g. amusement park)', '7': 'Unique activities (e.g. trekking, marine sport)', '8': 'Relaxed atmosphere', '9': 'Good place for shopping', '10': 'Good night entertainment', '11': 'Good cost', '12': 'Convenient location / transportation', '13': 'Good historical area', '14': 'As the location is famous in drama / TV'}],
            'Q9_Combine_6': ['Q9. Combine', 'MA', {'1': 'Good food selection', '2': 'Unique / special food', '3': 'Good accommodation', '4': 'Good / unique culture', '5': 'Good nature (beach, mountain)', '6': 'Good entertainment (e.g. amusement park)', '7': 'Unique activities (e.g. trekking, marine sport)', '8': 'Relaxed atmosphere', '9': 'Good place for shopping', '10': 'Good night entertainment', '11': 'Good cost', '12': 'Convenient location / transportation', '13': 'Good historical area', '14': 'As the location is famous in drama / TV'}],
            'Q9_Combine_7': ['Q9. Combine', 'MA', {'1': 'Good food selection', '2': 'Unique / special food', '3': 'Good accommodation', '4': 'Good / unique culture', '5': 'Good nature (beach, mountain)', '6': 'Good entertainment (e.g. amusement park)', '7': 'Unique activities (e.g. trekking, marine sport)', '8': 'Relaxed atmosphere', '9': 'Good place for shopping', '10': 'Good night entertainment', '11': 'Good cost', '12': 'Convenient location / transportation', '13': 'Good historical area', '14': 'As the location is famous in drama / TV'}],
            'Q9_Combine_8': ['Q9. Combine', 'MA', {'1': 'Good food selection', '2': 'Unique / special food', '3': 'Good accommodation', '4': 'Good / unique culture', '5': 'Good nature (beach, mountain)', '6': 'Good entertainment (e.g. amusement park)', '7': 'Unique activities (e.g. trekking, marine sport)', '8': 'Relaxed atmosphere', '9': 'Good place for shopping', '10': 'Good night entertainment', '11': 'Good cost', '12': 'Convenient location / transportation', '13': 'Good historical area', '14': 'As the location is famous in drama / TV'}],
            'Q9_Combine_9': ['Q9. Combine', 'MA', {'1': 'Good food selection', '2': 'Unique / special food', '3': 'Good accommodation', '4': 'Good / unique culture', '5': 'Good nature (beach, mountain)', '6': 'Good entertainment (e.g. amusement park)', '7': 'Unique activities (e.g. trekking, marine sport)', '8': 'Relaxed atmosphere', '9': 'Good place for shopping', '10': 'Good night entertainment', '11': 'Good cost', '12': 'Convenient location / transportation', '13': 'Good historical area', '14': 'As the location is famous in drama / TV'}],
            'Q9_Combine_10': ['Q9. Combine', 'MA', {'1': 'Good food selection', '2': 'Unique / special food', '3': 'Good accommodation', '4': 'Good / unique culture', '5': 'Good nature (beach, mountain)', '6': 'Good entertainment (e.g. amusement park)', '7': 'Unique activities (e.g. trekking, marine sport)', '8': 'Relaxed atmosphere', '9': 'Good place for shopping', '10': 'Good night entertainment', '11': 'Good cost', '12': 'Convenient location / transportation', '13': 'Good historical area', '14': 'As the location is famous in drama / TV'}],
            'Q9_Combine_11': ['Q9. Combine', 'MA', {'1': 'Good food selection', '2': 'Unique / special food', '3': 'Good accommodation', '4': 'Good / unique culture', '5': 'Good nature (beach, mountain)', '6': 'Good entertainment (e.g. amusement park)', '7': 'Unique activities (e.g. trekking, marine sport)', '8': 'Relaxed atmosphere', '9': 'Good place for shopping', '10': 'Good night entertainment', '11': 'Good cost', '12': 'Convenient location / transportation', '13': 'Good historical area', '14': 'As the location is famous in drama / TV'}],
            'Q9_Combine_12': ['Q9. Combine', 'MA', {'1': 'Good food selection', '2': 'Unique / special food', '3': 'Good accommodation', '4': 'Good / unique culture', '5': 'Good nature (beach, mountain)', '6': 'Good entertainment (e.g. amusement park)', '7': 'Unique activities (e.g. trekking, marine sport)', '8': 'Relaxed atmosphere', '9': 'Good place for shopping', '10': 'Good night entertainment', '11': 'Good cost', '12': 'Convenient location / transportation', '13': 'Good historical area', '14': 'As the location is famous in drama / TV'}],
            'Q9_Combine_13': ['Q9. Combine', 'MA', {'1': 'Good food selection', '2': 'Unique / special food', '3': 'Good accommodation', '4': 'Good / unique culture', '5': 'Good nature (beach, mountain)', '6': 'Good entertainment (e.g. amusement park)', '7': 'Unique activities (e.g. trekking, marine sport)', '8': 'Relaxed atmosphere', '9': 'Good place for shopping', '10': 'Good night entertainment', '11': 'Good cost', '12': 'Convenient location / transportation', '13': 'Good historical area', '14': 'As the location is famous in drama / TV'}],
            'Q9_Combine_14': ['Q9. Combine', 'MA', {'1': 'Good food selection', '2': 'Unique / special food', '3': 'Good accommodation', '4': 'Good / unique culture', '5': 'Good nature (beach, mountain)', '6': 'Good entertainment (e.g. amusement park)', '7': 'Unique activities (e.g. trekking, marine sport)', '8': 'Relaxed atmosphere', '9': 'Good place for shopping', '10': 'Good night entertainment', '11': 'Good cost', '12': 'Convenient location / transportation', '13': 'Good historical area', '14': 'As the location is famous in drama / TV'}],
        }

        for key, val in dict_q9_comb.items():
            df_info = pd.concat([df_info, pd.DataFrame(columns=['var_name', 'var_lbl', 'var_type', 'val_lbl'], data=[[key, val[0], val[1], val[2]]])], axis=0, ignore_index=True)


        lst_q9_col = [f'Q9_{f"0{i}" if i < 10 else i}_{j}' for j in range(1, 15) for i in range(1, 25)]
        df_data_temp = df_data.loc[:, lst_q9_col].copy()
        df_data_temp.replace({np.nan: 999}, inplace=True)
        df_data_temp['Q9_Combine_1'] = df_data_temp[lst_q9_col].values.tolist()

        for idx in df_data_temp.index:
            lst_val = df_data_temp.at[idx, 'Q9_Combine_1']
            lst_val = list(dict.fromkeys(lst_val))
            lst_val.sort()
            df_data_temp.at[idx, 'Q9_Combine_1'] = lst_val[:14]

        df_split = pd.DataFrame(df_data_temp['Q9_Combine_1'].values.tolist(), columns=list(dict_q9_comb.keys()))
        df_data = pd.concat([df_data, df_split], axis=1)

        for i in list(dict_q9_comb.keys()):
            df_data[i].replace({999: np.nan}, inplace=True)

        df_info.reset_index(drop=True, inplace=True)
        df_data.reset_index(drop=True, inplace=True)

        df_data_output, df_info_output = df_data.copy(), df_info.copy()

        # df_data.to_excel('zzzz_df_data.xlsx')
        # df_info.to_excel('zzzz_df_info.xlsx')

        # Pre processing------------------------------------------------------------------------------------------------
        self.logger.info('Pre processing')

        dict_qres_add_new = {

            'Q9_ATT_01': ['Q9. Att', 'SA', {'1': 'HCM', '2': 'Hanoi', '3': 'Da Nang', '4': 'Da Lat', '5': 'Hoi an', '6': 'Nhat Trang', '7': 'Can Tho', '8': 'Phu Quoc', '9': 'Sapa', '10': 'Vung Tau', '11': 'Mui Ne', '12': 'Ban Me Thuoc', '13': 'Ninh Bình', '14': 'Huế', '15': 'An Giang', '16': 'Hòa Bình', '17': 'Châu Đốc', '18': 'Mộc Châu', '19': 'Cát Bà', '20': 'Cô Tô', '21': 'Tam Đảo', '22': 'Others, specify 1', '23': 'Others, specify 2', '24': 'Others, specify 3'}, 1],
            'Q9_ATT_02': ['Q9. Att', 'SA', {'1': 'HCM', '2': 'Hanoi', '3': 'Da Nang', '4': 'Da Lat', '5': 'Hoi an', '6': 'Nhat Trang', '7': 'Can Tho', '8': 'Phu Quoc', '9': 'Sapa', '10': 'Vung Tau', '11': 'Mui Ne', '12': 'Ban Me Thuoc', '13': 'Ninh Bình', '14': 'Huế', '15': 'An Giang', '16': 'Hòa Bình', '17': 'Châu Đốc', '18': 'Mộc Châu', '19': 'Cát Bà', '20': 'Cô Tô', '21': 'Tam Đảo', '22': 'Others, specify 1', '23': 'Others, specify 2', '24': 'Others, specify 3'}, 2],
            'Q9_ATT_03': ['Q9. Att', 'SA', {'1': 'HCM', '2': 'Hanoi', '3': 'Da Nang', '4': 'Da Lat', '5': 'Hoi an', '6': 'Nhat Trang', '7': 'Can Tho', '8': 'Phu Quoc', '9': 'Sapa', '10': 'Vung Tau', '11': 'Mui Ne', '12': 'Ban Me Thuoc', '13': 'Ninh Bình', '14': 'Huế', '15': 'An Giang', '16': 'Hòa Bình', '17': 'Châu Đốc', '18': 'Mộc Châu', '19': 'Cát Bà', '20': 'Cô Tô', '21': 'Tam Đảo', '22': 'Others, specify 1', '23': 'Others, specify 2', '24': 'Others, specify 3'}, 3],
            'Q9_ATT_04': ['Q9. Att', 'SA', {'1': 'HCM', '2': 'Hanoi', '3': 'Da Nang', '4': 'Da Lat', '5': 'Hoi an', '6': 'Nhat Trang', '7': 'Can Tho', '8': 'Phu Quoc', '9': 'Sapa', '10': 'Vung Tau', '11': 'Mui Ne', '12': 'Ban Me Thuoc', '13': 'Ninh Bình', '14': 'Huế', '15': 'An Giang', '16': 'Hòa Bình', '17': 'Châu Đốc', '18': 'Mộc Châu', '19': 'Cát Bà', '20': 'Cô Tô', '21': 'Tam Đảo', '22': 'Others, specify 1', '23': 'Others, specify 2', '24': 'Others, specify 3'}, 4],
            'Q9_ATT_05': ['Q9. Att', 'SA', {'1': 'HCM', '2': 'Hanoi', '3': 'Da Nang', '4': 'Da Lat', '5': 'Hoi an', '6': 'Nhat Trang', '7': 'Can Tho', '8': 'Phu Quoc', '9': 'Sapa', '10': 'Vung Tau', '11': 'Mui Ne', '12': 'Ban Me Thuoc', '13': 'Ninh Bình', '14': 'Huế', '15': 'An Giang', '16': 'Hòa Bình', '17': 'Châu Đốc', '18': 'Mộc Châu', '19': 'Cát Bà', '20': 'Cô Tô', '21': 'Tam Đảo', '22': 'Others, specify 1', '23': 'Others, specify 2', '24': 'Others, specify 3'}, 5],
            'Q9_ATT_06': ['Q9. Att', 'SA', {'1': 'HCM', '2': 'Hanoi', '3': 'Da Nang', '4': 'Da Lat', '5': 'Hoi an', '6': 'Nhat Trang', '7': 'Can Tho', '8': 'Phu Quoc', '9': 'Sapa', '10': 'Vung Tau', '11': 'Mui Ne', '12': 'Ban Me Thuoc', '13': 'Ninh Bình', '14': 'Huế', '15': 'An Giang', '16': 'Hòa Bình', '17': 'Châu Đốc', '18': 'Mộc Châu', '19': 'Cát Bà', '20': 'Cô Tô', '21': 'Tam Đảo', '22': 'Others, specify 1', '23': 'Others, specify 2', '24': 'Others, specify 3'}, 6],
            'Q9_ATT_07': ['Q9. Att', 'SA', {'1': 'HCM', '2': 'Hanoi', '3': 'Da Nang', '4': 'Da Lat', '5': 'Hoi an', '6': 'Nhat Trang', '7': 'Can Tho', '8': 'Phu Quoc', '9': 'Sapa', '10': 'Vung Tau', '11': 'Mui Ne', '12': 'Ban Me Thuoc', '13': 'Ninh Bình', '14': 'Huế', '15': 'An Giang', '16': 'Hòa Bình', '17': 'Châu Đốc', '18': 'Mộc Châu', '19': 'Cát Bà', '20': 'Cô Tô', '21': 'Tam Đảo', '22': 'Others, specify 1', '23': 'Others, specify 2', '24': 'Others, specify 3'}, 7],
            'Q9_ATT_08': ['Q9. Att', 'SA', {'1': 'HCM', '2': 'Hanoi', '3': 'Da Nang', '4': 'Da Lat', '5': 'Hoi an', '6': 'Nhat Trang', '7': 'Can Tho', '8': 'Phu Quoc', '9': 'Sapa', '10': 'Vung Tau', '11': 'Mui Ne', '12': 'Ban Me Thuoc', '13': 'Ninh Bình', '14': 'Huế', '15': 'An Giang', '16': 'Hòa Bình', '17': 'Châu Đốc', '18': 'Mộc Châu', '19': 'Cát Bà', '20': 'Cô Tô', '21': 'Tam Đảo', '22': 'Others, specify 1', '23': 'Others, specify 2', '24': 'Others, specify 3'}, 8],
            'Q9_ATT_09': ['Q9. Att', 'SA', {'1': 'HCM', '2': 'Hanoi', '3': 'Da Nang', '4': 'Da Lat', '5': 'Hoi an', '6': 'Nhat Trang', '7': 'Can Tho', '8': 'Phu Quoc', '9': 'Sapa', '10': 'Vung Tau', '11': 'Mui Ne', '12': 'Ban Me Thuoc', '13': 'Ninh Bình', '14': 'Huế', '15': 'An Giang', '16': 'Hòa Bình', '17': 'Châu Đốc', '18': 'Mộc Châu', '19': 'Cát Bà', '20': 'Cô Tô', '21': 'Tam Đảo', '22': 'Others, specify 1', '23': 'Others, specify 2', '24': 'Others, specify 3'}, 9],
            'Q9_ATT_10': ['Q9. Att', 'SA', {'1': 'HCM', '2': 'Hanoi', '3': 'Da Nang', '4': 'Da Lat', '5': 'Hoi an', '6': 'Nhat Trang', '7': 'Can Tho', '8': 'Phu Quoc', '9': 'Sapa', '10': 'Vung Tau', '11': 'Mui Ne', '12': 'Ban Me Thuoc', '13': 'Ninh Bình', '14': 'Huế', '15': 'An Giang', '16': 'Hòa Bình', '17': 'Châu Đốc', '18': 'Mộc Châu', '19': 'Cát Bà', '20': 'Cô Tô', '21': 'Tam Đảo', '22': 'Others, specify 1', '23': 'Others, specify 2', '24': 'Others, specify 3'}, 10],
            'Q9_ATT_11': ['Q9. Att', 'SA', {'1': 'HCM', '2': 'Hanoi', '3': 'Da Nang', '4': 'Da Lat', '5': 'Hoi an', '6': 'Nhat Trang', '7': 'Can Tho', '8': 'Phu Quoc', '9': 'Sapa', '10': 'Vung Tau', '11': 'Mui Ne', '12': 'Ban Me Thuoc', '13': 'Ninh Bình', '14': 'Huế', '15': 'An Giang', '16': 'Hòa Bình', '17': 'Châu Đốc', '18': 'Mộc Châu', '19': 'Cát Bà', '20': 'Cô Tô', '21': 'Tam Đảo', '22': 'Others, specify 1', '23': 'Others, specify 2', '24': 'Others, specify 3'}, 11],
            'Q9_ATT_12': ['Q9. Att', 'SA', {'1': 'HCM', '2': 'Hanoi', '3': 'Da Nang', '4': 'Da Lat', '5': 'Hoi an', '6': 'Nhat Trang', '7': 'Can Tho', '8': 'Phu Quoc', '9': 'Sapa', '10': 'Vung Tau', '11': 'Mui Ne', '12': 'Ban Me Thuoc', '13': 'Ninh Bình', '14': 'Huế', '15': 'An Giang', '16': 'Hòa Bình', '17': 'Châu Đốc', '18': 'Mộc Châu', '19': 'Cát Bà', '20': 'Cô Tô', '21': 'Tam Đảo', '22': 'Others, specify 1', '23': 'Others, specify 2', '24': 'Others, specify 3'}, 12],
            'Q9_ATT_13': ['Q9. Att', 'SA', {'1': 'HCM', '2': 'Hanoi', '3': 'Da Nang', '4': 'Da Lat', '5': 'Hoi an', '6': 'Nhat Trang', '7': 'Can Tho', '8': 'Phu Quoc', '9': 'Sapa', '10': 'Vung Tau', '11': 'Mui Ne', '12': 'Ban Me Thuoc', '13': 'Ninh Bình', '14': 'Huế', '15': 'An Giang', '16': 'Hòa Bình', '17': 'Châu Đốc', '18': 'Mộc Châu', '19': 'Cát Bà', '20': 'Cô Tô', '21': 'Tam Đảo', '22': 'Others, specify 1', '23': 'Others, specify 2', '24': 'Others, specify 3'}, 13],
            'Q9_ATT_14': ['Q9. Att', 'SA', {'1': 'HCM', '2': 'Hanoi', '3': 'Da Nang', '4': 'Da Lat', '5': 'Hoi an', '6': 'Nhat Trang', '7': 'Can Tho', '8': 'Phu Quoc', '9': 'Sapa', '10': 'Vung Tau', '11': 'Mui Ne', '12': 'Ban Me Thuoc', '13': 'Ninh Bình', '14': 'Huế', '15': 'An Giang', '16': 'Hòa Bình', '17': 'Châu Đốc', '18': 'Mộc Châu', '19': 'Cát Bà', '20': 'Cô Tô', '21': 'Tam Đảo', '22': 'Others, specify 1', '23': 'Others, specify 2', '24': 'Others, specify 3'}, 14],
            'Q9_ATT_15': ['Q9. Att', 'SA', {'1': 'HCM', '2': 'Hanoi', '3': 'Da Nang', '4': 'Da Lat', '5': 'Hoi an', '6': 'Nhat Trang', '7': 'Can Tho', '8': 'Phu Quoc', '9': 'Sapa', '10': 'Vung Tau', '11': 'Mui Ne', '12': 'Ban Me Thuoc', '13': 'Ninh Bình', '14': 'Huế', '15': 'An Giang', '16': 'Hòa Bình', '17': 'Châu Đốc', '18': 'Mộc Châu', '19': 'Cát Bà', '20': 'Cô Tô', '21': 'Tam Đảo', '22': 'Others, specify 1', '23': 'Others, specify 2', '24': 'Others, specify 3'}, 15],
            'Q9_ATT_16': ['Q9. Att', 'SA', {'1': 'HCM', '2': 'Hanoi', '3': 'Da Nang', '4': 'Da Lat', '5': 'Hoi an', '6': 'Nhat Trang', '7': 'Can Tho', '8': 'Phu Quoc', '9': 'Sapa', '10': 'Vung Tau', '11': 'Mui Ne', '12': 'Ban Me Thuoc', '13': 'Ninh Bình', '14': 'Huế', '15': 'An Giang', '16': 'Hòa Bình', '17': 'Châu Đốc', '18': 'Mộc Châu', '19': 'Cát Bà', '20': 'Cô Tô', '21': 'Tam Đảo', '22': 'Others, specify 1', '23': 'Others, specify 2', '24': 'Others, specify 3'}, 16],
            'Q9_ATT_17': ['Q9. Att', 'SA', {'1': 'HCM', '2': 'Hanoi', '3': 'Da Nang', '4': 'Da Lat', '5': 'Hoi an', '6': 'Nhat Trang', '7': 'Can Tho', '8': 'Phu Quoc', '9': 'Sapa', '10': 'Vung Tau', '11': 'Mui Ne', '12': 'Ban Me Thuoc', '13': 'Ninh Bình', '14': 'Huế', '15': 'An Giang', '16': 'Hòa Bình', '17': 'Châu Đốc', '18': 'Mộc Châu', '19': 'Cát Bà', '20': 'Cô Tô', '21': 'Tam Đảo', '22': 'Others, specify 1', '23': 'Others, specify 2', '24': 'Others, specify 3'}, 17],
            'Q9_ATT_18': ['Q9. Att', 'SA', {'1': 'HCM', '2': 'Hanoi', '3': 'Da Nang', '4': 'Da Lat', '5': 'Hoi an', '6': 'Nhat Trang', '7': 'Can Tho', '8': 'Phu Quoc', '9': 'Sapa', '10': 'Vung Tau', '11': 'Mui Ne', '12': 'Ban Me Thuoc', '13': 'Ninh Bình', '14': 'Huế', '15': 'An Giang', '16': 'Hòa Bình', '17': 'Châu Đốc', '18': 'Mộc Châu', '19': 'Cát Bà', '20': 'Cô Tô', '21': 'Tam Đảo', '22': 'Others, specify 1', '23': 'Others, specify 2', '24': 'Others, specify 3'}, 18],
            'Q9_ATT_19': ['Q9. Att', 'SA', {'1': 'HCM', '2': 'Hanoi', '3': 'Da Nang', '4': 'Da Lat', '5': 'Hoi an', '6': 'Nhat Trang', '7': 'Can Tho', '8': 'Phu Quoc', '9': 'Sapa', '10': 'Vung Tau', '11': 'Mui Ne', '12': 'Ban Me Thuoc', '13': 'Ninh Bình', '14': 'Huế', '15': 'An Giang', '16': 'Hòa Bình', '17': 'Châu Đốc', '18': 'Mộc Châu', '19': 'Cát Bà', '20': 'Cô Tô', '21': 'Tam Đảo', '22': 'Others, specify 1', '23': 'Others, specify 2', '24': 'Others, specify 3'}, 19],
            'Q9_ATT_20': ['Q9. Att', 'SA', {'1': 'HCM', '2': 'Hanoi', '3': 'Da Nang', '4': 'Da Lat', '5': 'Hoi an', '6': 'Nhat Trang', '7': 'Can Tho', '8': 'Phu Quoc', '9': 'Sapa', '10': 'Vung Tau', '11': 'Mui Ne', '12': 'Ban Me Thuoc', '13': 'Ninh Bình', '14': 'Huế', '15': 'An Giang', '16': 'Hòa Bình', '17': 'Châu Đốc', '18': 'Mộc Châu', '19': 'Cát Bà', '20': 'Cô Tô', '21': 'Tam Đảo', '22': 'Others, specify 1', '23': 'Others, specify 2', '24': 'Others, specify 3'}, 20],
            'Q9_ATT_21': ['Q9. Att', 'SA', {'1': 'HCM', '2': 'Hanoi', '3': 'Da Nang', '4': 'Da Lat', '5': 'Hoi an', '6': 'Nhat Trang', '7': 'Can Tho', '8': 'Phu Quoc', '9': 'Sapa', '10': 'Vung Tau', '11': 'Mui Ne', '12': 'Ban Me Thuoc', '13': 'Ninh Bình', '14': 'Huế', '15': 'An Giang', '16': 'Hòa Bình', '17': 'Châu Đốc', '18': 'Mộc Châu', '19': 'Cát Bà', '20': 'Cô Tô', '21': 'Tam Đảo', '22': 'Others, specify 1', '23': 'Others, specify 2', '24': 'Others, specify 3'}, 21],
            'Q9_ATT_22': ['Q9. Att', 'SA', {'1': 'HCM', '2': 'Hanoi', '3': 'Da Nang', '4': 'Da Lat', '5': 'Hoi an', '6': 'Nhat Trang', '7': 'Can Tho', '8': 'Phu Quoc', '9': 'Sapa', '10': 'Vung Tau', '11': 'Mui Ne', '12': 'Ban Me Thuoc', '13': 'Ninh Bình', '14': 'Huế', '15': 'An Giang', '16': 'Hòa Bình', '17': 'Châu Đốc', '18': 'Mộc Châu', '19': 'Cát Bà', '20': 'Cô Tô', '21': 'Tam Đảo', '22': 'Others, specify 1', '23': 'Others, specify 2', '24': 'Others, specify 3'}, 22],
            'Q9_ATT_23': ['Q9. Att', 'SA', {'1': 'HCM', '2': 'Hanoi', '3': 'Da Nang', '4': 'Da Lat', '5': 'Hoi an', '6': 'Nhat Trang', '7': 'Can Tho', '8': 'Phu Quoc', '9': 'Sapa', '10': 'Vung Tau', '11': 'Mui Ne', '12': 'Ban Me Thuoc', '13': 'Ninh Bình', '14': 'Huế', '15': 'An Giang', '16': 'Hòa Bình', '17': 'Châu Đốc', '18': 'Mộc Châu', '19': 'Cát Bà', '20': 'Cô Tô', '21': 'Tam Đảo', '22': 'Others, specify 1', '23': 'Others, specify 2', '24': 'Others, specify 3'}, 23],
            'Q9_ATT_24': ['Q9. Att', 'SA', {'1': 'HCM', '2': 'Hanoi', '3': 'Da Nang', '4': 'Da Lat', '5': 'Hoi an', '6': 'Nhat Trang', '7': 'Can Tho', '8': 'Phu Quoc', '9': 'Sapa', '10': 'Vung Tau', '11': 'Mui Ne', '12': 'Ban Me Thuoc', '13': 'Ninh Bình', '14': 'Huế', '15': 'An Giang', '16': 'Hòa Bình', '17': 'Châu Đốc', '18': 'Mộc Châu', '19': 'Cát Bà', '20': 'Cô Tô', '21': 'Tam Đảo', '22': 'Others, specify 1', '23': 'Others, specify 2', '24': 'Others, specify 3'}, 24],
        }

        for key, val in dict_qres_add_new.items():
            df_info_output = pd.concat([df_info_output, pd.DataFrame(columns=['var_name', 'var_lbl', 'var_type', 'val_lbl'], data=[[key, val[0], val[1], val[2]]])], axis=0, ignore_index=True)
            df_data_output = pd.concat([df_data_output, pd.DataFrame(columns=[key], data=[val[-1]] * df_data_output.shape[0])], axis=1)

        # Define structure----------------------------------------------------------------------------------------------
        self.logger.info('Define structure')

        id_col = 'ID'
        sp_col = 'Q9_ATT'

        lst_scr = [
            'City',
            'Gender',
            'Age',
            'HHI',
            'Q1',
            'Q2',
            'Q3_1',
            'Q3_2',
            'Q3_3',
            'Q3_4',
            'Q3_5',
            'Q3_6',
            'Q3_7',
            'Q3_8',
            'Q3_9',
            'Q3_10',
            'Q3_11',
            'Q3_12',
            'Q3_o12',
            'Q4_1',
            'Q4_2',
            'Q4_3',
            'Q4_4',
            'Q4_5',
            'Q4_6',
            'Q4_7',
            'Q4_8',
            'Q4_9',
            'Q5_1',
            'Q5_2',
            'Q5_3',
            'Q5_4',
            'Q5_5',
            'Q5_6',
            'Q5_7',
            'Q5_8',
            'Q5_9',
            'Q5_10',
            'Q5_11',
            'Q5_12',
            'Q5_o12',
            'Q6_1',
            'Q6_2',
            'Q6_3',
            'Q6_4',
            'Q6_5',
            'Q6_6',
            'Q6_7',
            'Q6_8',
            'Q6_9',
            'Q7',
            'Q8_1',
            'Q8_2',
            'Q8_3',
            'Q8_4',
            'Q8_5',
            'Q8_6',
            'Q8_7',
            'Q8_8',
            'Q8_9',
            'Q8_10',
            'Q8_11',
            'Q8_12',
            'Q8_13',
            'Q8_14',
            'Q8_15',
            'Q8_16',
            'Q8_17',
            'Q8_18',
            'Q8_19',
            'Q8_20',
            'Q8_21',
            'Q8_22',
            'Q8_23',
            'Q8_24',
            'Q8_o22',
            'Q8_o23',
            'Q8_o24',
        ]

        lst_scr.extend(list(dict_q9_comb.keys()))

        # This project don't have FC Qres
        lst_fc = []

        dict_sp = {
            1: {
                'Q9_ATT_01': 'Q9_ATT',
                'Q9_01_1': 'Q9_1',
                'Q9_01_2': 'Q9_2',
                'Q9_01_3': 'Q9_3',
                'Q9_01_4': 'Q9_4',
                'Q9_01_5': 'Q9_5',
                'Q9_01_6': 'Q9_6',
                'Q9_01_7': 'Q9_7',
                'Q9_01_8': 'Q9_8',
                'Q9_01_9': 'Q9_9',
                'Q9_01_10': 'Q9_10',
                'Q9_01_11': 'Q9_11',
                'Q9_01_12': 'Q9_12',
                'Q9_01_13': 'Q9_13',
                'Q9_01_14': 'Q9_14',
            },
            2: {
                'Q9_ATT_02': 'Q9_ATT',
                'Q9_02_1': 'Q9_1',
                'Q9_02_2': 'Q9_2',
                'Q9_02_3': 'Q9_3',
                'Q9_02_4': 'Q9_4',
                'Q9_02_5': 'Q9_5',
                'Q9_02_6': 'Q9_6',
                'Q9_02_7': 'Q9_7',
                'Q9_02_8': 'Q9_8',
                'Q9_02_9': 'Q9_9',
                'Q9_02_10': 'Q9_10',
                'Q9_02_11': 'Q9_11',
                'Q9_02_12': 'Q9_12',
                'Q9_02_13': 'Q9_13',
                'Q9_02_14': 'Q9_14',
            },
            3: {
                'Q9_ATT_03': 'Q9_ATT',
                'Q9_03_1': 'Q9_1',
                'Q9_03_2': 'Q9_2',
                'Q9_03_3': 'Q9_3',
                'Q9_03_4': 'Q9_4',
                'Q9_03_5': 'Q9_5',
                'Q9_03_6': 'Q9_6',
                'Q9_03_7': 'Q9_7',
                'Q9_03_8': 'Q9_8',
                'Q9_03_9': 'Q9_9',
                'Q9_03_10': 'Q9_10',
                'Q9_03_11': 'Q9_11',
                'Q9_03_12': 'Q9_12',
                'Q9_03_13': 'Q9_13',
                'Q9_03_14': 'Q9_14',
            },
            4: {
                'Q9_ATT_04': 'Q9_ATT',
                'Q9_04_1': 'Q9_1',
                'Q9_04_2': 'Q9_2',
                'Q9_04_3': 'Q9_3',
                'Q9_04_4': 'Q9_4',
                'Q9_04_5': 'Q9_5',
                'Q9_04_6': 'Q9_6',
                'Q9_04_7': 'Q9_7',
                'Q9_04_8': 'Q9_8',
                'Q9_04_9': 'Q9_9',
                'Q9_04_10': 'Q9_10',
                'Q9_04_11': 'Q9_11',
                'Q9_04_12': 'Q9_12',
                'Q9_04_13': 'Q9_13',
                'Q9_04_14': 'Q9_14',
            },
            5: {
                'Q9_ATT_05': 'Q9_ATT',
                'Q9_05_1': 'Q9_1',
                'Q9_05_2': 'Q9_2',
                'Q9_05_3': 'Q9_3',
                'Q9_05_4': 'Q9_4',
                'Q9_05_5': 'Q9_5',
                'Q9_05_6': 'Q9_6',
                'Q9_05_7': 'Q9_7',
                'Q9_05_8': 'Q9_8',
                'Q9_05_9': 'Q9_9',
                'Q9_05_10': 'Q9_10',
                'Q9_05_11': 'Q9_11',
                'Q9_05_12': 'Q9_12',
                'Q9_05_13': 'Q9_13',
                'Q9_05_14': 'Q9_14',
            },
            6: {
                'Q9_ATT_06': 'Q9_ATT',
                'Q9_06_1': 'Q9_1',
                'Q9_06_2': 'Q9_2',
                'Q9_06_3': 'Q9_3',
                'Q9_06_4': 'Q9_4',
                'Q9_06_5': 'Q9_5',
                'Q9_06_6': 'Q9_6',
                'Q9_06_7': 'Q9_7',
                'Q9_06_8': 'Q9_8',
                'Q9_06_9': 'Q9_9',
                'Q9_06_10': 'Q9_10',
                'Q9_06_11': 'Q9_11',
                'Q9_06_12': 'Q9_12',
                'Q9_06_13': 'Q9_13',
                'Q9_06_14': 'Q9_14',
            },
            7: {
                'Q9_ATT_07': 'Q9_ATT',
                'Q9_07_1': 'Q9_1',
                'Q9_07_2': 'Q9_2',
                'Q9_07_3': 'Q9_3',
                'Q9_07_4': 'Q9_4',
                'Q9_07_5': 'Q9_5',
                'Q9_07_6': 'Q9_6',
                'Q9_07_7': 'Q9_7',
                'Q9_07_8': 'Q9_8',
                'Q9_07_9': 'Q9_9',
                'Q9_07_10': 'Q9_10',
                'Q9_07_11': 'Q9_11',
                'Q9_07_12': 'Q9_12',
                'Q9_07_13': 'Q9_13',
                'Q9_07_14': 'Q9_14',
            },
            8: {
                'Q9_ATT_08': 'Q9_ATT',
                'Q9_08_1': 'Q9_1',
                'Q9_08_2': 'Q9_2',
                'Q9_08_3': 'Q9_3',
                'Q9_08_4': 'Q9_4',
                'Q9_08_5': 'Q9_5',
                'Q9_08_6': 'Q9_6',
                'Q9_08_7': 'Q9_7',
                'Q9_08_8': 'Q9_8',
                'Q9_08_9': 'Q9_9',
                'Q9_08_10': 'Q9_10',
                'Q9_08_11': 'Q9_11',
                'Q9_08_12': 'Q9_12',
                'Q9_08_13': 'Q9_13',
                'Q9_08_14': 'Q9_14',
            },
            9: {
                'Q9_ATT_09': 'Q9_ATT',
                'Q9_09_1': 'Q9_1',
                'Q9_09_2': 'Q9_2',
                'Q9_09_3': 'Q9_3',
                'Q9_09_4': 'Q9_4',
                'Q9_09_5': 'Q9_5',
                'Q9_09_6': 'Q9_6',
                'Q9_09_7': 'Q9_7',
                'Q9_09_8': 'Q9_8',
                'Q9_09_9': 'Q9_9',
                'Q9_09_10': 'Q9_10',
                'Q9_09_11': 'Q9_11',
                'Q9_09_12': 'Q9_12',
                'Q9_09_13': 'Q9_13',
                'Q9_09_14': 'Q9_14',
            },
            10: {
                'Q9_ATT_10': 'Q9_ATT',
                'Q9_10_1': 'Q9_1',
                'Q9_10_2': 'Q9_2',
                'Q9_10_3': 'Q9_3',
                'Q9_10_4': 'Q9_4',
                'Q9_10_5': 'Q9_5',
                'Q9_10_6': 'Q9_6',
                'Q9_10_7': 'Q9_7',
                'Q9_10_8': 'Q9_8',
                'Q9_10_9': 'Q9_9',
                'Q9_10_10': 'Q9_10',
                'Q9_10_11': 'Q9_11',
                'Q9_10_12': 'Q9_12',
                'Q9_10_13': 'Q9_13',
                'Q9_10_14': 'Q9_14',
            },
            11: {
                'Q9_ATT_11': 'Q9_ATT',
                'Q9_11_1': 'Q9_1',
                'Q9_11_2': 'Q9_2',
                'Q9_11_3': 'Q9_3',
                'Q9_11_4': 'Q9_4',
                'Q9_11_5': 'Q9_5',
                'Q9_11_6': 'Q9_6',
                'Q9_11_7': 'Q9_7',
                'Q9_11_8': 'Q9_8',
                'Q9_11_9': 'Q9_9',
                'Q9_11_10': 'Q9_10',
                'Q9_11_11': 'Q9_11',
                'Q9_11_12': 'Q9_12',
                'Q9_11_13': 'Q9_13',
                'Q9_11_14': 'Q9_14',
            },
            12: {
                'Q9_ATT_12': 'Q9_ATT',
                'Q9_12_1': 'Q9_1',
                'Q9_12_2': 'Q9_2',
                'Q9_12_3': 'Q9_3',
                'Q9_12_4': 'Q9_4',
                'Q9_12_5': 'Q9_5',
                'Q9_12_6': 'Q9_6',
                'Q9_12_7': 'Q9_7',
                'Q9_12_8': 'Q9_8',
                'Q9_12_9': 'Q9_9',
                'Q9_12_10': 'Q9_10',
                'Q9_12_11': 'Q9_11',
                'Q9_12_12': 'Q9_12',
                'Q9_12_13': 'Q9_13',
                'Q9_12_14': 'Q9_14',
            },
            13: {
                'Q9_ATT_13': 'Q9_ATT',
                'Q9_13_1': 'Q9_1',
                'Q9_13_2': 'Q9_2',
                'Q9_13_3': 'Q9_3',
                'Q9_13_4': 'Q9_4',
                'Q9_13_5': 'Q9_5',
                'Q9_13_6': 'Q9_6',
                'Q9_13_7': 'Q9_7',
                'Q9_13_8': 'Q9_8',
                'Q9_13_9': 'Q9_9',
                'Q9_13_10': 'Q9_10',
                'Q9_13_11': 'Q9_11',
                'Q9_13_12': 'Q9_12',
                'Q9_13_13': 'Q9_13',
                'Q9_13_14': 'Q9_14',
            },
            14: {
                'Q9_ATT_14': 'Q9_ATT',
                'Q9_14_1': 'Q9_1',
                'Q9_14_2': 'Q9_2',
                'Q9_14_3': 'Q9_3',
                'Q9_14_4': 'Q9_4',
                'Q9_14_5': 'Q9_5',
                'Q9_14_6': 'Q9_6',
                'Q9_14_7': 'Q9_7',
                'Q9_14_8': 'Q9_8',
                'Q9_14_9': 'Q9_9',
                'Q9_14_10': 'Q9_10',
                'Q9_14_11': 'Q9_11',
                'Q9_14_12': 'Q9_12',
                'Q9_14_13': 'Q9_13',
                'Q9_14_14': 'Q9_14',
            },
            15: {
                'Q9_ATT_15': 'Q9_ATT',
                'Q9_15_1': 'Q9_1',
                'Q9_15_2': 'Q9_2',
                'Q9_15_3': 'Q9_3',
                'Q9_15_4': 'Q9_4',
                'Q9_15_5': 'Q9_5',
                'Q9_15_6': 'Q9_6',
                'Q9_15_7': 'Q9_7',
                'Q9_15_8': 'Q9_8',
                'Q9_15_9': 'Q9_9',
                'Q9_15_10': 'Q9_10',
                'Q9_15_11': 'Q9_11',
                'Q9_15_12': 'Q9_12',
                'Q9_15_13': 'Q9_13',
                'Q9_15_14': 'Q9_14',
            },
            16: {
                'Q9_ATT_16': 'Q9_ATT',
                'Q9_16_1': 'Q9_1',
                'Q9_16_2': 'Q9_2',
                'Q9_16_3': 'Q9_3',
                'Q9_16_4': 'Q9_4',
                'Q9_16_5': 'Q9_5',
                'Q9_16_6': 'Q9_6',
                'Q9_16_7': 'Q9_7',
                'Q9_16_8': 'Q9_8',
                'Q9_16_9': 'Q9_9',
                'Q9_16_10': 'Q9_10',
                'Q9_16_11': 'Q9_11',
                'Q9_16_12': 'Q9_12',
                'Q9_16_13': 'Q9_13',
                'Q9_16_14': 'Q9_14',
            },
            17: {
                'Q9_ATT_17': 'Q9_ATT',
                'Q9_17_1': 'Q9_1',
                'Q9_17_2': 'Q9_2',
                'Q9_17_3': 'Q9_3',
                'Q9_17_4': 'Q9_4',
                'Q9_17_5': 'Q9_5',
                'Q9_17_6': 'Q9_6',
                'Q9_17_7': 'Q9_7',
                'Q9_17_8': 'Q9_8',
                'Q9_17_9': 'Q9_9',
                'Q9_17_10': 'Q9_10',
                'Q9_17_11': 'Q9_11',
                'Q9_17_12': 'Q9_12',
                'Q9_17_13': 'Q9_13',
                'Q9_17_14': 'Q9_14',
            },
            18: {
                'Q9_ATT_18': 'Q9_ATT',
                'Q9_18_1': 'Q9_1',
                'Q9_18_2': 'Q9_2',
                'Q9_18_3': 'Q9_3',
                'Q9_18_4': 'Q9_4',
                'Q9_18_5': 'Q9_5',
                'Q9_18_6': 'Q9_6',
                'Q9_18_7': 'Q9_7',
                'Q9_18_8': 'Q9_8',
                'Q9_18_9': 'Q9_9',
                'Q9_18_10': 'Q9_10',
                'Q9_18_11': 'Q9_11',
                'Q9_18_12': 'Q9_12',
                'Q9_18_13': 'Q9_13',
                'Q9_18_14': 'Q9_14',
            },
            19: {
                'Q9_ATT_19': 'Q9_ATT',
                'Q9_19_1': 'Q9_1',
                'Q9_19_2': 'Q9_2',
                'Q9_19_3': 'Q9_3',
                'Q9_19_4': 'Q9_4',
                'Q9_19_5': 'Q9_5',
                'Q9_19_6': 'Q9_6',
                'Q9_19_7': 'Q9_7',
                'Q9_19_8': 'Q9_8',
                'Q9_19_9': 'Q9_9',
                'Q9_19_10': 'Q9_10',
                'Q9_19_11': 'Q9_11',
                'Q9_19_12': 'Q9_12',
                'Q9_19_13': 'Q9_13',
                'Q9_19_14': 'Q9_14',
            },
            20: {
                'Q9_ATT_20': 'Q9_ATT',
                'Q9_20_1': 'Q9_1',
                'Q9_20_2': 'Q9_2',
                'Q9_20_3': 'Q9_3',
                'Q9_20_4': 'Q9_4',
                'Q9_20_5': 'Q9_5',
                'Q9_20_6': 'Q9_6',
                'Q9_20_7': 'Q9_7',
                'Q9_20_8': 'Q9_8',
                'Q9_20_9': 'Q9_9',
                'Q9_20_10': 'Q9_10',
                'Q9_20_11': 'Q9_11',
                'Q9_20_12': 'Q9_12',
                'Q9_20_13': 'Q9_13',
                'Q9_20_14': 'Q9_14',
            },
            21: {
                'Q9_ATT_21': 'Q9_ATT',
                'Q9_21_1': 'Q9_1',
                'Q9_21_2': 'Q9_2',
                'Q9_21_3': 'Q9_3',
                'Q9_21_4': 'Q9_4',
                'Q9_21_5': 'Q9_5',
                'Q9_21_6': 'Q9_6',
                'Q9_21_7': 'Q9_7',
                'Q9_21_8': 'Q9_8',
                'Q9_21_9': 'Q9_9',
                'Q9_21_10': 'Q9_10',
                'Q9_21_11': 'Q9_11',
                'Q9_21_12': 'Q9_12',
                'Q9_21_13': 'Q9_13',
                'Q9_21_14': 'Q9_14',
            },
            22: {
                'Q9_ATT_22': 'Q9_ATT',
                'Q9_22_1': 'Q9_1',
                'Q9_22_2': 'Q9_2',
                'Q9_22_3': 'Q9_3',
                'Q9_22_4': 'Q9_4',
                'Q9_22_5': 'Q9_5',
                'Q9_22_6': 'Q9_6',
                'Q9_22_7': 'Q9_7',
                'Q9_22_8': 'Q9_8',
                'Q9_22_9': 'Q9_9',
                'Q9_22_10': 'Q9_10',
                'Q9_22_11': 'Q9_11',
                'Q9_22_12': 'Q9_12',
                'Q9_22_13': 'Q9_13',
                'Q9_22_14': 'Q9_14',
            },
            23: {
                'Q9_ATT_23': 'Q9_ATT',
                'Q9_23_1': 'Q9_1',
                'Q9_23_2': 'Q9_2',
                'Q9_23_3': 'Q9_3',
                'Q9_23_4': 'Q9_4',
                'Q9_23_5': 'Q9_5',
                'Q9_23_6': 'Q9_6',
                'Q9_23_7': 'Q9_7',
                'Q9_23_8': 'Q9_8',
                'Q9_23_9': 'Q9_9',
                'Q9_23_10': 'Q9_10',
                'Q9_23_11': 'Q9_11',
                'Q9_23_12': 'Q9_12',
                'Q9_23_13': 'Q9_13',
                'Q9_23_14': 'Q9_14',
            },
            24: {
                'Q9_ATT_24': 'Q9_ATT',
                'Q9_24_1': 'Q9_1',
                'Q9_24_2': 'Q9_2',
                'Q9_24_3': 'Q9_3',
                'Q9_24_4': 'Q9_4',
                'Q9_24_5': 'Q9_5',
                'Q9_24_6': 'Q9_6',
                'Q9_24_7': 'Q9_7',
                'Q9_24_8': 'Q9_8',
                'Q9_24_9': 'Q9_9',
                'Q9_24_10': 'Q9_10',
                'Q9_24_11': 'Q9_11',
                'Q9_24_12': 'Q9_12',
                'Q9_24_13': 'Q9_13',
                'Q9_24_14': 'Q9_14',
            },
        }

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

        # if dict_qre_OE_info:
        #
        #     # ADD OE to Data stack--------------------------------------------------------------------------------------
        #     lst_OE_col = list(dict_qre_OE_info.keys())
        #     df_data_stack[lst_OE_col] = pd.DataFrame([[np.nan] * len(lst_OE_col)], index=df_data_stack.index)
        #
        #     # Remember edit this
        #     for item in lst_addin_OE_value:
        #         df_data_stack.loc[
        #             (df_data_stack[item[0]] == item[1]) & (df_data_stack[item[2]] == item[3]), [item[4]]] = [item[5]]
        #
        #     # END ADD OE to Data stack----------------------------------------------------------------------------------
        #
        #     # ADD OE to Info stack--------------------------------------------------------------------------------------
        #     df_info_stack = pd.concat([df_info_stack,
        #                                pd.DataFrame(columns=['var_name', 'var_lbl', 'var_type', 'val_lbl'],
        #                                             data=list(dict_qre_OE_info.values()))], axis=0)
        #     # END ADD OE to Info stack----------------------------------------------------------------------------------

        # if dict_qre_net_info:
        #     pass
        #     # # ADD MA OE to Data stack-----------------------------------------------------------------------------------
        #     # # Remember edit this
        #     # for item in lst_addin_MA_value:
        #     #
        #     #     idx_item = df_data_stack.loc[(df_data_stack[item[0]] == item[1]) & (df_data_stack[item[2]] == item[3]), item[0]].index[0]
        #     #     str_ma_oe_name = item[4].rsplit('_', 1)[0]
        #     #     int_ma_oe_code = int(item[4].rsplit('_', 1)[1].replace('o', ''))
        #     #
        #     #     lst_ma_oe_col = df_info_stack.loc[df_info_stack['var_name'].str.contains(f'{str_ma_oe_name}_[0-9]+'), 'var_name'].values.tolist()
        #     #
        #     #     is_found = False
        #     #     for col in lst_ma_oe_col:
        #     #         if df_data_stack.at[idx_item, col] == int_ma_oe_code:
        #     #             is_found = True
        #     #             df_data_stack.at[idx_item, col] = item[5]
        #     #             break
        #     #
        #     #     if not is_found:
        #     #         for col in lst_ma_oe_col:
        #     #             if pd.isnull(df_data_stack.at[idx_item, col]):
        #     #                 df_data_stack.at[idx_item, col] = item[5]
        #     #                 break
        #     # # END ADD MA OE to Data stack-------------------------------------------------------------------------------
        #
        #     # # ADD MA OE to Info stack--------------------------------------------------------------------------------------
        #     # for key, val in dict_qre_net_info.items():
        #     #     df_info_stack.loc[df_info_stack['var_name'].str.contains(key), 'val_lbl'] = [val] * df_info_stack.loc[df_info_stack['var_name'].str.contains(key), 'val_lbl'].shape[0]
        #     # # END ADD MA OE to Info stack----------------------------------------------------------------------------------

        # Reset df_info_stack index
        df_info_stack['idx_var_name'] = df_info_stack['var_name']
        df_info_stack.set_index('idx_var_name', inplace=True)
        df_info_stack = df_info_stack.loc[list(df_data_stack.columns), :]
        df_info_stack.reindex(list(df_data_stack.columns))
        df_info_stack.reset_index(drop=True, inplace=True)

        # df_data_stack.to_excel('zzzz_df_data_stack.xlsx')
        # df_info_stack.to_excel('zzzz_df_info_stack.xlsx')


        # End Data stack format-----------------------------------------------------------------------------------------



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
            2: {
                'data': df_data_stack,
                'info': df_info_stack,
                'tail_name': 'Stack',
                'sheet_name': 'Stack',
                'is_recode_to_lbl': True,
            },
        }



        self.generate_multiple_sav_sps(dict_dfs=dict_dfs, is_md=False, is_export_xlsx=True)
        # END Generate SAV files----------------------------------------------------------------------------------------


