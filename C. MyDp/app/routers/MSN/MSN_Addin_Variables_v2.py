import pandas as pd
import numpy as np
import traceback
import re


class AddinVariables:
    def __init__(self, prj_addVars, df_data: pd.DataFrame, df_info: pd.DataFrame, lstProductCode: list):

        self.prj_addVars = prj_addVars
        self.df_data = df_data
        self.df_info = df_info
        self.lstProductCode = lstProductCode


    def addin_vars(self) -> (bool, None):

        try:
            print('----------------------------Addin variables----------------------------')
            
            for key, val in self.prj_addVars.items():

                availabelCols = list(self.df_data.columns)

                # if 'Main_PC9_Thoi_gian_cho_chin_mi_Codes' in val['name']:
                #     a = 1

                if '#prod' in val['name']:
                    varName = str(val['name']).replace('#prod1', str(self.lstProductCode[0])).replace('#prod2', str(self.lstProductCode[1]))
                else:
                    varName = val['name']

                if '#prod' in val['lbl']:
                    varLbl = str(val['lbl']).replace('#prod1', str(self.lstProductCode[0])).replace('#prod2', str(self.lstProductCode[1]))
                else:
                    varLbl = val['lbl']

                if varName in availabelCols:
                    print(f"{varName} is already in data. You are updating that.")
                else:
                    print(f'Addin variables {varName}')

                lst_info = [varName, varLbl, str(), dict()]

                self.df_data[varName] = [np.nan] * self.df_data.shape[0]

                for k, v in val['cats'].items():

                    catLbl = str()
                    if str(v['val']).upper() == 'CAL':
                        catVal = 999_999_999
                        lst_info[2] = 'FT'
                    else:
                        lst_info[2] = 'SA'
                        catVal = int(v['val'])
                        catLbl = str(v['lbl'])

                        lst_info[3].update({catVal: catLbl})

                    str_condition = v['condition']
                    if '#prod' in v['condition']:
                        str_condition = str_condition.replace('#prod1', str(self.lstProductCode[0])).replace('#prod2', str(self.lstProductCode[1]))

                    catLbl = self.execCondition(varName=varName, catVal=catVal, catLbl=catLbl,
                                                strCondition=str_condition, availabelCols=list(availabelCols))

                    if str(v['val']).upper() != 'CAL':
                        lst_info[3].update({catVal: catLbl})

                df_add_info = pd.DataFrame([lst_info], columns=['var_name', 'var_lbl', 'var_type', 'val_lbl'])

                self.df_info = pd.concat([self.df_info, df_add_info], axis=0)

            self.df_info.drop_duplicates(subset='var_name', keep='first', inplace=True)

            self.df_info.reset_index(drop=True, inplace=True)
            print('----------------------------Addin variables completed----------------------------')

            return True, None

        except Exception:
            print(traceback.format_exc())
            return False, traceback.format_exc()



    def execCondition(self, varName: str, catVal: int, catLbl: str, strCondition: str, availabelCols: list) -> str:

        if strCondition.upper() == 'SYSMISS':
            return catLbl

        df = self.df_data

        if 'MEDIAN' in strCondition:
            str_qre_median = str(re.findall(r'\[MEDIAN#\w+]', strCondition)[0]).replace('[MEDIAN#', '').replace(']', '')
            val_median = df[str_qre_median].median()

            strCondition = re.sub(r'\[MEDIAN#\w+]', str(val_median), strCondition)

            if pd.isna(val_median):
                catLbl = catLbl.replace('[Median]', f"[Median=Nan]")
                return catLbl

            catLbl = catLbl.replace('[Median]', f"[Median={str(val_median)}]")


        if catVal == 999_999_999:
            strCondition = strCondition.replace("[", "df['").replace("]", "']")
            strExec = f"df['{varName}'] = {strCondition}"
            exec(strExec)

        else:

            df_filter = df.query(strCondition).copy()
            df.loc[df_filter.index, varName] = [catVal] * df_filter.shape[0]

        return catLbl








