import pandas as pd
import numpy as np
import traceback
import re


class AddinVariables:
    def __init__(self, prj_addVars, df: pd.DataFrame, dictVarName: dict, dictValLbl: dict, lstProductCode: list):

        self.prj_addVars = prj_addVars
        self.df = df
        self.dictVarName = dictVarName
        self.dictValLbl = dictValLbl
        self.lstProductCode = lstProductCode


    def addin(self):

        try:
            print('----------------------------Addin variables----------------------------')

            availabelCols = self.df.columns

            for key, val in self.prj_addVars.items():

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

                self.dictVarName[varName] = varLbl
                self.df[varName] = [np.nan] * self.df.shape[0]

                for k, v in val['cats'].items():

                    if str(v['val']).upper() == 'CAL':
                        catVal = 999_999_999
                    else:
                        catVal = int(v['val'])
                        catLbl = str(v['lbl'])

                        if varName in self.dictValLbl.keys():
                            self.dictValLbl[varName].update({catVal: catLbl})
                        else:
                            self.dictValLbl[varName] = {catVal: catLbl}


                    catLbl = self.execCondition(varName=varName, catVal=catVal, catLbl=catLbl,
                                                strCondition=v['condition'], availabelCols=list(availabelCols))

                    if str(v['val']).upper() != 'CAL':
                        self.dictValLbl[varName].update({catVal: catLbl})



            print('----------------------------Addin variables completed----------------------------')

            return True, None

        except Exception:
            print(traceback.format_exc())
            return False, traceback.format_exc()



    def execCondition(self, varName: str, catVal: int, catLbl: str, strCondition: str, availabelCols: list):

        if strCondition.upper() != 'SYSMISS':
            df = self.df

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

                strCondition = str(strCondition).replace('=', '==').replace('<>', '!=')
                strCondition = str(strCondition).replace('>==', '>=').replace('<==', '<=')

                lstCondition = strCondition.split(' ')

                dictQreCond = dict()
                dictQreCond['a0'] = varName

                idx = 1
                for item in lstCondition:
                    if item in availabelCols and item != varName:
                        dictQreCond[f'a{idx}'] = item
                        idx += 1

                if len(dictQreCond.keys()) > 1:
                    strZip = f"zip(df[{'], df['.join(list(dictQreCond.values()))}])"
                    strZip = strZip.replace("[", "['").replace("]", "']")

                    strFor = f"for {', '.join(list(dictQreCond.keys()))} in "

                    strConditionReplace = strCondition
                    for key, val in dictQreCond.items():
                        if key == 'a0':
                            continue

                        strConditionReplace = strConditionReplace.replace(val, key)

                    strIf = f"{catVal} if {strConditionReplace} else a0"

                    strExec = f"df['{varName}'] = [{strIf} {strFor} {strZip}]"
                    strExec = strExec.replace('AND', 'and').replace('OR', 'or')

                    exec(strExec)

        return catLbl








