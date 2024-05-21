from .export_online_survey_table_formater import TableFormatter
import os
import pandas as pd
import numpy as np
import time
import json
from scipy import stats



class DataTableGenerator(TableFormatter):

    def __init__(self, logger, df_data: pd.DataFrame, df_info: pd.DataFrame, is_md: bool, xlsx_name: str, lst_qre_group: list, lst_qre_mean: list):

        super().__init__(xlsx_name, logger)

        self.df_data = df_data.copy()
        self.df_info = df_info.copy()

        self.is_md = is_md
        if is_md:
            self.convert_md_to_mc(df_data.copy(), df_info.copy())

        self.file_name = xlsx_name

        self.lst_qre_group = lst_qre_group
        self.lst_qre_mean = lst_qre_mean

        self.dict_unnetted_qres = dict()
        for idx in self.df_info.index:

            # if 'Q6a' in self.df_info.at[idx, 'var_name']:
            #     here = 1

            if 'net_code' in self.df_info.at[idx, 'val_lbl'].keys():
                self.dict_unnetted_qres.update({
                    self.df_info.at[idx, 'var_name']: self.unnetted_qre_val(self.df_info.at[idx, 'val_lbl']),
                })


    def convert_md_to_mc(self, df_data: pd.DataFrame, df_info: pd.DataFrame):

        df_info_qre_ma_all_1st_col = df_info.query("var_type.isin(['MA', 'MA_mtr']) & var_name.str.contains('[A-Za-z]+_1$')").copy()

        for idx in df_info_qre_ma_all_1st_col.index:
            qre = df_info_qre_ma_all_1st_col.at[idx, 'var_name'].rsplit('_', 1)[0]

            df_info_ma = df_info.query(f"var_name.str.contains('{qre}_[0-9]+')").copy()

            dict_val_lbl = dict()
            for idx_ma in df_info_ma.index:
                str_qre, str_val = str(df_info_ma.at[idx_ma, 'var_name']).rsplit('_', 1)
                dict_val_lbl[str_val] = df_info_ma.at[idx_ma, 'val_lbl']['1']

                df_data[df_info_ma.at[idx_ma, 'var_name']].replace({1: int(str_val)}, inplace=True)

            df_info.loc[df_info_ma.index,  ['val_lbl']] = [dict_val_lbl]

            lst_ma_col_name = df_info_ma['var_name'].values.tolist()

            df_data[lst_ma_col_name[0]] = df_data[lst_ma_col_name].values.tolist()

            for idx_dt in df_data.index:
                arr = np.sort(df_data.at[idx_dt, lst_ma_col_name[0]], axis=None)
                df_data.at[idx_dt, lst_ma_col_name[0]] = arr

            df_data[lst_ma_col_name] = pd.DataFrame(df_data[lst_ma_col_name[0]].tolist(), index=df_data.index)

        self.df_data = df_data
        self.df_info = df_info


    def run_tables(self, is_standard: bool, is_matrix_by_answers: bool, is_matrix_by_qres: bool):

        self.add_group()
        self.add_mean()

        if os.path.exists(self.file_name):
            os.remove(self.file_name)

        df_content_null = pd.DataFrame(data=[], columns=['#', 'Content'])

        with pd.ExcelWriter(self.file_name) as writer:
            df_content_null.to_excel(writer, sheet_name='Content', index=False)  # encoding='utf-8-sig'

        lst_func_to_run = list()

        if is_standard:
            lst_func_to_run.append(
                {
                    'json_file': './app/routers/Online_Survey/tables_standard.json',
                    'func_name': 'run_multi_standard_header',
                    'tables_to_run': ['T0', 'T1', 'T2'],  # null for FULL tables
                }
            )

        if is_matrix_by_answers:
            lst_func_to_run.append(
                {
                    'json_file': './app/routers/Online_Survey/tables_matrix_by_answers.json',
                    'func_name': 'run_multi_matrix_header_by_answers',
                    'tables_to_run': ['MT01'],  # null for FULL tables
                }
            )

        if is_matrix_by_qres:
            lst_func_to_run.append(
                {
                    'json_file': './app/routers/Online_Survey/tables_matrix_by_qres.json',
                    'func_name': 'run_multi_matrix_header_by_qres',
                    'tables_to_run': ['MT01'],  # null for FULL tables
                }
            )

        for item in lst_func_to_run:
            self.run_tables_by_item(item)


    def run_tables_by_js_files(self, lst_func_to_run: list):

        self.add_group()
        self.add_mean()

        if os.path.exists(self.file_name):
            os.remove(self.file_name)

        df_content_null = pd.DataFrame(data=[], columns=['#', 'Content'])

        with pd.ExcelWriter(self.file_name) as writer:
            df_content_null.to_excel(writer, sheet_name='Content', index=False)  # encoding='utf-8-sig'

        for item in lst_func_to_run:
            self.run_tables_by_item(item)


    def run_tables_by_item(self, item: dict):

        if 'json_file' in item.keys():

            with open(item['json_file'], encoding="UTF-8") as json_file:
                dict_tables = json.load(json_file)

        else:

            dict_tables = item['tables_format']

        if item['tables_to_run']:

            dict_tables_selected = dict()

            for tbl in item['tables_to_run']:
                dict_tables_selected[tbl] = dict_tables[tbl]

            dict_tables = dict_tables_selected

        for tbl_key, tbl_val in dict_tables.items():
            start_time = time.time()

            self.logger.info('run table: %s' % tbl_val['tbl_name'])

            df_tbl = getattr(self, item['func_name'])(tbl_val)

            self.logger.info('create sheet: %s' % tbl_val['tbl_name'])

            with pd.ExcelWriter(self.file_name, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
                df_tbl.to_excel(writer, sheet_name=tbl_val['tbl_name'], index=False)  # encoding='utf-8-sig'

            self.logger.info('create sheet: %s in %s seconds' % (tbl_val['tbl_name'], (time.time() - start_time)))

    # STANDARD TABLES---------------------------------------------------------------------------------------------------


    def run_multi_standard_header(self, tbl: dict) -> pd.DataFrame:

        tbl_name = tbl['tbl_name']
        tbl_filter = tbl['tbl_filter']
        dict_header = tbl['header']
        side_query = tbl['side_query']
        is_count = tbl['is_count']
        is_mean_only = tbl['is_mean_only']
        pct_sign = tbl['pct_sign']

        df_info = self.df_info.query(side_query).copy() if len(side_query) > 0 else self.df_info.copy()

        df_tbl = pd.DataFrame()

        for key, val in dict_header.items():

            self.logger.info('run table: %s -> header: %s' % (tbl_name, val['label']))

            df_data = self.df_data.copy()
            if tbl_filter:
                df_data = df_data.query(tbl_filter).copy()

            df_data = df_data.query(val['header_query']).copy() if len(val['header_query']) > 0 else df_data.copy()

            tbl_info = {
                'label': val['label'],
                'is_count': is_count,
                'pct_sign': pct_sign,
                'is_mean_only': is_mean_only,
                'side_query': side_query
            }

            df_temp = self.run_standard_header(df_data, df_info, tbl_info=tbl_info)

            if df_tbl.empty:
                df_tbl = df_temp
            else:
                df_tbl = pd.concat([df_tbl, df_temp.loc[:, [val['label']]]], axis=1)

        df_tbl.dropna(how='all', inplace=True)
        df_tbl.reset_index(drop=True, inplace=True)

        # df_tbl.to_excel('../df_tbl.xlsx', encoding='utf-8-sig', index=False)

        return df_tbl


    @staticmethod
    def run_standard_header(df_data: pd.DataFrame, df_info: pd.DataFrame, tbl_info: dict) -> pd.DataFrame:

        label = tbl_info['label']
        is_count = tbl_info['is_count']
        is_mean_only = tbl_info['is_mean_only']
        pct_sign = tbl_info['pct_sign']

        pct_num = 100
        if pct_sign:
            pct_num = 1

        df_info['idx_by_var'] = df_info['var_name']
        df_info.set_index('idx_by_var', inplace=True)

        count_grp_qres = 0
        if tbl_info['side_query']:
            lst_qres = list(eval(tbl_info['side_query'].replace('var_name in ', '')))
            df_info = df_info.reindex(lst_qres)

            if is_mean_only:
                lst_grp = list()
                for item in lst_qres:
                    arr_item = item.rsplit('_', 3)
                    if arr_item[0] not in lst_grp:
                        lst_grp.append(arr_item[0])

                count_grp_qres = len(lst_grp)

        lst_col = ['qre_name', 'qre_lbl', 'qre_type', 'cat_val', 'cat_lbl', label]
        lst_ignore = list()

        arr_label = label.split('|')

        lst_tbl_data = [
            ['mean_only' if tbl_info['is_mean_only'] else np.nan, np.nan, np.nan, np.nan, np.nan, arr_label[0]],
            ['count' if is_count else ('pct_sign' if pct_sign else 'pct'), np.nan, np.nan, np.nan, np.nan, arr_label[1]]
        ]

        for i in range(2, 5):
            if i < len(arr_label):
                lst_tbl_data.append([np.nan, np.nan, np.nan, np.nan, np.nan, arr_label[i]])

            else:
                lst_tbl_data.append([np.nan, np.nan, np.nan, np.nan, np.nan, np.nan])

        count_process_qres = 0

        for idx in df_info.index:

            count_process_qres += 1

            qre_name = df_info.at[idx, 'var_name']
            qre_lbl = df_info.at[idx, 'var_lbl']
            qre_type = df_info.at[idx, 'var_type']
            qre_val = eval(df_info.at[idx, 'val_lbl']) if isinstance(df_info.at[idx, 'val_lbl'], str) else df_info.at[
                idx, 'val_lbl']

            if qre_type in ['FT', 'FT_mtr']:
                qre_base_count = df_data[qre_name].count()
                lst_tbl_data.append([qre_name, qre_lbl, qre_type, 'base', 'Base', qre_base_count])

            elif qre_type in ['NUM']:
                qre_base_count = df_data[qre_name].count()
                lst_tbl_data.append([qre_name, qre_lbl, qre_type, 'base', 'Base', qre_base_count])

                df_mean = df_data[qre_name].copy()
                mean_lbl = qre_lbl
                mean_val = df_mean.mean()
                lst_tbl_data.append([qre_name, mean_lbl, qre_type, 'mean', 'Mean', mean_val])


            elif qre_type in ['SA', 'SA_mtr', 'RANKING']:
                qre_base_count = df_data[qre_name].count()
                lst_tbl_data.append([qre_name, qre_lbl, qre_type, 'base', 'Base', qre_base_count])

                for cat, lbl in qre_val.items():

                    # ADD-IN Net Code
                    if 'net_code' in str(cat):

                        for net_cat, net_val in qre_val['net_code'].items():

                            if isinstance(net_val, dict):

                                qre_cat_count = 0

                                lst_sub_cat = net_val.keys()
                                lst_sub_cat = [int(i) for i in lst_sub_cat]

                                df_sa_data = df_data.loc[:, [qre_name]].copy()
                                dict_to_count = {qre_name: lst_sub_cat}

                                if qre_base_count > 0:
                                    qre_cat_count = df_sa_data.isin(dict_to_count).any(1).sum()

                                list_net_cat = net_cat.split('|')

                                qre_cat_pct = qre_cat_count / qre_base_count * pct_num if qre_cat_count and qre_base_count else 0
                                lst_tbl_data.append([qre_name, qre_lbl, qre_type, list_net_cat[0], list_net_cat[-1], qre_cat_count if is_count else qre_cat_pct])

                                for cat2, lbl2 in net_val.items():
                                    cat2 = int(cat2)

                                    qre_cat_count = 0
                                    if qre_base_count:
                                        qre_cat_count += df_data.loc[df_data[qre_name] == cat2, qre_name].count()

                                    qre_cat_pct = qre_cat_count / qre_base_count * pct_num if qre_cat_count and qre_base_count else 0
                                    lst_tbl_data.append([qre_name, qre_lbl, qre_type, cat2, lbl2, qre_cat_count if is_count else qre_cat_pct])

                            else:

                                net_cat = int(net_cat)
                                qre_cat_count = df_data.loc[df_data[qre_name] == net_cat, qre_name].count()
                                qre_cat_pct = qre_cat_count / qre_base_count * pct_num if qre_cat_count and qre_base_count else 0

                                lst_tbl_data.append([qre_name, qre_lbl, qre_type, net_cat, net_val, qre_cat_count if is_count else qre_cat_pct])

                    else:

                        cat = int(cat)
                        qre_cat_count = df_data.loc[df_data[qre_name] == cat, qre_name].count()
                        qre_cat_pct = qre_cat_count / qre_base_count * pct_num if qre_cat_count and qre_base_count else 0

                        lst_tbl_data.append([qre_name, qre_lbl, qre_type, cat, lbl, qre_cat_count if is_count else qre_cat_pct])


            elif qre_type in ['MA', 'MA_mtr']:

                if qre_name not in lst_ignore:

                    qre_name_ma = qre_name.rsplit('_', 1)[0]
                    df_ma = df_info.loc[df_info['var_type'].str.contains('MA|MA_mtr'), :].copy()

                    if qre_name_ma == 'COI':
                        df_ma = df_ma.loc[(df_ma['var_name'].str.contains('COI_')) & ~(
                            df_ma['var_name'].str.contains('COI_RANK_')), :]
                    else:
                        df_ma = df_ma.loc[df_ma['var_name'].str.contains(f'{qre_name_ma}_[1-9]+'), :]

                    qre_base_count = df_data[qre_name].count()
                    lst_tbl_data.append([qre_name_ma, qre_lbl, qre_type, 'base', 'Base', qre_base_count])

                    lst_ignore.extend(df_ma.index.tolist())

                    for cat, lbl in qre_val.items():

                        # ADD-IN Net Code
                        if 'net_code' in cat:

                            for net_cat, net_val in qre_val['net_code'].items():

                                qre_cat_count = 0

                                if isinstance(net_val, dict):

                                    lst_sub_cat = net_val.keys()
                                    lst_sub_cat = [int(i) for i in lst_sub_cat]

                                    lst_ma_col_name = df_ma['var_name'].values.tolist()
                                    df_ma_data = df_data.loc[:, lst_ma_col_name].copy()
                                    dict_to_count = {i: lst_sub_cat for i in lst_ma_col_name}

                                    if qre_base_count > 0:
                                        qre_cat_count = df_ma_data.isin(dict_to_count).any(1).sum()

                                    list_net_cat = net_cat.split('|')

                                    qre_cat_pct = qre_cat_count / qre_base_count * pct_num if qre_cat_count and qre_base_count else 0
                                    lst_tbl_data.append([qre_name_ma, qre_lbl, qre_type, list_net_cat[0], list_net_cat[1], qre_cat_count if is_count else qre_cat_pct])

                                    if 'NET' in list_net_cat[1]:
                                        for cat2, lbl2 in net_val.items():
                                            cat2 = int(cat2)

                                            qre_cat_count = 0
                                            if qre_base_count:
                                                for ma_item in df_ma.index:
                                                    qre_cat_count += df_data.loc[df_data[ma_item] == cat2, ma_item].count()

                                            qre_cat_pct = qre_cat_count / qre_base_count * pct_num if qre_cat_count and qre_base_count else 0
                                            lst_tbl_data.append([qre_name_ma, qre_lbl, qre_type, cat2, lbl2, qre_cat_count if is_count else qre_cat_pct])

                                else:

                                    if qre_base_count:
                                        for ma_item in df_ma.index:
                                            qre_cat_count += df_data.loc[df_data[ma_item] == int(net_cat), ma_item].count()

                                    qre_cat_pct = qre_cat_count / qre_base_count * pct_num if qre_cat_count and qre_base_count else 0
                                    lst_tbl_data.append([qre_name_ma, qre_lbl, qre_type, net_cat, net_val, qre_cat_count if is_count else qre_cat_pct])

                        else:
                            cat = int(cat)

                            qre_cat_count = 0
                            if qre_base_count:
                                for ma_item in df_ma.index:
                                    qre_cat_count += df_data.loc[df_data[ma_item] == cat, ma_item].count()

                            qre_cat_pct = qre_cat_count / qre_base_count * pct_num if qre_cat_count and qre_base_count else 0
                            lst_tbl_data.append([qre_name_ma, qre_lbl, qre_type, cat, lbl, qre_cat_count if is_count else qre_cat_pct])

            elif qre_type in ['MEAN']:

                org_qre_name = qre_name.replace('_Mean', '')
                df_mean = df_data[org_qre_name].copy()
                mean_lbl = qre_lbl

                if -999 not in qre_val.keys():
                    df_mean.replace(qre_val, inplace=True)

                mean_val = df_mean.mean()

                if is_mean_only:
                    qre_base_count = df_data[org_qre_name].count()

                    mean_lbl = mean_lbl.rsplit('_', 3)[-2]
                    mean_name = qre_name.rsplit('_', 3)[0]

                    if count_process_qres % count_grp_qres == 1:
                        lst_tbl_data.append([np.nan, np.nan, qre_type, 'base', 'Base', qre_base_count])

                    lst_tbl_data.append([mean_lbl, mean_name, qre_type, 'mean', 'Mean', mean_val])

                else:

                    lst_tbl_data.append([qre_name, mean_lbl, qre_type, 'mean', 'Mean', mean_val])

            elif qre_type in ['GROUP']:
                org_qre_name = qre_name.replace('_Group', '')
                df_group = df_data[org_qre_name].copy()
                df_group.replace(qre_val['recode'], inplace=True)

                qre_base_count = df_group.count()

                for cat, lbl in qre_val['cats'].items():
                    cat = int(cat)
                    qre_cat_count = df_group.loc[df_group == cat].count()
                    qre_cat_pct = qre_cat_count / qre_base_count * pct_num if qre_cat_count and qre_base_count else 0

                    lst_tbl_data.append(
                        [qre_name, qre_lbl, qre_type, cat, lbl, qre_cat_count if is_count else qre_cat_pct])

        df_tbl = pd.DataFrame(columns=lst_col, data=lst_tbl_data)

        return df_tbl

    # MATRIX TABLES BY ANSWERS------------------------------------------------------------------------------------------

    def run_multi_matrix_header_by_answers(self, tbl: dict) -> pd.DataFrame:

        tbl_name = tbl['tbl_name']
        tbl_filter = tbl['tbl_filter']
        is_count = tbl['is_count']
        fil_cats = tbl['fil_cats']

        arr_side_qres = tbl['side_qres'].rsplit('|', 2)
        arr_side_qres[1] = list(eval(f"range{arr_side_qres[1]}"))
        arr_side_qres[2] = list(eval(f"range{arr_side_qres[2]}"))
        lst_side_qres = [f"{arr_side_qres[0]}_{i if i > 9 else f'0{i}'}_{j}" for i in arr_side_qres[1] for j in
                         arr_side_qres[2]]

        arr_base_qre = tbl['base_qre'].rsplit('|', 1)
        arr_base_qre[1] = list(eval(f"range{arr_base_qre[1]}"))
        lst_base_qre = [f"{arr_base_qre[0]}_{i}" for i in arr_base_qre[1]]

        side_query = "{0}'{1}'{2}".format("var_name in (", "', '".join(lst_base_qre + lst_side_qres), ")")
        df_info = self.df_info.query(side_query).copy() if len(side_query) > 0 else self.df_info.copy()
        df_tbl = pd.DataFrame()

        for key, val in tbl['header'].items():

            self.logger.info('run table: %s -> header: %s' % (tbl_name, val['label']))
            df_data = self.df_data.copy()

            if tbl_filter:
                df_data = df_data.query(tbl_filter).copy()

            df_data = df_data.query(val['header_query']).copy() if len(val['header_query']) > 0 else df_data.copy()

            tbl_info = {
                'label': val['label'],
                'is_count': is_count,
                'fil_cats': fil_cats,
                'lst_side_qres': lst_side_qres,
                'lst_base_qre': lst_base_qre
            }

            df_temp = self.run_matrix_header_by_answers(df_data, df_info, tbl_info=tbl_info)

            if df_tbl.empty:
                df_tbl = df_temp
            else:
                df_tbl = pd.concat([df_tbl, df_temp.loc[:, [f"{val['label']}|{i}" for i in fil_cats]]], axis=1)

        return df_tbl


    def run_matrix_header_by_answers(self, df_data: pd.DataFrame, df_info: pd.DataFrame, tbl_info: dict) -> pd.DataFrame:

        df_info['idx_by_var'] = df_info['var_name']
        df_info.set_index('idx_by_var', inplace=True)

        label = tbl_info['label']
        fil_cats = tbl_info['fil_cats']
        is_count = tbl_info['is_count']
        lst_side_qres = tbl_info['lst_side_qres']
        lst_base_qre = tbl_info['lst_base_qre']

        fil_cats_lbl = list()
        for cat, lbl in eval(df_info.loc[df_info['var_name'] == lst_base_qre[0], 'val_lbl'][0]).items():
            if int(cat) in fil_cats:
                fil_cats_lbl.append(lbl)

        lst_col = ['qre_name', 'qre_lbl', 'qre_type', 'cat_val', 'cat_lbl']
        lst_col += [f"{label}|{cat}" for cat in fil_cats]

        arr_label = label.split('|')

        dict_base = dict()
        for cat in fil_cats:
            str_base_query = f"({'|'.join(f'{qre} == {cat}' for qre in lst_base_qre)})"
            dict_base[cat] = df_data.query(str_base_query).shape[0]

        lst_tbl_data = [
            ['matrix_table', len(fil_cats), np.nan, np.nan, np.nan] + [arr_label[0]] * len(fil_cats),
            ['count' if is_count else 'pct', np.nan, np.nan, np.nan, np.nan] + [arr_label[1]] * len(fil_cats),
            [np.nan, np.nan, np.nan, np.nan, 'Brand code'] + fil_cats,
            [np.nan, np.nan, np.nan, np.nan, 'Brand label'] + fil_cats_lbl,
            [np.nan, np.nan, np.nan, 'base', f'Base on {lst_base_qre[0].rsplit("_", 1)[0]}'] + list(dict_base.values()),
        ]

        dict_att = dict()
        for col in lst_side_qres:
            att = col.rsplit('_', 1)[0]
            code = int(att.rsplit('_', 1)[1])

            if att not in dict_att.keys():

                full_lbl = df_info.loc[df_info['var_name'] == col, 'var_lbl'][0].rsplit('_', 1)

                dict_att.update({att: {
                    'qre_lbl': full_lbl[0],
                    'qre_type': 'MA_mtr',
                    'cat_val': code,
                    'cat_lbl': full_lbl[1],
                    'cols': [col]
                }
                })

            else:
                dict_att[att]['cols'].append(col)

        for att, att_info in dict_att.items():

            lst_row_data = [att.rsplit('_', 1)[0], att_info['qre_lbl'], att_info['qre_type'], att_info['cat_val'],
                            att_info['cat_lbl']]

            self.logger.info('%s -> att: %s -> cats: %s' % (label, att, fil_cats))

            for cat in fil_cats:
                str_att_query = f"({'|'.join(f'{c} == {cat}' for c in att_info['cols'])})"

                value = df_data.query(str_att_query).shape[0]

                if not is_count:
                    value = value / dict_base[cat] * 100

                lst_row_data.append(value)

            lst_tbl_data.append(lst_row_data)

        df_tbl = pd.DataFrame(columns=lst_col, data=lst_tbl_data)

        return df_tbl

    # MATRIX TABLES BY QRES------------------------------------------------------------------------------------------

    def run_multi_matrix_header_by_qres(self, tbl: dict) -> pd.DataFrame:

        tbl_name = tbl['tbl_name']
        is_count = tbl['is_count']
        tbl_filter = tbl['tbl_filter']
        is_cal_BES = tbl['is_cal_BES']

        dict_base_qres = dict()
        lst_group = list()
        for item in tbl['base_qres']:

            if '|' in item:
                qre_name = item.rsplit('|', 1)[0]
                col_rng = list(eval(f"range{item.rsplit('|', 1)[1]}"))
                dict_base_qres[qre_name] = [f"{qre_name}_{i if i > 9 else f'0{i}'}" for i in col_rng]

                if is_cal_BES:
                    lst_group.append([f"{qre_name}_{i if i > 9 else f'0{i}'}_Group" for i in col_rng])

            else:
                dict_base_qres[item] = [item]

        side_query = "{0}'{1}'{2}".format("var_name in (", "', '".join([i for sublist in list(dict_base_qres.values()) + lst_group for i in sublist]), ")")
        df_info = self.df_info.query(side_query).copy() if len(side_query) > 0 else self.df_info.copy()
        df_tbl = pd.DataFrame()

        for key, val in tbl['header'].items():

            self.logger.info('run table by qres: %s -> header: %s' % (tbl_name, val['label']))
            df_data = self.df_data.copy()

            if tbl_filter:
                df_data = df_data.query(tbl_filter).copy()

            df_data = df_data.query(val['header_query']).copy() if len(val['header_query']) > 0 else df_data.copy()

            tbl_info = {
                'label': val['label'],
                'is_count': is_count,
                'is_cal_BES': is_cal_BES,
                'dict_base_qres': dict_base_qres
            }

            df_temp = self.run_matrix_header_by_qres(df_data, df_info, tbl_info=tbl_info)

            if df_tbl.empty:
                df_tbl = df_temp
            else:
                df_tbl = pd.concat([df_tbl, df_temp.loc[:, [f"{val['label']}|{i}" for i in dict_base_qres.keys()]]],
                                   axis=1)

        return df_tbl

    @staticmethod
    def run_matrix_header_by_qres(df_data: pd.DataFrame, df_info: pd.DataFrame, tbl_info: dict) -> pd.DataFrame:

        df_info['idx_by_var'] = df_info['var_name']
        df_info.set_index('idx_by_var', inplace=True)

        label = tbl_info['label']
        is_count = tbl_info['is_count']
        is_cal_BES = tbl_info['is_cal_BES']
        dict_base_qres = tbl_info['dict_base_qres']

        lst_col = ['qre_name', 'qre_lbl', 'qre_type', 'cat_val', 'cat_lbl']
        lst_col += [f"{label}|{qre}" for qre in dict_base_qres.keys()]

        qres_count = len(list(dict_base_qres.keys()))

        arr_label = label.split('|')

        lst_tbl_data = [
            ['matrix_table', qres_count, np.nan, np.nan, np.nan] + [arr_label[0]] * qres_count,
            ['count' if is_count else 'pct', np.nan, np.nan, np.nan, np.nan] + [arr_label[1]] * qres_count,
            [np.nan, np.nan, np.nan, np.nan, 'Question name'] + list(dict_base_qres.keys()),
            [np.nan, np.nan, np.nan, np.nan, 'Question label'] + list(dict_base_qres.keys()),
        ]

        df_base_qres = pd.DataFrame.from_dict(dict_base_qres)
        for idx in df_base_qres.index:

            row_data = dict()
            count = 0
            qre_lbl = str()
            qre_type = str()
            for col_idx, col in enumerate(list(df_base_qres.columns)):

                qre_name = df_base_qres.at[idx, col]

                qre_lbl = df_info.loc[df_info['var_name'] == qre_name, ['var_lbl']].values[0][0]

                if '_' in qre_lbl:
                    lst_qre_lbl = qre_lbl.rsplit('_', 1)
                    qre_lbl = lst_qre_lbl[1]
                    lst_tbl_data[3][-(qres_count - col_idx)] = lst_qre_lbl[0]


                qre_type = df_info.loc[df_info['var_name'] == qre_name, ['var_type']].values[0][0]

                if is_cal_BES:
                    dict_cats = eval(df_info.loc[df_info['var_name'] == f'{qre_name}_Group', ['val_lbl']].values[0][0])
                    dict_recode = dict_cats['recode']
                    dict_cats = dict_cats['cats']
                    df_data[qre_name].replace(dict_recode, inplace=True)
                else:
                    dict_cats = df_info.loc[df_info['var_name'] == qre_name, ['val_lbl']].values[0][0]

                    if isinstance(dict_cats, str):
                        dict_cats = eval(dict_cats)

                dict_row_data = {
                    'base': int(df_data.loc[:, [qre_name]].count()[0])
                }
                for val in dict_cats.keys():

                    value = int(df_data.loc[df_data[qre_name] == int(val), [qre_name]].count()[0])

                    if not is_count:

                        if dict_row_data['base'] == 0:
                            value = 0
                        else:
                            value = value / dict_row_data['base'] * 100

                    dict_row_data[val] = value

                if not count:
                    row_data['base'] = [qre_lbl, qre_lbl, qre_type, 'base', 'Base', dict_row_data['base']]
                    for val, lbl in dict_cats.items():
                        row_data[val] = [qre_lbl, qre_lbl, qre_type, val, lbl, dict_row_data[val]]

                else:
                    row_data['base'].append(dict_row_data['base'])
                    for val, lbl in dict_cats.items():
                        row_data[val].append(dict_row_data[val])

                count += 1

            if is_cal_BES:

                bes_val = 0
                for i in range(1, qres_count + 1):
                    bes_val += row_data[3][-i] - row_data[1][-i]

                bes_val /= qres_count

                row_data['BES'] = [qre_lbl, qre_lbl, qre_type, 'bes', 'BES', bes_val, np.nan]

            lst_tbl_data.extend(list(row_data.values()))

        df_tbl = pd.DataFrame(columns=lst_col, data=lst_tbl_data)

        return df_tbl


    def add_group(self):
        self.logger.info('add_group')
        df_info = self.df_info.copy()

        for qre_mean in self.lst_qre_group:
            idx = df_info.loc[df_info['var_name'] == qre_mean[0], :].index[0]

            df_info = self.insert_row(idx + 1, df_info, [f'{qre_mean[0]}_Group', f"{df_info.at[idx, 'var_lbl']}_Group", 'GROUP', qre_mean[1]])

        self.df_info = df_info


    def add_mean(self):
        self.logger.info('add_mean')
        df_info = self.df_info.copy()

        for qre_mean in self.lst_qre_mean:
            idx = df_info.loc[df_info['var_name'] == qre_mean[0], :].index[0]
            df_info = self.insert_row(idx + 1, df_info, [f'{qre_mean[0]}_Mean',	f"{df_info.at[idx, 'var_lbl']}_Mean", 'MEAN', qre_mean[1]])

        self.df_info = df_info


    @staticmethod
    def insert_row(row_number, df, row_value):
        # Slice the upper half of the dataframe
        df1 = df[0:row_number].copy()

        # Store the result of lower half of the dataframe
        df2 = df[row_number:].copy()

        # Insert the row in the upper half dataframe
        df1.loc[row_number] = row_value

        # Concat the two dataframes
        df_result = pd.concat([df1, df2])

        # Reassign the index labels
        df_result.index = [*range(df_result.shape[0])]

        # Return the updated dataframe
        return df_result


    # SIG TABLE---------------------------------------------------------------------------------------------------------


    def group_sig_table_header(self, lst_header_qres: list) -> list:

        lst_group_header = list()

        while len(lst_header_qres) != 5:
            lst_header_qres.append([])

        lvl1, lvl2, lvl3, lvl4, lvl5 = lst_header_qres

        for a in lvl1:

            if not lvl2:
                dict_grp_hd = dict()
                dict_idx = {list(a['cats'].keys())[i]: i for i in range(len(a['cats'].keys()))}

                for a_k, a_v in a['cats'].items():
                    # qa = f"{a['qre_name']} > 0" if str(a_k).upper() == 'TOTAL' else (a_k if '@' in a['qre_name'] else f"{a['qre_name']} == {a_k}")
                    qa = self.group_sig_table_header_query(a, a_k)

                    dict_grp_hd.update({
                        dict_idx[a_k]: {
                            "lbl": f"{a['qre_lbl']}@{a_v}",
                            "query": f"{qa}"
                        }
                    })

                lst_group_header.append(dict_grp_hd)
                continue

            for a_k, a_v in a['cats'].items():
                # qa = f"{a['qre_name']} > 0" if str(a_k).upper() == 'TOTAL' else (a_k if '@' in a['qre_name'] else f"{a['qre_name']} == {a_k}")
                qa = self.group_sig_table_header_query(a, a_k)

                for b in lvl2:

                    if not lvl3:
                        dict_grp_hd = dict()
                        dict_idx = {list(b['cats'].keys())[i]: i for i in range(len(b['cats'].keys()))}

                        for b_k, b_v in b['cats'].items():
                            # qb = f"{b['qre_name']} > 0" if str(b_k).upper() == 'TOTAL' else f"{b['qre_name']} == {b_k}"
                            qb = self.group_sig_table_header_query(b, b_k)

                            dict_grp_hd.update({
                                dict_idx[b_k]: {
                                    "lbl": f"{a['qre_lbl']}@{a_v}@{b['qre_lbl']}@{b_v}",
                                    "query": f"{qa} & {qb}"
                                }
                            })

                        lst_group_header.append(dict_grp_hd)
                        continue

                    for b_k, b_v in b['cats'].items():
                        # qb = f"{b['qre_name']} > 0" if str(b_k).upper() == 'TOTAL' else f"{b['qre_name']} == {b_k}"
                        qb = self.group_sig_table_header_query(b, b_k)

                        for c in lvl3:

                            if not lvl4:
                                dict_grp_hd = dict()
                                dict_idx = {list(c['cats'].keys())[i]: i for i in range(len(c['cats'].keys()))}

                                for c_k, c_v in c['cats'].items():
                                    # qc = f"{c['qre_name']} > 0" if str(c_k).upper() == 'TOTAL' else f"{c['qre_name']} == {c_k}"
                                    qc = self.group_sig_table_header_query(c, c_k)

                                    dict_grp_hd.update({
                                        dict_idx[c_k]: {
                                            "lbl": f"{a['qre_lbl']}@{a_v}@{b['qre_lbl']}@{b_v}@{c['qre_lbl']}@{c_v}",
                                            "query": f"{qa} & {qb} & {qc}"
                                        }
                                    })

                                lst_group_header.append(dict_grp_hd)
                                continue

                            for c_k, c_v in c['cats'].items():
                                # qc = f"{c['qre_name']} > 0" if str(c_k).upper() == 'TOTAL' else f"{c['qre_name']} == {c_k}"
                                qc = self.group_sig_table_header_query(c, c_k)

                                for d in lvl4:

                                    if not lvl5:
                                        dict_grp_hd = dict()
                                        dict_idx = {list(d['cats'].keys())[i]: i for i in range(len(d['cats'].keys()))}

                                        for d_k, d_v in d['cats'].items():
                                            # qd = f"{d['qre_name']} > 0" if str(d_k).upper() == 'TOTAL' else f"{d['qre_name']} == {d_k}"
                                            qd = self.group_sig_table_header_query(d, d_k)

                                            dict_grp_hd.update({
                                                dict_idx[d_k]: {
                                                    "lbl": f"{a['qre_lbl']}@{a_v}@{b['qre_lbl']}@{b_v}@{c['qre_lbl']}@{c_v}@{d['qre_lbl']}@{d_v}",
                                                    "query": f"{qa} & {qb} & {qc} & {qd}"
                                                }
                                            })

                                        lst_group_header.append(dict_grp_hd)
                                        continue

                                    for d_k, d_v in d['cats'].items():
                                        # qd = f"{d['qre_name']} > 0" if str(d_k).upper() == 'TOTAL' else f"{d['qre_name']} == {d_k}"
                                        qd = self.group_sig_table_header_query(d, d_k)

                                        for e in lvl5:
                                            dict_grp_hd = dict()
                                            dict_idx = {list(e['cats'].keys())[i]: i for i in range(len(e['cats'].keys()))}

                                            for e_k, e_v in e['cats'].items():
                                                # qe = f"{e['qre_name']} > 0" if str(e_k).upper() == 'TOTAL' else f"{e['qre_name']} == {e_k}"
                                                qe = self.group_sig_table_header_query(e, e_k)

                                                dict_grp_hd.update({
                                                    dict_idx[e_k]: {
                                                        "lbl": f"{a['qre_lbl']}@{a_v}@{b['qre_lbl']}@{b_v}@{c['qre_lbl']}@{c_v}@{d['qre_lbl']}@{d_v}@{e['qre_lbl']}@{e_v}",
                                                        "query": f"{qa} & {qb} & {qc} & {qd} & {qe}"
                                                    }
                                                })

                                            lst_group_header.append(dict_grp_hd)

        return lst_group_header


    def group_sig_table_header_query(self, dict_qre: dict, str_cat_or_query) -> str:

        if '$' in dict_qre['qre_name']:

            lst_qre_ma_name = self.df_info.query(f"var_name.str.contains('{dict_qre['qre_name'][1:]}_[0-9]+')")['var_name'].values.tolist()

            if str(str_cat_or_query).upper() == 'TOTAL':
                return f"({' | '.join([f'{i} > 0' for i in lst_qre_ma_name])})"

            return f"({' | '.join([f'{i} == {str_cat_or_query}' for i in lst_qre_ma_name])})"

        if str(str_cat_or_query).upper() == 'TOTAL':
            return f"{dict_qre['qre_name']} > 0"

        if '@' in dict_qre['qre_name']:
            return str_cat_or_query

        return f"{dict_qre['qre_name']} == {str_cat_or_query}"


    def unnetted_qre_val(self, dict_netted) -> dict:
        dict_unnetted = dict()

        if 'net_code' not in dict_netted.keys():
            return dict_netted

        for key, val in dict_netted.items():

            if 'net_code' in key:
                val_lbl_lv1 = dict_netted['net_code']

                for net_key, net_val in val_lbl_lv1.items():

                    if isinstance(net_val, str):
                        dict_unnetted.update({str(net_key): net_val})
                    else:
                        self.logger.info('Unnetted %s' % net_key)
                        dict_unnetted.update(net_val)

            else:
                dict_unnetted.update({str(key): val})

        return dict_unnetted


    def run_standard_table_sig(self, tbl: dict) -> pd.DataFrame:

        df_tbl = pd.DataFrame()

        # create df_data with tbl_filter in json file
        df_data = self.df_data.query(tbl['tbl_filter']).copy()

        # create df_info with lst_side_qres in json file
        df_info = pd.DataFrame(columns=['var_name', 'var_lbl', 'var_type', 'val_lbl', 'qre_fil'], data=[])
        for qre in tbl['lst_side_qres']:

            if '$' in qre['qre_name']:
                lst_qre_col = self.df_info.loc[self.df_info['var_name'].str.contains(f"{qre['qre_name'][1:]}_[0-9]+"), 'var_name'].values.tolist()
                var_name = qre['qre_name'].replace('$', '')
            elif '#combine' in qre['qre_name']:
                var_name, str_comb = qre['qre_name'].split('#combine')
                lst_qre_col = str_comb.replace('(', '').replace(')', '').split(',')
            else:
                lst_qre_col = [qre['qre_name']]
                var_name = qre['qre_name']

            # NEW-------------------------------------------------------------------------------------------------------
            df_qre_info = self.df_info.query(f"var_name.isin({lst_qre_col})").copy()
            df_qre_info.reset_index(drop=True, inplace=True)

            dict_row = {
                'var_name': var_name,
                'var_lbl': qre['qre_lbl'].replace('{lbl}', df_qre_info.at[0, 'var_lbl']) if 'qre_lbl' in qre.keys() else df_qre_info.at[0, 'var_lbl'],
                'var_type': 'MA_comb' if '#combine' in qre['qre_name'] else df_qre_info.at[0, 'var_type'],
                'val_lbl': df_qre_info.at[0, 'val_lbl'],
                'qre_fil': qre['qre_filter'] if 'qre_filter' in qre.keys() else "",
                'lst_qre_col': lst_qre_col,
            }

            df_info = pd.concat([df_info, pd.DataFrame(columns=list(dict_row.keys()), data=[list(dict_row.values())])], axis=0, ignore_index=True)
            # ----------------------------------------------------------------------------------------------------------

            # # OLD
            # dict_row = {
            #     'var_name': "",
            #     'var_lbl': qre['qre_lbl'].replace('{lbl}', df_qre_info.at[0, 'var_lbl']) if 'qre_lbl' in qre.keys() else df_qre_info.at[0, 'var_lbl'],
            #     'var_type': 'MA_comb' if '#combine' in qre['qre_name'] else df_qre_info.at[0, 'var_type'],
            #     'val_lbl': {},
            #     'qre_fil': qre['qre_filter'] if 'qre_filter' in qre.keys() else "",
            #     'lst_col': lst_qre_col,
            # }
            # for idx, col in enumerate(lst_qre_col):
            #     dict_row['var_name'] = col
            #     dict_row['val_lbl'] = df_qre_info.at[idx, 'val_lbl']
            #     df_info = pd.concat([df_info, pd.DataFrame(columns=list(dict_row.keys()), data=[list(dict_row.values())])], axis=0, ignore_index=True)


        # Maximum 5 levels of header
        lst_group_header = self.group_sig_table_header(tbl['lst_header_qres'])

        for grp_hd in lst_group_header:

            self.logger.info('run table: %s -> group header:' % tbl['tbl_name'])

            for i in grp_hd.values():
                self.logger.info("\t%s" % i['lbl'])

            tbl_info_sig = {
                'tbl_name': tbl['tbl_name'],
                'is_count': tbl['is_count'],
                'is_pct_sign': tbl['is_pct_sign'],
                'sig_test_info': tbl['sig_test_info'],
                'dict_grp_header': grp_hd
            }

            df_temp = self.run_standard_header_sig(df_data, df_info, tbl_info_sig=tbl_info_sig)

            if df_tbl.empty:
                df_tbl = df_temp
            else:
                lst_col_temp_to_add = list(df_temp.columns)[5:]
                df_tbl = pd.concat([df_tbl, df_temp[lst_col_temp_to_add]], axis=1)


        # drop row which have all value is nan
        df_tbl.dropna(how='all', inplace=True)

        # drop row in qre oe have all columns are 0
        if tbl['is_hide_oe_zero_cats']:

            df_sum_oe_val = df_tbl.query("qre_name.str.contains('_OE') & qre_type == 'MA'").copy()

            if not df_sum_oe_val.empty:
                fil_col = list(df_sum_oe_val.columns)
                df_sum_oe_val = df_sum_oe_val.loc[:, fil_col[5:]]
                df_sum_oe_val.replace({'': np.nan}, inplace=True)
                df_sum_oe_val = df_sum_oe_val.astype(float)
                df_sum_oe_val['sum_val'] = df_sum_oe_val.sum(axis=1, skipna=True, numeric_only=True)
                df_sum_oe_val = df_sum_oe_val.query('sum_val == 0')

                df_tbl.drop(df_sum_oe_val.index, inplace=True)

        # Reset df table index
        df_tbl.reset_index(drop=True, inplace=True)

        # df_tbl.to_excel('zzz_df_tbl.xlsx', encoding='utf-8-sig')
        
        return df_tbl


    @staticmethod
    def add_base_to_tbl_sig(df_data: pd.DataFrame, df_tbl: pd.DataFrame, df_qre: pd.DataFrame, qre_info: dict, dict_header_col_name: dict, lst_sig_pair: list) -> pd.DataFrame:

        lst_tbl_row_data = list()
        lst_ignore_col = list()

        for idx_pair, sig_pair in enumerate(lst_sig_pair):

            for idx_item, item in enumerate(sig_pair):

                if item in lst_ignore_col:
                    continue

                str_hd_query = df_tbl.at[0, dict_header_col_name[item]['val_col']]

                if len(qre_info['lst_qre_col']) > 1:
                    lst_qre_col = qre_info['lst_qre_col']
                    df_filter = df_data.query(str_hd_query).copy()
                    df_fil_base = df_filter[lst_qre_col].dropna(how='all')
                    df_filter = df_filter.loc[df_fil_base.index, :]

                else:
                    str_query = f"{str_hd_query} & {qre_info['qre_name'].replace('_Mean', '').replace('_Group', '')} > 0"
                    df_filter = df_data.query(str_query).copy()

                if df_data.empty:
                    num_base = 0
                else:
                    num_base = df_filter.shape[0]

                if len(lst_tbl_row_data) == 0:
                    # str_qre_name = qre_info['qre_name'].rsplit('_', 1)[0] if qre_info['qre_type'] in ['MA', 'MA_mtr', 'MA_comb'] else qre_info['qre_name']
                    str_qre_name = qre_info['qre_name']
                    lst_tbl_row_data = [str_qre_name, qre_info['qre_lbl'], qre_info['qre_type'], 'base', 'Base', num_base, np.nan]
                else:
                    lst_tbl_row_data.extend([num_base, np.nan])

                lst_ignore_col.append(item)

        df_qre.loc[len(df_qre)] = lst_tbl_row_data
        return df_qre


    def add_sa_qre_val_to_tbl_sig(self, df_data: pd.DataFrame, df_tbl: pd.DataFrame, df_qre: pd.DataFrame,
                                  qre_info: dict, dict_header_col_name: dict, lst_sig_pair: list,
                                  sig_type: str, lst_sig_lvl: list, cat: str, lbl: str, lst_sub_cat: list = None) -> pd.DataFrame:

        qre_name = qre_info['qre_name']
        qre_lbl = qre_info['qre_lbl']
        qre_type = qre_info['qre_type']
        qre_val = qre_info['qre_val']
        is_count = qre_info['is_count']
        val_pct = qre_info['val_pct']

        dict_new_row = {col: '' if '@sig@' in col else np.nan for col in df_qre.columns}
        dict_new_row.update({
            'qre_name': qre_name,
            'qre_lbl': qre_lbl,
            'qre_type': qre_type,
            'cat_val': cat,
            'cat_lbl': lbl,
        })

        df_qre = pd.concat([df_qre, pd.DataFrame(columns=list(dict_new_row.keys()), data=[list(dict_new_row.values())])], axis=0, ignore_index=True)

        for idx_pair, sig_pair in enumerate(lst_sig_pair):

            dict_pair_to_sig = dict()

            for idx_item, item in enumerate(sig_pair):
                str_query = f"{df_tbl.at[0, dict_header_col_name[item]['val_col']]}"

                df_filter = df_data.query(str_query).loc[:, [qre_name]].copy()

                if df_filter.empty:
                    continue

                if lst_sub_cat:
                    dict_re_qre_val = {int(k): 1 if k in lst_sub_cat else 0 for k, v in qre_val.items()}
                else:
                    dict_re_qre_val = {int(k): 1 if k == cat else 0 for k, v in qre_val.items()}

                df_filter.replace(dict_re_qre_val, inplace=True)

                dict_pair_to_sig.update({item: df_filter})

                num_val = (df_filter[qre_name] == 1).sum() if is_count else df_filter[qre_name].mean() * val_pct

                val_col_name, sig_col_name = dict_header_col_name[item]['val_col'], dict_header_col_name[item]['sig_col']

                if sig_type and lst_sig_lvl:
                    num_val_old = df_qre.loc[df_qre['cat_val'] == cat, [val_col_name]].values[0, 0]

                    if pd.isnull(num_val_old):
                        df_qre.loc[df_qre['cat_val'] == cat, [val_col_name, sig_col_name]] = [num_val, np.nan]

                else:

                    df_qre.loc[df_qre['cat_val'] == cat, [val_col_name, sig_col_name]] = [num_val, np.nan]


            if sig_type and lst_sig_lvl:
                df_qre = self.mark_sig_to_df_qre(df_qre, dict_pair_to_sig, sig_pair, dict_header_col_name, sig_type, lst_sig_lvl)

        return df_qre


    def add_sa_qre_mean_to_tbl_sig(self, df_data: pd.DataFrame, df_tbl: pd.DataFrame, df_qre: pd.DataFrame,
                                   qre_info: dict, dict_header_col_name: dict, lst_sig_pair: list,
                                   sig_type: str, lst_sig_lvl: list, is_mean=True) -> pd.DataFrame:

        qre_name = qre_info['qre_name']
        qre_lbl = qre_info['qre_lbl']
        qre_type = qre_info['qre_type']
        qre_val = qre_info['qre_val']
        # is_count = qre_info['is_count']
        # val_pct = qre_info['val_pct']

        if is_mean:
            dict_new_row = {col: '' if '@sig@' in col else np.nan for col in df_qre.columns}
            dict_new_row.update({
                'qre_name': qre_name,
                'qre_lbl': qre_lbl,
                'qre_type': qre_type,
                'cat_val': 'mean',
                'cat_lbl': 'Mean',
            })
        else:
            dict_new_row = {col: '' if '@sig@' in col else np.nan for col in df_qre.columns}
            dict_new_row.update({
                'qre_name': qre_name.replace('_Mean', '_Std'),
                'qre_lbl': qre_lbl,
                'qre_type': 'STD',
                'cat_val': 'std',
                'cat_lbl': 'Std',
            })

        org_qre_name = qre_name.replace('_Mean', '')

        df_qre = pd.concat([df_qre, pd.DataFrame(columns=list(dict_new_row.keys()), data=[list(dict_new_row.values())])], axis=0, ignore_index=True)

        for idx_pair, sig_pair in enumerate(lst_sig_pair):

            dict_pair_to_sig = dict()

            for idx_item, item in enumerate(sig_pair):
                str_query = f"{df_tbl.at[0, dict_header_col_name[item]['val_col']]}"

                df_filter = df_data.query(str_query).loc[:, [org_qre_name]].copy()

                if df_filter.empty:
                    continue

                if -999 not in qre_val.keys():
                    df_filter.replace(qre_val, inplace=True)

                dict_pair_to_sig.update({item: df_filter})

                if is_mean:

                    num_val = df_filter[org_qre_name].mean()
                    val_col_name, sig_col_name = dict_header_col_name[item]['val_col'], dict_header_col_name[item]['sig_col']

                    if sig_type and lst_sig_lvl:

                        num_val_old = df_qre.loc[df_qre['cat_val'] == 'mean', [val_col_name]].values[0, 0]

                        if pd.isnull(num_val_old):
                            df_qre.loc[df_qre['cat_val'] == 'mean', [val_col_name, sig_col_name]] = [num_val, np.nan]

                    else:
                        df_qre.loc[df_qre['cat_val'] == 'mean', [val_col_name, sig_col_name]] = [num_val, np.nan]

                else:

                    num_val_std = df_filter[org_qre_name].std()
                    val_col_name, sig_col_name = dict_header_col_name[item]['val_col'], dict_header_col_name[item]['sig_col']

                    df_qre.loc[df_qre['cat_val'] == 'std', [val_col_name, sig_col_name]] = [num_val_std, np.nan]

            if sig_type and lst_sig_lvl and is_mean:
                df_qre = self.mark_sig_to_df_qre(df_qre, dict_pair_to_sig, sig_pair, dict_header_col_name, sig_type, lst_sig_lvl)


        return df_qre


    def add_sa_qre_group_to_tbl_sig(self, df_data: pd.DataFrame, df_tbl: pd.DataFrame, df_qre: pd.DataFrame,
                                    qre_info: dict, dict_header_col_name: dict, lst_sig_pair: list,
                                    sig_type: str, lst_sig_lvl: list, cat: str, lbl: str) -> pd.DataFrame:  # lst_sub_cat: list = None

        qre_name = qre_info['qre_name']
        qre_lbl = qre_info['qre_lbl']
        qre_type = qre_info['qre_type']
        qre_val = qre_info['qre_val']
        is_count = qre_info['is_count']
        val_pct = qre_info['val_pct']

        dict_new_row = {col: '' if '@sig@' in col else np.nan for col in df_qre.columns}
        dict_new_row.update({
            'qre_name': qre_name,
            'qre_lbl': qre_lbl,
            'qre_type': qre_type,
            'cat_val': cat,
            'cat_lbl': lbl,
        })

        org_qre_name = qre_name.replace('_Group', '')

        df_qre = pd.concat([df_qre, pd.DataFrame(columns=list(dict_new_row.keys()), data=[list(dict_new_row.values())])], axis=0, ignore_index=True)

        for idx_pair, sig_pair in enumerate(lst_sig_pair):

            dict_pair_to_sig = dict()

            for idx_item, item in enumerate(sig_pair):
                str_query = f"{df_tbl.at[0, dict_header_col_name[item]['val_col']]}"

                df_filter = df_data.query(str_query).loc[:, [org_qre_name]].copy()

                if df_filter.empty:
                    continue

                df_filter.replace(qre_val['recode'], inplace=True)

                dict_re_qre_val = {int(k): 1 if int(k) == int(cat) else 0 for k, v in qre_val['cats'].items()}
                df_filter.replace(dict_re_qre_val, inplace=True)

                dict_pair_to_sig.update({item: df_filter})

                num_val = (df_filter[org_qre_name] == 1).sum() if is_count else df_filter[org_qre_name].mean() * val_pct
                val_col_name, sig_col_name = dict_header_col_name[item]['val_col'], dict_header_col_name[item]['sig_col']

                if sig_type and lst_sig_lvl:

                    num_val_old = df_qre.loc[df_qre['cat_val'] == cat, [val_col_name]].values[0, 0]

                    if pd.isnull(num_val_old):
                        df_qre.loc[df_qre['cat_val'] == cat, [val_col_name, sig_col_name]] = [num_val, np.nan]

                else:

                    df_qre.loc[df_qre['cat_val'] == cat, [val_col_name, sig_col_name]] = [num_val, np.nan]


            if sig_type and lst_sig_lvl:
                df_qre = self.mark_sig_to_df_qre(df_qre, dict_pair_to_sig, sig_pair, dict_header_col_name, sig_type, lst_sig_lvl)

        return df_qre


    def add_num_qre_to_tbl_sig(self, df_data: pd.DataFrame, df_tbl: pd.DataFrame, df_qre: pd.DataFrame,
                               qre_info: dict, dict_header_col_name: dict, lst_sig_pair: list,
                               sig_type: str, lst_sig_lvl: list) -> pd.DataFrame:

        qre_name = qre_info['qre_name']
        qre_lbl = qre_info['qre_lbl']
        qre_type = qre_info['qre_type']

        dict_new_row = {col: '' if '@sig@' in col else np.nan for col in df_qre.columns}
        dict_new_row.update({
            'qre_name': qre_name,
            'qre_lbl': qre_lbl,
            'qre_type': qre_type,
            'cat_val': 'mean',
            'cat_lbl': 'Mean',
        })

        df_qre = pd.concat([df_qre, pd.DataFrame(columns=list(dict_new_row.keys()), data=[list(dict_new_row.values())])], axis=0, ignore_index=True)

        for idx_pair, sig_pair in enumerate(lst_sig_pair):

            dict_pair_to_sig = dict()

            for idx_item, item in enumerate(sig_pair):
                str_query = f"{df_tbl.at[0, dict_header_col_name[item]['val_col']]}"
                df_filter = df_data.query(str_query).loc[:, [qre_name]].copy()

                if df_filter.empty:
                    continue

                dict_pair_to_sig.update({item: df_filter})

                num_val = df_filter[qre_name].mean()
                val_col_name, sig_col_name = dict_header_col_name[item]['val_col'], dict_header_col_name[item]['sig_col']

                if sig_type and lst_sig_lvl:

                    num_val_old = df_qre.loc[df_qre['cat_val'] == 'mean', [val_col_name]].values[0, 0]

                    if pd.isnull(num_val_old):
                        df_qre.loc[df_qre['cat_val'] == 'mean', [val_col_name, sig_col_name]] = [num_val, np.nan]

                else:

                    df_qre.loc[df_qre['cat_val'] == 'mean', [val_col_name, sig_col_name]] = [num_val, np.nan]

            if sig_type and lst_sig_lvl:
                df_qre = self.mark_sig_to_df_qre(df_qre, dict_pair_to_sig, sig_pair, dict_header_col_name, sig_type, lst_sig_lvl)

        return df_qre


    def add_ma_qre_val_to_tbl_sig(self, df_data: pd.DataFrame, df_tbl: pd.DataFrame, df_qre: pd.DataFrame,
                                  qre_info: dict, dict_header_col_name: dict, lst_sig_pair: list,
                                  sig_type: str, lst_sig_lvl: list, cat: str, lbl: str, lst_sub_cat: list = None) -> pd.DataFrame:

        if lst_sub_cat is None:
            lst_sub_cat = []

        qre_name = qre_info['qre_name']
        qre_lbl = qre_info['qre_lbl']
        qre_type = qre_info['qre_type']
        qre_val = qre_info['qre_val']
        is_count = qre_info['is_count']
        val_pct = qre_info['val_pct']
        # df_ma_info = qre_info['df_ma_info']
        lst_qre_col = qre_info['lst_qre_col']

        dict_new_row = {col: '' if '@sig@' in col else np.nan for col in df_qre.columns}
        dict_new_row.update({
            'qre_name': qre_name,  # qre_name.rsplit('_', 1)[0],
            'qre_lbl': qre_lbl,
            'qre_type': qre_type,
            'cat_val': cat,
            'cat_lbl': lbl,
        })

        df_qre = pd.concat([df_qre, pd.DataFrame(columns=list(dict_new_row.keys()), data=[list(dict_new_row.values())])], axis=0, ignore_index=True)

        for idx_pair, sig_pair in enumerate(lst_sig_pair):

            dict_pair_to_sig = dict()

            for idx_item, item in enumerate(sig_pair):
                str_query = f"{df_tbl.at[0, dict_header_col_name[item]['val_col']]}"
                # df_filter = df_data.query(str_query).loc[:, df_ma_info['var_name'].values.tolist()].copy()
                df_filter = df_data.query(str_query).loc[:, lst_qre_col].copy()

                if df_filter.empty:
                    continue

                if lst_sub_cat:
                    dict_re_qre_val = {int(k): 1 if k in lst_sub_cat else 0 for k, v in qre_val.items()}

                else:
                    dict_re_qre_val = {int(k): 1 if k == cat else 0 for k, v in qre_val.items()}

                df_filter.replace(dict_re_qre_val, inplace=True)

                # df_filter['ma_val_sum'] = df_filter.loc[df_filter[qre_name] >= 0, :].sum(axis='columns')

                df_fil_base = df_filter[lst_qre_col].dropna(how='all')
                df_filter.loc[df_fil_base.index, 'ma_val_sum'] = df_filter.loc[df_fil_base.index, lst_qre_col].sum(axis='columns')


                if lst_sub_cat or qre_type == 'MA_comb':
                    df_filter.loc[df_filter['ma_val_sum'] > 1, 'ma_val_sum'] = 1

                dict_pair_to_sig.update({item: df_filter['ma_val_sum']})

                num_val = (df_filter['ma_val_sum'] == 1).sum() if is_count else df_filter['ma_val_sum'].mean() * val_pct
                val_col_name, sig_col_name = dict_header_col_name[item]['val_col'], dict_header_col_name[item]['sig_col']

                if sig_type and lst_sig_lvl:
                    num_val_old = df_qre.loc[df_qre['cat_val'] == cat, [val_col_name]].values[0, 0]

                    if pd.isnull(num_val_old):
                        df_qre.loc[df_qre['cat_val'] == cat, [val_col_name, sig_col_name]] = [num_val, np.nan]

                else:
                    df_qre.loc[df_qre['cat_val'] == cat, [val_col_name, sig_col_name]] = [num_val, np.nan]

            if sig_type and lst_sig_lvl:
                df_qre = self.mark_sig_to_df_qre(df_qre, dict_pair_to_sig, sig_pair, dict_header_col_name, sig_type, lst_sig_lvl)

        return df_qre


    @staticmethod
    def mark_sig_to_df_qre(df_qre: pd.DataFrame, dict_pair_to_sig: dict, sig_pair: list, dict_header_col_name: dict,
                           sig_type: str, lst_sig_lvl: list) -> pd.DataFrame:

        if not lst_sig_lvl or not sig_type or not dict_pair_to_sig:
            return df_qre

        if sig_pair[0] not in dict_pair_to_sig.keys() or sig_pair[1] not in dict_pair_to_sig.keys():
            return df_qre

        df_left, df_right = dict_pair_to_sig[sig_pair[0]], dict_pair_to_sig[sig_pair[1]]

        if df_left.shape[0] < 30 > df_right.shape[0]:
            return df_qre

        # ------------------------------------------------

        is_df_left_null = df_left.isnull().values.all()
        is_df_right_null = df_right.isnull().values.all()

        if is_df_left_null or is_df_right_null:
            return df_qre

        try:
            if df_left.mean()[0] == 0 or df_right.mean()[0] == 0:
                return df_qre
        except Exception:
            if df_left.mean() == 0 or df_right.mean() == 0:
                return df_qre

        if sig_type == 'rel':
            if df_left.shape[0] != df_right.shape[0]:
                return df_qre

            sigResult = stats.ttest_rel(df_left, df_right)
        else:
            sigResult = stats.ttest_ind(df_left, df_right)

        if sigResult.pvalue:
            if sigResult.statistic > 0:
                mark_sig_char = sig_pair[1]
                sig_col_name = dict_header_col_name[sig_pair[0]]['sig_col']
            else:
                mark_sig_char = sig_pair[0]
                sig_col_name = dict_header_col_name[sig_pair[1]]['sig_col']

            if sigResult.pvalue <= lst_sig_lvl[0]:
                df_qre.at[df_qre.index[-1], sig_col_name] = str(df_qre.at[df_qre.index[-1], sig_col_name]).replace('nan', '') + mark_sig_char.upper()
            elif len(lst_sig_lvl) >= 2:
                if sigResult.pvalue <= lst_sig_lvl[1]:
                    df_qre.loc[df_qre.index[-1], sig_col_name] = str(df_qre.at[df_qre.index[-1], sig_col_name]).replace('nan', '') + mark_sig_char.lower()

        return df_qre



    def run_standard_header_sig(self, df_data: pd.DataFrame, df_info: pd.DataFrame, tbl_info_sig: dict) -> pd.DataFrame:

        is_count = tbl_info_sig['is_count']
        val_pct = 1 if tbl_info_sig['is_pct_sign'] else 100

        sig_type = tbl_info_sig['sig_test_info']['sig_type']

        lst_sig_lvl_pct = tbl_info_sig['sig_test_info']['lst_sig_lvl']
        lst_sig_lvl = [(100 - a) / 100 for a in lst_sig_lvl_pct]
        lst_sig_lvl.reverse()

        dict_grp_header = tbl_info_sig['dict_grp_header']

        dict_char_sig = {
            0: 'A',
            1: 'B',
            2: 'C',
            3: 'D',
            4: 'E',
            5: 'F',
            6: 'G',
            7: 'H',
            8: 'I',
            9: 'J',
            10: 'K',
            11: 'L',
            12: 'M',
            13: 'N',
            14: 'O',
            15: 'P',
            16: 'Q',
            17: 'R',
            18: 'S',
            19: 'T',
            20: 'U',
            21: 'V',
            22: 'W',
            23: 'X',
            24: 'Y',

        }

        # lst_tbl_col = ['qre_name', 'qre_lbl', 'qre_type', 'cat_val', 'cat_lbl']
        dict_tbl_data = {
            'qre_name': list(),
            'qre_lbl': list(),
            'qre_type': list(),
            'cat_val': list(),
            'cat_lbl': list(),
        }

        dict_header_col_name = dict()

        for hd_k, hd_v in dict_grp_header.items():
            str_hd_val = f"{hd_v['query']}@{hd_v['lbl']}@val@{dict_char_sig[hd_k]}"
            str_hd_sig = f"{hd_v['query']}@{hd_v['lbl']}@sig@{dict_char_sig[hd_k]}"

            dict_tbl_data.update({
                str_hd_val: str_hd_val.split('@'),
                str_hd_sig: str_hd_sig.split('@'),
            })

            dict_header_col_name.update({
                dict_char_sig[hd_k]: {
                    'val_col': str_hd_val,
                    'sig_col': str_hd_sig,
                }
            })

            if len(dict_tbl_data['qre_name']) == 0:
                arr_nan = [np.nan] * len(str_hd_val.split('@'))
                dict_tbl_data.update({
                    'qre_name': arr_nan,
                    'qre_lbl': arr_nan,
                    'qre_type': arr_nan,
                    'cat_val': arr_nan,
                    'cat_lbl': arr_nan,
                })

        if sig_type in ["ind", "rel"]:

            if tbl_info_sig['sig_test_info']['sig_cols']:
                lst_sig_pair = tbl_info_sig['sig_test_info']['sig_cols']
            else:
                lst_sig_char = list(dict_header_col_name.keys())
                lst_sig_pair = list()
                for i in range(len(lst_sig_char)-1):
                    for j in range(i + 1, len(lst_sig_char)):
                        lst_sig_pair.append([lst_sig_char[i], lst_sig_char[j]])

        else:
            lst_sig_char = list(dict_header_col_name.keys())
            lst_sig_pair = list()
            # for j in range(1, len(lst_sig_char)):
            #     lst_sig_pair.append([lst_sig_char[0], lst_sig_char[j]])

            for i in lst_sig_char:
                lst_sig_pair.append([i])


        df_tbl = pd.DataFrame.from_dict(dict_tbl_data)

        df_tbl.loc[1:4, ['qre_lbl']] = [
            f"Cell content: {'count' if is_count else ('percentage(%)' if tbl_info_sig['is_pct_sign'] else 'percentage')}",
            '' if is_count or sig_type == '' else f"{'Dependent' if sig_type == 'rel' else 'Independent'} Pair T-test at level {' & '.join([f'{i}%' for i in lst_sig_lvl_pct])}",
            '' if is_count or sig_type == '' else f"Columns Tested: {', '.join(['/'.join(i) for i in lst_sig_pair])}",
            '' if is_count or sig_type == '' else f"Uppercase for {lst_sig_lvl_pct[-1]}%, lowercase for {lst_sig_lvl_pct[0]}%" if len(lst_sig_lvl_pct) > 1 else np.nan
        ]

        # lst_ignore_col_name = list()

        for idx in df_info.index:

            qre_name = df_info.at[idx, 'var_name']
            qre_lbl = df_info.at[idx, 'var_lbl']
            qre_type = df_info.at[idx, 'var_type']
            qre_val = eval(df_info.at[idx, 'val_lbl']) if isinstance(df_info.at[idx, 'val_lbl'], str) else df_info.at[idx, 'val_lbl']
            qre_fil = df_info.at[idx, 'qre_fil']
            lst_qre_col = df_info.at[idx, 'lst_qre_col']

            self.logger.info(qre_name)

            if qre_name == 'Q2_OL_Com':
                a = 1

            # filter data base on qre_fil
            df_data_qre_fil = df_data.query(qre_fil).copy() if qre_fil else df_data.copy()

            qre_info = {
                'qre_name': qre_name,
                'qre_lbl': qre_lbl,
                'qre_type': qre_type,
                'qre_val': qre_val,
                'is_count': is_count,
                'val_pct': val_pct,
                'lst_qre_col': lst_qre_col,
            }

            df_qre = pd.DataFrame(columns=df_tbl.columns, data=[])

            # qre_name_ma, qre_ma_col_order = 0, 0
            # if qre_type in ['MA', 'MA_mtr', 'MA_comb']:
            #     qre_name_ma, qre_ma_col_order = qre_name.rsplit('_', 1)
            #     qre_ma_col_order = int(qre_ma_col_order)

            # BASE------------------------------------------------------------------------------------------------------
            # if qre_type in ['MEAN', 'GROUP'] or (qre_type in ['MA', 'MA_mtr', 'MA_comb'] and qre_ma_col_order > 1):
            if qre_type in ['MEAN', 'GROUP']:
                pass
            else:
                df_qre = self.add_base_to_tbl_sig(df_data_qre_fil, df_tbl, df_qre, qre_info, dict_header_col_name, lst_sig_pair)

            # END BASE--------------------------------------------------------------------------------------------------

            if qre_type in ['FT', 'FT_mtr']:
                # Not run free text questions
                pass

            elif qre_type in ['SA', 'SA_mtr', 'RANKING']:

                if qre_name in self.dict_unnetted_qres.keys():
                    qre_val_unnetted = self.dict_unnetted_qres[qre_name]
                else:
                    qre_val_unnetted = qre_val

                qre_info['qre_val'] = qre_val_unnetted

                for cat, lbl in qre_val.items():

                    # ADD-IN Net Code
                    if 'net_code' in str(cat):

                        for net_cat, net_val in qre_val['net_code'].items():

                            if isinstance(net_val, dict):

                                lst_sub_cat = list(net_val.keys())

                                net_cat_val = net_cat.split('|')[0]
                                net_cat_lbl = net_cat.split('|')[-1]

                                df_qre = self.add_sa_qre_val_to_tbl_sig(df_data_qre_fil, df_tbl, df_qre, qre_info, dict_header_col_name, lst_sig_pair, sig_type, lst_sig_lvl, net_cat_val, net_cat_lbl, lst_sub_cat)

                                for cat2, lbl2 in net_val.items():
                                    df_qre = self.add_sa_qre_val_to_tbl_sig(df_data_qre_fil, df_tbl, df_qre, qre_info, dict_header_col_name, lst_sig_pair, sig_type, lst_sig_lvl, cat2, lbl2)
                            else:
                                df_qre = self.add_sa_qre_val_to_tbl_sig(df_data_qre_fil, df_tbl, df_qre, qre_info, dict_header_col_name, lst_sig_pair, sig_type, lst_sig_lvl, net_cat, net_val)

                    else:
                        df_qre = self.add_sa_qre_val_to_tbl_sig(df_data_qre_fil, df_tbl, df_qre, qre_info, dict_header_col_name, lst_sig_pair, sig_type, lst_sig_lvl, cat, lbl)

            elif qre_type in ['MEAN']:

                df_qre = self.add_sa_qre_mean_to_tbl_sig(df_data_qre_fil, df_tbl, df_qre, qre_info, dict_header_col_name, lst_sig_pair, sig_type, lst_sig_lvl)

                df_qre_std = self.add_sa_qre_mean_to_tbl_sig(df_data_qre_fil, df_tbl, df_qre, qre_info, dict_header_col_name, lst_sig_pair, sig_type, lst_sig_lvl, False)

                df_qre = df_qre_std

            elif qre_type in ['GROUP']:

                for cat, lbl in qre_val['cats'].items():
                    df_qre = self.add_sa_qre_group_to_tbl_sig(df_data_qre_fil, df_tbl, df_qre, qre_info, dict_header_col_name, lst_sig_pair, sig_type, lst_sig_lvl, cat, lbl)

            elif qre_type in ['NUM']:

                df_qre = self.add_num_qre_to_tbl_sig(df_data_qre_fil, df_tbl, df_qre, qre_info, dict_header_col_name, lst_sig_pair, sig_type, lst_sig_lvl)

            elif qre_type in ['MA', 'MA_mtr', 'MA_comb']:

                # if 'Q1a' in qre_name:
                #     a = 1

                if f'{qre_name}_1' in self.dict_unnetted_qres.keys() and f'{qre_name}_2' in self.dict_unnetted_qres.keys():
                    qre_val_unnetted = self.dict_unnetted_qres[f'{qre_name}_1']
                else:
                    qre_val_unnetted = qre_val

                qre_info['qre_val'] = qre_val_unnetted

                for cat, lbl in qre_val.items():

                    # ADD-IN Net Code
                    if 'net_code' in cat:

                        for net_cat, net_val in qre_val['net_code'].items():

                            if isinstance(net_val, dict):

                                lst_sub_cat = list(net_val.keys())

                                # "900001|combine|POSITIVE (NET)"
                                list_net_cat = net_cat.split('|')

                                df_qre = self.add_ma_qre_val_to_tbl_sig(df_data_qre_fil, df_tbl, df_qre, qre_info, dict_header_col_name, lst_sig_pair, sig_type, lst_sig_lvl, list_net_cat[0], list_net_cat[2], lst_sub_cat)

                                if 'NET' in list_net_cat[1].upper():
                                    for cat2, lbl2 in net_val.items():
                                        df_qre = self.add_ma_qre_val_to_tbl_sig(df_data_qre_fil, df_tbl, df_qre, qre_info, dict_header_col_name, lst_sig_pair, sig_type, lst_sig_lvl, cat2, lbl2)

                            else:
                                df_qre = self.add_ma_qre_val_to_tbl_sig(df_data_qre_fil, df_tbl, df_qre, qre_info, dict_header_col_name, lst_sig_pair, sig_type, lst_sig_lvl, net_cat, net_val)

                    else:
                        df_qre = self.add_ma_qre_val_to_tbl_sig(df_data_qre_fil, df_tbl, df_qre, qre_info, dict_header_col_name, lst_sig_pair, sig_type, lst_sig_lvl, cat, lbl)

            df_tbl = pd.concat([df_tbl, df_qre], axis=0, ignore_index=True)




        return df_tbl