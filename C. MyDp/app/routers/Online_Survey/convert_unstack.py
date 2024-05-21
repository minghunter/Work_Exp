import pandas as pd



class ConvertUnstack:

    @staticmethod
    def convert_to_unstack(df_data_stack: pd.DataFrame, df_info_stack: pd.DataFrame, dict_stack_structure: dict) -> (pd.DataFrame, pd.DataFrame):

        id_col = dict_stack_structure['id_col']
        sp_col = dict_stack_structure['sp_col']

        dict_sp_val = df_info_stack.query(f"var_name == '{sp_col}'")['val_lbl'].values[0]

        df_part_head = df_data_stack.query(f"{sp_col} == {list(dict_sp_val.keys())[0]}")[[id_col] + dict_stack_structure['lst_col_part_head']].copy()
        df_part_tail = df_data_stack.query(f"{sp_col} == {list(dict_sp_val.keys())[0]}")[[id_col] + dict_stack_structure['lst_col_part_tail']].copy()

        df_part_body = pd.DataFrame()
        df_info_part_body = pd.DataFrame()

        dict_sort_col = dict()
        for k, v in dict_sp_val.items():

            sp_lbl = str(v).replace(' ', '_')

            df_info_by_k = df_info_stack.query(f"var_name.isin({dict_stack_structure['lst_col_part_body']})").copy()
            df_data_by_k = df_data_stack.query(f"{sp_col} == {k}")[[id_col] + dict_stack_structure['lst_col_part_body']].copy()

            dict_rename_col = dict()
            dict_rename_var_lbl = dict()
            for idx in df_info_by_k.index:

                var_name = df_info_by_k.at[idx, 'var_name']
                var_lbl = df_info_by_k.at[idx, 'var_lbl']
                var_type = df_info_by_k.at[idx, 'var_type']

                str_ma_name = str()

                if var_type in ['MA']:
                    str_ma_name, str_ma_cat = var_name.rsplit('_', 1)
                    str_var_name_new = f"{str_ma_name}_{sp_lbl}_{str_ma_cat}"
                else:
                    str_var_name_new = f"{var_name}_{sp_lbl}"

                dict_rename_col.update({var_name: str_var_name_new})
                dict_rename_var_lbl.update({var_lbl: f"{var_lbl}_{sp_lbl}"})

                if var_type in ['MA']:
                    if str_ma_name in dict_sort_col.keys():
                        dict_sort_col[str_ma_name].append(str_var_name_new)
                    else:
                        dict_sort_col.update({str_ma_name: [str_var_name_new]})
                else:
                    if var_name in dict_sort_col.keys():
                        dict_sort_col[var_name].append(str_var_name_new)
                    else:
                        dict_sort_col.update({var_name: [str_var_name_new]})

            df_data_by_k.rename(columns=dict_rename_col, inplace=True)
            df_info_by_k['var_name'].replace(dict_rename_col, inplace=True)
            df_info_by_k['var_lbl'].replace(dict_rename_var_lbl, inplace=True)

            if df_part_body.empty:
                df_part_body = df_data_by_k.copy()
                df_info_part_body = df_info_by_k.copy()
            else:
                df_part_body = df_part_body.merge(df_data_by_k, how='left', on=id_col)
                df_info_part_body = pd.concat([df_info_part_body, df_info_by_k], ignore_index=True)

        # Need to sort vars
        lst_sort_col = list()
        for v in dict_sort_col.values():
            lst_sort_col.extend(v)

        df_part_body = df_part_body.reindex(columns=[id_col] + lst_sort_col)

        df_info_part_body['idx_by_var_name'] = df_info_part_body['var_name']
        df_info_part_body.set_index('idx_by_var_name', inplace=True)
        df_info_part_body = df_info_part_body.reindex(lst_sort_col)
        df_info_part_body.reset_index(drop=True, inplace=True)
        # Need to sort vars

        df_data_unstack = df_part_head.copy()
        df_data_unstack = df_data_unstack.merge(df_part_body, how='left', on=id_col)
        df_data_unstack = df_data_unstack.merge(df_part_tail, how='left', on=id_col)

        df_info_unstack = df_info_stack.query(f"var_name.isin({[id_col] + dict_stack_structure['lst_col_part_head']})").copy()
        df_info_unstack = pd.concat([df_info_unstack, df_info_part_body], ignore_index=True)
        df_info_unstack = pd.concat([df_info_unstack, df_info_stack.query(f"var_name.isin({dict_stack_structure['lst_col_part_tail']})").copy()], ignore_index=True)

        df_data_unstack.reset_index(drop=True, inplace=True)
        df_info_unstack.reset_index(drop=True, inplace=True)

        return df_data_unstack, df_info_unstack
