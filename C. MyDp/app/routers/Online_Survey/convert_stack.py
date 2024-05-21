import pandas as pd



class ConvertStack:

    @staticmethod
    def convert_to_stack(df_data: pd.DataFrame, df_info: pd.DataFrame, id_col: str, sp_col: str, lst_scr: list, dict_sp: dict, lst_fc: list) -> (pd.DataFrame, pd.DataFrame):

        # df_data_stack generate
        df_data_scr = df_data.loc[:, [id_col] + lst_scr].copy()

        df_data_df = pd.DataFrame()
        if lst_fc:
            df_data_df = df_data.loc[:, [id_col] + lst_fc].copy()

        lst_df_data_sp = [df_data.loc[:, [id_col] + list(val.keys())].copy() for val in dict_sp.values()]

        for i, df in enumerate(lst_df_data_sp):
            df.rename(columns=dict_sp[i + 1], inplace=True)

        df_data_stack = pd.concat(lst_df_data_sp, axis=0, ignore_index=True)

        df_data_stack = df_data_scr.merge(df_data_stack, how='left', on=[id_col])

        if lst_fc:
            df_data_stack = df_data_stack.merge(df_data_df, how='left', on=[id_col])

        df_data_stack.reset_index(drop=True, inplace=True)

        df_data_stack.sort_values(by=[id_col, sp_col], inplace=True)
        df_data_stack.reset_index(drop=True, inplace=True)

        df_info_stack = df_info.copy()

        for key, val in dict_sp[1].items():
            df_info_stack.loc[df_info_stack['var_name'] == key, ['var_name']] = [val]

        df_info_stack.drop_duplicates(subset=['var_name'], keep='first', inplace=True)

        # Reset df_info_stack index
        df_info_stack['idx_var_name'] = df_info_stack['var_name']
        df_info_stack.set_index('idx_var_name', inplace=True)
        df_info_stack = df_info_stack.loc[list(df_data_stack.columns), :]
        df_info_stack.reindex(list(df_data_stack.columns))
        df_info_stack.reset_index(drop=True, inplace=True)

        return df_data_stack, df_info_stack