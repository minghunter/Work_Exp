

class ToplineDesign:
    def __init__(self):
        pass


    def auto_generate_topline_side_axis(self, prj: dict) -> dict:

        dict_detail = prj['detail']
        dict_side = dict()

        dict_side = self.generate_topline_cols(dict_side, dict_detail['plm_to_prod_cols'], "PLACEMENT", is_ua=False)
        dict_side = self.generate_topline_cols(dict_side, dict_detail['product_cols'], "PRODUCT", is_ua=False)
        dict_side = self.generate_topline_cols(dict_side, dict_detail['fc_cols'], "FC", is_ua=False)

        dict_side = self.generate_topline_cols(dict_side, dict_detail['scr_cols'], "SCREENER", is_ua=True)
        dict_side = self.generate_topline_cols(dict_side, dict_detail['plm_to_scr_cols'], "PLACEMENT", is_ua=True)

        return dict_side


    @staticmethod
    def generate_topline_cols(dict_side, dict_cols, grp_lbl, is_ua) -> dict:

        count = len(dict_side) + 1
        lst_qre_type = ['OL', 'JR', 'FC', 'SA', 'MA', 'NUM']

        lst_ignore = list()
        for key, val in dict_cols.items():

            qre_type = val[-1]
            qre_name = val[0]

            if qre_type in ['MA']:
                qre_name = val[0].rsplit('_', 1)[0]

                if qre_name in lst_ignore:
                    continue

                lst_ignore.append(qre_name)

            qre_lbl = f"{qre_name}. change label"

            if qre_type in lst_qre_type:

                dict_side.update({
                    f"{count}": {
                        "group_lbl": grp_lbl,
                        "name": qre_name,
                        "lbl": qre_lbl,
                        "type": qre_type,
                        "t2b": True if qre_type in ['OL', 'JR'] else False,
                        "b2b": True if qre_type in ['OL', 'JR'] else False,
                        "mean": True if qre_type in ['OL', 'JR'] else False,
                        "ma_cats": "1" if qre_type in ['MA'] else "",
                        "hidden_cats": "",
                        "is_count": False,
                        "is_corr": False,
                        "is_ua": is_ua
                    },
                })

                count += 1

        return dict_side
