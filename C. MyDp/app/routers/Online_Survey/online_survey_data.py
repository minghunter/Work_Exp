from fastapi import APIRouter, Request, UploadFile, HTTPException, status, Form, BackgroundTasks
from fastapi.responses import HTMLResponse, RedirectResponse, FileResponse
from starlette.background import BackgroundTask
from fastapi.templating import Jinja2Templates
from app.routers.Online_Survey.export_online_survey_data import ExportOnlineSurveyData
from app.classes.CleanUpResponseFiles import CleanupFiles
from app.routers.Auth import token
import traceback
from os.path import exists
from app.classes.Logging_Custom_Formatter import Logger



credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail='Could not validate credentials',
        headers={'WWW-Authenticate': 'Bearer'},
    )


templates = Jinja2Templates(directory='./app/frontend/templates')
router = APIRouter(prefix='/online-survey-data', tags=['online-survey-data'])


@router.get('', response_class=HTMLResponse)
async def online_survey_data(request: Request):
    user_info = token.get_token_userinfo(request)
    return templates.TemplateResponse('online_survey/online_survey_data_v2.html', {'request': request, 'user_info': user_info, 'strTask': 'Online Survey Data', 'wait_secs': -999, 'file_name': ''})


@router.post('', response_class=FileResponse)
async def online_survey_data_process(request: Request, background_tasks: BackgroundTasks,
                                     files: list[UploadFile], prj_choose: str = Form(default=''),
                                     py_script_file: UploadFile = None, tables_format_file: UploadFile = None,
                                     coding_file: UploadFile = None, codelist_file: UploadFile = None):

    user_info = token.get_token_userinfo(request)
    logger = Logger.logger(f"{user_info['name']}")

    try:
        if not files[0].filename:
            return templates.TemplateResponse('online_survey/online_survey_data_v2.html', {
                'request': request,
                'user_info': user_info,
                'wait_secs': -999,
                'strTask': 'Online Survey Data',
                'strErr': 'Please upload data file(s).'
            })

        export_onl_data = ExportOnlineSurveyData(files, logger)

        lst_dup_vars = export_onl_data.check_duplicate_variables()

        if lst_dup_vars:
            logger.error('Duplicate variables: %s' % lst_dup_vars)

            return templates.TemplateResponse('online_survey/online_survey_data_v2.html', {
                'request': request,
                'user_info': user_info,
                'strTask': 'Online Survey Data',
                'strErr': f'Duplicate variables: {lst_dup_vars}'
            })

        wait_secs = 100 if tables_format_file.filename else 30

        if len(files) >= 2:
            str_file_name = f"{files[0].filename.rsplit('_', 1)[0]}.zip"
        else:
            str_file_name = files[0].filename.replace('.xlsx', '.zip')

        if prj_choose in ['VN8168_TVC_testing_v1']:
            background_tasks.add_task(export_onl_data.convert_tvc_to_sav)

        elif prj_choose in ['VN8168_TVC_testing_v2']:
            background_tasks.add_task(export_onl_data.convert_tvc_to_sav_v2)

        elif prj_choose in ['VN8186_ED']:
            background_tasks.add_task(export_onl_data.convert_VN8186_ED)

        elif prj_choose in ['KKM_DAI']:
            background_tasks.add_task(export_onl_data.convert_KKM_Dai)

        elif prj_choose in ['VN8191_TVC_WINMART']:
            background_tasks.add_task(export_onl_data.convert_tvc_winmart)

        elif prj_choose in ['VN8194_Unilever_PS']:
            background_tasks.add_task(export_onl_data.convert_unilever_ps)

        elif prj_choose in ['Masan_Lagom_ED_CLT_HCM']:
            background_tasks.add_task(export_onl_data.convert_VN8206PjLagomED)

        elif prj_choose in ['VN8212_Masan_ED_Packaging_test']:
            background_tasks.add_task(export_onl_data.convert_vn8212_ed_packaging_test)

        elif prj_choose in ['VN8194_Unilever_PS_v2']:
            background_tasks.add_task(export_onl_data.convert_unilever_ps_v2)

        elif prj_choose in ['VN8216_Cooking_Sauce']:
            background_tasks.add_task(export_onl_data.convert_vn8216_Cooking_Sauce, coding_file)

        elif prj_choose in ['VN8226_Lapental']:
            background_tasks.add_task(export_onl_data.convert_vn8226_lapental, coding_file)

        elif prj_choose in ['VN8231_Bon_Bon_1']:
            background_tasks.add_task(export_onl_data.convert_vn8231_bon_bon_1, coding_file)

        elif prj_choose in ['VN9999_TrueID']:
            background_tasks.add_task(export_onl_data.convert_vn9999_trueid, coding_file)

        elif prj_choose in ['VN8228_Wakeup']:
            background_tasks.add_task(export_onl_data.convert_vn8228_wakeup, coding_file)

        elif prj_choose in ['VN8247_Rice_RTB']:
            background_tasks.add_task(export_onl_data.convert_vn8247_rice_rtb, py_script_file, tables_format_file, codelist_file, coding_file)

        elif prj_choose in ['VN8161_Aurora_VN']:
            background_tasks.add_task(export_onl_data.convert_vn8161_aurora_vn, py_script_file, tables_format_file, codelist_file, coding_file)

        elif prj_choose in ['Online_shopping_behavior']:
            background_tasks.add_task(export_onl_data.convert_int0001_online_shopping_behavior, py_script_file, tables_format_file, codelist_file, coding_file)

        elif prj_choose in ['VN8263_Lapental']:
            background_tasks.add_task(export_onl_data.convert_vn8263_lapental, py_script_file, tables_format_file, codelist_file, coding_file)

        elif prj_choose in ['VN8274_McKinsey_VN_SME']:
            background_tasks.add_task(export_onl_data.convert_vn8274_mckinsey_vn_sme, py_script_file, tables_format_file, codelist_file, coding_file)

        elif prj_choose in ['VN8189_Acecook']:
            background_tasks.add_task(export_onl_data.convert_vn8189_acecook, py_script_file, tables_format_file, codelist_file, coding_file)

        elif prj_choose in ['VN8280_Noodle_STB_Test']:
            background_tasks.add_task(export_onl_data.convert_vn8280_noodle_stb_test, py_script_file, tables_format_file, codelist_file, coding_file)

        elif prj_choose in ['VN8286_Crust_CLT']:
            background_tasks.add_task(export_onl_data.convert_vn8286_crust_clt, py_script_file, tables_format_file, codelist_file, coding_file)

        elif prj_choose in ['VN8306_MKT_Activities_Mi']:
            background_tasks.add_task(export_onl_data.convert_vn8306_mkt_activities_mi, py_script_file, tables_format_file, codelist_file, coding_file)

        elif prj_choose in ['VN8303_MKT_Activities_Pho']:
            background_tasks.add_task(export_onl_data.convert_vn8303_mkt_activities_pho, py_script_file, tables_format_file, codelist_file, coding_file)

        elif prj_choose in ['VN8307_Rakan_CLT']:
            background_tasks.add_task(export_onl_data.convert_vn8307_rakan_clt, py_script_file, tables_format_file, codelist_file, coding_file)

        elif prj_choose in ['VN8314_MKT_Activities_Mi_R2']:
            background_tasks.add_task(export_onl_data.convert_vn8314_mkt_activities_mi_r2, py_script_file, tables_format_file, codelist_file, coding_file)

        elif prj_choose in ['VN8319_TvcMixTest_Intercept']:
            background_tasks.add_task(export_onl_data.convert_vn8319_tvc_mix_test_intercept, py_script_file, tables_format_file, codelist_file, coding_file)

        elif prj_choose in ['VN8324_Lapental']:
            background_tasks.add_task(export_onl_data.convert_vn8324_lapental, py_script_file, tables_format_file, codelist_file, coding_file)

        elif prj_choose in ['VN8326_TVC_Test_R2']:
            background_tasks.add_task(export_onl_data.convert_vn8326_tvc_test_r2, py_script_file, tables_format_file, codelist_file, coding_file)

        elif prj_choose in ['VN8332_TOKYO_3']:
            background_tasks.add_task(export_onl_data.convert_vn8332_tokyo_3, py_script_file, tables_format_file, codelist_file, coding_file)

        elif prj_choose in ['VN8333_Charity_F1']:
            background_tasks.add_task(export_onl_data.convert_vn8333_charity_f1, py_script_file, tables_format_file, codelist_file, coding_file)

        elif prj_choose in ['VN8313_Customer_Understanding']:
            background_tasks.add_task(export_onl_data.convert_vn8313_customer_understanding, py_script_file, tables_format_file, codelist_file, coding_file)

        elif prj_choose in ['INT0002_Pricing_Change']:
            background_tasks.add_task(export_onl_data.convert_int0002_pricing_change, py_script_file, tables_format_file, codelist_file, coding_file)

        elif prj_choose in ['VN8256_KEWPIE']:
            background_tasks.add_task(export_onl_data.convert_vn8256_kewpie, py_script_file, tables_format_file, codelist_file, coding_file)

        elif prj_choose in ['VN8347_PS_Promotion']:
            background_tasks.add_task(export_onl_data.convert_vn8347_ps_promotion, py_script_file, tables_format_file, codelist_file, coding_file)

        elif prj_choose in ['VN8053_Promind_Mosquito_2022']:
            background_tasks.add_task(export_onl_data.convert_vn8053_promind_mosquito_2022, py_script_file, tables_format_file, codelist_file, coding_file)

        elif prj_choose in ['VN8346_Personal_Care']:
            background_tasks.add_task(export_onl_data.convert_vn8346_personal_care, py_script_file, tables_format_file, codelist_file, coding_file)

        elif prj_choose in ['VN8363_Shampoo']:
            background_tasks.add_task(export_onl_data.convert_vn8363_shampoo, py_script_file, tables_format_file, codelist_file, coding_file)

        elif prj_choose in ['VN8378_TVC_Test']:
            background_tasks.add_task(export_onl_data.convert_vn8378_tvc_test, py_script_file, tables_format_file, codelist_file, coding_file)

        elif prj_choose in ['VN8374_Deodorant_Women']:
            background_tasks.add_task(export_onl_data.convert_vn8374_deodorant_women, py_script_file, tables_format_file, codelist_file, coding_file)

        elif prj_choose in ['VN8387_Chili_Sauce']:
            background_tasks.add_task(export_onl_data.convert_vn8387_chili_sauce, py_script_file, tables_format_file, codelist_file, coding_file)

        elif prj_choose in ['VN8383_Oral_Skin']:
            background_tasks.add_task(export_onl_data.convert_vn8383_oral_skin, py_script_file, tables_format_file, codelist_file, coding_file)

        elif prj_choose in ['VN8352_Promind_Femskin_2023']:
            background_tasks.add_task(export_onl_data.convert_vn8352_promind_femskin_2023, py_script_file, tables_format_file, codelist_file, coding_file)

        elif prj_choose in ['VN8393_Pantene']:
            background_tasks.add_task(export_onl_data.convert_vn8393_pantene, py_script_file, tables_format_file, codelist_file, coding_file)

        elif prj_choose in ['VN8394_Shampoo']:
            background_tasks.add_task(export_onl_data.convert_vn8394_shampoo, py_script_file, tables_format_file, codelist_file, coding_file)

        elif prj_choose in ['VN8383_Oral_Skin_v2']:
            background_tasks.add_task(export_onl_data.convert_vn8383_oral_skin_v2, py_script_file, tables_format_file, codelist_file, coding_file)

        elif prj_choose in ['VN8413_Red_Ruby']:
            background_tasks.add_task(export_onl_data.convert_vn8413_red_ruby, py_script_file, tables_format_file, codelist_file, coding_file)

        elif prj_choose in ['INT0003_Tiktok_Shopping']:
            background_tasks.add_task(export_onl_data.convert_int0003_tiktok_shopping, py_script_file, tables_format_file, codelist_file, coding_file)

        elif prj_choose in ['VN8327_Etika']:
            background_tasks.add_task(export_onl_data.convert_vn8327_etika, py_script_file, tables_format_file, codelist_file, coding_file)

        elif prj_choose in ['VN8420_Tet_Storyboard']:
            background_tasks.add_task(export_onl_data.convert_vn8420_tet_storyboard, py_script_file, tables_format_file, codelist_file, coding_file)

        elif prj_choose in ['VN8384_Battery']:
            background_tasks.add_task(export_onl_data.convert_vn8384_battery, py_script_file, tables_format_file, codelist_file, coding_file)

        else:
            return templates.TemplateResponse('online_survey/online_survey_data_v2.html', {
                'request': request,
                'user_info': user_info,
                'wait_secs': -999,
                'strTask': 'Online Survey Data',
                'strErr': 'Please select project.'
            })

        return templates.TemplateResponse('online_survey/online_survey_data_v2.html', {
            'request': request, 'user_info': user_info,
            'strTask': 'Online Survey Data', 'wait_secs': wait_secs,
            'file_name': str_file_name
        })

    except Exception:
        logger.error(traceback.format_exc())
        return templates.TemplateResponse('online_survey/online_survey_data_v2.html', {
            'request': request,
            'user_info': user_info,
            'wait_secs': -999,
            'strTask': 'Online Survey Data',
            'strErr': traceback.format_exc()
        })


@router.get('/isexist/{file_name}')
async def is_exist_online_survey_data(request: Request, file_name: str):

    user_info = token.get_token_userinfo(request)
    logger = Logger.logger(f"{user_info['name']}")

    str_err_log_name = file_name.replace('.zip', '_Errors.txt')
    if exists(str_err_log_name):
        with open(str_err_log_name) as err_log_txt:
            str_err_log = err_log_txt.readlines()

        cleanup = CleanupFiles(lstFileName=[str_err_log_name], logger=logger)
        cleanup.cleanup()

        return {
            'isexist': -1,
            'str_result': str_err_log
        }

    str_topline_name = file_name.replace('.zip', '_Topline.xlsx')
    if exists(str_topline_name):
        return {
            'isexist': 0,
            'str_result': 'Not yet completed, please wait'
        }

    if exists(file_name):
        return {
            'isexist': 1,
            'str_result': file_name
        }

    return {
        'isexist': 0,
        'str_result': 'Not yet complete, please wait'
    }


@router.get('/download/{file_name}', response_class=FileResponse)
async def download_online_survey_data(request: Request, file_name: str = ''):

    user_info = token.get_token_userinfo(request)
    logger = Logger.logger(f"{user_info['name']}")

    try:
        if exists(file_name):
            cleanup = CleanupFiles(lstFileName=[file_name], logger=logger)
            return FileResponse(file_name, filename=file_name, background=BackgroundTask(cleanup.cleanup))
        else:
            return templates.TemplateResponse('online_survey/online_survey_data_v2.html', {
                'request': request,
                'user_info': user_info,
                'wait_secs': -999,
                'strTask': 'Online Survey Data',
                'strErr': 'Please rerun project.'
            })

    except Exception:
        logger.error(traceback.format_exc())
        return templates.TemplateResponse('online_survey/online_survey_data_v2.html', {
            'request': request,
            'user_info': user_info,
            'wait_secs': -999,
            'strTask': 'Online Survey Data',
            'strErr': traceback.format_exc()
        })

