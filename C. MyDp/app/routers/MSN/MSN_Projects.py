from fastapi import APIRouter, Request, UploadFile, Body, status, Depends, Form, BackgroundTasks, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, FileResponse
from fastapi.templating import Jinja2Templates
from starlette.background import BackgroundTask
from typing import Optional
from .MSN_Database import MsnPrj
from ..Auth import oauth2, token
from app.classes.CleanUpResponseFiles import CleanupFiles
from app.classes.Logging_Custom_Formatter import Logger
from os.path import exists
from .MSN_Models import *
import traceback
import time



msn_prj = MsnPrj()


templates = Jinja2Templates(directory='./app/frontend/templates')
# router = APIRouter(prefix='/msn-prj', tags=['msn-prj'], dependencies=[Depends(oauth2.get_current_user)])
router = APIRouter(prefix='/msn-prj', tags=['msn-prj'])


@router.get('', response_class=HTMLResponse)
async def retrieve(request: Request):
    user_info = token.get_token_userinfo(request)
    return templates.TemplateResponse('msn/msn_prj.html', {'request': request, 'user_info': user_info})

    # result = await msn_prj.retrieve(page)
    #
    # if result['isSuccess']:
    #     return templates.TemplateResponse('msn/msn_prj.html', {'request': request, 'overView': result['overView'], 'lst_prj': result['lst_prj'], 'page_sel': int(page), 'page_count': result['page_count'], 'user_info': user_info})
    # else:
    #     return templates.TemplateResponse('error.html', {
    #         'request': request,
    #         'strTask': 'Retrieve project error',
    #         'strErr': result['strErr']
    #     })


@router.get('/retrieve/page/{page}/kw/{kword}', response_class=JSONResponse)
async def retrieve_msn_prjs(request: Request, page: int = 1, kword: str = 'all'):
    # user_info = token.get_token_userinfo(request)
    try:
        result = await msn_prj.retrieve_search(page, kword)
        return {
            'lst_prj': result['lst_prj'],
            'page_count': result['page_count'],
            'page_sel': int(page),
            'kword': kword,
        }
    except Exception:
        return templates.TemplateResponse('error.html', {
            'request': request,
            'strTask': 'Retrieve/search project error',
            'strErr': traceback.format_exc()
        })


# @router.get('/search', response_class=HTMLResponse)
# async def search(request: Request, search_prj_name: str = ''):
#
#     user_info = token.get_token_userinfo(request)
#
#     if search_prj_name == '':
#         result = await msn_prj.retrieve(1)
#     else:
#         result = await msn_prj.search(search_prj_name)
#
#     if result['isSuccess']:
#         return templates.TemplateResponse('msn/msn_prj.html', {'request': request, 'overView': result['overView'], 'lst_prj': result['lst_prj'], 'search_prj_name': search_prj_name, 'page_sel': 1, 'page_count': result['page_count'], 'user_info': user_info})
#     else:
#         return templates.TemplateResponse('error.html', {
#             'request': request,
#             'strTask': 'Search project by name error',
#             'strErr': result['strErr']
#         })

# OLD
# @router.get('/add', response_class=HTMLResponse, dependencies=[Depends(oauth2.get_current_user)])
# async def prj_add(request: Request, internal_id, prj_name, categorical, prj_type, prj_status):
#
#     result = await msn_prj.add(internal_id, prj_name, categorical, prj_type, prj_status)
#
#     if result['isSuccess']:
#         redirect_url = request.url_for('retrieve')
#         return RedirectResponse(redirect_url, status_code=status.HTTP_303_SEE_OTHER)
#
#     else:
#         return templates.TemplateResponse('error.html', {
#             'request': request,
#             'strTask': 'Add project error',
#             'strErr': result['strErr']
#         })


# NEW
@router.get('/add', response_class=JSONResponse, dependencies=[Depends(oauth2.get_current_user)])
async def prj_add(request: Request, internal_id, prj_name, categorical, prj_type, prj_status):
    try:
        await msn_prj.add(internal_id, prj_name, categorical, prj_type, prj_status)
        return True
    except Exception:
        return templates.TemplateResponse('error.html', {
            'request': request,
            'strTask': 'Add project error',
            'strErr': traceback.format_exc()
        })


# OLD
# @router.get('/copy/{_id}', response_class=HTMLResponse, dependencies=[Depends(oauth2.get_current_user)])
# async def prj_copy(request: Request, _id):
#
#     result = await msn_prj.copy_prj(_id)
#
#     if result['isSuccess']:
#         redirect_url = request.url_for('retrieve')
#         return RedirectResponse(redirect_url, status_code=status.HTTP_303_SEE_OTHER)
#
#     else:
#         return templates.TemplateResponse('error.html', {
#             'request': request,
#             'strTask': 'Copy project error',
#             'strErr': result['strErr']
#         })


# OLD
# @router.get('/delete/{_id}', response_class=HTMLResponse, dependencies=[Depends(oauth2.get_current_user)])
# async def prj_delete_id(_id, request: Request):
#     email = token.verify_token(request.cookies['ap-login'])['email']
#
#     result = await msn_prj.delete(_id, email)
#
#     if result['isSuccess']:
#         redirect_url = request.url_for('retrieve')
#         return RedirectResponse(redirect_url, status_code=status.HTTP_303_SEE_OTHER)
#     else:
#         return templates.TemplateResponse('error.html', {
#             'request': request,
#             'strTask': 'Delete project error',
#             'strErr': result['strErr']
#         })


# NEW
@router.get('/copy/{_id}', response_class=JSONResponse, dependencies=[Depends(oauth2.get_current_user)])
async def prj_copy(request: Request, _id):
    try:
        await msn_prj.copy_prj(_id)
        return True
    except Exception:
        return templates.TemplateResponse('error.html', {
            'request': request,
            'strTask': 'Copy project error',
            'strErr': traceback.format_exc()
        })


# NEW
@router.get('/delete/{_id}', response_class=JSONResponse, dependencies=[Depends(oauth2.get_current_user)])
async def prj_delete_id(request: Request, _id):
    try:
        email = token.verify_token(request.cookies['ap-login'])['email']
        await msn_prj.delete(_id, email)
        return True
    except Exception:
        return templates.TemplateResponse('error.html', {
            'request': request,
            'strTask': 'Delete project error',
            'strErr': traceback.format_exc()
        })


@router.get('/{_id}/{tabname}', response_class=HTMLResponse)
async def retrieve_id(request: Request, _id, tabname: str = 'info'):

    user_info = token.get_token_userinfo(request)

    result = await msn_prj.retrieve_id(_id) if tabname != 'openend' else await msn_prj.retrieve_id(_id, True)

    if result['isSuccess']:
        return templates.TemplateResponse('msn/msn_prj_id.html', {'request': request, 'prj': result['prj'], 'user_info': user_info, 'tabname': tabname})
    else:
        return templates.TemplateResponse('error.html', {
            'request': request,
            'strTask': 'Retrieve project id error',
            'strErr': result['strErr']
        })


@router.get('/{_id}/{tabname}/wait_secs/{wait_secs}', response_class=HTMLResponse)
async def retrieve_id_wait_secs(request: Request, _id, tabname: str = 'data-export-topline', wait_secs: int = 0):

    user_info = token.get_token_userinfo(request)

    result = await msn_prj.retrieve_id(_id)

    if result['isSuccess']:
        return templates.TemplateResponse('msn/msn_prj_id.html', {'request': request, 'prj': result['prj'], 'user_info': user_info, 'tabname': tabname, 'wait_secs': wait_secs})
    else:
        return templates.TemplateResponse('error.html', {
            'request': request,
            'strTask': 'Retrieve project id error',
            'strErr': result['strErr']
        })


@router.post('/update/{_id}/{tabname}', response_class=RedirectResponse, dependencies=[Depends(oauth2.get_current_user)])
async def update_prj_data(request: Request, _id: str, tabname: str, strBody: str = Body(...)):
    result = await msn_prj.update_prj(_id, strBody)

    if result['isSuccess']:
        redirect_url = request.url_for('retrieve_id', **{'_id': _id, 'tabname': tabname})
        return RedirectResponse(redirect_url, status_code=status.HTTP_303_SEE_OTHER)
    else:
        return templates.TemplateResponse('error.html', {
            'request': request,
            'strTask': 'Update project data error',
            'strErr': result['strErr']
        })


@router.post('/data_upload/{_id}', response_class=RedirectResponse, dependencies=[Depends(oauth2.get_current_user)])
async def upload_prj_data(request: Request, _id: str, file_scr: UploadFile, file_main: UploadFile, file_plm: Optional[UploadFile] = None,
                          file_coding: Optional[UploadFile] = None, file_codelist: Optional[UploadFile] = None):

    result = await msn_prj.upload_prj_data(_id, file_scr, file_plm, file_main, file_coding, file_codelist)

    if result['isSuccess']:
        redirect_url = request.url_for('retrieve_id', **{'_id': _id, 'tabname': 'data-upload'})
        return RedirectResponse(redirect_url, status_code=status.HTTP_303_SEE_OTHER)
    else:
        return templates.TemplateResponse('error.html', {
            'request': request,
            'strTask': 'Upload project data error',
            'strErr': result['strErr']
        })


@router.post('/data_clear/{_id}', response_class=RedirectResponse, dependencies=[Depends(oauth2.get_current_user)])
async def clear_prj_data(request: Request, _id: str):

    result = await msn_prj.clear_prj_data(_id)

    if result['isSuccess']:
        redirect_url = request.url_for('retrieve_id', **{'_id': _id, 'tabname': 'data-upload'})
        return RedirectResponse(redirect_url, status_code=status.HTTP_303_SEE_OTHER)
    else:
        return templates.TemplateResponse('error.html', {
            'request': request,
            'strTask': 'Clear project data error',
            'strErr': result['strErr']
        })


# OLD
# @router.post('/data_export/{_id}', response_class=FileResponse)
# async def prj_data_export(request: Request, _id: str, export_section: str = Form(default=''), export_option: str = Form(default='')):
#
#     result = await msn_prj.data_export(_id, export_section, export_option)
#
#     if result['isSuccess']:
#
#         user_info = token.get_token_userinfo(request)
#         logger = Logger.logger(f"{user_info['name']}")
#
#         cleanup = CleanupFiles(lstFileName=[result['zipName']], logger=logger)
#         return FileResponse(result['zipName'], filename=result['zipName'], background=BackgroundTask(cleanup.cleanup))
#
#     else:
#         return templates.TemplateResponse('error.html', {
#             'request': request,
#             'strTask': 'Export data error',
#             'strErr': result['strErr']
#         })


# NEW
@router.post('/data_export/{_id}', response_class=RedirectResponse)
async def prj_data_export(request: Request, background_tasks: BackgroundTasks, _id: str, export_section: str = Form(default=''), export_option: str = Form(default='')):

    try:
        background_tasks.add_task(msn_prj.data_export, _id, export_section, export_option)
        redirect_url = request.url_for('retrieve_id_wait_secs', **{'_id': _id, 'tabname': 'data-export-raw', 'wait_secs': 1})
        return RedirectResponse(redirect_url, status_code=status.HTTP_303_SEE_OTHER)

    except Exception:
        return templates.TemplateResponse('error.html', {
            'request': request,
            'strTask': 'Export data error',
            'strErr': traceback.format_exc()
        })


@router.get('/data_export/{_id}/processing')
async def prj_data_export_processing(request: Request, _id: str):

    try:
        dict_rawdata_exporter = await msn_prj.get_rawdata_exporter_stt(_id)
        dict_rawdata_exporter = dict_rawdata_exporter['rawdata_exporter']

        time.sleep(1)

        return dict_rawdata_exporter

    except Exception:
        return templates.TemplateResponse('error.html', {
            'request': request,
            'strTask': 'Export data error',
            'strErr': traceback.format_exc()
        })


@router.get('/data_export/{_id}/download/{zipName}', response_class=FileResponse)
async def prj_data_export_download(request: Request, _id: str, zipName: str):
    try:
        user_info = token.get_token_userinfo(request)
        logger = Logger.logger(f"{user_info['name']}")
        await msn_prj.clear_rawdata_exporter_stt(_id)

        if not exists(zipName):
            redirect_url = request.url_for('retrieve_id_wait_secs', **{'_id': _id, 'tabname': 'data-export-raw', 'wait_secs': 0})
            return RedirectResponse(redirect_url, status_code=status.HTTP_303_SEE_OTHER)

        cleanup = CleanupFiles(lstFileName=[zipName], logger=logger)
        return FileResponse(zipName, filename=zipName, background=BackgroundTask(cleanup.cleanup))

    except Exception:
        return templates.TemplateResponse('error.html', {
            'request': request,
            'strTask': 'Download rawdata error',
            'strErr': traceback.format_exc()
        })


@router.post('/topline_process/{_id}', response_class=RedirectResponse)
async def prj_topline_process(request: Request, background_tasks: BackgroundTasks, _id: str, export_section: str = Form(default=''), export_sheets: list = Form(default='')):

    try:
        wait_secs = await msn_prj.get_proc_time(_id)

        background_tasks.add_task(msn_prj.topline_process, _id, export_section, export_sheets)

        redirect_url = request.url_for('retrieve_id_wait_secs', **{'_id': _id, 'tabname': 'data-process-topline', 'wait_secs': round(wait_secs)})
        return RedirectResponse(redirect_url, status_code=status.HTTP_303_SEE_OTHER)

    except Exception:
        return templates.TemplateResponse('error.html', {
            'request': request,
            'strTask': 'Process topline error',
            'strErr': traceback.format_exc()
        })


@router.post('/topline_export/{_id}/download', response_class=FileResponse)
async def prj_topline_download(request: Request, _id: str):

    try:
        result = await msn_prj.retrieve_id(_id)

        if result['isSuccess']:
            user_info = token.get_token_userinfo(request)
            logger = Logger.logger(f"{user_info['name']}")

            file_name = result['prj']['download_file_name']
            cleanup = CleanupFiles(lstFileName=[file_name], logger=logger)
            await msn_prj.clear_topline_info(_id)

            if file_name:
                return FileResponse(file_name, filename=file_name, background=BackgroundTask(cleanup.cleanup))
            else:
                return templates.TemplateResponse('error.html', {
                    'request': request,
                    'strTask': 'Download topline error',
                    'strErr': 'Please re-process the topline.'
                })

    except Exception:
        return templates.TemplateResponse('error.html', {
            'request': request,
            'strTask': 'Download topline error',
            'strErr': traceback.format_exc()
        })


@router.post('/topline_export/{_id}/download-clear', response_class=RedirectResponse)
async def prj_topline_download_clear(request: Request, _id: str):

    try:
        result = await msn_prj.retrieve_id(_id)

        if result['isSuccess']:
            user_info = token.get_token_userinfo(request)
            logger = Logger.logger(f"{user_info['name']}")

            file_name = result['prj']['download_file_name']
            cleanup = CleanupFiles(lstFileName=[file_name], logger=logger)
            await msn_prj.clear_topline_info(_id)

            redirect_url = request.url_for('retrieve_id', **{'_id': _id, 'tabname': 'data-process-topline'})
            return RedirectResponse(redirect_url, status_code=status.HTTP_303_SEE_OTHER, background=BackgroundTask(cleanup.cleanup))

    except Exception:
        return templates.TemplateResponse('error.html', {
            'request': request,
            'strTask': 'Download topline clear error',
            'strErr': traceback.format_exc()
        })


@router.get('/tl-bulkup-export/{_id}/{strWhich}', response_class=FileResponse)
async def tl_bulkup_export(request: Request, _id: str, strWhich: str):

    result = await msn_prj.topline_bulkup_export(_id, strWhich)

    if result['isSuccess']:

        user_info = token.get_token_userinfo(request)
        logger = Logger.logger(f"{user_info['name']}")

        cleanup = CleanupFiles(lstFileName=[result['csvName']], logger=logger)
        return FileResponse(result['csvName'], filename=result['csvName'], background=BackgroundTask(cleanup.cleanup))

    else:
        return templates.TemplateResponse('error.html', {
            'request': request,
            'strTask': 'Export topline error',
            'strErr': result['strErr']
        })


@router.post('/tl-side-auto-generate/{_id}', response_class=RedirectResponse, dependencies=[Depends(oauth2.get_current_user)])
async def tl_side_auto_generate(request: Request, _id: str):

    result = await msn_prj.topline_side_auto_generate(_id)

    if result['isSuccess']:
        redirect_url = request.url_for('retrieve_id', **{'_id': _id, 'tabname': 'topline'})
        return RedirectResponse(redirect_url, status_code=status.HTTP_303_SEE_OTHER)
    else:
        return templates.TemplateResponse('error.html', {
            'request': request,
            'strTask': 'Topline side-axis auto generate error',
            'strErr': result['strErr']
        })


@router.post('/openend/{_id}/create_codeframe', response_class=RedirectResponse, dependencies=[Depends(oauth2.get_current_user)])
async def create_codeframe(request: Request, _id: str, strBody: str = Body(...)):

    result = await msn_prj.create_codeframe(_id, strBody)

    if result['isSuccess']:
        redirect_url = request.url_for('retrieve_id', **{'_id': _id, 'tabname': 'openend'})
        return RedirectResponse(redirect_url, status_code=status.HTTP_303_SEE_OTHER)
    else:
        return templates.TemplateResponse('error.html', {
            'request': request,
            'strTask': 'Create codeframe error',
            'strErr': result['strErr']
        })


# START ON 05/07/2023
# NEW USING AJAX--------------------------------------------------------------------------------------------------------
@router.get('/{_id}', response_class=HTMLResponse)
async def retrieve_id_tab_all(request: Request, _id):
    try:

        user_info = token.get_token_userinfo(request)

        return templates.TemplateResponse('msn/msn_prj_id/all_tabs.html',
                                          {'request': request, 'prj': {'id': _id}, 'user_info': user_info})

    except Exception:
        print(traceback.format_exc())
        return {
            'strTask': 'Retrieve project id error',
            'strErr': traceback.format_exc()
        }


@router.get('/{_id}/tab/info', response_class=JSONResponse)
async def retrieve_id_tab_info(_id: str):
    try:
        if (msn_prj_info := await msn_prj.get_msn_prj_info(_id)) is not None:
            return msn_prj_info

        raise HTTPException(status_code=404, detail=f"Project {_id} not found")

    except Exception:
        print(traceback.format_exc())
        return {
            'strTask': 'Retrieve project id tab info error',
            'strErr': traceback.format_exc()
        }


@router.put('/{_id}/tab/info', response_class=JSONResponse, dependencies=[Depends(oauth2.get_current_user)])
async def update_id_tab_info(_id: str, model_msn_prj_info: Annotated[ModelMsnPrjInfo, Any]):
    try:
        if (msn_prj_info := await msn_prj.put_msn_prj_info(_id, model_msn_prj_info)) is not None:
            return msn_prj_info

        raise HTTPException(status_code=404, detail=f"Project {_id} not found")

    except Exception:
        return {
            'strTask': 'Update project id tab info error',
            'strErr': traceback.format_exc()
        }


@router.get('/{_id}/tab/sections', response_class=JSONResponse, dependencies=[Depends(oauth2.get_current_user)])
async def retrieve_id_tab_sections(_id: str):
    try:
        if (msn_prj_sections := await msn_prj.get_msn_prj_sections(_id)) is not None:
            return msn_prj_sections

        raise HTTPException(status_code=404, detail=f"Project {_id} not found")

    except Exception:
        print(traceback.format_exc())
        return {
            'strTask': 'Retrieve project id tab sections error',
            'strErr': traceback.format_exc()
        }


@router.put('/{_id}/tab/sections', response_class=JSONResponse, dependencies=[Depends(oauth2.get_current_user)])
async def update_id_tab_sections(_id: str, model_msn_prj_sections: Annotated[ModelMsnPrjSections, Any]):
    try:
        if (msn_prj_sections := await msn_prj.put_msn_prj_sections(_id, model_msn_prj_sections)) is not None:
            return msn_prj_sections

        raise HTTPException(status_code=404, detail=f"Project {_id} not found")

    except Exception:
        return {
            'strTask': 'Update project id tab sections error',
            'strErr': traceback.format_exc()
        }


@router.get('/{_id}/tab/structure/{tab_key}', response_class=JSONResponse, dependencies=[Depends(oauth2.get_current_user)])
async def retrieve_id_tab_structure(_id: str, tab_key: int):
    try:

        if (msn_prj_structure := await msn_prj.get_msn_prj_structure(_id, tab_key)) is not None:
            return msn_prj_structure

        raise HTTPException(status_code=404, detail=f"Project {_id} at {tab_key} not found")

    except Exception:
        print(traceback.format_exc())
        return {
            'strTask': 'Retrieve project id tab structure error',
            'strErr': traceback.format_exc()
        }






# ----------------------------------------------------------------------------------------------------------------------