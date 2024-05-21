from fastapi import APIRouter, Request, UploadFile, Body, status, Depends, Form, BackgroundTasks, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, PlainTextResponse, FileResponse
from fastapi.templating import Jinja2Templates
# from starlette.responses import FileResponse
from starlette.background import BackgroundTask
from typing import Optional, Annotated, Union
from ..Auth import oauth2, token
from ...classes.CleanUpResponseFiles import CleanupFiles
from ...classes.Logging_Custom_Formatter import Logger
from .ANLZ_Database import AnlzDb
from .ANLZ_Models import *
from datetime import datetime
import traceback


logger = Logger.logger('anlz')
anlz_db = AnlzDb(logger)

credentials_exception = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail='Could not validate credentials',
    headers={'WWW-Authenticate': 'Bearer'},
)

templates = Jinja2Templates(directory='./app/frontend/templates')
router = APIRouter(prefix='/anlz', tags=['analyze-data'], dependencies=[Depends(oauth2.get_current_user)])


def anlz_error(**kwargs):
    return templates.TemplateResponse('anlz/anlz_error.html', kwargs)


@router.get('', response_class=HTMLResponse)
async def retrieve_anlz_prjs(request: Request, page: int = 1):

    user_info = token.get_token_userinfo(request)
    anlz_db.logger = Logger.logger(user_info.get('name'))
    result = await anlz_db.retrieve_prjs(page=page)

    if result['isSuccess']:
        return templates.TemplateResponse('anlz/all_prjs/anlz_all_prjs.html', {
            'request': request,
            'lst_prj': result['lst_prj'],
            'page_sel': int(page),
            'page_count': result['page_count'],
            'user_info': user_info
        })
    else:
        return anlz_error(
            request=request,
            user_info=user_info,
            strTask='Retrieve project error',
            strErr=result['strErr'],
            back_url='/anlz'
        )


@router.get('/search', response_class=HTMLResponse)
async def search_anlz_prjs(request: Request, anlz_search_value: str = ''):

    user_info = token.get_token_userinfo(request)
    anlz_db.logger = Logger.logger(user_info.get('name'))

    if anlz_search_value:
        result = await anlz_db.search_prjs(str_key_search=anlz_search_value)
    else:
        result = await anlz_db.retrieve_prjs(page=1)

    if result['isSuccess']:
        return templates.TemplateResponse('anlz/all_prjs/anlz_all_prjs.html', {
            'request': request,
            'lst_prj': result['lst_prj'],
            'anlz_search_value': anlz_search_value,
            'page_sel': 1,
            'page_count': result['page_count'],
            'user_info': user_info
        })
    else:
        return anlz_error(
            request=request,
            user_info=user_info,
            strTask='Search project by name error',
            strErr=result['strErr'],
            back_url='/anlz'
        )


@router.post('/add', response_class=HTMLResponse, dependencies=[Depends(oauth2.get_current_user)])
async def add_anlz_prj(request: Request, prj_info: AnlzPrjInfo = Depends()):

    user_info = token.get_token_userinfo(request)
    anlz_db.logger = Logger.logger(user_info.get('name'))

    result = await anlz_db.add_prj(prj_info, user_info)

    if result['isSuccess']:
        redirect_url = request.url_for('retrieve_anlz_prjs')
        return RedirectResponse(redirect_url, status_code=status.HTTP_303_SEE_OTHER)

    else:
        return anlz_error(
            request=request,
            user_info=user_info,
            strTask='Add project error',
            strErr=result['strErr'],
            back_url='/anlz'
        )


@router.post('/delete', response_class=HTMLResponse, dependencies=[Depends(oauth2.get_current_user)])
async def prj_delete_id(request: Request, prj_id: Annotated[str, Form()]):

    user_info = token.get_token_userinfo(request)
    anlz_db.logger = Logger.logger(user_info.get('name'))

    result = await anlz_db.delete_prj(prj_id, user_info)

    if result['isSuccess']:
        redirect_url = request.url_for('retrieve_anlz_prjs')
        return RedirectResponse(redirect_url, status_code=status.HTTP_303_SEE_OTHER)
    else:
        return anlz_error(
            request=request,
            user_info=user_info,
            strTask=f'Delete project ID {prj_id} error',
            strErr=result['strErr'],
            back_url='/anlz'
        )


@router.post('/copy', response_class=HTMLResponse, dependencies=[Depends(oauth2.get_current_user)])
async def prj_copy(request: Request, prj_id: Annotated[str, Form()]):

    user_info = token.get_token_userinfo(request)
    anlz_db.logger = Logger.logger(user_info.get('name'))

    result = await anlz_db.copy_prj(prj_id, user_info)

    if result['isSuccess']:
        redirect_url = request.url_for('retrieve_anlz_prjs')
        return RedirectResponse(redirect_url, status_code=status.HTTP_303_SEE_OTHER)

    else:
        return anlz_error(
            request=request,
            user_info=user_info,
            strTask=f'Copy project ID {prj_id} error',
            strErr=result['strErr'],
            back_url='/anlz'
        )


@router.get('/{_id}/tab/{tab_name}', response_class=HTMLResponse)
async def retrieve_anlz_prj_id(request: Request, _id: str, tab_name: str):

    user_info = token.get_token_userinfo(request)
    anlz_db.logger = Logger.logger(user_info.get('name'))
    result = await anlz_db.retrieve_prj_by_id(_id=_id, tab_name=tab_name)

    if result['isSuccess']:
        return templates.TemplateResponse('anlz/prj_by_id/anlz_prj.html', {
            'request': request,
            'prj': result['prj'],
            'user_info': user_info,
            'tab_name': tab_name,
            'lst_users': result['lst_users']
        })
    else:
        return anlz_error(
            request=request,
            user_info=user_info,
            strTask=f'Retrieve project ID {_id} fail',
            strErr=result['strErr'],
            back_url='/anlz'
        )


@router.get('/{_id}/tab/pyscript/{tab_name}', response_class=HTMLResponse)
async def retrieve_anlz_prj_id_pyscript(request: Request, _id: str, tab_name: str):

    user_info = token.get_token_userinfo(request)
    anlz_db.logger = Logger.logger(user_info.get('name'))

    full_tab_name = f"pyscript_{tab_name}"
    result = await anlz_db.retrieve_prj_by_id(_id=_id, tab_name=full_tab_name)

    if result['isSuccess']:
        return templates.TemplateResponse('anlz/prj_by_id/anlz_prj.html', {
            'request': request,
            'prj': result['prj'],
            'user_info': user_info,
            'tab_name': f'pyscript/{tab_name}',
            'lst_users': result['lst_users']
        })
    else:
        return anlz_error(
            request=request,
            user_info=user_info,
            strTask=f'Retrieve project ID {_id} fail',
            strErr=result['strErr'],
            back_url=f'/anlz/{_id}/tab/info'
        )


@router.post('/{_id}/tab/info/update', response_class=HTMLResponse, dependencies=[Depends(oauth2.get_current_user)])
async def update_anlz_prj_info(request: Request, _id: str, prj_info: AnlzPrjInfo = Depends()):

    user_info = token.get_token_userinfo(request)
    anlz_db.logger = Logger.logger(user_info.get('name'))

    result = await anlz_db.update_prj_info(_id, prj_info, user_info)

    if result['isSuccess']:
        redirect_url = request.url_for('retrieve_anlz_prj_id', **{'_id': _id, 'tab_name': 'info'})
        return RedirectResponse(redirect_url, status_code=status.HTTP_303_SEE_OTHER)
    else:
        return anlz_error(
            request=request,
            user_info=user_info,
            strTask=f'Update project ID {_id} data fail',
            strErr=result['strErr'],
            back_url=f'/anlz/{_id}/tab/info'
        )


@router.post('/{_id}/tab/pyscript/{tab_name}/update', response_class=HTMLResponse, dependencies=[Depends(oauth2.get_current_user)])
async def update_anlz_prj_pyscript(request: Request, _id: str, tab_name: str, txt_py_script: Annotated[str, Form()]):

    user_info = token.get_token_userinfo(request)
    anlz_db.logger = Logger.logger(user_info.get('name'))

    result = await anlz_db.update_prj_pyscript(_id, tab_name, txt_py_script, user_info)

    if result['isSuccess']:
        redirect_url = request.url_for('retrieve_anlz_prj_id_pyscript', **{'_id': _id, 'tab_name': tab_name})
        return RedirectResponse(redirect_url, status_code=status.HTTP_303_SEE_OTHER)
    else:
        return anlz_error(
            request=request,
            user_info=user_info,
            strTask=f'Update project ID {_id} data fail',
            strErr=result['strErr'],
            back_url=f'/anlz/{_id}/tab/pyscript/{tab_name}'
        )


@router.post('/{_id}/tab/dtables/update', response_class=HTMLResponse, dependencies=[Depends(oauth2.get_current_user)])
async def update_anlz_prj_dtables(request: Request, _id: str, lst_dtables: Annotated[str, Form()]):

    user_info = token.get_token_userinfo(request)
    anlz_db.logger = Logger.logger(user_info.get('name'))

    result = await anlz_db.update_prj_dtables(_id, lst_dtables, user_info)

    if result['isSuccess']:
        redirect_url = request.url_for('retrieve_anlz_prj_id', **{'_id': _id, 'tab_name': 'dtables'})
        return RedirectResponse(redirect_url, status_code=status.HTTP_303_SEE_OTHER)
    else:
        return anlz_error(
            request=request,
            user_info=user_info,
            strTask=f'Update project ID {_id} data fail',
            strErr=result['strErr'],
            back_url=f'/anlz/{_id}/tab/dtables'
        )


@router.post('/{_id}/tab/rawdata/upload', response_class=HTMLResponse, dependencies=[Depends(oauth2.get_current_user)])
async def upload_anlz_prj_rawdata(request: Request, _id: str, upload_files: AnlzUploadRawdataFiles = Depends()):

    user_info = token.get_token_userinfo(request)
    anlz_db.logger = Logger.logger(user_info.get('name'))

    result = await anlz_db.upload_prj_rawdata(_id, user_info, upload_files)

    if result['isSuccess']:
        redirect_url = request.url_for('retrieve_anlz_prj_id', **{'_id': _id, 'tab_name': 'rawdata'})
        return RedirectResponse(redirect_url, status_code=status.HTTP_303_SEE_OTHER)
    else:
        return anlz_error(
            request=request,
            user_info=user_info,
            strTask=f'Project ID {_id} uploading rawdata fail',
            strErr=result['strErr'],
            back_url=f'/anlz/{_id}/tab/rawdata'
        )


@router.post('/{_id}/tab/rawdata/delete', response_class=HTMLResponse, dependencies=[Depends(oauth2.get_current_user)])
async def delete_anlz_prj_rawdata(request: Request, _id: str):

    user_info = token.get_token_userinfo(request)
    anlz_db.logger = Logger.logger(user_info.get('name'))

    result = await anlz_db.delete_prj_rawdata(_id, user_info)

    if result['isSuccess']:
        redirect_url = request.url_for('retrieve_anlz_prj_id', **{'_id': _id, 'tab_name': 'rawdata'})
        return RedirectResponse(redirect_url, status_code=status.HTTP_303_SEE_OTHER)
    else:
        return anlz_error(
            request=request,
            user_info=user_info,
            strTask=f'Project ID {_id} deleting rawdata fail',
            strErr=result['strErr'],
            back_url=f'/anlz/{_id}/tab/rawdata'
        )


@router.get('/{_id}/tab/rawdata/download', response_class=FileResponse, dependencies=[Depends(oauth2.get_current_user)])
async def download_anlz_prj_rawdata(request: Request, _id: str):

    user_info = token.get_token_userinfo(request)
    anlz_db.logger = Logger.logger(user_info.get('name'))

    result = await anlz_db.download_prj_rawdata(_id, user_info)

    if result['isSuccess']:
        cleanup = CleanupFiles(lstFileName=[result['file_name']], logger=anlz_db.logger)
        return FileResponse(result['file_name'], filename=result['file_name'], background=BackgroundTask(cleanup.cleanup))
    else:
        return anlz_error(
            request=request,
            user_info=user_info,
            strTask=f'Project ID {_id} downloading rawdata fail',
            strErr=result['strErr'],
            back_url=f'/anlz/{_id}/tab/rawdata'
        )


@router.get('/{_id}/tab/pyscript/{tab_name}/run', response_class=HTMLResponse, dependencies=[Depends(oauth2.get_current_user)])
async def run_anlz_prj_pyscript(request: Request, background_tasks: BackgroundTasks, _id: str, tab_name: str):

    user_info = token.get_token_userinfo(request)
    anlz_db.logger = Logger.logger(user_info.get('name'))

    background_tasks.add_task(anlz_db.run_prj_pyscript, _id, tab_name, user_info)

    redirect_url = request.url_for('retrieve_anlz_prj_id', **{'_id': _id, 'tab_name': 'logging'})
    return RedirectResponse(redirect_url, status_code=status.HTTP_303_SEE_OTHER)



@router.get('/{_id}/log', response_model=AnlzLogging, response_model_exclude_unset=True)
async def anlz_prj_log(request: Request, _id: str):
    user_info = token.get_token_userinfo(request)
    anlz_db.logger = Logger.logger(user_info.get('name'))
    log = await anlz_db.get_prj_log(_id)
    return AnlzLogging(is_running=log['is_running'], lst_logging=log['lst_logging'])