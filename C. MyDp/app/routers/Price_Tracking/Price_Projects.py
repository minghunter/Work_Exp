from fastapi import APIRouter, Request, status, Depends, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, FileResponse, JSONResponse
from fastapi.templating import Jinja2Templates
from starlette.background import BackgroundTask
from ..Auth import oauth2, token
from ...classes.CleanUpResponseFiles import CleanupFiles
from ...classes.Logging_Custom_Formatter import Logger
from .Price_Database import PriceDb
from .Price_Models import *


logger = Logger.logger('price-tracking')
price_db = PriceDb(logger)

credentials_exception = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail='Could not validate credentials',
    headers={'WWW-Authenticate': 'Bearer'},
)

templates = Jinja2Templates(directory='./app/frontend/templates')
router = APIRouter(prefix='/price-tracking', tags=['price-tracking'], dependencies=[Depends(oauth2.get_current_user)])


def price_error(**kwargs):
    return templates.TemplateResponse('price_tracking/price_error.html', kwargs)


@router.get('', response_class=HTMLResponse)
async def retrieve_price_prjs(request: Request, page: int = 1):

    user_info = token.get_token_userinfo(request)
    price_db.logger = Logger.logger(user_info.get('name'))
    result = await price_db.retrieve_prjs(page=page)

    if result['isSuccess']:
        return templates.TemplateResponse('price_tracking/all_prjs/price_all_prjs.html', {
            'request': request,
            'lst_prj': result['lst_prj'],
            'page_sel': int(page),
            'page_count': result['page_count'],
            'user_info': user_info
        })
    else:
        return price_error(
            request=request,
            user_info=user_info,
            strTask='Retrieve price tracking project error',
            strErr=result['strErr'],
            htmlErr=result['htmlErr'],
            back_url='/price-tracking'
        )


@router.get('/{_id}/tab/{tab_name}', response_class=HTMLResponse)
async def retrieve_price_prj_id(request: Request, _id: str, tab_name: str):

    user_info = token.get_token_userinfo(request)
    price_db.logger = Logger.logger(user_info.get('name'))

    result = await price_db.retrieve_prj_by_id(_id=_id, tab_name=tab_name)

    if result['isSuccess']:
        return templates.TemplateResponse('price_tracking/prj_by_id/price_prj.html', {
            'request': request,
            'prj': result['prj'],
            'user_info': user_info,
            'tab_name': tab_name,
            'lst_users': result['lst_users']
        })
    else:
        return price_error(
            request=request,
            user_info=user_info,
            strTask=f'Retrieve project ID {_id} fail',
            strErr=result['strErr'],
            htmlErr=result['htmlErr'],
            back_url='/price-tracking'
        )


@router.post('/{_id}/tab/info/update', response_class=HTMLResponse, dependencies=[Depends(oauth2.get_current_user)])
async def update_price_prj_info(request: Request, _id: str, prj_info: PricePrjInfo = Depends(PricePrjInfo.as_form)):

    user_info = token.get_token_userinfo(request)
    price_db.logger = Logger.logger(user_info.get('name'))

    result = await price_db.update_prj_info(_id, prj_info, user_info)

    if result['isSuccess']:
        redirect_url = request.url_for('retrieve_price_prj_id', **{'_id': _id, 'tab_name': 'info'})
        return RedirectResponse(redirect_url, status_code=status.HTTP_303_SEE_OTHER)
    else:
        return price_error(
            request=request,
            user_info=user_info,
            strTask=f'Update project ID {_id} data fail',
            strErr=result['strErr'],
            htmlErr=result['htmlErr'],
            back_url=f'/price-tracking/{_id}/tab/info'
        )


@router.post('/{_id}/tab/sku_info/upload', response_class=HTMLResponse, dependencies=[Depends(oauth2.get_current_user)])
async def upload_price_prj_sku_info(request: Request, _id: str, file_sku_info: PriceUploadSkuInfo = Depends(PriceUploadSkuInfo.as_form)):

    user_info = token.get_token_userinfo(request)
    price_db.logger = Logger.logger(user_info.get('name'))

    result = await price_db.upload_prj_sku_info(_id, user_info, file_sku_info)

    if result['isSuccess']:
        redirect_url = request.url_for('retrieve_price_prj_id', **{'_id': _id, 'tab_name': 'sku_info'})
        return RedirectResponse(redirect_url, status_code=status.HTTP_303_SEE_OTHER)
    else:
        return price_error(
            request=request,
            user_info=user_info,
            strTask=f'Project ID {_id} uploading SKU Info file fail',
            strErr=result['strErr'],
            htmlErr=result['htmlErr'],
            back_url=f'/price-tracking/{_id}/tab/sku_info'
        )


@router.get('/{_id}/tab/sku_info/display', response_class=HTMLResponse, dependencies=[Depends(oauth2.get_current_user)])
async def get_price_prj_sku_info(request: Request, _id: str):
    user_info = token.get_token_userinfo(request)
    price_db.logger = Logger.logger(user_info.get('name'))
    return await price_db.get_sku_info(_id, user_info, is_export_df=False)


@router.post('/{_id}/tab/sku_data/upload', response_class=HTMLResponse, dependencies=[Depends(oauth2.get_current_user)])
async def upload_price_prj_sku_data(request: Request, _id: str, sku_data_upload: PriceUploadSkuDataExt = Depends(PriceUploadSkuDataExt.as_form)):

    user_info = token.get_token_userinfo(request)
    price_db.logger = Logger.logger(user_info.get('name'))

    result = await price_db.upload_prj_sku_data(_id, user_info, sku_data_upload)

    if result['isSuccess']:
        redirect_url = request.url_for('retrieve_price_prj_id', **{'_id': _id, 'tab_name': 'sku_data'})
        return RedirectResponse(redirect_url, status_code=status.HTTP_303_SEE_OTHER)
    else:
        return price_error(
            request=request,
            user_info=user_info,
            strTask=f'Project ID {_id} uploading SKU data file fail',
            strErr=result['strErr'],
            htmlErr=result['htmlErr'],
            back_url=f'/price-tracking/{_id}/tab/sku_data'
        )


@router.get('/{_id}/tab/sku_data/errors/{week}', response_class=HTMLResponse, dependencies=[Depends(oauth2.get_current_user)])
async def get_price_prj_sku_data_errors(request: Request, _id: str, week: int):
    user_info = token.get_token_userinfo(request)
    price_db.logger = Logger.logger(user_info.get('name'))
    return await price_db.get_sku_data_errors(_id, user_info, week)


@router.post('/{_id}/tab/processing/export/data', response_class=FileResponse, dependencies=[Depends(oauth2.get_current_user)])
async def export_price_prj_data(request: Request, _id: str, price_export_data: PriceExportData = Depends(PriceExportData.as_form)):

    user_info = token.get_token_userinfo(request)
    price_db.logger = Logger.logger(user_info.get('name'))

    result = await price_db.export_prj_data(_id, user_info, price_export_data)

    if result['isSuccess']:
        cleanup = CleanupFiles(lstFileName=[result['file_name']], logger=price_db.logger)
        return FileResponse(result['file_name'], filename=result['file_name'], background=BackgroundTask(cleanup.cleanup))
    else:
        return price_error(
            request=request,
            user_info=user_info,
            strTask=f'Project ID {_id} exporting data file fail',
            strErr=result['strErr'],
            htmlErr=result['htmlErr'],
            back_url=f'/price-tracking/{_id}/tab/processing'
        )




@router.get('/{_id}/tab/dashboard/get_sku_info', response_class=JSONResponse, dependencies=[Depends(oauth2.get_current_user)])
async def get_price_prj_sku_info_dashboard(request: Request, _id: str):

    user_info = token.get_token_userinfo(request)
    price_db.logger = Logger.logger(user_info.get('name'))

    dict_sku_info = await price_db.get_sku_info_to_dict(_id, user_info, is_export_df=True, is_check_permission=False)
    dict_week = await price_db.get_week(_id, user_info, is_check_permission=False)

    return {
        'sku_info': dict_sku_info,
        'week': dict_week
    }





@router.post('/{_id}/tab/dashboard/display/tables', response_class=JSONResponse, dependencies=[Depends(oauth2.get_current_user)])
async def display_price_prj_dashboard_tables(request: Request, _id: str, price_dashboard_input: Annotated[PriceDashboardInput, Any]):

    user_info = token.get_token_userinfo(request)
    price_db.logger = Logger.logger(user_info.get('name'))

    result = await price_db.get_sku_data_dashboard(_id, user_info, price_dashboard_input)

    return result