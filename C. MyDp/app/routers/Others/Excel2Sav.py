from app.classes.Logging_Custom_Formatter import Logger
from fastapi import APIRouter, Request, UploadFile, HTTPException, status, Form
from fastapi.responses import HTMLResponse
from starlette.responses import FileResponse
from starlette.background import BackgroundTask
from fastapi.templating import Jinja2Templates
from app.classes.AP_DataConverter import APDataConverter
from app.classes.CleanUpResponseFiles import CleanupFiles
from app.routers.Auth import token
import traceback



credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail='Could not validate credentials',
        headers={'WWW-Authenticate': 'Bearer'},
    )


templates = Jinja2Templates(directory='./app/frontend/templates')
router = APIRouter(prefix='/convert-sav', tags=['convert-sav'])


@router.get('', response_class=HTMLResponse)
async def load_xlsx(request: Request):
    user_info = token.get_token_userinfo(request)

    return templates.TemplateResponse('others/load_xlsx.html', {'request': request, 'user_info': user_info, 'strTask': 'Convert to SAV'})


@router.post('', response_class=FileResponse)
async def convert_sav(files: list[UploadFile], request: Request, is_md: str = Form(default="True")):
    user_info = token.get_token_userinfo(request)

    logger = Logger.logger(f"{user_info['name']}")

    try:
        is_md = eval(is_md)

        apCvt = APDataConverter(files, logger)

        lst_dup_vars = apCvt.check_duplicate_variables()

        if lst_dup_vars:
            print('Duplicate variables:', lst_dup_vars)
            return templates.TemplateResponse('others/load_xlsx.html', {
                'request': request,
                'user_info': user_info,
                'strTask': 'Convert to SAV',
                'strErr': f'Duplicate variables: {lst_dup_vars}'
            })

        apCvt.convert_to_sav(is_md=is_md)

        cleanup = CleanupFiles(lstFileName=[apCvt.zip_name], logger=logger)
        return FileResponse(apCvt.zip_name, filename=apCvt.zip_name, background=BackgroundTask(cleanup.cleanup))

    except Exception:
        print(traceback.format_exc())
        return templates.TemplateResponse('others/load_xlsx.html', {
            'request': request,
            'user_info': user_info,
            'strTask': 'Convert to SAV',
            'strErr': traceback.format_exc()
        })




