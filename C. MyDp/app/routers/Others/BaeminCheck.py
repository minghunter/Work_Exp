from fastapi import APIRouter, Request, UploadFile, HTTPException, status
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from starlette.responses import FileResponse
from starlette.background import BackgroundTask
from app.classes.Baemin_Checking import BaeminCheck
from app.classes.CleanUpResponseFiles import CleanupFiles
from app.routers.Auth import token
import traceback


credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail='Could not validate credentials',
        headers={'WWW-Authenticate': 'Bearer'},
    )


templates = Jinja2Templates(directory='./app/frontend/templates')
router = APIRouter(prefix='/baemin-check', tags=['baemin-check'])


@router.get('', response_class=HTMLResponse)
async def load_baemin_xlsx(request: Request):
    user_info = token.get_token_userinfo(request)

    return templates.TemplateResponse('others/baemin_check.html', {'request': request, 'user_info': user_info, 'strTask': 'Baemin checking'})


@router.post('', response_class=HTMLResponse)
async def baemin_checking(file: UploadFile, request: Request):
    user_info = token.get_token_userinfo(request)

    try:
        bmc = BaeminCheck()
        bmc.load(file)
        bmc.check()

        cleanup = CleanupFiles(lstFileName=[bmc.strFileName])

        return FileResponse(bmc.strFileName, filename=bmc.strFileName, background=BackgroundTask(cleanup.cleanup))

    except Exception:
        print(traceback.format_exc())

        return templates.TemplateResponse('others/baemin_check.html', {
            'request': request,
            'user_info': user_info,
            'strTask': 'Baemin checking',
            'strErr': traceback.format_exc()
        })