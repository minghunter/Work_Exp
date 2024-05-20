from app.classes.Logging_Custom_Formatter import Logger
from fastapi import FastAPI, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from app.routers.Others import BaeminCheck, Excel2Sav
from app.routers.Online_Survey import online_survey_data
from app.routers.MSN import MSN_Projects
from app.routers.Auth import authentication, token
from app.routers.User import Users
from app.routers.Charts import Charts
# from app.routers.ANLZ import ANLZ_Projects  # Dang deploy loi do _id
from app.routers.Price_Tracking import Price_Projects
import uvicorn


logger = Logger.logger('my-dp')

templates = Jinja2Templates(directory='app/frontend/templates')

app = FastAPI(docs_url=None)

app.include_router(Excel2Sav.router)
app.include_router(BaeminCheck.router)
app.include_router(MSN_Projects.router)
app.include_router(authentication.router)
app.include_router(Users.router)
app.include_router(Charts.router)
app.include_router(online_survey_data.router)
# app.include_router(ANLZ_Projects.router)  # Dang deploy loi do _id
app.include_router(Price_Projects.router)

app.mount('/static', StaticFiles(directory='app/frontend/static'), name='static')


@app.get('/', response_class=HTMLResponse)
async def home(request: Request):
    user_info = token.get_token_userinfo(request)
    return templates.TemplateResponse('home.html', {'request': request, 'user_info': user_info})


@app.get('/index', response_class=HTMLResponse)
async def index(request: Request):
    user_info = token.get_token_userinfo(request)
    return templates.TemplateResponse('home.html', {'request': request, 'user_info': user_info})


@app.get('/workload', response_class=HTMLResponse)
async def workload(request: Request):
    user_info = token.get_token_userinfo(request)
    return templates.TemplateResponse('others/workload.html', {'request': request, 'user_info': user_info})


# EXCEPTION_HANDLER-----------------------------------------------------------------------------------------------------
@app.exception_handler(status.HTTP_404_NOT_FOUND)
async def custom_404_handler(request: Request, _):
    logger.info('HTTP_404_NOT_FOUND')
    user_info = token.get_token_userinfo(request)
    return templates.TemplateResponse('404.html', {'request': request, 'user_info': user_info})


@app.exception_handler(status.HTTP_403_FORBIDDEN)
async def custom_403_handler(_, __):
    logger.info('HTTP_403_FORBIDDEN')
    return RedirectResponse('/login', status_code=status.HTTP_303_SEE_OTHER)


@app.exception_handler(status.HTTP_401_UNAUTHORIZED)
async def custom_401_handler(_, __):
    logger.info('HTTP_401_UNAUTHORIZED')
    return RedirectResponse('/login', status_code=status.HTTP_303_SEE_OTHER)


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000)