from fastapi import APIRouter, Request, WebSocket
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from ..Auth import token
from ..MSN.MSN_Database import MsnPrj



msn_prj = MsnPrj()

templates = Jinja2Templates(directory='./app/frontend/templates')
router = APIRouter(prefix='/charts', tags=['charts'])


@router.get('', response_class=HTMLResponse)
async def load_charts(request: Request):
    user_info = token.get_token_userinfo(request)

    result = await msn_prj.get_overView()

    if result['isSuccess']:
        return templates.TemplateResponse('others/charts.html', {'request': request, 'overView': result['overView'], 'user_info': user_info})
    else:
        return templates.TemplateResponse('error.html', {
            'request': request,
            'strTask': 'Charting error',
            'strErr': result['strErr']
        })


@router.websocket("/mess_ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    while True:
        data = await websocket.receive_text()
        await websocket.send_text(f"Message text was: {data}")








