from datetime import datetime, timedelta
from typing import Optional
from jose import JWTError, jwt
from fastapi import HTTPException, status



SECRET_KEY = '09d25e094faa6ca2556c818166b7a9563b93f7099f6f0f4caa6cf63b88e8d3e7'
ALGORITHM = 'HS256'
ACCESS_TOKEN_EXPIRE_MINUTES = 300


credentials_exception = HTTPException(
    status_code=status.HTTP_401_UNAUTHORIZED,
    detail='Could not validate credentials',
    headers={'WWW-Authenticate': 'Bearer'},
)


def create_access_token(data: dict, expires_delta: Optional[timedelta] = None):
    to_encode = data.copy()

    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=30)

    to_encode.update({'exp': expire})

    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

    return encoded_jwt


def verify_token(token: str):

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])

        email: str = payload.get('sub')

        if email is None:
            raise credentials_exception

        token_data = {
            'email': email,
            'name': payload.get('name'),
            'role': payload.get('role')
        }

        return token_data

    except JWTError:
        raise credentials_exception


def get_token_userinfo(request):
    if 'ap-login' in request.cookies.keys():
        if request.cookies['ap-login']:
            return {
                'name': verify_token(request.cookies['ap-login'])['name'],
                'role': verify_token(request.cookies['ap-login'])['role']
            }

    return {
        'name': 'Guest',
        'role': 'Guest'
    }


