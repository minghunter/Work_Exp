from fastapi import Form, UploadFile
from pydantic import BaseModel
from typing import Annotated, Any



class PricePrjInfo(BaseModel):
    internal_id: str
    name: str
    categorical: str
    type: str
    status: str
    owner: str
    current_week: int


    @classmethod
    def as_form(
        cls,
        internal_id: str = Form(...),
        name: str = Form(...),
        categorical: str = Form(...),
        type: str = Form(...),
        status: str = Form(...),
        owner: str = Form(...),
        current_week: int = Form(...)
    ):

        return cls(
            internal_id=internal_id,
            name=name,
            categorical=categorical,
            type=type,
            status=status,
            owner=owner,
            current_week=current_week
        )



class PriceUploadSkuInfo(BaseModel):
    file_sku_info: UploadFile = Form(...)


    @classmethod
    def as_form(cls, file_sku_info: UploadFile = Form(...)):
        return cls(file_sku_info=file_sku_info)



class PriceUploadSkuDataExt(BaseModel):
    is_qme: bool
    is_bhx: bool
    week: int
    upload_file: UploadFile


    @classmethod
    def as_form(
            cls,
            is_qme: bool = Form(...),
            is_bhx: bool = Form(...),
            week: int = Form(...),
            upload_file: UploadFile = Form(...)
    ):
        return cls(
            is_qme=is_qme,
            is_bhx=is_bhx,
            week=week,
            upload_file=upload_file
        )


class PriceExportData(BaseModel):
    is_to_client: Annotated[bool, Form()]
    is_to_ggdrive: Annotated[bool, Form()]
    week: Annotated[int, Form()]


    @classmethod
    def as_form(
            cls,
            is_to_client: bool = Form(...),
            is_to_ggdrive: bool = Form(...),
            week: int = Form(...)
    ):
        return cls(
            is_to_client=is_to_client,
            is_to_ggdrive=is_to_ggdrive,
            week=week
        )



class PriceDashboardInput(BaseModel):
    lst_sku: Annotated[list, Form()]
    lst_week: Annotated[list[int], Form()]



    @classmethod
    def as_form(
            cls,
            lst_sku: list = Form(...),
            lst_week: list = Form(...),
    ):


        return cls(
            lst_sku=lst_sku,
            lst_week=lst_week,
        )

# HERE