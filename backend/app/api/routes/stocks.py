from typing import Any, List
from fastapi import APIRouter, Depends, HTTPException
from fastapi import File, UploadFile
from app import stock_crud 

from app.api.deps import (
    SessionDep,
    get_current_active_superuser,
)


from app.models import Message
    
from app.base_models import StockBasic

router = APIRouter()


@router.get(
    "/",
    dependencies=[Depends(get_current_active_superuser)],
    response_model=List[StockBasic],
)
def read_stocks(
    session: SessionDep,
    skip: int = 0,
    limit: int = 100
) -> List[StockBasic]:
    """
        Get all stocks.
    """

    stocks = stock_crud.get_by_ts_code(db=session, skip=skip, limit=limit)
    return stocks


@router.get(
    "/{ts_code}",
    dependencies=[Depends(get_current_active_superuser)],
    response_model=StockBasic,
)
def read_stock(*, ts_code: str, session: SessionDep) -> StockBasic:
    """
        Get  a stock by ts_code.
    """

    stock = stock_crud.get_by_ts_code(db=session, ts_code=ts_code)
    if not stock:
        raise HTTPException(status_code=404, detail="Stock not found")
    return stock


@router.post(
    "/",
    dependencies=[Depends(get_current_active_superuser)],
    response_model=StockBasic,
)
def create_stock(
    *,
    stockbasic_in: StockBasic,
    session: SessionDep,
) -> Any:
    """
        Create a new stock.
    """
    stock = None
    stock = stock_crud.get_by_ts_code(
        db=session, ts_code=stockbasic_in.ts_code)
    if stock:
        raise HTTPException(
            status_code=400,
            detail="The stock  already exists in the system.",
        )
    stock = stock_crud.create_stock(db=session, obj_in=stockbasic_in)
    return stock


@router.put(
    "/{ts_code}",
    dependencies=[Depends(get_current_active_superuser)],
    response_model=StockBasic,
)
def update_stock(
    *,
    stockbasic_in: StockBasic,
    session: SessionDep,
    ts_code: str
) -> Any:
    """
        Update a stock.TODO: Fix this, need to 判断stockbasic_in的值是否有效,例如不能使用"string"来更新字段
    """
    stock = stock_crud.get_by_ts_code(db=session, ts_code=ts_code)
    if not stock:
        raise HTTPException(status_code=404, detail="Stock not found")
    stock = stock_crud.update_stock(
        db=session, db_stock=stock, stock_in=stockbasic_in)
    return stock


@router.delete(
    "/{ts_code}",
    dependencies=[Depends(get_current_active_superuser)],
    response_model=Message,
)
def delete_stock(
    *,
    ts_code: str,
    session: SessionDep
) -> Any:
    """
        Delete a stock.
    """
    stock = stock_crud.get_by_ts_code(db=session, ts_code=ts_code)
    if not stock:
        raise HTTPException(status_code=404, detail="Stock not found")
    stock_crud.remove_stock(db=session, ts_code=ts_code)
    return {"message": "Stock deleted successfully"}


@router.post(
    "/ImportStockBasicFromTushare",
    dependencies=[Depends(get_current_active_superuser)],

)
def importStockBasic(session: SessionDep) -> Any:
    """
        Import stock basic data from tushare.
    """
    result = stock_crud.import_stock_basic(
        db=session)
    
    return result


@router.post(
    "/ImportStockBasicFromTushareByPandas",
    dependencies=[Depends(get_current_active_superuser)],

)
def importStockBasicByPandas(
    session: SessionDep
) -> Any:
    """
        Import stock basic data from tushare.
    """
    result = stock_crud.import_stock_basic_by_pandas(
        db=session)
    
    return result


@router.post(
    "/ImportStockBasicFromCSV",
    dependencies=[Depends(get_current_active_superuser)],

)
def import_stock_basic_from_csv(
    session: SessionDep,
    FileName: UploadFile = File(...)
) -> Any:
    """
        Import stock basic data from local CSV file.
    """
    if FileName is None:
        raise HTTPException(status_code=404, detail="File not found")
    stocks = stock_crud.import_stock_basic_from_csv(
        db=session, file=FileName)
    return stocks
