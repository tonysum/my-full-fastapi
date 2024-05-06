from typing import Any, List
from fastapi import APIRouter, Depends, HTTPException
# from sqlalchemy import select
from datetime import datetime, timedelta
# from fastapi import File, UploadFile
# from fastapi.encoders import jsonable_encoder
# import pandas as pd
from app import stock_crud
from app.core.config import settings
from app.models import Message
# from app.utils import read_csv_random
# from utils import get_last_stock_daily_by_rownumber
# from app.core.celery_app import celery_app
# from app.core.tasks import celery_app, upload_daily_first
from app.api.deps import (
    SessionDep,
    # SourceSessionDep,
    # get_source_db,
    get_current_active_superuser,
)
# from app.utils import inspect_table
from app.base_models import StockDaily
# from app.models.stockbasic import StockBasic



FIELDS = ["ts_code", "trade_date", "open", "high", "low",
          "close", "pre_close", "change", "pct_chg", "vol", "amount"]

router = APIRouter()


@router.get(
    "/",
    dependencies=[Depends(get_current_active_superuser)],
    response_model=List[StockDaily],
)
def read_stock_daily(
    session: SessionDep,
    skip: int = 0,
    limit: int = 100
) -> List[StockDaily]:
    stocks = stock_crud.get_daily(db=session, skip=skip, limit=limit)
    return stocks


@router.get(
    "/{ts_code}",
    dependencies=[Depends(get_current_active_superuser)],
    response_model=List[StockDaily],
)
def read_stock_daily_by_ts_code(
    *,
    ts_code: str,
    session: SessionDep,
    start: str = '20230101',
    end: str = '20240531'
) -> Any:
    """
        Get  a stock by ts_code.
    """
    stock = stock_crud.get_by_ts_code(db=session, ts_code=ts_code)
    if not stock:
        raise HTTPException(status_code=404, detail="Stock not found")
    
    # 对日期是否合法进行判断
    try:
        datetime.strptime(start, '%Y%m%d')
        datetime.strptime(end, '%Y%m%d')
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format")
    
    # 对结束日期是否大于今天进行判断
    today = datetime.now().strftime('%Y%m%d')
    if end > today:
        raise HTTPException(
            status_code=400, 
            detail="End date cannot be greater than today")
    
    stock = stock_crud.get_daily_by_code(
        db=session, ts_code=ts_code, start=start, end=end)
    if not stock:
        raise HTTPException(status_code=404, detail="Stock not found")
    return stock


@router.post(
    "/",
    dependencies=[Depends(get_current_active_superuser)],
    response_model=StockDaily,
)
def create_stock_daily(
    *,
    StockDaily_in: StockDaily,
    session: SessionDep,
) -> Any:
    """
    Create a new stock daily data.
    """
    stock = stock_crud.get_by_ts_code(
        db=session, ts_code=StockDaily_in.ts_code)
    if stock is None:
        raise HTTPException(
            status_code=404,
            detail="The Stock doesn't exist in the system.",
        )
    # 验证 In data 完整性
    # result = crud.StockDaily.validator(StockDaily_in)
    # if result :
    stock = stock_crud.create_daily(db=session, obj_in=StockDaily_in)
    return stock


@router.post(
    "/bulk",
    dependencies=[Depends(get_current_active_superuser)],
    response_model=List[StockDaily],
)
def create_stock_daily_bulk(
    *,
    StockDaily_in: List[StockDaily],
    session: SessionDep,
) -> Any:
    """
    Get some bulk stock daily data.
    """
    stocks = []
    for stock in StockDaily_in:
        new_stock = stock_crud.create_daily(db=session, obj_in=stock)
        stocks.append(new_stock)
    # return stocks
        print(stock,stocks)
    return stocks


@router.delete(
    "/{ts_code}",
    dependencies=[Depends(get_current_active_superuser)],
    response_model=Message,
)
def delete_daily(
    *,
    ts_code: str,
    session: SessionDep
) -> Any:
    """
    Delete a stock daily data.
    """
    stock = stock_crud.get_daily_by_code(db=session, ts_code=ts_code)
    if not stock:
        raise HTTPException(status_code=404, detail="Stock daily data not found")
    
    stock_crud.remove_daily(db=session, ts_code=ts_code)
    return {"message": "Stock deleted successfully"}


@router.post(
    "/uploadStocksDailyTest",
    dependencies=[Depends(get_current_active_superuser)],
    response_model=Any,
)
def upload_stocks_test(
    session: SessionDep
    # start: str = None,
    # end: str = None,
    # offset: int = 0,
    # limit: int = 0,
) -> Any:
    """
    Upload stocks daily data from TuShare.
    """

    result = upload_daily_first.apply_async()

    return str(result)


@router.post(
    "/uploadStocksDaily_First",
    dependencies=[Depends(get_current_active_superuser)],
    response_model=Any,
)
def upload_stock_daily(
    session: SessionDep
    # start: str = None,
    # end: str = None,
    # offset: int = 0,
    # limit: int = 0,
) -> Any:
    """
    Upload stocks daily data from TuShare for First Time.
    """

    result = celery_app.send_task("app.worker.upload_daily_first")

    return str(result.id)


@router.post(
    "/uploadStocksDaily_ForNew",
    dependencies=[Depends(get_current_active_superuser)],
    response_model=Any,
)
def upload_stock_daily_for_new(
    session: SessionDep
    # start: str = None,
    # end: str = None,
    # offset: int = 0,
    # limit: int = 0,
) -> Any:
    """
    Upload stocks daily data from TuShare for increase.
    """
    # APIKey = crud.get_app_key("tushare")
    APIKey = settings.TUSHARE_APIKEY
    myTs = crud.Tushare(APIKey)

    end_date = crud.stockdaily.get_end_date()
    lastDaily = crud.stockdaily.get_last_daily(session)

    i = 0
    for row in lastDaily.iterrows():
        if i > 3:
            break
        ts_code = row[1].iloc[0]
        start_date = (datetime.strptime(
            row[1].iloc[1], '%Y%m%d')+timedelta(days=1)).strftime('%Y%m%d')
        # print(ts_code,start_date)
        data = myTs.get_daily(
            ticker=ts_code,
            start_date=start_date,
            end_date=end_date,
            offset=0,
            limit=0)
        if data.shape[0] > 0:
            result = crud.stockdaily.create_stock_daily_bulk(
                db=session, obj_bulk=data.to_dict(orient='records'))

        i += 1

    return result
