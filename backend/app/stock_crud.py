from typing import Any, Dict, Optional, Union, List
# from app.db import engine
# from sqlalchemy import select, asc, text, delete
from sqlmodel import Session, select, asc, text, delete
# from sqlalchemy.orm import Session
import pandas as pd
from datetime import datetime, timezone
from fastapi import HTTPException



from app.core.config import settings
from app.core.tushare import mytushare as Tushare

from app.base_models import StockBasic, StockDaily

from app import stock_crud as crud
from app.core.config import settings

# import warnings
# warnings.simplefilter('error')

FIELDS = [
    'ts_code',
    'symbol',
    'name',
    'area',
    'industry',
    'fullname',
    'enname',
    'cnspell',
    'market',
    'exchange',
    'curr_type',
    'list_status',
    'list_date',
    'delist_date',
    'is_hs',
    'act_name',
    'act_ent_type'
]

def create_stock(*, db: Session, obj_in: StockBasic) -> StockBasic:
        db_obj = StockBasic(
            ts_code=obj_in.ts_code,
            symbol=obj_in.symbol,
            name=obj_in.name,
            area=obj_in.area,
            industry=obj_in.industry,
            fullname=obj_in.fullname,
            enname=obj_in.enname,
            cnspell=obj_in.cnspell,
            market=obj_in.market,
            exchange=obj_in.exchange,
            curr_type=obj_in.curr_type,
            list_status=obj_in.list_status,
            list_date=obj_in.list_date,
            delist_date=obj_in.delist_date,
            is_hs=obj_in.is_hs,
            act_name=obj_in.act_name,
            act_ent_type=obj_in.act_ent_type,

        )
        db.add(db_obj)
        db.commit()
        db.refresh(db_obj)
        return db_obj


def update_stock(
    *, 
    db: Session,
    db_stock: StockBasic,
    stock_in: StockBasic
) -> StockBasic:
    # TODO:在用用model_dump的时候，会出现错误
    
    # try:
    stock_data = stock_in.model_dump(exclude_unset=True)
    # except Exception as e:
        # raise HTTPException(
            # status_code=503, detail=str(e))
    # if isinstance(stock_in, dict):
    #     stock_data = stock_in
    # else:
    #     stock_data = stock_in.dict(exclude_unset=True)
    
    db_stock.sqlmodel_update(stock_data)
    db.add(db_stock)
    db.commit()
    db.refresh(db_stock)
    return db_stock
    
def get_by_ts_code(
    *,
    db: Session,
    ts_code: Optional[str] = None,
    skip: Optional[int] = 0,
    limit: Optional[int] = 0,
) -> Optional[StockBasic]:
    # TODO: Fix this, it's statement is old
    if ts_code is not None:
        statement = select(StockBasic).where(
            StockBasic.ts_code == ts_code)
        result = db.exec(statement).first()
    else:
        if skip == 0:
            skip = None
        if limit == 0:
            limit = None
        statement = select(StockBasic).offset(skip).limit(
            limit).order_by(asc(StockBasic.ts_code))
        result = db.exec(statement).all()
    return result


def remove_stock(*, db: Session,  ts_code: str) -> StockBasic:
    statement = select(StockBasic).where(StockBasic.ts_code == ts_code)
    db_obj = db.execute(statement).scalar()

    db.delete(db_obj)
    db.commit()
    return db_obj

def verify_columns(
        *,
        local: StockBasic,
        remote:  pd.DataFrame) -> Any:
    local_columns = set([c.name for c in local.__table__.columns])
    remote_columns = set(remote.columns)
    not_in_local = remote_columns - local_columns
    not_in_remote = local_columns - remote_columns
    return {"not_in_local": not_in_local, "not_in_remote": not_in_remote}

def import_stock_basic_by_pandas(
        db: Session
) -> Any:

    stmt = text("select count(ts_code)  from stockbasic")
    innerCount = db.execute(stmt).scalar()

    try:
        df_tushare = Tushare.get_stock_basic()
    except Exception as e:
        raise HTTPException(
            status_code=503, detail=str(e))
    
    if df_tushare is None:
        return {"error": "No data from tushare"}
    
    to_file = "tu_share"+str(datetime.now(timezone.utc))+".csv"
    df_tushare.to_csv(to_file, index=False)

    verifyColumns = verify_columns(
        local=StockBasic, remote=df_tushare)
    if not verifyColumns:
        return {"error": "Columns not in local DB"}
    else:
        print("Columns verified")
    outerCount = df_tushare.shape[0]
    stmt = delete(StockBasic)
    db.execute(stmt)
    db.commit()
    result = df_tushare.to_sql(
        'stockbasic', db.bind, if_exists='append', index=False)
    return {"In the db stock basic's numbers:": innerCount,
            "In the tushare stock basic's numbers:": outerCount,
            "Updated:": result}


def import_stock_basic(db: Session) -> Any:

    statement = select(StockBasic)
    stocks = db.execute(statement).scalars().all()

    try:
        df_tushare = Tushare.get_stock_basic()
    except Exception as e:
        raise HTTPException(
            status_code=503, detail=str(e))

    df_tushare = df_tushare.fillna("")
    
    to_file = "tu_share"+str(datetime.now(timezone.utc))+".csv"
    df_tushare.to_csv(to_file, index=False)

    verifyColumns = verify_columns(
        local=StockBasic, remote=df_tushare)
    if not verifyColumns:
        return {"error": "Columns not in local DB"}
    else:
        print("Columns verified")

    for row in df_tushare.iterrows():
        ts_code = row[1].iloc[0]

        stock = get_by_ts_code(db=db, ts_code=ts_code)
        if stock is None:
            stock = StockBasic(
                ts_code=ts_code,
                symbol=row[1].iloc[1],
                name=row[1].iloc[2],
                area=row[1].iloc[3],
                industry=row[1].iloc[4],
                fullname=row[1].iloc[5],
                enname=row[1].iloc[6],
                cnspell=row[1].iloc[7],
                market=row[1].iloc[8],
                exchange=row[1].iloc[9],
                curr_type=row[1].iloc[10],
                list_status=row[1].iloc[11],
                list_date=row[1].iloc[12],
                delist_date=row[1].iloc[13],
                is_hs=row[1].iloc[14],
                act_name=row[1].iloc[15],
                act_ent_type=row[1].iloc[16],
            )
            db.add(stock)
            db.commit()
            db.refresh(stock)
        else:
            data_in=StockBasic(
                ts_code=ts_code,
                symbol=row[1].iloc[1],
                name=row[1].iloc[2],
                area=row[1].iloc[3],
                industry=row[1].iloc[4],
                fullname=row[1].iloc[5],
                enname=row[1].iloc[6],
                cnspell=row[1].iloc[7],
                market=row[1].iloc[8],
                exchange=row[1].iloc[9],
                curr_type=row[1].iloc[10],
                list_status=row[1].iloc[11],
                list_date=row[1].iloc[12],
                delist_date=row[1].iloc[13],
                is_hs=row[1].iloc[14],
                act_name=row[1].iloc[15],
                act_ent_type=row[1].iloc[16],
            )
            stock = update_stock(
                db=db, db_stock=stock, stock_in=data_in)

    stocks = db.execute(statement).scalars().all()
    result = {
        "df_tushare records": df_tushare.shape[0],
        "stocks in local DB": stocks.__len__(),
    }

    return result

def import_stock_basic_from_csv(
        *,
        db: Session,
        file: Any
) -> Any:
    from io import BytesIO

    contents = file.file.read()

    stream = BytesIO(contents)
    df = pd.read_csv(stream, encoding="utf-8", dtype={1: str,12:str})
    df = df.fillna("")
    verifyColumns = verify_columns(local=StockBasic, remote=df)

    if not verifyColumns:
        return {"error": "Columns not in local DB"}
    else:
        print("Columns verified")

    for row in df.iterrows():
        ts_code = row[1].iloc[0]
        stock = get_by_ts_code(db=db, ts_code=ts_code)

        if stock is None:
            stock = StockBasic(
                ts_code=ts_code,
                symbol=row[1].iloc[1],
                name=row[1].iloc[2],
                area=row[1].iloc[3],
                industry=row[1].iloc[4],
                fullname=row[1].iloc[5],
                enname=row[1].iloc[6],
                cnspell=row[1].iloc[7],
                market=row[1].iloc[8],
                exchange=row[1].iloc[9],
                curr_type=row[1].iloc[10],
                list_status=row[1].iloc[11],
                list_date=row[1].iloc[12],
                delist_date=row[1].iloc[13],
                is_hs=row[1].iloc[14],
                act_name=row[1].iloc[15],
                act_ent_type=row[1].iloc[16],
            )
            db.add(stock)
            db.commit()
            db.refresh(stock)
        else:
            data_in = StockBasic(
                ts_code=ts_code,
                symbol=str(row[1].iloc[1]),
                name=row[1].iloc[2],
                area=row[1].iloc[3],
                industry=row[1].iloc[4],
                fullname=row[1].iloc[5],
                enname=row[1].iloc[6],
                cnspell=row[1].iloc[7],
                market=row[1].iloc[8],
                exchange=row[1].iloc[9],
                curr_type=row[1].iloc[10],
                list_status=row[1].iloc[11],
                list_date=row[1].iloc[12],
                delist_date=row[1].iloc[13],
                is_hs=row[1].iloc[14],
                act_name=row[1].iloc[15],
                act_ent_type=row[1].iloc[16],
            )
            result = update_stock(
                db=db, db_stock=stock, stock_in=data_in)

    statement = select(StockBasic)
    stocks = db.execute(statement).scalars().all()
    # TODO:这个返回结果不真实
    result = {
        "TODO": "这个返回结果不真实",
        "csv file rows": df.shape[0],
        "updated stocks": stocks.__len__()
    }

    return result

# -----above stockbasic--------

def get_daily(
    *,
    db: Session,
    skip: Optional[int] = 0,
    limit: Optional[int] = 100,
) -> Optional[StockDaily]:
    statement = select(StockDaily).offset(skip).limit(limit)
    result = db.exec(statement).all()
    if len(result)==0:
        raise HTTPException(status_code=404, detail="No data in the database.")
    return result

def get_daily_by_code(
    *,
    db: Session,
    ts_code: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
) -> List[StockDaily]:
    # TODO: Fix this, it's statement is old
    if start is None and end is None:
        statement = select(StockDaily).where(StockDaily.ts_code == ts_code)
    
    if start is not None and end is not None:
        statement = select(StockDaily).\
            where(StockDaily.ts_code == ts_code)\
                .where(StockDaily.trade_date >= start)\
                    .where(StockDaily.trade_date < end)
    
    if start is not None and end is None:
        statement = select(StockDaily).\
            where(StockDaily.ts_code == ts_code)\
                .where(StockDaily.trade_date >= start)
                
    if start is None and end is not None:
        statement = select(StockDaily).\
            where(StockDaily.ts_code == ts_code)\
                .where(StockDaily.trade_date < end)

    result = db.exec(statement).all()
    if len(result)==0:
        raise HTTPException(status_code=404, detail="No data in the database.")
    return result


def create_daily(*, db: Session, obj_in: StockDaily) -> StockDaily:
    db_obj = StockDaily(
        ts_code=obj_in.ts_code,
        trade_date=obj_in.trade_date,
        open=obj_in.open,
        high=obj_in.high,
        low=obj_in.low,
        close=obj_in.close,
        pre_close=obj_in.pre_close,
        change=obj_in.change,
        pct_chg=obj_in.pct_chg,
        vol=obj_in.vol,
        amount=obj_in.amount,
    )
    db.add(db_obj)
    try:
        db.commit()
    except Exception as e:
        raise HTTPException(
            status_code=503, detail=str(e))
        
    db.refresh(db_obj)
    return db_obj

def create_stock_daily_bulk(
    *,
    db: Session,
    obj_bulk: List[StockDaily]
) -> Any:

    for obj_in in obj_bulk:
        db_obj = StockDaily(
            ts_code=obj_in['ts_code'],
            trade_date=obj_in['trade_date'],
            open=obj_in['open'],
            high=obj_in['high'],
            low=obj_in['low'],
            close=obj_in['close'],
            pre_close=obj_in['pre_close'],
            change=obj_in['change'],
            pct_chg=obj_in['pct_chg'],
            vol=obj_in['vol'],
            amount=obj_in['amount'],
        )
        db.add(db_obj)

    db.commit()

    if db_obj is not None:
        db.refresh(db_obj)

    return "Bulk bulk bulk data upload!!!"


def remove_daily(*, db: Session, ts_code: str) -> StockDaily:
    statement = select(StockDaily).where(StockDaily.ts_code == ts_code)
    db_obj = db.execute(statement).scalars().all()
    for obj in db_obj:
        # print(obj)
        db.delete(obj)
    db.commit()
    return db_obj

def verify_daily_columns(
        *,
        local: StockDaily,
        remote:  pd.DataFrame) -> Any:
    local_columns = set([c.name for c in local.__table__.columns])
    remote_columns = set(remote.columns)
    not_in_local = remote_columns - local_columns
    not_in_remote = local_columns - remote_columns
    return {"not_in_local": not_in_local, "not_in_remote": not_in_remote}


def update_daily(
    *, 
    db: Session,
    db_daily: StockDaily,
    daily_in: Union[StockDaily, Dict[str, Any]]
) -> StockDaily:
    daily_data = daily_in.model_dump(exclude_unset=True)
    db_daily.sqlmodel_update(daily_data)
    db.add(db_daily)
    db.commit()
    db.refresh(db_daily)
    return db_daily




#     def get_end_date(self):
#         from datetime import datetime, timedelta
#         result = datetime.now()
#         if result.hour < 18:
#             result = result+timedelta(days=-1)
#             result = result.strftime('%Y%m%d')
#         else:
#             result = result.strftime('%Y%m%d')
#         return result

#     def get_last_daily(
#         self,
#         db: Session
#     ) -> pd.DataFrame:

#         stmt = select(StockBasic.ts_code)
#         ts_codes = db.execute(stmt).scalars().all()
#         stmt = "select ts_code, MAX(trade_date) as last_date from stockdaily group by ts_code"
#         df = pd.read_sql(stmt, db.bind)
#         result = df[df['ts_code'].isin(ts_codes)]
#         end_date = self.get_end_date()
#         result = df[df['ts_code'].isin(ts_codes)]
#         idx = result[result['last_date'] == end_date].index.to_list()
#         result = result.drop(idx)

#         return result

