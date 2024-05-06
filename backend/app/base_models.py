from sqlmodel import Field,  SQLModel # type: ignore
from typing import Union

class StockBasic(SQLModel, table=True):
    ts_code: str = Field(primary_key=True)  # 使用 Field 来代替 mapped_column
    symbol: str = Field(unique=True)
    name: str
    area: Union[str, None] = None
    industry: Union[str, None] = None
    fullname: Union[str, None] = None
    enname: Union[str, None] = None
    cnspell: Union[str, None] = None
    market: Union[str, None] = None
    exchange: Union[str, None] = None
    curr_type: Union[str, None] = None
    list_status: Union[str, None] = None
    # 对于日期字段，我们可以使用 datetime 类型，但这里保持字符串类型
    list_date: Union[str, None] = None  
    delist_date: Union[str, None] = None  # Union 可以用来表示多种类型
    is_hs: Union[str, None] = None
    act_name: Union[str, None] = None
    act_ent_type: Union[str, None] = None
    
    
class StockDaily(SQLModel, table=True):
    ts_code: str = Field(primary_key=True, index=True)
    trade_date: str = Field(primary_key=True, index=True)
    open: Union[float, None] = None
    high: Union[float, None] = None
    low: Union[float, None] = None
    close: Union[float, None] = None
    pre_close: Union[float, None] = None
    change: Union[float, None] = None
    pct_chg: Union[float, None] = None
    vol: Union[float, None] = None
    amount: Union[float, None] = None