import tushare as ts
import pandas as pd
from time import sleep
from fastapi import HTTPException
from app.core.config import settings

API_KEY = settings.TUSHARE_APIKEY

class Tushare():
    def __init__(self, ApiKey: str = API_KEY):
        self.pro = ts.pro_api(ApiKey)
        print(self.pro)

    def get_stock_basic(self):
        stock_basic_tushare = None
        fields = ["ts_code", "symbol", "name", "area", "industry",
                  "fullname", "enname", "cnspell", "market", "exchange",
                  "curr_type", "list_status", "list_date", "delist_date",
                  "is_hs", "act_name", "act_ent_type"]
        try:
            stock_basic_tushare = self.pro.stock_basic(
                exchange='', list_status='L', fields=fields)
        except Exception as e:
            raise HTTPException(status_code=503, detail=str(e))
            
        return stock_basic_tushare

    def get_daily(
            self,
            ticker,
            start_date,
            end_date,
            offset,
            limit,
            retry_count=2,
            pause=2):

        df = pd.DataFrame()
        for _ in range(retry_count):
            try:
                df = self.pro.daily(**{
                    "ts_code": ticker,
                    "trade_date": "",
                    "start_date": start_date,
                    "end_date": end_date,
                    "offset": offset,
                    "limit": limit
                }, fields=", ".join([
                    "ts_code",
                    "trade_date",
                    "open",
                    "high",
                    "low",
                    "close",
                    "pre_close",
                    "change",
                    "pct_chg",
                    "vol",
                    "amount"
                ]))
                sleep(1)
            except Exception as e:
                print("Error:", e)
                sleep(pause)
        return df

mytushare = Tushare()