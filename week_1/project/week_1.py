import csv
from datetime import datetime
from typing import Iterator, List

from dagster import In, Nothing, Out, String, job, op, usable_as_dagster_type
from pydantic import BaseModel


@usable_as_dagster_type(description="Stock data")
class Stock(BaseModel):
    date: datetime
    close: float
    volume: int
    open: float
    high: float
    low: float

    @classmethod
    def from_list(cls, input_list: List[List]):
        """Do not worry about this class method for now"""
        return cls(
            date=datetime.strptime(input_list[0], "%Y/%m/%d"),
            close=float(input_list[1]),
            volume=int(float(input_list[2])),
            open=float(input_list[3]),
            high=float(input_list[4]),
            low=float(input_list[5]),
        )


@usable_as_dagster_type(description="Aggregation of stock data")
class Aggregation(BaseModel):
    date: datetime
    high: float


def csv_helper(file_name: str) -> Iterator[Stock]:
    with open(file_name) as csvfile:
        reader = csv.reader(csvfile)
        for row in reader:
            yield Stock.from_list(row)


@op(config_schema={"s3_key": String})
def get_s3_data(context):
    s3_key = context.op_config["s3_key"]
    stock_list = csv_helper(s3_key)

    return list(stock_list)

@op
def process_data(context,stocks: list):
    max_stock = stocks[0]
    for stock in stocks[1:]:
        if stock.high > max_stock.high:
            max_stock = stock
    
    # print(max_stock.date,max_stock.high)
    
    return Aggregation(date=max_stock.date, high=max_stock.high)



@op
def put_redis_data(context,agg: Aggregation):
    pass

default_config = {"ops": {"get_s3_data": {"config": {"s3_key": "week_1/data/stock.csv"}}}}

@job(config=default_config)
def week_1_pipeline():
    # get_data = get_s3_data()
    put_redis_data(process_data(get_s3_data()))