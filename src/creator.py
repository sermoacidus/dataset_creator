import json
import time
from dataclasses import dataclass
from random import randint, choices, choice, randrange
import logging
import datetime
import socket
from pyspark.sql.functions import *
from pyspark.sql.types import *




from pyspark.sql import SparkSession

logging.basicConfig(level=logging.DEBUG)

EXAMPLE_INPUT = [
    {
        "col_name": "ID",
        "type": "int",
        "range": (100, 500),
        "nulls_weight": 5,
        "join_keys_and_weight": {2: 15, 4: 15},
    },
    {
        "col_name": "name",
        "type": "str",
        "str_len": 10,
        "nulls_weight": 5,
        "email": True,
        "join_keys_and_weight": {"Hello": 15, "im a join key": 15},
    },
    {
        "col_name": "date",
        "type": "date",
        "range": ("31.12.1990", "31.12.2024"),
        "date_format": "%m-%d-%Y",
        "nulls_weight": 5,
        "join_keys_and_weight": {"01-01-2022": 15, "01-01-2021": 15},
    },
]


@dataclass
class Creator:
    input: list
    output_format: str
    rows: int

    @staticmethod
    def percentage_check(prc: int, join_key: dict):
        overall_join_key_weight = 0
        if join_key:
            for weight in join_key.values():
                overall_join_key_weight += weight
        if prc + overall_join_key_weight > 100:
            logging.warning(
                f"Overall weight of {join_key} and nulls - {prc} more then 100"
            )

    @staticmethod
    def _eval_int(rng: tuple, prc: int, join_key: dict) -> int:
        Creator.percentage_check(prc, join_key)
        population = [randint(*rng), None]
        r_weight = [100 - prc, prc]
        if join_key:
            for value, weight in join_key.items():
                population.append(value)
                r_weight[0] -= weight
                r_weight.append(weight)
        return choices(population, r_weight)[0]

    @staticmethod
    def _eval_str(str_len: int, prc: int, email: bool, join_key: dict) -> str:
        Creator.percentage_check(prc, join_key)
        random_word = "".join(choice(chr(randint(97, 122))) for _ in range(str_len))
        if email:
            random_word += "@gmail.com"
        population = [random_word, None]
        r_weight = [100 - prc, prc]
        if join_key:
            for value, weight in join_key.items():
                population.append(value)
                r_weight[0] -= weight
                r_weight.append(weight)
        return choices(population, r_weight)[0]

    @staticmethod
    def _eval_date(rng: tuple, prc: int, date_format: str, join_key: dict) -> str:
        Creator.percentage_check(prc, join_key)
        start_date = datetime.date(
            int(rng[0].split(".")[2]),
            int(rng[0].split(".")[1]),
            int(rng[0].split(".")[0]),
        )
        end_date = datetime.date(
            int(rng[1].split(".")[2]),
            int(rng[1].split(".")[1]),
            int(rng[1].split(".")[0]),
        )
        rand_days_between_dates = randrange((end_date - start_date).days)
        random_date = (
            start_date + datetime.timedelta(days=rand_days_between_dates)
        ).strftime(date_format)
        population = [random_date, None]
        r_weight = [100 - prc, prc]
        if join_key:
            for value, weight in join_key.items():
                population.append(value)
                r_weight[0] -= weight
                r_weight.append(weight)
        return choices(population, r_weight)[0]

    def evaluate(self, column: dict):
        if column["type"] == "int":
            return self._eval_int(
                rng=column.get("range") or (-1000000, 1000000),
                prc=column.get("nulls_weight") or 0,
                join_key=column.get("join_keys_and_weight"),
            )
        elif column["type"] == "str":
            return self._eval_str(
                str_len=column.get("str_len") or 5,
                prc=column.get("nulls_weight") or 0,
                email=column.get("email"),
                join_key=column.get("join_keys_and_weight"),
            )
        elif column["type"] == "date":
            return self._eval_date(
                rng=column.get("range"),
                prc=column.get("nulls_weight") or 0,
                date_format=column.get("date_format") or "%m-%d-%Y",
                join_key=column.get("join_keys_and_weight"),
            )
        else:
            raise AttributeError("No types are set for a field")

    def give_one_line(self):
        line = {}
        for column in self.input:
            line[column["col_name"]] = self.evaluate(column)
        json_data = json.dumps(line)

        return json_data

    @staticmethod
    def session_creator():
        return SparkSession \
            .builder \
            .appName("parquet_creator") \
            .getOrCreate()

    def send_to_socket(self):
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect(("127.0.0.1", 9090))
        for _ in range(self.rows):
            client_socket.sendall(bytes(self.give_one_line(), encoding="utf-8"))
            # client_socket.send("smth".encode())

    def write_to_console(self):
        spark = self.session_creator()
        lines = spark \
            .readStream \
            .format("socket") \
            .option("host", "127.0.0.1") \
            .option("port", 9090) \
            .load()




        schema = (
            StructType()
                .add("ID", IntegerType())
                .add("name", StringType())
                .add("date", DateType())

        )
        time.sleep(10)
        self.send_to_socket()


        query = lines.select(col("value")) \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .start()

        time.sleep(10)
        self.send_to_socket()

        query.awaitTermination()

    def write_data(self, format_: str):
        if format_ == "parquet":
            self.write_to_console()

    def execute(self):
        self.write_data(format_=self.output_format)

Creator(EXAMPLE_INPUT, "parquet", 10000).execute()
