from pyspark.sql.types import (
    IntegerType,
    BooleanType,
    DateType,
    DoubleType,
    StringType,
    TimestampType,
)

TYPE_MAP = {
    "int": IntegerType,
    "bool": BooleanType,
    "date": DateType,
    "float": DoubleType,
    "str": StringType,
    "datetime": TimestampType,
}
