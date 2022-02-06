# BinaryType – Binary data.
# BooleanType – Boolean values.
# DateType – A datetime value.
# DoubleType – A floating-point double value.
# IntegerType – An integer value.
# LongType – A long integer value.
# NullType – A null value.
# ShortType – A short integer value.
# StringType – A text string.
# TimestampType – A timestamp value (typically in seconds from 1/1/1970).
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
