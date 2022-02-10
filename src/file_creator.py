import json
from datetime import datetime
from pathlib import Path

from src.input_schema import EXAMPLE_INPUT
from typeeval import TypeEval


def write_to_json(mb: int, _schema: list):
    start_time = datetime.now()
    size = mb * 1000000
    with open("./test.json", "w") as fh:
        fh.write("[\n")
        json.dump(TypeEval(_schema).give_line(), fh)
        while Path("test.json").stat().st_size < size:
            fh.write(",\n")
            json.dump(TypeEval(_schema).give_line(), fh)
        fh.write("]")
    finish_time = datetime.now()
    print(finish_time - start_time)


write_to_json(mb=500, _schema=EXAMPLE_INPUT)
