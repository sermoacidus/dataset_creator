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
    {
        "col_name": "is_valid",
        "type": "bool",
        "nulls_weight": 5,
    },
    {
        "col_name": "price",
        "type": "float",
        "range": (4.5, 35.45),
        "signs": 2,
        "nulls_weight": 5,
        "join_keys_and_weight": {99.6: 15, 100.5: 15},
    },
]
