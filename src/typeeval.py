from dataclasses import dataclass
from random import randint, choices, choice, randrange, uniform
import logging
import datetime

logging.basicConfig(level=logging.DEBUG)


@dataclass
class TypeEval:
    input: list

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
    def choice_func(seed, prc: int, join: dict = None):
        population = [seed, None]
        r_weight = [100 - prc, prc]
        if join:
            for value, weight in join.items():
                population.append(value)
                r_weight[0] -= weight
                r_weight.append(weight)
        return choices(population, r_weight)[0]

    @staticmethod
    def _eval_int(rng: tuple, prc: int, join_key: dict) -> int:
        TypeEval.percentage_check(prc, join_key)
        return TypeEval.choice_func(seed=randint(*rng), join=join_key, prc=prc)

    @staticmethod
    def _eval_float(rng: tuple, prc: int, join_key: dict, signs: int) -> int:
        TypeEval.percentage_check(prc, join_key)
        return TypeEval.choice_func(
            seed=f"{uniform(*rng):.{signs}f}", join=join_key, prc=prc
        )

    @staticmethod
    def _eval_str(str_len: int, prc: int, email: bool, join_key: dict) -> str:
        TypeEval.percentage_check(prc, join_key)
        random_word = "".join(choice(chr(randint(97, 122))) for _ in range(str_len))
        if email:
            random_word += "@gmail.com"
        return TypeEval.choice_func(seed=random_word, join=join_key, prc=prc)

    @staticmethod
    def _eval_date(rng: tuple, prc: int, _format: str, join_key: dict) -> str:
        TypeEval.percentage_check(prc, join_key)
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
        ).strftime(_format)
        return TypeEval.choice_func(seed=random_date, join=join_key, prc=prc)

    @staticmethod
    def _eval_bool(prc: int):
        return TypeEval.choice_func(seed=choice([True, False]), prc=prc)

    def evaluate(self, field: dict):
        if field["type"] == "int":
            return self._eval_int(
                rng=field.get("range") or (-1000000, 1000000),
                prc=field.get("nulls_weight") or 0,
                join_key=field.get("join_keys_and_weight"),
            )
        elif field["type"] == "str":
            return self._eval_str(
                str_len=field.get("str_len") or 5,
                prc=field.get("nulls_weight") or 0,
                email=field.get("email"),
                join_key=field.get("join_keys_and_weight"),
            )
        elif field["type"] == "date":
            return self._eval_date(
                rng=field.get("range"),
                prc=field.get("nulls_weight") or 0,
                _format=field.get("date_format") or "%m-%d-%Y",
                join_key=field.get("join_keys_and_weight"),
            )
        elif field["type"] == "bool":
            return self._eval_bool(
                prc=field.get("nulls_weight") or 0,
            )
        elif field["type"] == "float":
            return self._eval_float(
                rng=field.get("range") or (-1000000, 1000000),
                prc=field.get("nulls_weight") or 0,
                signs=field.get("signs") or 2,
                join_key=field.get("join_keys_and_weight"),
            )

        else:
            raise AttributeError("No types are set for a field")

    def give_line(self):
        line = {}
        for entry in self.input:
            line[entry["col_name"]] = self.evaluate(entry)
        return line
