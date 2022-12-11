import copy
from pathlib import Path

import orjson


class Settings:
    def __init__(self, ultima_scraper_directory: Path) -> None:
        self.ultima_scraper_directory = Path(ultima_scraper_directory)

    def export(self, config_file: Path):
        obj = copy.copy(self)
        obj.ultima_scraper_directory = obj.ultima_scraper_directory.as_posix()
        config_file.parent.mkdir(exist_ok=True)
        with config_file.open(mode="wb") as io:
            io.write(
                orjson.dumps(
                    obj.__dict__, option=orjson.OPT_INDENT_2 | orjson.OPT_APPEND_NEWLINE
                )
            )
