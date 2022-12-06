from pathlib import Path
import orjson
from usr_settings import Settings
def start():
    config_file = Path(".settings","config.json")
    if not config_file.exists():
        ultima_scraper_directory = Path(input(f"""Enter UltimaScraper's root directory e.g. "generic folder/UltimaScraper":\n"""))
        if ultima_scraper_directory.exists():
            config = Settings(ultima_scraper_directory)
            config.export(config_file)
            return config
        else:
            raise Exception("UltimaScraper directory not found")
    else:
        return Settings(*orjson.loads( config_file.read_bytes()).values())