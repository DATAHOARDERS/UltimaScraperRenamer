import asyncio

import ultima_scraper_api
from ultima_scraper_collection.managers.database_manager.connections.sqlite.models import (
    user_database,
)
from ultima_scraper_collection.managers.database_manager.database_manager import (
    DatabaseManager,
)
from ultima_scraper_collection.managers.datascraper_manager.datascraper_manager import (
    DataScraperManager,
)

import setup
from ultima_scraper_renamer import renamer

if __name__ == "__main__":

    async def main():
        # WORK IN PROGRESS
        import ultima_scraper_api.helpers.main_helper as main_helper

        config = setup.start()
        config_path = config.ultima_scraper_directory.joinpath(
            "__settings__/config.json"
        )
        us_config, _updated = main_helper.get_config(config_path)
        datascraper_manager = DataScraperManager()

        api = ultima_scraper_api.select_api("onlyfans", us_config)
        datascraper = datascraper_manager.select_datascraper(api)
        site_settings = us_config.supported.onlyfans.settings
        auth = api.add_auth()
        authed = await auth.login(guest=True)
        site_settings.metadata_directories = [
            config.ultima_scraper_directory.joinpath(metadata_directory)
            if not metadata_directory.is_absolute()
            else metadata_directory
            for metadata_directory in site_settings.metadata_directories
        ]
        datascraper.filesystem_manager.activate_directory_manager(datascraper.api)
        # api.filesystem_manager.activate_directory_manager(api)
        for metadata_directory in site_settings.metadata_directories:
            for sites_directory in metadata_directory.iterdir():
                if sites_directory.stem != "OnlyFans":
                    continue
                # In the future, the script must allow users to choose which folder to resolve
                for user_directory in sites_directory.iterdir():
                    folder_identifier = user_directory.stem
                    success_string = f"Renamed {folder_identifier}'s files"
                    failure_string = f"Failed to rename {folder_identifier}'s files"
                    print(f"Renaming {user_directory}'s files")
                    db_path = user_directory.joinpath("Metadata", "user_data.db")
                    resolved = False
                    if db_path.exists():
                        db_manager = DatabaseManager().get_sqlite_db(db_path)
                        database_session = db_manager.session
                        content_types = authed.api.ContentTypes()
                        for api_type, _ in content_types:
                            if resolved:
                                break
                            if api_type != "Posts":
                                continue
                            db_table = user_database.table_picker(api_type)
                            db_posts = database_session.query(db_table).order_by(db_table.created_at.asc()).all()  # type: ignore
                            for db_post in db_posts:
                                user = await authed.resolve_user(
                                    post_id=db_post.post_id
                                )
                                if user:
                                    directory_manager = await datascraper.filesystem_manager.create_directory_manager(
                                        datascraper.api, user
                                    )
                                    await datascraper.filesystem_manager.format_directories(
                                        user
                                    )
                                    if directory_manager:
                                        _metadata = await renamer.start(
                                            user,
                                            directory_manager,
                                            "Posts",
                                            db_manager.session_factory,
                                        )
                                        resolved = True
                                    break
                                pass
                        print(f"{success_string if resolved else failure_string}")
                    pass
                pass
        pass
        pass

    asyncio.run(main())

    # Enhance the resolver:
    # # Can subscribe to FREE users to avoid seeing "Post Not Found" since some models hide their wall posts
    # # Can assume the folder identifier is the model's username
