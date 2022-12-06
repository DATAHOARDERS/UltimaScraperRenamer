import asyncio
from ultima_scraper_renamer import renamer
import ultima_scraper_api
from ultima_scraper_api.database.db_manager import DBManager
from ultima_scraper_api.database.databases.user_data import user_database
import setup
if __name__ == "__main__":
    async def main():
        # WORK IN PROGRESS
        import ultima_scraper_api.helpers.main_helper as main_helper
        config = setup.start()
        config_path = config.ultima_scraper_directory.joinpath(".settings/config.json")
        us_config, _updated = main_helper.get_config(config_path)
        api = ultima_scraper_api.select_api("onlyfans",us_config)
        authed = api.add_auth()
        sites_directories = config.ultima_scraper_directory.joinpath(".sites")
        for sites_directory in sites_directories.iterdir():
            if sites_directory.stem != "OnlyFans":
                continue
            # In the future, the script must allow users to choose which folder to resolve
            for user_directory in sites_directory.iterdir():
                folder_identifier = user_directory.stem
                success_string = f"Renamed {folder_identifier}'s files"
                failure_string = f"Failed to rename {folder_identifier}'s files"
                print(f"Renaming {user_directory}'s files")
                db_path = user_directory.joinpath("Metadata","user_data.db")
                resolved = False
                if db_path.exists():
                    db_manager = DBManager(db_path)
                    Session ,_=await db_manager.create_database_session()
                    database_session, _engine = await db_manager.import_database()
                    content_types = authed.api.ContentTypes()
                    for api_type,_ in content_types:
                        if resolved:
                            break
                        if api_type != "Posts":
                            continue
                        db_table = user_database.table_picker(api_type)
                        db_posts = database_session.query(db_table).order_by(db_table.created_at.asc()).all()  # type: ignore
                        for db_post in db_posts:
                            user = await authed.resolve_user(post_id=db_post.post_id)
                            if user:
                                user.create_directory_manager()
                                await main_helper.format_directories(
                                    user.directory_manager, user
                                )
                                _metadata = await renamer.start(user,"Posts",Session,api.get_site_settings())
                                resolved = True
                                break
                            pass
                    print(f'{success_string if resolved else failure_string}')
                pass
            pass
        pass
        pass
    asyncio.run(main())

    # Enhance the resolver:
    # # Can subscribe to FREE users to avoid seeing "Post Not Found" since some models hide their wall posts
    # # Can assume the folder identifier is the model's username