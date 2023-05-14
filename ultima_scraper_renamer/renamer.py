#!/usr/bin/env python3
import asyncio
import os
import shutil
import traceback
import urllib.parse as urlparse
from datetime import datetime
from itertools import chain
from pathlib import Path

import ultima_scraper_api
from sqlalchemy.orm import Session, sessionmaker
from tqdm.asyncio import tqdm
from ultima_scraper_collection.managers.database_manager.connections.sqlite.models import (
    user_database,
)
from ultima_scraper_collection.managers.database_manager.connections.sqlite.models.api_model import (
    ApiModel,
)
from ultima_scraper_collection.managers.database_manager.connections.sqlite.models.media_model import (
    TemplateMediaModel,
)
from ultima_scraper_collection.managers.filesystem_manager import DirectoryManager

from ultima_scraper_renamer.reformat import prepare_reformat

user_types = ultima_scraper_api.user_types


async def fix_directories(
    posts: list[ApiModel],
    subscription: user_types,
    directory_manager: DirectoryManager,
    database_session: Session,
    api_type: str,
):
    new_directories = []
    authed = subscription.get_authed()
    api = authed.api
    site_settings = api.get_site_settings()

    async def fix_directories2(
        post: ApiModel, media_db: list[TemplateMediaModel], all_files: list[Path]
    ):
        delete_rows = []
        final_api_type = (
            os.path.join("Archived", api_type) if post.archived else api_type
        )
        post_id = post.post_id
        media_db = [x for x in media_db if x.post_id == post_id]
        for media in media_db:
            media_id = media.media_id
            if media.link:
                url_path = urlparse.urlparse(media.link).path
                url_path = Path(url_path)
            else:
                url_path = Path(media.filename)
            new_filename = url_path.name
            original_filename, ext = (url_path.stem, url_path.suffix)
            ext = ext.replace(".", "")

            file_directory_format = site_settings.file_directory_format
            filename_format = site_settings.filename_format
            date_format = site_settings.date_format
            text_length = site_settings.text_length
            download_path = directory_manager.root_download_directory
            option = {}
            option["site_name"] = api.site_name
            option["post_id"] = post_id
            option["media_id"] = media_id
            option["filename"] = original_filename
            option["ext"] = ext
            option["api_type"] = final_api_type
            option["media_type"] = media.media_type
            option["text"] = post.text
            option["profile_username"] = authed.username
            option["model_username"] = subscription.username
            option["date_format"] = date_format
            option["postedAt"] = media.created_at
            option["text_length"] = text_length
            option["directory"] = download_path
            option["price"] = post.price
            option["preview"] = media.preview
            option["archived"] = post.archived
            prepared_format = prepare_reformat(option)
            file_directory = await prepared_format.reformat_2(file_directory_format)
            prepared_format.directory = file_directory
            old_filepath = ""
            if media.linked:
                filename_format = filename_format.with_name(f"linked_{filename_format}")
            if post.archived:
                prepared_format.api_type = final_api_type.replace("/", "")
            new_filepath = await prepared_format.reformat_2(filename_format)
            old_filepaths = [
                x
                for x in all_files
                if original_filename in x.name and x.parts != new_filepath.parts
            ]
            if not old_filepaths:
                old_filepaths = [x for x in all_files if str(media_id) in x.name]
            if not media.linked:
                old_filepaths: list[Path] = [
                    x for x in old_filepaths if "linked_" not in x.parts
                ]
            if old_filepaths:
                old_filepath = old_filepaths[0]
            # a = randint(0,1)
            # await asyncio.sleep(a)
            if old_filepath and old_filepath != new_filepath:
                moved = None
                while not moved:
                    try:
                        if old_filepath.exists():
                            _old_filename, old_ext = (url_path.stem, url_path.suffix)
                            if ".part" == old_ext:
                                old_filepath.unlink()
                                continue
                            if media.size:
                                media.downloaded = True
                            found_dupes = [
                                x
                                for x in media_db
                                if x.filename == new_filename and x.id != media.id
                            ]
                            delete_rows.extend(found_dupes)
                            os.makedirs(os.path.dirname(new_filepath), exist_ok=True)
                            if media.linked:
                                if os.path.dirname(old_filepath) == os.path.dirname(
                                    new_filepath
                                ):
                                    moved = shutil.move(old_filepath, new_filepath)
                                else:
                                    moved = shutil.copy(old_filepath, new_filepath)
                            else:
                                moved = shutil.move(old_filepath, new_filepath)
                        else:
                            break
                    except OSError as _e:
                        print(traceback.format_exc())

            if os.path.exists(new_filepath):
                if media.size:
                    media.downloaded = True
            if prepared_format.text:
                pass
            media.directory = file_directory.as_posix()
            media.filename = os.path.basename(new_filepath)
            new_directories.append(os.path.dirname(new_filepath))
        return delete_rows

    base_directory = directory_manager.user.find_legacy_directory("download", api_type)
    temp_files: list[Path] = await directory_manager.walk(base_directory)
    result = database_session.query(user_database.media_table)
    media_db = result.all()
    tasks = [
        asyncio.ensure_future(fix_directories2(post, media_db, temp_files))
        for post in posts
    ]
    settings = {"colour": "MAGENTA", "disable": False}
    delete_rows = await tqdm.gather(*tasks, **settings)
    delete_rows = list(chain(*delete_rows))
    for delete_row in delete_rows:
        database_session.query(user_database.media_table).filter(
            user_database.media_table.id == delete_row.id
        ).delete()
    database_session.commit()
    new_directories = list(set(new_directories))
    return posts, new_directories


async def start(
    subscription: user_types,
    directory_manager: DirectoryManager,
    api_type: str,
    Session: sessionmaker[Session],
):
    metadata = getattr(subscription.scrape_manager.scraped, api_type)
    return metadata
    try:
        api_table_ = user_database.table_picker(api_type)
        database_session = Session()
        result: list[ApiModel] = database_session.query(api_table_).all()

        await fix_directories(
            result,
            subscription,
            directory_manager,
            database_session,
            api_type,
        )
        database_session.close()
    except Exception as _e:
        pass
