from __future__ import annotations

import copy
import os
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import urlparse

from ultima_scraper_api.helpers import main_helper

if TYPE_CHECKING:
    import ultima_scraper_api
    from ultima_scraper_collection.managers.filesystem_manager import (
        DirectoryManager,
        FilesystemManager,
    )
    from ultima_scraper_collection.managers.metadata_manager.metadata_manager import (
        MediaMetadata,
    )

    auth_types = ultima_scraper_api.auth_types


class FormatAttributes(object):
    def __init__(self):
        self.site_name = "{site_name}"
        self.first_letter = "{first_letter}"
        self.content_id = "{content_id}"
        self.media_id = "{media_id}"
        self.profile_username = "{profile_username}"
        self.model_username = "{model_username}"
        self.api_type = "{api_type}"
        self.media_type = "{media_type}"
        self.filename = "{filename}"
        self.value = "{value}"
        self.text = "{text}"
        self.date = "{date}"
        self.ext = "{ext}"

    def whitelist(self, wl: list[str]):
        new_wl: list[str] = []
        new_format_copied = copy.deepcopy(self)
        for _key, value in new_format_copied:
            if value not in wl:
                new_wl.append(value)
        return new_wl

    def __iter__(self):
        for attr, value in self.__dict__.items():
            yield attr, value


class ReformatItem:
    def __init__(self, option: dict[str, Any] = {}, keep_vars: bool = False) -> None:
        format_variables = FormatAttributes()
        self.site_name = option.get("site_name", format_variables.site_name)
        self.content_id = option.get("content_id", format_variables.content_id)
        self.media_id = option.get("media_id", format_variables.media_id)
        self.profile_username = option.get(
            "profile_username", format_variables.profile_username
        )
        self.model_username = option.get(
            "model_username", format_variables.model_username
        )
        self.api_type = option.get("api_type", format_variables.api_type)
        self.media_type = option.get("media_type", format_variables.media_type)
        self.filename = option.get("filename", format_variables.filename)
        self.ext = option.get("ext", format_variables.ext)
        text: str = option.get("text", format_variables.text)
        self.text = str(text or "")
        self.date = option.get("postedAt", format_variables.date)
        self.price = option.get("price", 0)
        self.archived = option.get("archived", False)
        self.date_format = option.get("date_format", "%d-%m-%Y")
        self.maximum_length = 255
        self.text_length = option.get("text_length", self.maximum_length)
        self.directory: Path | None = option.get("directory")
        self.preview = option.get("preview")
        self.ignore_value = False
        if not keep_vars:
            for key, value in self.__dict__.items():
                if isinstance(value, str):
                    key = main_helper.find_between(value, "{", "}")
                    e = getattr(format_variables, key, None)
                    if e:
                        setattr(self, key, "")

    def reformat(self, unformatted: Path):
        content_id = self.content_id
        media_id = self.media_id
        date = self.date
        text = self.text
        value = "Free"
        maximum_length = self.maximum_length
        text_length = self.text_length
        content_id = "" if content_id is None else str(content_id)
        media_id = "" if media_id is None else str(media_id)
        unformatted_string = unformatted.as_posix()
        extra_count = 0
        if type(date) is str:
            format_variables2 = FormatAttributes()
            if date != format_variables2.date and date != "":
                date = datetime.fromisoformat(date)
                date = date.strftime(self.date_format)
        else:
            if isinstance(date, datetime):
                date = date.strftime(self.date_format)
            elif isinstance(date, int):
                date = datetime.fromtimestamp(date)
                date = date.strftime(self.date_format)
        has_text = False
        if "{text}" in unformatted_string:
            has_text = True
            text = main_helper.clean_text(text)
            extra_count = len("{text}")
        if "{value}" in unformatted_string:
            if self.price:
                if self.preview:
                    value = "Previews"
                else:
                    value = "Paid"
        directory = self.directory
        if not directory:
            raise Exception("Directory not found")
        path = unformatted_string.replace("{site_name}", self.site_name)
        path = path.replace("{first_letter}", self.model_username[0].capitalize())
        path = path.replace("{content_id}", content_id)
        path = path.replace("{media_id}", media_id)
        path = path.replace("{profile_username}", self.profile_username)
        path = path.replace("{model_username}", self.model_username)
        path = path.replace("{api_type}", self.api_type)
        path = path.replace("{media_type}", self.media_type)
        path = path.replace("{filename}", self.filename)
        path = path.replace("{ext}", self.ext)
        path = path.replace("{value}", value)
        path = path.replace("{date}", date)
        directory_count = len(str(directory))
        path_count = len(path)
        maximum_length = maximum_length - (directory_count + path_count - extra_count)
        text_length = text_length if text_length < maximum_length else maximum_length
        if has_text:
            # https://stackoverflow.com/a/43848928
            def utf8_lead_byte(b: int):
                """A UTF-8 intermediate byte starts with the bits 10xxxxxx."""
                return (b & 0xC0) != 0x80

            def utf8_byte_truncate(text: str, max_bytes: int):
                """If text[max_bytes] is not a lead byte, back up until a lead byte is
                found and truncate before that character."""
                utf8 = text.encode("utf8")
                if len(utf8) <= max_bytes:
                    return utf8
                i = max_bytes
                while i > 0 and not utf8_lead_byte(utf8[i]):
                    i -= 1
                return utf8[:i]

            filtered_text = utf8_byte_truncate(text, text_length).decode("utf8")
            path = path.replace("{text}", filtered_text)
        else:
            path = path.replace("{text}", "")
        x_path = directory.joinpath(path)
        return x_path

    def remove_non_unique(
        self, directory_manager: DirectoryManager, format_key: str = ""
    ):
        formats = directory_manager.formats
        unique_formats: dict[str, Any] = formats.check_unique()
        final_path = Path()

        def takewhile_including(iterable: list[str], value: str):
            for it in iterable:
                yield it
                if it == value:
                    return

        for key, unique_format in unique_formats["unique"].__dict__.items():
            if "filename" in key or format_key != key:
                continue
            unique_format: str = unique_format[0]
            path_parts = Path(getattr(formats, key)).parts
            p = Path(*takewhile_including(list(path_parts), unique_format))
            w = self.reformat(p)
            if format_key:
                final_path = w
                break
        assert final_path != Path()
        return final_path


class ReformatManager:
    def __init__(
        self, authed: auth_types, filesystem_manager: FilesystemManager
    ) -> None:
        self.authed = authed
        self.filesystem_manager = filesystem_manager
        self.api = self.authed.get_api()

    def prepare_reformat(self, media_item: MediaMetadata):
        content_metadata = media_item.__content_metadata__
        final_api_type = "Uncategorized"
        content_metadata_dict: dict[str, Any] = {}
        if content_metadata:
            final_api_type = content_metadata.api_type
            content_metadata_dict = content_metadata.__dict__
            content_metadata_dict["content_id"] = content_metadata.content_id
            content_metadata_dict["text"] = content_metadata.text
            content_metadata_dict["price"] = content_metadata.price
            content_metadata_dict["archived"] = content_metadata.archived

        api = self.authed.get_api()
        author = media_item.get_author()

        filename = urlparse(media_item.urls[0]).path.split("/")[-1]
        name, ext = filename.rsplit(".", 1)
        directory_manager = self.filesystem_manager.get_directory_manager(author.id)
        site_config = directory_manager.site_config
        download_path = directory_manager.root_download_directory
        option: dict[str, Any] = content_metadata_dict
        option["site_name"] = api.site_name
        option["media_id"] = media_item.id
        option["filename"] = name
        option["ext"] = ext
        option["api_type"] = final_api_type
        option["media_type"] = media_item.media_type
        option["profile_username"] = self.authed.username
        option["model_username"] = author.username
        option["date_format"] = site_config.download_setup.date_format
        option["postedAt"] = media_item.created_at
        option["text_length"] = site_config.download_setup.text_length
        option["directory"] = download_path
        option["preview"] = media_item.preview
        return ReformatItem(option)

    def drm_format(self, media_url: str, media_item: MediaMetadata):
        directory_manager = self.filesystem_manager.get_directory_manager(
            media_item.user_id
        )
        site_config = directory_manager.site_config
        temp_url = Path(media_url)
        name, ext = self.parse_filename(media_url)
        if "audio" in name:
            name = f"{name}.enc.{ext}a"
        else:
            name = f"{name}.enc.{ext}"
        temp_url = temp_url.with_name(name).as_posix()
        media_item.urls = [temp_url]
        reformat_item = self.prepare_reformat(media_item)
        file_directory = reformat_item.reformat(
            site_config.download_setup.directory_format
        ).joinpath("__drm__")
        reformat_item.directory = file_directory
        file_path = reformat_item.reformat(site_config.download_setup.filename_format)
        media_item.urls = [media_url]
        media_item.directory = file_directory
        media_item.filename = file_path.name
        return media_item

    def parse_filename(self, media_url: str):
        filename = urlparse(media_url).path.split("/")[-1]
        name, ext = filename.rsplit(".", 1)
        return name, ext
