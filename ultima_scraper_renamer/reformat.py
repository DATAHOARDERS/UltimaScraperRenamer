from __future__ import annotations

import copy
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ultima_scraper_api.classes.prepare_metadata import format_attributes
from ultima_scraper_api.helpers import main_helper

if TYPE_CHECKING:

    from ultima_scraper_collection.managers.filesystem_manager import DirectoryManager


class prepare_reformat(object):
    def __init__(self, option: dict[str, Any] = {}, keep_vars: bool = False):
        format_variables2 = format_attributes()
        self.site_name = option.get("site_name", format_variables2.site_name)
        self.post_id = option.get("post_id", format_variables2.post_id)
        self.media_id = option.get("media_id", format_variables2.media_id)
        self.profile_username = option.get(
            "profile_username", format_variables2.profile_username
        )
        self.model_username = option.get(
            "model_username", format_variables2.model_username
        )
        self.api_type = option.get("api_type", format_variables2.api_type)
        self.media_type = option.get("media_type", format_variables2.media_type)
        self.filename = option.get("filename", format_variables2.filename)
        self.ext = option.get("ext", format_variables2.ext)
        text: str = option.get("text", format_variables2.text)
        self.text = str(text or "")
        self.date = option.get("postedAt", format_variables2.date)
        self.price = option.get("price", 0)
        self.archived = option.get("archived", False)
        self.date_format = option.get("date_format", "%d-%m-%Y")
        self.maximum_length = 255
        self.text_length = option.get("text_length", self.maximum_length)
        self.directory: Path|None = option.get("directory")
        self.preview = option.get("preview")
        self.ignore_value = False
        if not keep_vars:
            for key, value in self:
                if isinstance(value, str):
                    key = main_helper.find_between(value, "{", "}")
                    e = getattr(format_variables2, key, None)
                    if e:
                        setattr(self, key, "")

    def __iter__(self):
        for attr, value in self.__dict__.items():
            yield attr, value

    async def standard(
        self,
        site_name: str,
        profile_username: str,
        user_username: str,
        date: datetime,
        date_format: str,
        text_length: int,
        directory: Path,
    ):
        p_r = prepare_reformat()
        p_r.site_name = site_name
        p_r.profile_username = profile_username
        p_r.model_username = user_username
        p_r.date = date
        p_r.date_format = date_format
        p_r.text_length = text_length
        p_r.directory = directory
        return p_r

    # async def reformat(self, unformatted_list:dict[str,str]) -> list[str]:
    #     x:list[str] = []
    #     format_variables2 = format_variables()
    #     for key, unformatted_item in unformatted_list.items():
    #         if "filename_format" == key:
    #             unformatted_item = os.path.join(x[1], unformatted_item)
    #             print
    #         string = await self.reformat_2(unformatted_item)
    #         final_path = []
    #         paths = string.split(os.sep)
    #         for path in paths:
    #             key = main_helper.find_between(path, "{", "}")
    #             e = getattr(format_variables2, key, None)
    #             if path == e:
    #                 break
    #             final_path.append(path)
    #         final_path = os.sep.join(final_path)
    #         print
    #         x.append(final_path)
    #     return x

    async def reformat_2(self, unformatted: Path):
        post_id = self.post_id
        media_id = self.media_id
        date = self.date
        text = self.text
        value = "Free"
        maximum_length = self.maximum_length
        text_length = self.text_length
        post_id = "" if post_id is None else str(post_id)
        media_id = "" if media_id is None else str(media_id)
        unformatted_string = unformatted.as_posix()
        extra_count = 0
        if type(date) is str:
            format_variables2 = format_attributes()
            if date != format_variables2.date and date != "":
                date = datetime.strptime(date, "%d-%m-%Y %H:%M:%S")
                date = date.strftime(self.date_format)
        else:
            if isinstance(date, datetime):
                date = date.strftime(self.date_format)
        has_text = False
        if "{text}" in unformatted_string:
            has_text = True
            text = main_helper.clean_text(text)
            extra_count = len("{text}")
        if "{value}" in unformatted_string:
            if self.price:
                if not self.preview:
                    value = "Paid"
        directory = self.directory
        if not directory:
            raise Exception("Directory not found")
        path = unformatted_string.replace("{site_name}", self.site_name)
        path = path.replace("{first_letter}", self.model_username[0].capitalize())
        path = path.replace("{post_id}", post_id)
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

    def remove_empty(self):
        copied = copy.deepcopy(self)
        for k, v in copied:
            if not v:
                delattr(self, k)
        return self

    async def remove_non_unique(
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
            w = await self.reformat_2(p)
            if format_key:
                final_path = w
                break
        assert final_path != Path()
        return final_path

    async def find_metadata_files(
        self, filepaths: list[Path], legacy_files: bool = True
    ):
        new_list: list[Path] = []
        for filepath in filepaths:
            if not legacy_files:
                if "__legacy_metadata__" in filepath.parts:
                    continue
            match filepath.suffix:
                case ".db":
                    red_list = ["thumbs.db"]
                    status = [x for x in red_list if x == filepath.name.lower()]
                    if status:
                        continue
                    new_list.append(filepath)
                case ".json":
                    new_list.append(filepath)
                case _:
                    pass
        return new_list
