from abc import ABC, abstractmethod
from tempfile import NamedTemporaryFile
from typing import List, Iterable

from mastodon import Mastodon


class PushContent:

    def __init__(
            self,
            title: str,
            text: str,
            medias: List[bytes]
    ):
        """
        Content to push
        :param title: The text shown in content warning
        :param text: The content
        :param medias: several media files' data (less than 4)
        """
        self.medias = medias
        self.text = text
        self.title = title

    def push(self, m: Mastodon, visibility: str, in_reply_to_id: int = None):
        media_id_list = []
        for media_content in self.medias:
            with NamedTemporaryFile() as fp:
                fp.write(media_content)
                fp.seek(0)
                media_upload_resp = m.media_post(fp.name)
                media_id_list.append(media_upload_resp["id"])
        return m.status_post(
            status=self.text,
            media_ids=media_id_list,
            sensitive=True,
            spoiler_text=f"[ #NSFW ][{self.title}]",
            visibility=visibility,
            in_reply_to_id=in_reply_to_id
        )


class BaseChannel(ABC):
    """
    The channel to create the content
    """

    @abstractmethod
    def create_contents(self) -> Iterable[PushContent]: pass
