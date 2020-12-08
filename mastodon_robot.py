import datetime
import logging
import os

import numpy
from mastodon import Mastodon

from ghs.channels.jav import JAVChannel
from ghs.channels.sex8 import Sex8ImageChannel
from ghs.channels.xart import XartVideoChannel, XartImageChannel

log = logging.getLogger(__file__)

# How many candidates to sample
candidate_count = 20

# The channel and it's weight to push
channel_weight = [
    (JAVChannel("censored", "有码AV", candidate_count=candidate_count), 1.5),
    (JAVChannel("uncensored", "无码AV", candidate_count=candidate_count), 2),
    (JAVChannel("h-anime", "动画", candidate_count=candidate_count), 0.5),
    (JAVChannel("h-manga", "漫画", candidate_count=candidate_count), 0.5),
    (XartVideoChannel(candidate_count=candidate_count), 4),
    (XartImageChannel(candidate_count=candidate_count), 3),
    (Sex8ImageChannel(157, "网友自拍", candidate_count=candidate_count), 3),
    (Sex8ImageChannel(158, "网友自拍", candidate_count=candidate_count), 3),
]


def publish_toot_as_public() -> bool:
    """
    If the this function set to true, the toot will be shown in public timeline
    :return:
    """
    t = datetime.datetime.now()
    ft = t.hour + t.minute / 60 + t.second / 3600.0
    return 17.9 < ft < 23.6


def push_recommendations():
    """
    Push recommendation content
    :return:
    """
    mastodon = Mastodon(
        access_token=os.environ["MASTODON_TOKEN"],
        api_base_url=os.environ["MASTODON_HOST"],
    )
    log.info("Pushing")
    # Select on channel randomly by channels' weight
    ws = numpy.array([w for c, w in channel_weight])
    ws = ws / ws.sum()
    selected_channel = channel_weight[numpy.random.choice(len(channel_weight), p=ws)][0]
    in_reply_to_id = None
    for i, content in enumerate(selected_channel.create_contents()[:4]):
        if i == 0:
            if publish_toot_as_public():
                visibility = "public"
            else:
                visibility = "unlisted"
        else:
            visibility = "unlisted"
        push_result = content.push(mastodon, visibility, in_reply_to_id)
        if in_reply_to_id is None:
            in_reply_to_id = push_result["id"]


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    push_recommendations()
