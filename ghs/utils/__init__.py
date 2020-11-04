import base64
import hashlib

import bencodepy


def create_magnet_uri(data: bytes):
    # noinspection PyTypeChecker
    metadata: dict = bencodepy.decode(data)
    subj = metadata[b'info']
    hashcontents = bencodepy.encode(subj)
    digest = hashlib.sha1(hashcontents).digest()
    b32hash = base64.b32encode(digest).decode()
    magnet_uri = 'magnet:?' + 'xt=urn:btih:' + b32hash
    if b"announce" in metadata:
        magnet_uri += ('&tr=' + metadata[b'announce'].decode())
    if b"info" in metadata:
        metadata_info = metadata[b'info']
        if b"name" in metadata_info:
            magnet_uri += ('&dn=' + metadata[b'info'][b'name'].decode())
        if b"length" in metadata_info:
            magnet_uri += ('&xl=' + str(metadata[b'info'][b'length']))
    return magnet_uri
