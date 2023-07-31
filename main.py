#!/usr/bin/env python3
import asyncio

import sys
import src.global_value as gv
import src.application.bittorrent as b
from src.domain.entity.torrent import Torrent


def main():

    paths = [
        "128MB.dummy.torrent",
        "256MB.dummy.torrent",
        "512MB.dummy.torrent",
        "1024MB.dummy.torrent",
        "2048MB.dummy.torrent",
    ]
    gv.EVALUATION_PATH = gv.EVALUATION_PATH + paths[int(sys.argv[1])]
    path = gv.TORRENT_FILE_PATH + paths[int(sys.argv[1])]
    
    torrent = Torrent(path)
    bp = b.BitTorrent(torrent)
    bp.run()


if __name__ == "__main__":
    main()
