#!/usr/bin/env python3
import sys
import src.global_value as gv
import src.application.bittorrent as b
from src.domain.entity.torrent import Torrent


def main():

    paths = [
        "128MB.torrent",
        "256MB.torrent",
        "512MB.torrent",
        "1024MB.torrent",
        "2048MB.torrent",
    ]
    gv.EVALUATION_PATH = gv.EVALUATION_PATH + paths[int(sys.argv[1])]
    path = gv.TORRENT_FILE_PATH + paths[int(sys.argv[1])]
    
    torrent = Torrent(path)
    bp = b.BitTorrent(torrent)
    bp.start()


if __name__ == "__main__":
    main()
