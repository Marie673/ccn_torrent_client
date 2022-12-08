import src.application.bittorrent as b
from src.domain.entity.torrent import Torrent


def main():
    path = "/root/evaluation/torrent_file/128MB.torrent"
    torrent = Torrent(path)
    bp = b.BitTorrent(torrent)
    bp.start()
    bp.join()


if __name__ == "__main__":
    main()
