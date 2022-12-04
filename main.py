import src.application.bittorrent as b
from src.domain.entity.torrent import Torrent


def main():
    path = "/evaluation/torrent/128MB.torrent"
    torrent = Torrent(path)
    bp = b.BitTorrent(torrent)
    bp.start()


if __name__ == "__main__":
    main()
