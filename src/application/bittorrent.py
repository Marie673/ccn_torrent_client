import datetime
import os
import cefpyco
import time
from threading import Thread
import bitstring
from src.domain.entity.piece.piece import Piece
from src.domain.entity.torrent import Torrent, Info, FileMode
from typing import List


CHUNK_SIZE = 1024 * 4
CACHE_PATH = os.environ['HOME']+"/proxy_cache/"
MAX_PEER_CONNECT = 1
EVALUATION = True
EVALUATION_PATH = "/root/evaluation/ccn_client/test"


class BitTorrent(Thread):
    def __init__(self, torrent: Torrent):
        """
        トレントファイル解析
        ↓
        CCN Interest送信
        ↓
        CCN Data受信
        """
        super().__init__()
        self.torrent = torrent
        self.info: Info = torrent.info
        self.info_hash = torrent.info_hash
        self.file_path = CACHE_PATH + self.info.name
        try:
            os.makedirs(self.file_path)
        except Exception as e:
            pass
        # number_of_pieces の計算
        if torrent.file_mode == FileMode.single_file:
            self.number_of_pieces = int(self.info.length / self.info.piece_length)
        else:
            length: int = 0
            for file in self.info.files:
                length += file.length
            self.number_of_pieces = int(length / self.info.piece_length)

        self.bitfield: bitstring.BitArray = bitstring.BitArray(self.number_of_pieces)
        self.pieces = self._generate_pieces()
        self.complete_pieces = 0

        self.name = "ccnx:/BitTorrent/" + str(self.info_hash.hex()) + "/"

        self.cef_handle = cefpyco.CefpycoHandle()
        self.cef_handle.begin()

        self.timer = time.time()

        if EVALUATION:
            with open(EVALUATION_PATH, "a") as file:
                data = str(datetime.datetime.now()) + " bittorrent process is start\n"
                file.write(data)

    def run(self) -> None:
        """
        Interest bitfield
        Data bitfield

        bitfieldに持っていないピースが存在
            Interest piece
        :return:
        """
        self.get_bitfield()
        while not self.all_pieces_completed():
            if time.time() - self.timer > 5:
                self.get_bitfield()
                self.timer = time.time()

            for index, piece in enumerate(self.pieces):
                if piece.is_full:
                    continue
                if self.bitfield[index] != 1:
                    continue
                self.request_piece(index)

            time.sleep(1)

    def get_bitfield(self):
        name = self.name + "bitfield"
        print(name)
        self.cef_handle.send_interest(name=name)

        # TODO 複数チャンクを想定できていないので対応する
        packet = self.cef_handle.receive(timeout_ms=20000)
        if packet.is_failed and packet.name != name:
            raise Exception("packet receive failed")

        data: bytes = packet.payload
        self.bitfield = bitstring.BitArray(bytes=bytes(data))

    def _generate_pieces(self) -> List[Piece]:
        """
        torrentの全てのpieceを生成して初期化
        :return: List[Piece]
        """
        pieces: List[Piece] = []
        last_piece = self.number_of_pieces - 1

        for i in range(self.number_of_pieces):
            start = i * 20
            end = start + 20

            if i == last_piece:
                piece_length = self.info.length - (self.number_of_pieces - 1) * self.info.piece_length
                pieces.append(Piece(i, piece_length, self.info.pieces[start:end], self.file_path))
            else:
                pieces.append(Piece(i, self.info.piece_length, self.info.pieces[start:end], self.file_path))

        return pieces

    def request_piece(self, piece_index):
        """
        just send message. this function don't wait for response.
        make blocks request to many peers.
        """
        piece = self.pieces[piece_index]
        self.cef_handle.receive()
        # TODO ここでInterest送信

        if EVALUATION:
            with open(EVALUATION_PATH, "aw") as file:
                data = str(datetime.datetime.now()) + f" piece_index: {piece_index}, status: send_request"
                file.write(data)

    def all_pieces_completed(self) -> bool:
        for piece in self.pieces:
            if not piece.is_full:
                return False

        return True
