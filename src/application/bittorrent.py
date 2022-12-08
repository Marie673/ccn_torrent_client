import datetime
import os
import cefpyco
import time
from concurrent.futures import ThreadPoolExecutor
import bitstring
from src.domain.entity.piece.piece import Piece
from src.domain.entity.torrent import Torrent, Info, FileMode
from typing import List


CHUNK_SIZE = 1024 * 4
CACHE_PATH = os.environ['HOME']+"/proxy_cache/"
MAX_PEER_CONNECT = 1
EVALUATION = True
EVALUATION_PATH = "/root/evaluation/ccn_client/test"


class BitTorrent:
    def __init__(self, torrent: Torrent):
        """
        トレントファイル解析
        ↓
        CCN Interest送信
        ↓
        CCN Data受信
        """
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

    def start(self) -> None:
        """
        Interest bitfield
        Data bitfield

        bitfieldに持っていないピースが存在
            Interest piece
        :return:
        """
        self.get_bitfield()
        tpe = ThreadPoolExecutor(max_workers=1)
        while not self.all_pieces_completed():
            if time.time() - self.timer > 5:
                self.get_bitfield()
                self.timer = time.time()

            """index, piece = (0, self.pieces[0])
            if piece.is_full:
                continue
            if self.bitfield[index] != 1:
                continue
            if piece.state == 1:
                continue
            self.pieces[0].state = 1
            self.request_piece(index)"""
            # tpe.submit(self.request_piece(index))
            for index, piece in enumerate(self.pieces):
                if piece.is_full:
                    continue
                if self.bitfield[index] != 1:
                    continue
                if self.pieces[index].state == 1:
                    continue
                self.pieces[index].state = 1
                tpe.submit(self.request_piece(index))
                time.sleep(1)

            time.sleep(2)
        tpe.shutdown()

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
        name = self.name + str(piece_index)

        handle = cefpyco.CefpycoHandle()
        handle.begin()
        handle.send_interest(name=name, chunk_num=0)

        try:
            packet = handle.receive()
        except Exception as e:
            raise e
        end_chunk_num = packet.end_chunk_num

        def send_interest():
            for chunk_num in range(0, end_chunk_num):
                handle.send_interest(name=name, chunk_num=chunk_num)
        send_interest()

        while True:
            try:
                packet = handle.receive()
                if packet.is_failed and packet.name != name:
                    send_interest()

                if packet.is_succeeded:
                    # TODO データを受信した際の処理
                    payload = packet.payload
                    offset = packet.chunk_num * CHUNK_SIZE

                    self.pieces[piece_index].set_block(offset=offset, data=payload)
                    if self.pieces[piece_index].are_all_blocks_full():
                        if self.pieces[piece_index].set_to_full():
                            self.complete_pieces += 1
                            self.pieces[piece_index].write_on_disk()
                            break
            except KeyboardInterrupt:
                return
            except Exception as e:
                print(e)
                raise e
            finally:
                handle.end()

    """
        if EVALUATION:
            with open(EVALUATION_PATH, "aw") as file:
                data = str(datetime.datetime.now()) + f" piece_index: {piece_index}, status: send_request"
                file.write(data)
    """
    def all_pieces_completed(self) -> bool:
        for piece in self.pieces:
            if not piece.is_full:
                return False

        return True
