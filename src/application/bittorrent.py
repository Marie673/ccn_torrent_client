import os

import cefpyco
import time
import bitstring

from src.application.cubic import Cubic
from src.domain.entity.piece.piece import Piece
from src.domain.entity.piece.block import State
from src.domain.entity.torrent import Torrent, Info, FileMode
from typing import List
from threading import Thread
import datetime


from logger import logger

CHUNK_SIZE = 1024 * 4
CACHE_PATH = os.environ['HOME']+"/proxy_cache/"
MAX_PEER_CONNECT = 1
TIME_OUT = 4
EVALUATION = True
EVALUATION_PATH = "/client/evaluation/ccn_client/test"


class CefListener(Thread):
    def __init__(self, bittorrent):
        super().__init__()
        self.bittorrent = bittorrent
        self.cef_handle = cefpyco.CefpycoHandle()
        self.cef_handle.begin()

    def run(self):
        try:
            while not self.bittorrent.all_pieces_completed():
                info = self.cef_handle.receive()
                logger.debug(info)
                if info.is_succeeded and info.is_data:
                    prefix = info.name.split('/')
                    if prefix[0] != 'ccnx:':
                        continue
                    if prefix[1] != 'BitTorrent':
                        continue
                    if prefix[2] != self.bittorrent.info_hash:
                        continue

                    self.bittorrent.handle_piece(info)
                    self.bittorrent.print_progress()
        except Exception as e:
            logger.error(e)
        except KeyboardInterrupt:
            return


class BitTorrent:
    def __init__(self, torrent: Torrent):
        """
        トレントファイル解析
        ↓
        CCN Interest送信
        ↓
        CCN Data受信
        """
        self.compete_block = 0
        self.torrent = torrent
        self.info: Info = torrent.info
        self.info_hash = torrent.info_hash
        self.file_path = CACHE_PATH + self.info.name
        try:
            os.makedirs(self.file_path)
        except Exception as e:
            logger.error(e)

        # number_of_pieces の計算
        if torrent.file_mode == FileMode.single_file:
            self.number_of_pieces = int(self.info.length / self.info.piece_length)
        else:
            length: int = 0
            for file in self.info.files:
                length += file.length
            self.number_of_pieces = int(length / self.info.piece_length)

        # 1ピース当たりのチャンク数
        # ピースの最後を表現するときに、チャンクサイズで余りが出ても次のピースデータを含めない.
        if self.info.piece_length % CHUNK_SIZE == 0:
            self.chunks_per_piece = self.info.piece_length // CHUNK_SIZE
        else:
            self.chunks_per_piece = (self.info.piece_length // CHUNK_SIZE) + 1

        # end_chunk_numの計算.
        # chunk_numは0から数え始めるので、-1する.
        self.end_chunk_num = self.chunks_per_piece * self.number_of_pieces - 1

        self.bitfield: bitstring.BitArray = bitstring.BitArray(self.number_of_pieces)
        self.pieces = self._generate_pieces()
        self.complete_pieces = 0

        self.name = "ccnx:/BitTorrent/" + str(self.info_hash.hex())

        self.cef_handle = cefpyco.CefpycoHandle()
        self.cef_handle.begin()

        self.cubic = Cubic()

    def run(self):
        listener = CefListener(self)
        listener.start()
        try:
            self.request_piece_handle()
        except Exception as e:
            logger.error(e)
        except KeyboardInterrupt:
            return
        finally:
            listener.join()

    def request_piece_handle(self):
        logger.debug("requester is start")
        while not self.all_pieces_completed():
            self.check_chunk_state()
            for chunk_num in range(self.end_chunk_num + 1):

                piece_index = chunk_num // self.chunks_per_piece
                piece = self.pieces[piece_index]
                block_index = chunk_num % self.chunks_per_piece

                if piece.blocks[block_index].state == State.FREE:
                    if self.cubic.now_wind < self.cubic.cwind:
                        self.cef_handle.send_interest(
                            name=self.name,
                            chunk_num=chunk_num
                        )
                        piece.blocks[block_index].state = State.PENDING
                        piece.blocks[block_index].last_seen = time.time()
                        self.cubic.now_wind += 1
                        # logger.debug(f"Send interest: {piece_index}, {chunk_num}")
                    else:
                        break

            logger.debug(f"c_window: {int(self.cubic.cwind)}")
            time.sleep(1)

    def check_chunk_state(self):
        pending_chunk_num = 0
        for chunk_num in range(self.end_chunk_num + 1):
            piece_index = chunk_num // self.chunks_per_piece
            piece = self.pieces[piece_index]
            block_index = chunk_num % self.chunks_per_piece

            if piece.blocks[block_index].state == State.PENDING:
                if time.time() - piece.blocks[block_index].last_seen > TIME_OUT:
                    piece.blocks[block_index].state = State.FREE
                    piece.blocks[block_index].last_seen = time.time()
                    self.cubic.last_time_loss = time.time()
                    self.cubic.w_max = self.cubic.cwind
                else:
                    pending_chunk_num += 1

        self.cubic.now_wind = pending_chunk_num
        self.cubic.cals_cwind()

    def handle_piece(self, info):
        logger.debug("get piece data")
        payload = info.payload
        chunk_num = info.chunk_num

        piece_index = chunk_num // self.chunks_per_piece
        offset = (chunk_num % self.chunks_per_piece) * CHUNK_SIZE
        self.pieces[piece_index].set_block(offset=offset, data=payload)
        if self.pieces[piece_index].are_all_blocks_full():
            if self.pieces[piece_index].set_to_full():
                self.complete_pieces += 1
                self.pieces[piece_index].write_on_disk()

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

    def all_pieces_completed(self) -> bool:
        for piece in self.pieces:
            if not piece.is_full:
                return False
        return True

    def print_progress(self):
        progress = (self.compete_block / self.end_chunk_num) * 100
        print(f"[piece: {self.complete_pieces} / {self.number_of_pieces}]"
              f"[block: {self.compete_block} / {self.end_chunk_num}, "
              f"{progress:.2f}%]")
