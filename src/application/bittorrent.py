import os
import math
import cefpyco
import time
import bitstring
from typing import List
import multiprocessing

from src.application.cubic import Cubic
from src.domain.entity.piece.piece import Piece
from src.domain.entity.piece.block import State
from src.domain.entity.torrent import Torrent, Info, FileMode

from logger import logger

CHUNK_SIZE = 1024 * 4
CACHE_PATH = os.environ['HOME'] + "/proxy_cache/"
MAX_PEER_CONNECT = 1
TIME_OUT = 4
EVALUATION = True
EVALUATION_PATH = "/client/evaluation/ccn_client/test"


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
        self.info_hash = str(torrent.info_hash.hex())
        self.file_path = CACHE_PATH + self.info.name

        self.name = "ccnx:/BitTorrent/" + self.info_hash

        try:
            os.makedirs(self.file_path)
        except Exception as e:
            logger.error(e)

        # number_of_pieces の計算
        if torrent.file_mode == FileMode.single_file:
            if self.info.length % self.info.piece_length == 0:
                self.number_of_pieces = int(self.info.length / self.info.piece_length)
            else:
                self.number_of_pieces = int(self.info.length / self.info.piece_length) + 1
        else:
            length: int = 0
            for file in self.info.files:
                length += file.length
            self.number_of_pieces = int(length / self.info.piece_length)

        self.bitfield: bitstring.BitArray = bitstring.BitArray(self.number_of_pieces)
        self.pieces = self._generate_pieces()
        self.num_of_all_of_blocks = 0
        for piece in self.pieces:
            for _ in piece.blocks:
                self.num_of_all_of_blocks += 1

        self.cef_handle = cefpyco.CefpycoHandle()
        self.cef_handle.begin()

        self.cubic = Cubic()

        self.compete_block = 0
        self.complete_pieces = 0
        self.started_time = None

        self.queue = multiprocessing.Queue()

    def run(self):
        req_p = None
        try:
            self.started_time = time.time()
            req_p = multiprocessing.Process(target=self.request_piece_handle)
            req_p.start()

            self.cef_listener()

            logger.info(f'download time: {(time.time() - self.started_time):.2f}')
        except Exception as e:
            logger.error(e)
            raise e
        except KeyboardInterrupt:
            return
        finally:
            req_p.kill()

    def cef_listener(self):
        logger.debug("start cef listener")
        try:
            last_seen_time = time.time()
            while not self.all_pieces_completed():
                if time.time() - last_seen_time > 1:
                    self.print_progress()
                    last_seen_time = time.time()

                info = self.cef_handle.receive(timeout_ms=1000)
                # logger.debug(f"{info.name}, {info.chunk_num}")
                if info.is_succeeded and info.is_data:
                    prefix = info.name.split('/')
                    if prefix[0] != 'ccnx:':
                        logger.debug("incorrect prefix")
                        continue
                    if prefix[1] != 'BitTorrent':
                        logger.debug("incorrect protocol")
                        continue
                    if prefix[2] != self.info_hash:
                        logger.debug(f"incorrect info_hash: {prefix[2]}:{self.info_hash}")
                        continue
                    # logger.debug(f"{info.name}, {info.chunk_num}")
                    self.handle_piece(info)
                    # self.bittorrent.print_progress()
        except Exception as e:
            logger.error(e)
        except KeyboardInterrupt:
            return

    def request_piece_handle(self):
        logger.debug("requester is start")
        last_time = time.time()
        while not self.all_pieces_completed():
            self.check_chunk_state()

            if time.time() - last_time > 1:
                logger.debug(f'cubic_window: {self.cubic.cwind}, now_window: {self.cubic.now_wind}')
                last_time = time.time()

            for piece in self.pieces:
                piece_index = piece.piece_index

                for block_index, block in enumerate(piece.blocks):

                    if self.cubic.now_wind > self.cubic.cals_cwind():
                        break

                    if block.state == State.FREE:
                        self.cef_handle.send_interest(
                            name=self.name + '/' + str(piece_index),
                            chunk_num=block_index
                        )
                        # logger.debug(f"piece_index: {piece_index}, chunk: {block_index}")
                        piece.blocks[block_index].state = State.PENDING
                        piece.blocks[block_index].last_seen = time.time()
                        self.cubic.now_wind += 1
                        # logger.debug(f"Send interest: {piece_index}, {chunk_num}")

    def check_chunk_state(self):
        while self.queue.qsize() > 0:
            (piece_index, block_index, state) = self.queue.get()
            piece = self.pieces[piece_index]

            if state == 'get':
                block = piece.blocks[block_index]
                block.state = State.FULL
                block.last_seen = time.time()
            elif state == 'error':
                for block in piece.blocks:
                    block.state = State.FREE
                    block.last_seen = time.time()

        pending_chunk_num = 0
        for piece in self.pieces:
            for block in piece.blocks:
                if block.state == State.PENDING:
                    if time.time() - block.last_seen > TIME_OUT:
                        block.state = State.FREE
                        block.last_seen = time.time()
                        self.cubic.last_time_loss = time.time()
                        self.cubic.w_max = self.cubic.cwind
                    else:
                        pending_chunk_num += 1

        self.cubic.now_wind = pending_chunk_num

    def handle_piece(self, info):
        piece_index = int(info.name.split('/')[3])
        payload = info.payload
        chunk_num = info.chunk_num

        offset = chunk_num * CHUNK_SIZE
        piece = self.pieces[piece_index]
        block_index = chunk_num

        if piece.is_full:
            return

        piece.set_block(offset=offset, data=payload)

        if piece.are_all_blocks_full():
            if piece.set_to_full():
                self.bitfield[piece_index] = 1
                self.complete_pieces += 1
                piece.write_on_disk()
            else:
                self.queue.put((piece_index, None, "error"))
                return
        self.queue.put((piece_index, block_index, "get"))

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

    last_bock_num = 0

    def print_progress(self):
        block_num = 0
        for piece in self.pieces:
            for block in piece.blocks:
                if block.state == State.FULL:
                    block_num += 1

        progress = (block_num / (self.num_of_all_of_blocks + 1)) * 100
        throughput = ((block_num - self.last_bock_num) * CHUNK_SIZE * 8) / 1024 ** 2
        self.last_bock_num = block_num
        # throughput = (block_num * CHUNK_SIZE * 8 / (time.time() - self.started_time)) / 1024 ** 2
        print(f"[piece: {self.complete_pieces} / {self.number_of_pieces}]"
              f"[block: {block_num} / {self.num_of_all_of_blocks + 1}, "
              f"{progress:.2f}%], "
              f"[Throughput: {throughput:.2f}Mbps]")
        print(self.bitfield)
