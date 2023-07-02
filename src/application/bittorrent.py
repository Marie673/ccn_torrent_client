import asyncio

import os
import cefpyco
import time
import src.global_value as gv
from concurrent.futures import ProcessPoolExecutor
from concurrent import futures
import bitstring
from src.domain.entity.piece.piece import Piece
from src.domain.entity.piece.block import State
from src.domain.entity.torrent import Torrent, Info, FileMode
from typing import List
from threading import Lock, Thread
import datetime

lock = Lock()


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
        self.file_path = gv.CACHE_PATH + self.info.name
        try:
            os.makedirs(self.file_path)
        except FileExistsError:
            pass
        except Exception as e:
            raise e
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

        self.cef_handle = cefpyco.CefpycoHandle(enable_log=False)
        self.cef_handle.begin()
        self.name = "ccnx:/BitTorrent/" + str(self.info_hash.hex()) + "/"
        self.pending_interest_num = 0

        self.compete_block = 0
        self.total_block = 0
        for piece in self.pieces:
            self.total_block += piece.number_of_blocks
        if gv.EVALUATION:
            log("start")

    def start(self):
        handle = cefpyco.CefpycoHandle(enable_log=False)
        handle.begin()

        while not self.all_pieces_completed():
            try:
                info = await self.cef_handle.receive()
                if info.is_succeeded and info.is_data:
                    name = info.name
                    prefix = name.split('/')
                    """
                    prefix[0] = ccnx:
                    prefix[1] = BitTorrent
                    prefix[2] = info_hash
                    prefix[3] = piece_index
                    """
                    info_hash = prefix[2]
                    piece_index = prefix[3]

                    if piece_index == "bitfield":
                        self.handle_bitfield(info)
                    else:
                        self.handle_piece(info, piece_index)
            except Exception as e:
                log(e)

    def handle_bitfield(self, info):
        chunk_num = info.chunk_num
        end_chunk_num = info.end_chunk_num
        payload = info.payload
        new_bitfield = bitstring.BitArray(bytes=bytes(payload))
        if len(self.bitfield) == len(new_bitfield):
            self.bitfield = new_bitfield

    def handle_piece(self, info, piece_index):
        chunk_num = info.chunk_num
        end_chunk_num = info.end_chunk_num
        payload = info.payload
        offset = chunk_num * gv.CHUNK_SIZE

        piece = self.pieces[piece_index]
        if piece.set_block(offset=offset, data=payload):
            pass
        if piece.are_all_blocks_full():
            if piece.set_to_full():
                piece.write_on_disk()
                log(f"{piece.piece_index}, complete")
                piece.state = State.FULL
            else:
                pass

    async def send_interest_handle(self):
        last_time = time.time()
        while not self.all_pieces_completed():
            if time.time() - last_time > 10:
                self.get_bitfield()
                last_time = time.time()

            for piece_index, piece in enumerate(self.pieces):
                if piece.state == State.FULL:
                    continue
                elif piece.state == State.PENDING:
                    if (time.time() - piece.last_seen) > 5:
                        piece.state = State.FREE
                        self.pending_interest_num -= 1

                if self.pending_interest_num >= 50:
                    await asyncio.sleep(5)
                    continue

                if piece.state == State.FREE and self.bitfield[piece_index] == True:
                    self.send_piece_interest(piece.piece_index)
                    piece.state = State.PENDING
                    piece.last_seen = time.time()
                    self.pending_interest_num += 1

            await asyncio.sleep(0)

    def send_piece_interest(self, piece_index):
        name = self.name + str(piece_index)
        piece = self.pieces[piece_index]
        for chunk_num, block in enumerate(piece.blocks):
            if block.state == State.FULL:
                continue
            block.state = State.PENDING
            block.last_seen = time.time()
            self.cef_handle.send_interest(name=name, chunk_num=chunk_num)

    def get_bitfield(self):
        name = self.name + "bitfield"

        log("bitfield, Interest")
        self.cef_handle.send_interest(name=name)

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
        progress = (self.compete_block / self.total_block) * 100
        print(f"[piece: {self.complete_pieces} / {self.number_of_pieces}]"
              f"[block: {self.compete_block} / {self.total_block}, "
              f"{progress:.2f}%]")


def log(msg):
    msg = str(datetime.datetime.now()) + ", " + msg + "\n"
    lock.acquire()
    with open(gv.EVALUATION_PATH, "a+") as file:
        file.write(msg)
    lock.release()
