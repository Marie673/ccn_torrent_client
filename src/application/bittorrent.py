import os
import cefpyco
import time
import src.global_value as gv
from concurrent.futures import ProcessPoolExecutor
from concurrent import futures
from multiprocessing import Value
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

        self.name = "ccnx:/BitTorrent/" + str(self.info_hash.hex()) + "/"

        self.healthy = True

        self.compete_block = Value('i', 0)
        self.total_block = 0
        for piece in self.pieces:
            self.total_block += piece.number_of_blocks
        if gv.EVALUATION:
            log("start")

    def start(self) -> None:
        """
        Interest bitfield
        Data bitfield

        bitfieldに持っていないピースが存在
            Interest piece
        :return:
        """
        self.get_bitfield()

        def schedule():
            bitfield_timer = time.time()
            screen_timer = time.time()
            while self.healthy:
                if time.time() - bitfield_timer > 10:
                    self.get_bitfield()
                    bitfield_timer = time.time()

                if time.time() - screen_timer > 1:
                    self.print_progress()
                    screen_timer = time.time()

        th = Thread(target=schedule)
        th.start()

        futures_list = []
        try:
            with ProcessPoolExecutor(max_workers=gv.MAX_PEER_CONNECT) as executor:
                while not self.all_pieces_completed():
                    for index, piece in enumerate(self.pieces):
                        if piece.is_full:
                            continue
                        if self.bitfield[index] != 1 and self.bitfield[0] == 1:
                            continue
                        if self.pieces[index].state == 1:
                            continue
                        if len(futures_list) > 2:
                            break
                        future = executor.submit(self.request_piece, piece)
                        print(len(futures_list))

                        self.pieces[index].state = 1
                        futures_list.append(future)

                    for future in futures.as_completed(futures_list):
                        res_piece = future.result()
                        self.pieces[res_piece.piece_index] = res_piece
                        self.complete_pieces += 1
                        print(len(futures_list))

            self.healthy = False
            self.print_progress()
        except KeyboardInterrupt:
            self.healthy = False
            for future in futures_list:
                future.cancel()
        except Exception as e:
            print(e)
        finally:
            th.join()

    def get_bitfield(self):
        try:
            cef_handle = cefpyco.CefpycoHandle(enable_log=False)
            cef_handle.begin()

            name = self.name + "bitfield"
            for b in self.bitfield:
                if b is True:
                    continue
                elif b is False:
                    break

            log("bitfield, Interest")
            cef_handle.send_interest(name=name)

            # TODO 複数チャンクを想定できていないので対応する
            packet = cef_handle.receive()

            if packet.is_failed and packet.name != name:
                return
                # raise Exception("packet receive failed")
            elif packet.is_succeeded and packet.is_data:
                log("bitfield, Data")
                data: bytes = packet.payload
                new_bitfield = bitstring.BitArray(bytes=bytes(data))
                if len(self.bitfield) == len(new_bitfield):
                    self.bitfield = new_bitfield
        except Exception as e:
            print(e)

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

    def request_piece(self, piece: Piece):
        print("start")
        log(f"{piece.piece_index}, start")
        name = self.name + str(piece.piece_index)

        handle = cefpyco.CefpycoHandle(enable_log=False)
        handle.begin()

        handle.send_interest(name=name, chunk_num=0)
        log(f"{piece.piece_index}, Interest, {0}")

        try:
            packet = handle.receive()
        except Exception as e:
            raise e
        log(f"{piece.piece_index}, Data, {packet.chunk_num}")
        end_chunk_num = packet.end_chunk_num

        def send_interest():
            for chunk_num, block in enumerate(piece.blocks):
                if block.state == State.FULL:
                    continue
                handle.send_interest(name=name, chunk_num=chunk_num)
                log(f"{piece.piece_index}, Interest, {chunk_num}")

        send_interest()

        try:
            while self.healthy:
                packet = handle.receive()
                if packet.is_failed and packet.name != name:
                    send_interest()

                if packet.is_succeeded and packet.is_data:
                    # TODO データを受信した際の処理
                    log(f"{piece.piece_index}, Data, {packet.chunk_num}")
                    payload = packet.payload
                    offset = packet.chunk_num * gv.CHUNK_SIZE

                    if piece.set_block(offset=offset, data=payload):
                        #complete_block.value += 1
                        pass
                    if piece.are_all_blocks_full():
                        if piece.set_to_full():
                            piece.write_on_disk()
                            # self.pieces[piece_index].raw_data = b""
                            log(f"{piece.piece_index}, complete")
                            break
                        else:
                            #complete_block.value -= piece.number_of_blocks
                            send_interest()

        except KeyboardInterrupt:
            return
        except Exception as e:
            print(e)
            raise e
        finally:
            handle.end()
        return piece

    def all_pieces_completed(self) -> bool:
        for piece in self.pieces:
            if not piece.is_full:
                return False
        return True

    def print_progress(self):
        progress = (self.compete_block.value / self.total_block) * 100
        print(f"[piece: {self.complete_pieces} / {self.number_of_pieces}]"
              f"[block: {self.compete_block.value} / {self.total_block}, "
              f"{progress:.2f}%]")


def log(msg):
    msg = str(datetime.datetime.now()) + ", " + msg + "\n"
    lock.acquire()
    with open(gv.EVALUATION_PATH, "a+") as file:
        file.write(msg)
    lock.release()
