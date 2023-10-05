import cefpyco
import threading
import time
import math

handle = cefpyco.handle()
handle.begin()

C = 0.4
beta_cubic = 0.7

W_last_max = 0
W_cubic_target = 0
K = 0

cwnd = 1
ssthresh = 64
RTT = 0.1

pending_interests = {}  # Interestの名前をキーとして、送信時刻を保存

def send_interest():
    global cwnd, ssthresh, W_last_max, W_cubic_target, K, RTT

    while True:
        # CUBICの計算
        if cwnd < ssthresh:
            cwnd += 1
        else:
            t = time.time() - time_last_congestion
            W_cubic = C * (t-K)**3 + W_last_max
            if W_cubic > cwnd:
                cwnd += (W_cubic - cwnd) / cwnd

        # Interestの送信
        for _ in range(int(cwnd)):
            interest_name = "example_name"
            handle.send_interest(interest_name, payload=b"sample_payload")
            pending_interests[interest_name] = time.time()

        # タイムアウトのチェック
        current_time = time.time()
        for interest_name, sent_time in list(pending_interests.items()):
            if current_time - sent_time > 2 * RTT:
                handle_congestion_event()
                del pending_interests[interest_name]

        time.sleep(RTT)

def handle_congestion_event():
    global cwnd, ssthresh, W_last_max, K
    cwnd *= beta_cubic
    ssthresh = cwnd
    W_last_max = max(W_last_max, cwnd)
    K = (W_last_max - ssthresh) / (C * W_last_max)**(1/3)

def listener():
    while True:
        data = handle.receive()
        if data.is_data():
            # Interestの応答を受け取った場合の処理
            if data.name in pending_interests:
                del pending_interests[data.name]

if __name__ == "__main__":
    time_last_congestion = time.time()

    listen_thread = threading.Thread(target=listener)
    listen_thread.start()

    send_interest()