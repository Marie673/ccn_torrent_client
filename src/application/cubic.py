import time


class Cubic:
    def __init__(self):
        self.now_wind = 0

        self.cwind = 0
        self.w_max = 0  # パケロス検出時のウィンドウサイズ

        self.C = 0.4  # 増幅幅を決めるパラメータ
        self.B = 0.2  # パケロス検出時のウィンドウサイズ減少幅

        self.t = 0  # パケロス検出時からの経過時間
        self.last_time_loss = time.time()

    def cals_cwind(self):
        self.t = time.time() - self.last_time_loss
        self.cwind = self.C * (self.t - self.calc_k()) ** 3 + self.w_max
        return self.cwind

    def calc_k(self):
        return (self.w_max * self.B / self.C) ** (1/3)
