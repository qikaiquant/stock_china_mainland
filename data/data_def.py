class Stock4Pick:
    def __init__(self, sid, score, ext=None):
        self._sid = sid
        self._score = score
        self._ext = ext


class Position:
    def __init__(self, sid=None, dt=None, price=None, amount=None):
        self._sid = sid
        self._dt = dt
        self._price = price
        self._amount = amount

    def isEmpty(self):
        return False


class BTContext:
    def __init__(self, sdt, edt, ps, index=None, res=None):
        self._sdt = sdt
        self._edt = edt
        self._ps = ps
        self._index = index
        self._res = res

    def get_res(self):
        return self._res
