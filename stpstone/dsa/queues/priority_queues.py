from heapq import heappush, heappop

class PriorityQueue:

    def __init__(self):
        self._queue = []
        self._index = 0

    def push(self, item, priority):
        heappush(self._queue, (-priority, self._index, item))
        self._index += 1

    def pop(self):
        return heappop(self._queue)[-1]
