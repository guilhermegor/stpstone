from heapq import heappush, heappop

class PriorityQueue:
    """
    REFERENCES: PYTHON COOKBOOK - DAVID BEASZLEY, BRIAN K. JONES
    DOCSTRING: CREATE A PRIORITY QUEUE LIST
    INPUTS: -
    OUTPUTS: -
    """

    def __init__(self):
        self._queue = []
        self._index = 0

    def push(self, item, priority):
        """
        DOCSTRING: PUSH ITEMS TO LIST
        INPUTS: ITEM, PRIORITY
        OUTPUTS: -
        """
        heappush(self._queue, (-priority, self._index, item))
        self._index += 1

    def pop(self):
        """
        DOCSTRING: REMOVE LAST ITEM FROM LIST
        INPUTS: -
        OUTPUTS: OBJECT
        """
        return heappop(self._queue)[-1]
