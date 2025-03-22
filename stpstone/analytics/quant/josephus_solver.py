from typing import List, Any
from stpstone.dsa.queues.simple_queue import Queue


class JosephusSolver:
    """
    A class to solve the Josephus problem, which simulates an elimination process
    where every k-th participant is removed until only one survivor remains.
    """

    def __init__(self, list_: List[Any], inst_steps: int) -> None:
        self.list_ = list_
        self.inst_steps = inst_steps

    @property
    def last_survivor(self) -> Any:
        cls_queue = Queue()
        for item in self.list_:
            cls_queue.enqueue(item)
        while cls_queue.size > 1:
            for _ in range(self.inst_steps):
                cls_queue.enqueue(cls_queue.dequeue)
            cls_queue.dequeue
        return cls_queue.dequeue
