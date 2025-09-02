"""Module for solving the Josephus problem using a queue.

The Josephus problem simulates an elimination process where every k-th
participant is removed until only one survivor remains.
"""

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker
from stpstone.utils.dsa.queues.simple_queue import Queue


class JosephusSolver(metaclass=TypeChecker):
    """
    A class to solve the Josephus problem.

    The Josephus problem simulates an elimination process where every k-th
    participant is removed until only one survivor remains.

    Parameters
    ----------
    list_ : list[object]
        List of participants in the Josephus problem.
    step_interval : int
        Step interval for elimination (k-th participant to be removed).
    
    Raises
    ------
    TypeError
        If list_ is not a list or step_interval is not an integer.
    ValueError
        If step_interval is not positive.
    """

    def __init__(self, list_: list[object], step_interval: int) -> None:
        """Initialize the JosephusSolver object.
        
        Parameters
        ----------
        list_ : list[object]
            List of participants in the Josephus problem.
        step_interval : int
            Step interval for elimination (k-th participant to be removed).
        
        Raises
        ------
        TypeError
            If list_ is not a list or step_interval is not an integer.
        ValueError
            If step_interval is not positive.
        """
        if not isinstance(list_, list):
            raise TypeError("list_ must be a list")
        if not isinstance(step_interval, int):
            raise TypeError("Step interval must be an integer")
        if step_interval <= 0:
            raise ValueError("Step interval must be positive")
        self.list_ = list_
        self.step_interval = step_interval

    def solve(self) -> object:
        """Solve the Josephus problem and return the last surviving participant.

        Returns
        -------
        object
            The last remaining participant from the initial list.

        Raises
        ------
        ValueError
            If the list is empty.

        References
        ----------
        .. [1] https://en.wikipedia.org/wiki/Josephus_problem
        """
        if not self.list_:
            raise ValueError("Cannot solve with empty list")
        
        cls_queue = Queue()
        for item in self.list_:
            cls_queue.enqueue(item)
        
        while cls_queue.size > 1:
            for _ in range(self.step_interval - 1):
                cls_queue.enqueue(cls_queue.dequeue())
            cls_queue.dequeue()
            
        return cls_queue.dequeue()