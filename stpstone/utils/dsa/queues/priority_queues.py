"""Priority queue implementation using heapq.

This module provides a priority queue implementation that supports
pushing items with priorities and popping the highest priority item.
Uses Python's heapq module internally for efficient operations.

References
----------
.. [1] https://docs.python.org/3/library/heapq.html
"""

from heapq import heappop, heappush
from typing import Generic, TypeVar

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


T = TypeVar("T")


class PriorityQueue(Generic[T], metaclass=TypeChecker):
	"""Priority queue implementation using heapq.

	Attributes
	----------
	_queue : list[tuple[float, int, T]]
		Internal heap storage
	_index : int
		Counter to break ties when priorities are equal
	"""

	def __init__(self) -> None:
		"""Initialize empty priority queue.

		Returns
		-------
		None
		"""
		self._queue: list[tuple[float, int, T]] = []
		self._index: int = 0
		self._entry_finder: dict[tuple[T, int], float] = {}
		self._item_to_latest_index: dict[T, int] = {}

	def push(self, item: T, priority: float, update_priority: bool = False) -> None:
		"""Push an item into the queue with given priority.

		Parameters
		----------
		item : T
			Item to be pushed into the queue
		priority : float
			Priority of the item (higher values come out first)
		update_priority : bool
			Whether to update the priority of the item if it's already in the queue

		Returns
		-------
		None

		Notes
		-----
		Internally stores (-priority, index, item) to create a max-heap
		from heapq's min-heap implementation.
		"""
		self._index += 1
		if update_priority and item in self._item_to_latest_index:
			# mark the previous entry as invalid by removing it from entry_finder
			old_index = self._item_to_latest_index[item]
			self._entry_finder.pop((item, old_index), None)
		# track the latest index for this item if updating
		if update_priority:
			self._item_to_latest_index[item] = self._index
		# store the new entry
		self._entry_finder[(item, self._index)] = priority
		heappush(self._queue, (-priority, self._index, item))

	def pop(self) -> T:
		"""Pop the highest priority item from the queue.

		Returns
		-------
		T
			The highest priority item

		Raises
		------
		IndexError
			If the queue is empty when pop is called
		"""
		while self._queue:
			priority, index, item = heappop(self._queue)
			# check if this entry is still valid
			entry_key = (item, index)
			# skip outdated or invalid entries
			if entry_key in self._entry_finder:
				# remove the entry from entry_finder and item_to_latest_index
				del self._entry_finder[entry_key]
				if (
					item in self._item_to_latest_index
					and self._item_to_latest_index[item] == index
				):
					del self._item_to_latest_index[item]
				return item
		raise IndexError("pop from empty priority queue")

	def __len__(self) -> int:
		"""Return the number of valid items in the queue.

		Returns
		-------
		int
			Number of valid items
		"""
		return len(self._entry_finder)
