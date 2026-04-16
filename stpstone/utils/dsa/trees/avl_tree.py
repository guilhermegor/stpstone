"""Cache-oblivious AVL tree implementation.

This module implements a self-balancing binary search tree optimized for cache performance
without explicit knowledge of cache parameters. The AVL tree maintains balance through
rotations during insertions to ensure O(log n) time complexity for operations.

References
----------
.. [1] https://medium.com/pythoneers/master-avl-tree-in-python-7e756f72d07b
"""

from typing import Optional

from stpstone.transformations.validation.metaclass_type_checker import TypeChecker


class CacheObliviousAVLNode(metaclass=TypeChecker):
	"""Node class for cache-oblivious AVL tree implementation.

	Parameters
	----------
	key : int
		The key value stored in the node
	"""

	def __init__(self, key: int) -> None:
		"""Initialize a new node.

		Parameters
		----------
		key : int
			The key value stored in the node

		Returns
		-------
		None
		"""
		self.key = key
		self.left: Optional[CacheObliviousAVLNode] = None
		self.right: Optional[CacheObliviousAVLNode] = None
		self.height: int = 1


class CacheObliviousAVLTree(metaclass=TypeChecker):
	"""Cache-oblivious AVL tree implementation.

	Cache-oblivious AVL trees optimize memory access patterns to improve performance across
	different levels of the memory hierarchy. By exploiting cache locality and minimizing
	cache misses, cache-oblivious AVL trees can achieve efficient operations without explicit
	knowledge of the cache size or cache line length.
	"""

	def __init__(self) -> None:
		"""Initialize an empty AVL tree."""
		self.root: Optional[CacheObliviousAVLNode] = None

	def _height(self, node: Optional[CacheObliviousAVLNode]) -> int:
		"""Get the height of a node.

		Parameters
		----------
		node : Optional[CacheObliviousAVLNode]
			The node to check height for

		Returns
		-------
		int
			Height of the node (0 if None)
		"""
		if not node:
			return 0
		return node.height

	def _update_height(self, node: CacheObliviousAVLNode) -> None:
		"""Update the height of a node based on its children.

		Parameters
		----------
		node : CacheObliviousAVLNode
			The node to update
		"""
		node.height = 1 + max(self._height(node.left), self._height(node.right))

	def _balance_factor(self, node: Optional[CacheObliviousAVLNode]) -> int:
		"""Compute the balance factor of the given node.

		The balance factor is defined as the difference in height between the left and right
		subtrees of the node. A balance factor of zero indicates that the node is balanced.
		A positive balance factor indicates that the left subtree is higher than the right
		subtree, and a negative balance factor indicates that the right subtree is higher
		than the left subtree.

		Parameters
		----------
		node : Optional[CacheObliviousAVLNode]
			The node whose balance factor is to be computed

		Returns
		-------
		int
			The balance factor of the given node
		"""
		if not node:
			return 0
		return self._height(node.left) - self._height(node.right)

	def _rotate_right(self, y: CacheObliviousAVLNode) -> CacheObliviousAVLNode:
		"""Perform a right rotation around the given node.

		Parameters
		----------
		y : CacheObliviousAVLNode
			The node to rotate around

		Returns
		-------
		CacheObliviousAVLNode
			The new root node after rotation
		"""
		x = y.left
		T2 = x.right
		x.right = y
		y.left = T2
		self._update_height(y)
		self._update_height(x)
		return x

	def _rotate_left(self, x: CacheObliviousAVLNode) -> CacheObliviousAVLNode:
		"""Perform a left rotation around the given node.

		Parameters
		----------
		x : CacheObliviousAVLNode
			The node to rotate around

		Returns
		-------
		CacheObliviousAVLNode
			The new root node after rotation
		"""
		y = x.right
		T2 = y.left
		y.left = x
		x.right = T2
		self._update_height(x)
		self._update_height(y)
		return y

	def _insert(self, root: Optional[CacheObliviousAVLNode], key: int) -> CacheObliviousAVLNode:
		"""Insert a key into the subtree rooted at the given node.

		Parameters
		----------
		root : Optional[CacheObliviousAVLNode]
			The root of the subtree to insert into
		key : int
			The key to insert

		Returns
		-------
		CacheObliviousAVLNode
			The new root of the subtree after insertion and balancing
		"""
		if not root:
			return CacheObliviousAVLNode(key)

		if key < root.key:
			root.left = self._insert(root.left, key)
		else:
			root.right = self._insert(root.right, key)

		self._update_height(root)
		balance = self._balance_factor(root)

		if balance > 1:
			if key < root.left.key:
				return self._rotate_right(root)
			root.left = self._rotate_left(root.left)
			return self._rotate_right(root)

		if balance < -1:
			if key > root.right.key:
				return self._rotate_left(root)
			root.right = self._rotate_right(root.right)
			return self._rotate_left(root)

		return root

	def insert(self, key: int) -> None:
		"""Insert a key into the AVL tree.

		Parameters
		----------
		key : int
			The key to insert into the tree
		"""
		self.root = self._insert(self.root, key)
