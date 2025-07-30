"""B-Tree implementation with search, insert, and delete operations.

This module implements a B-tree data structure that maintains balanced tree structure
and guarantees O(log n) time complexity for search, insert, and delete operations.
The implementation follows standard B-tree properties where each node contains
between t-1 and 2t-1 keys (for minimum degree t).

References
----------
.. [1] https://www.geeksforgeeks.org/b-tree-in-python/
.. [2] https://github.com/msambol/dsa/blob/master/trees/b_tree.py
.. [3] https://www.youtube.com/michaelsambol
"""

from typing import Optional


class BTreeNode:
    """Node class for B-tree implementation.

    Parameters
    ----------
    is_leaf : bool
        Indicates if the node is a leaf node (default: False)
    """

    def __init__(self, is_leaf: bool = False) -> None:
        """Initialize a BTreeNode.
        
        Parameters
        ----------
        is_leaf : bool
            Indicates if the node is a leaf node (default: False)
        
        Returns
        -------
        None
        """
        self.keys: list[int] = []
        self.children: list[BTreeNode] = []
        self.is_leaf: bool = is_leaf


class BTree:
    """B-tree implementation with search, insert, and delete operations.

    Parameters
    ----------
    min_degree : int
        Minimum degree of the B-tree (defines the range of children for each node)
    """

    def __init__(self, min_degree: int) -> None:
        """Initialize a BTree.

        Parameters
        ----------
        min_degree : int
            Minimum degree of the B-tree (defines the range of children for each node)

        Returns
        -------
        None
        """
        self._validate_min_degree(min_degree)
        self.root: BTreeNode = BTreeNode(is_leaf=True)
        self.min_degree: int = min_degree

    def _validate_min_degree(self, min_degree: int) -> None:
        """Validate that the minimum degree is at least 2.

        Parameters
        ----------
        min_degree : int
            Minimum degree to validate

        Raises
        ------
        ValueError
            If min_degree is less than 2
        """
        if min_degree < 2:
            raise ValueError("Minimum degree must be at least 2")

    def search(
        self, 
        key: int, 
        node: Optional[BTreeNode] = None
    ) -> Optional[tuple[BTreeNode, int]]:
        """Search for a key in the B-tree starting from a given node.

        Parameters
        ----------
        key : int
            The key to search for
        node : Optional[BTreeNode]
            The node to start the search from (default is root)

        Returns
        -------
        Optional[tuple[BTreeNode, int]]
            tuple containing the node and index of the key if found, otherwise None
        """
        current_node = self.root if node is None else node
        i = 0
        while i < len(current_node.keys) and key > current_node.keys[i]:
            i += 1

        if i < len(current_node.keys) and key == current_node.keys[i]:
            return current_node, i
        if current_node.is_leaf:
            return None
        return self.search(key, current_node.children[i])

    def insert(self, key: int) -> None:
        """Insert a key into the B-tree.

        Parameters
        ----------
        key : int
            The key to insert
        """
        if len(self.root.keys) == (2 * self.min_degree) - 1:
            new_root = BTreeNode(is_leaf=False)
            new_root.children.append(self.root)
            self._split_child(new_root, 0)
            self.root = new_root
        self._insert_non_full(self.root, key)

    def _split_child(self, parent: BTreeNode, child_index: int) -> None:
        """Split a full child node and adjust the parent node.

        Parameters
        ----------
        parent : BTreeNode
            The parent node containing the child to split
        child_index : int
            Index of the child to split in parent's children list
        """
        full_child = parent.children[child_index]
        new_child = BTreeNode(is_leaf=full_child.is_leaf)

        # Insert median key into parent
        parent.keys.insert(child_index, full_child.keys[self.min_degree - 1])

        # Split keys between the two children
        new_child.keys = full_child.keys[self.min_degree:(2 * self.min_degree) - 1]
        full_child.keys = full_child.keys[0:self.min_degree - 1]

        # Split children if not leaf
        if not full_child.is_leaf:
            new_child.children = full_child.children[self.min_degree:2 * self.min_degree]
            full_child.children = full_child.children[0:self.min_degree]

        parent.children.insert(child_index + 1, new_child)

    def _insert_non_full(self, node: BTreeNode, key: int) -> None:
        """Insert a key into a non-full node.

        Parameters
        ----------
        node : BTreeNode
            The node to insert into
        key : int
            The key to insert
        """
        i = len(node.keys) - 1
        if node.is_leaf:
            node.keys.append(None)
            while i >= 0 and key < node.keys[i]:
                node.keys[i + 1] = node.keys[i]
                i -= 1
            node.keys[i + 1] = key
        else:
            while i >= 0 and key < node.keys[i]:
                i -= 1
            i += 1
            if len(node.children[i].keys) == (2 * self.min_degree) - 1:
                self._split_child(node, i)
                if key > node.keys[i]:
                    i += 1
            self._insert_non_full(node.children[i], key)

    def delete(self, key: int, node: Optional[BTreeNode] = None) -> None:
        """Delete a key from the B-tree.

        Parameters
        ----------
        key : int
            The key to delete
        node : Optional[BTreeNode]
            The node to start deletion from (default is root)
        """
        current_node = self.root if node is None else node
        i = 0
        while i < len(current_node.keys) and key > current_node.keys[i]:
            i += 1

        if current_node.is_leaf:
            if i < len(current_node.keys) and current_node.keys[i] == key:
                current_node.keys.pop(i)
            return

        if i < len(current_node.keys) and current_node.keys[i] == key:
            self._delete_internal_node(current_node, key, i)
        else:
            if len(current_node.children[i].keys) >= self.min_degree:
                self.delete(key, current_node.children[i])
            else:
                self._fix_child_before_delete(current_node, i)
                self.delete(key, current_node.children[i])

    def _delete_internal_node(self, node: BTreeNode, key: int, index: int) -> None:
        """Delete a key from an internal node.

        Parameters
        ----------
        node : BTreeNode
            The node containing the key to delete
        key : int
            The key to delete
        index : int
            Index of the key in the node's keys list
        """
        if node.children[index].keys >= self.min_degree:
            predecessor = self._get_predecessor(node.children[index])
            node.keys[index] = predecessor
            self.delete(predecessor, node.children[index])
        elif node.children[index + 1].keys >= self.min_degree:
            successor = self._get_successor(node.children[index + 1])
            node.keys[index] = successor
            self.delete(successor, node.children[index + 1])
        else:
            self._merge_children(node, index, index + 1)
            self.delete(key, node.children[index])

    def _fix_child_before_delete(self, node: BTreeNode, child_index: int) -> None:
        """Ensure a child node has at least min_degree keys before deletion.

        Parameters
        ----------
        node : BTreeNode
            The parent node
        child_index : int
            Index of the child in the parent's children list
        """
        if child_index > 0 and len(node.children[child_index - 1].keys) >= self.min_degree:
            self._borrow_from_left(node, child_index)
        elif (child_index < len(node.children) - 1 and 
              len(node.children[child_index + 1].keys) >= self.min_degree):
            self._borrow_from_right(node, child_index)
        else:
            if child_index < len(node.children) - 1:
                self._merge_children(node, child_index, child_index + 1)
            else:
                self._merge_children(node, child_index - 1, child_index)

    def _borrow_from_left(self, parent: BTreeNode, child_index: int) -> None:
        """Borrow a key from left sibling.

        Parameters
        ----------
        parent : BTreeNode
            The parent node
        child_index : int
            Index of the child in the parent's children list
        """
        child = parent.children[child_index]
        left_sibling = parent.children[child_index - 1]

        child.keys.insert(0, parent.keys[child_index - 1])
        parent.keys[child_index - 1] = left_sibling.keys.pop()

        if not left_sibling.is_leaf:
            child.children.insert(0, left_sibling.children.pop())

    def _borrow_from_right(self, parent: BTreeNode, child_index: int) -> None:
        """Borrow a key from right sibling.

        Parameters
        ----------
        parent : BTreeNode
            The parent node
        child_index : int
            Index of the child in the parent's children list
        """
        child = parent.children[child_index]
        right_sibling = parent.children[child_index + 1]

        child.keys.append(parent.keys[child_index])
        parent.keys[child_index] = right_sibling.keys.pop(0)

        if not right_sibling.is_leaf:
            child.children.append(right_sibling.children.pop(0))

    def _merge_children(self, parent: BTreeNode, left_index: int, right_index: int) -> None:
        """Merge two child nodes.

        Parameters
        ----------
        parent : BTreeNode
            The parent node
        left_index : int
            Index of left child in parent's children list
        right_index : int
            Index of right child in parent's children list
        """
        left_child = parent.children[left_index]
        right_child = parent.children[right_index]

        left_child.keys.append(parent.keys.pop(left_index))
        left_child.keys.extend(right_child.keys)
        left_child.children.extend(right_child.children)

        parent.children.pop(right_index)

        if parent == self.root and not parent.keys:
            self.root = left_child

    def _get_predecessor(self, node: BTreeNode) -> int:
        """Get predecessor key for deletion.

        Parameters
        ----------
        node : BTreeNode
            The node to find predecessor in

        Returns
        -------
        int
            The predecessor key
        """
        while not node.is_leaf:
            node = node.children[-1]
        return node.keys[-1]

    def _get_successor(self, node: BTreeNode) -> int:
        """Get successor key for deletion.

        Parameters
        ----------
        node : BTreeNode
            The node to find successor in

        Returns
        -------
        int
            The successor key
        """
        while not node.is_leaf:
            node = node.children[0]
        return node.keys[0]

    def print_tree(self, node: Optional[BTreeNode] = None, level: int = 0) -> None:
        """Print the structure of the B-tree.

        Parameters
        ----------
        node : Optional[BTreeNode]
            The node to start printing from (default is root)
        level : int
            Current level in the tree (default: 0)
        """
        current_node = self.root if node is None else node
        print(f"Level {level}: {' '.join(str(k) for k in current_node.keys)}")
        if not current_node.is_leaf:
            for child in current_node.children:
                self.print_tree(child, level + 1)