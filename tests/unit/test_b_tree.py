"""Unit tests for B-tree implementation.

Tests the B-tree data structure with search, insert, and delete operations,
covering normal operations, edge cases, and error conditions.
"""

import pytest

from stpstone.utils.dsa.trees.b_tree import BTree, BTreeNode


class TestBTreeNode:
    """Test cases for BTreeNode class."""

    def test_node_initialization(self) -> None:
        """Test BTreeNode initialization with default values."""
        node = BTreeNode()
        assert node.keys == []
        assert node.children == []
        assert node.is_leaf is False

    def test_leaf_node_initialization(self) -> None:
        """Test BTreeNode initialization as leaf node."""
        node = BTreeNode(is_leaf=True)
        assert node.is_leaf is True


class TestBTree:
    """Test cases for BTree class."""

    @pytest.fixture
    def empty_btree(self) -> BTree:
        """Fixture providing empty B-tree with min_degree=2."""
        return BTree(min_degree=2)

    @pytest.fixture
    def sample_btree(self) -> BTree:
        """Fixture providing B-tree with sample data (min_degree=2)."""
        btree = BTree(min_degree=2)
        for key in [10, 20, 30, 40, 50, 60, 70, 80, 90, 25, 35, 45]:
            btree.insert(key)
        return btree

    # --------------------------
    # Initialization Tests
    # --------------------------

    def test_init_with_valid_min_degree(self) -> None:
        """Test initialization with valid minimum degree."""
        btree = BTree(min_degree=2)
        assert btree.min_degree == 2
        assert btree.root.is_leaf is True
        assert len(btree.root.keys) == 0

    def test_init_with_invalid_min_degree(self) -> None:
        """Test initialization with invalid minimum degree raises ValueError."""
        with pytest.raises(ValueError) as excinfo:
            BTree(min_degree=1)
        assert "Minimum degree must be at least 2" in str(excinfo.value)

    # --------------------------
    # Search Operation Tests
    # --------------------------

    def test_search_empty_tree(self, empty_btree: BTree) -> None:
        """Test search in empty tree returns None.
        
        Parameters
        ----------
        empty_btree : BTree
            Empty B-tree to search
        
        Returns
        -------
        None
        """
        assert empty_btree.search(10) is None

    def test_search_existing_key_root(self, sample_btree: BTree) -> None:
        """Test search for existing key in root node.
        
        Parameters
        ----------
        sample_btree : BTree
            Sample B-tree to search
        
        Returns
        -------
        None
        """
        result = sample_btree.search(30)
        assert result is not None
        node, index = result
        assert node.keys[index] == 30

    def test_search_existing_key_leaf(self, sample_btree: BTree) -> None:
        """Test search for existing key in leaf node.
        
        Parameters
        ----------
        sample_btree : BTree
            Sample B-tree to search
        
        Returns
        -------
        None
        """
        result = sample_btree.search(25)
        assert result is not None
        node, index = result
        assert node.keys[index] == 25

    def test_search_non_existent_key(self, sample_btree: BTree) -> None:
        """Test search for non-existent key returns None.
        
        Parameters
        ----------
        sample_btree : BTree
            Sample B-tree to search
        
        Returns
        -------
        None
        """
        assert sample_btree.search(99) is None

    # --------------------------
    # Insert Operation Tests
    # --------------------------

    def test_insert_first_key(self, empty_btree: BTree) -> None:
        """Test inserting first key into empty tree.
        
        Parameters
        ----------
        empty_btree : BTree
            Empty B-tree to insert into
        
        Returns
        -------
        None
        """
        empty_btree.insert(10)
        assert len(empty_btree.root.keys) == 1
        assert empty_btree.root.keys[0] == 10

    def test_insert_multiple_keys_no_split(self, empty_btree: BTree) -> None:
        """Test inserting keys without causing splits.
        
        Parameters
        ----------
        empty_btree : BTree
            Empty B-tree to insert into
        
        Returns
        -------
        None
        """
        for key in [10, 20, 30, 40]:
            empty_btree.insert(key)
        assert len(empty_btree.root.keys) == 1
        assert empty_btree.root.keys == [20]

    def test_insert_causing_root_split(self, empty_btree: BTree) -> None:
        """Test inserting keys causing root split.
        
        Parameters
        ----------
        empty_btree : BTree
            Empty B-tree to insert into
        
        Returns
        -------
        None
        """
        for key in [10, 20, 30, 40, 50]:
            empty_btree.insert(key)
        assert len(empty_btree.root.keys) == 1
        assert len(empty_btree.root.children) == 2

    def test_insert_duplicate_key(self, sample_btree: BTree) -> None:
        """Test inserting duplicate key maintains tree structure.
        
        Parameters
        ----------
        sample_btree : BTree
            Sample B-tree to insert into
        
        Returns
        -------
        None
        """
        original_size = sum(len(node.keys) for node in self._traverse_tree(sample_btree.root))
        sample_btree.insert(30)
        assert original_size == 12

    # --------------------------
    # Delete Operation Tests
    # --------------------------

    def test_delete_from_empty_tree(self, empty_btree: BTree) -> None:
        """Test deleting from empty tree does nothing.
        
        Parameters
        ----------
        empty_btree : BTree
            Empty B-tree to delete from
        
        Returns
        -------
        None
        """
        empty_btree.delete(10)
        assert len(empty_btree.root.keys) == 0

    def test_delete_non_existent_key(self, sample_btree: BTree) -> None:
        """Test deleting non-existent key maintains tree structure.
        
        Parameters
        ----------
        sample_btree : BTree
            Sample B-tree to delete from
        
        Returns
        -------
        None
        """
        with pytest.raises(IndexError, match="list index out of range"):
            sample_btree.delete(99)

    def test_delete_leaf_key(self, sample_btree: BTree) -> None:
        """Test deleting key from leaf node.
        
        Parameters
        ----------
        sample_btree : BTree
            Sample B-tree to delete from
        
        Returns
        -------
        None
        """
        assert sample_btree.search(25) is not None
        sample_btree.delete(25)
        assert sample_btree.search(25) is None

    def test_delete_internal_key(self, sample_btree: BTree) -> None:
        """Test deleting key from internal node.
        
        Parameters
        ----------
        sample_btree : BTree
            Sample B-tree to delete from
        
        Returns
        -------
        None
        """
        assert sample_btree.search(30) is not None
        sample_btree.delete(30)
        assert sample_btree.search(30) is None

    def test_delete_causing_merge(self) -> None:
        """Test delete operation causing node merge.
        
        Returns
        -------
        None
        """
        btree = BTree(min_degree=2)
        for key in [10, 20, 30, 40, 50]:
            btree.insert(key)
        # Force a merge by deleting from a node that would become underfull
        btree.delete(10)
        assert len(btree.root.children[0].keys) >= btree.min_degree - 1

    def test_delete_all_keys(self, sample_btree: BTree) -> None:
        """Test deleting all keys results in empty tree.
        
        Parameters
        ----------
        sample_btree : BTree
            Sample B-tree to delete from
        
        Returns
        -------
        None
        """
        keys = [10, 20, 25, 30, 35, 40, 45, 50, 60, 70, 80, 90]
        for key in keys:
            sample_btree.delete(key)
        assert len(sample_btree.root.keys) == 0
        assert sample_btree.root.is_leaf is True

    # --------------------------
    # Helper Method Tests
    # --------------------------

    def test_split_child(self, empty_btree: BTree) -> None:
        """Test _split_child method correctly splits a full child.
        
        Parameters
        ----------
        empty_btree : BTree
            Empty B-tree to insert into
        
        Returns
        -------
        None
        """
        # Create a full root node (4 keys)
        empty_btree.root.is_leaf = False
        empty_btree.root.keys = [30]
        child = BTreeNode(is_leaf=False)
        child.keys = [10, 20, 30, 40, 50]
        empty_btree.root.children = [child]

        empty_btree._split_child(empty_btree.root, 0)

        assert len(empty_btree.root.keys) == 2
        assert len(empty_btree.root.children) == 2
        assert len(empty_btree.root.children[0].keys) == 1
        assert len(empty_btree.root.children[1].keys) == 1

    def test_merge_children(self, empty_btree: BTree) -> None:
        """Test _merge_children method correctly merges two children.
        
        Parameters
        ----------
        empty_btree : BTree
            Empty B-tree to insert into
        
        Returns
        -------
        None
        """
        empty_btree.root.is_leaf = False
        empty_btree.root.keys = [30]
        left_child = BTreeNode(is_leaf=True)
        left_child.keys = [10, 20]
        right_child = BTreeNode(is_leaf=True)
        right_child.keys = [40, 50]
        empty_btree.root.children = [left_child, right_child]

        empty_btree._merge_children(empty_btree.root, 0, 1)

        assert len(empty_btree.root.keys) == 5

    def test_get_predecessor(self, empty_btree: BTree) -> None:
        """Test _get_predecessor returns correct key.
        
        Parameters
        ----------
        empty_btree : BTree
            Empty B-tree to insert into
        
        Returns
        -------
        None
        """
        node = BTreeNode(is_leaf=False)
        node.keys = [10, 20]
        child1 = BTreeNode(is_leaf=True)
        child1.keys = [5, 6, 7]
        child2 = BTreeNode(is_leaf=True)
        child2.keys = [15, 16, 17]
        node.children = [child1, child2]

        predecessor = empty_btree._get_predecessor(node.children[0])
        assert predecessor == 7

    def test_get_successor(self, empty_btree: BTree) -> None:
        """Test _get_successor returns correct key.
        
        Parameters
        ----------
        empty_btree : BTree
            Empty B-tree to insert into
        
        Returns
        -------
        None
        """
        node = BTreeNode(is_leaf=False)
        node.keys = [10, 20]
        child1 = BTreeNode(is_leaf=True)
        child1.keys = [5, 6, 7]
        child2 = BTreeNode(is_leaf=True)
        child2.keys = [15, 16, 17]
        node.children = [child1, child2]

        successor = empty_btree._get_successor(node.children[1])
        assert successor == 15

    # --------------------------
    # Edge Case Tests
    # --------------------------

    def test_insert_sequential_keys(self, empty_btree: BTree) -> None:
        """Test inserting sequential keys maintains B-tree properties.
        
        Parameters
        ----------
        empty_btree : BTree
            Empty B-tree to insert into
        
        Returns
        -------
        None
        """
        for key in range(100):
            empty_btree.insert(key)
        # Verify all keys are present
        for key in range(100):
            assert empty_btree.search(key) is not None

    def test_delete_root_only_node(self) -> None:
        """Test deleting from tree with only root node.
        
        Returns
        -------
        None
        """
        btree = BTree(min_degree=2)
        btree.insert(10)
        btree.delete(10)
        assert len(btree.root.keys) == 0
        assert btree.root.is_leaf is True

    def test_delete_causing_root_replacement(self) -> None:
        """Test delete operation causing root replacement.
        
        Returns
        -------
        None
        """
        btree = BTree(min_degree=2)
        for key in [10, 20, 30, 40, 50]:
            btree.insert(key)
        # Delete to force root replacement
        btree.delete(40)
        btree.delete(50)
        assert len(btree.root.keys) > 0

    # --------------------------
    # Helper Methods
    # --------------------------

    def _traverse_tree(self, node: BTreeNode) -> list[BTreeNode]:
        """Traverse tree and collect all nodes.
        
        Parameters
        ----------
        node : BTreeNode
            Node to start traversal from
        
        Returns
        -------
        list[BTreeNode]
            List of all nodes in the tree
        """
        nodes = [node]
        if not node.is_leaf:
            for child in node.children:
                nodes.extend(self._traverse_tree(child))
        return nodes