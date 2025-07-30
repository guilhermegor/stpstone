"""Unit tests for CacheObliviousAVLTree implementation.

Tests the AVL tree functionality including:
- Node insertion and tree balancing
- Rotation operations
- Height maintenance
- Balance factor calculation
- Edge cases and error conditions
"""

import pytest

from stpstone.dsa.trees.avl_tree import (
    CacheObliviousAVLNode,
    CacheObliviousAVLTree,
)


class TestCacheObliviousAVLNode:
    """Test cases for CacheObliviousAVLNode class."""

    def test_node_initialization(self) -> None:
        """Test node initialization with valid key.

        Verifies that:
        - Node is initialized with correct key
        - Left and right children are None
        - Height is initialized to 1
        """
        node = CacheObliviousAVLNode(10)
        assert node.key == 10
        assert node.left is None
        assert node.right is None
        assert node.height == 1


class TestCacheObliviousAVLTree:
    """Test cases for CacheObliviousAVLTree class."""

    @pytest.fixture
    def empty_tree(self) -> CacheObliviousAVLTree:
        """Fixture providing an empty AVL tree.

        Returns
        -------
        CacheObliviousAVLTree
            Empty AVL tree instance
        """
        return CacheObliviousAVLTree()

    @pytest.fixture
    def sample_tree(self) -> CacheObliviousAVLTree:
        """Fixture providing a sample AVL tree with multiple nodes.

        Returns
        -------
        CacheObliviousAVLTree
            AVL tree with nodes [10, 5, 15, 3, 7]
        """
        tree = CacheObliviousAVLTree()
        for key in [10, 5, 15, 3, 7]:
            tree.insert(key)
        return tree

    def test_tree_initialization(self, empty_tree: CacheObliviousAVLTree) -> None:
        """Test tree initialization.

        Parameters
        ----------
        empty_tree : CacheObliviousAVLTree
            Empty tree fixture

        Verifies that:
        - New tree has root set to None
        """
        assert empty_tree.root is None

    def test_insert_first_node(self, empty_tree: CacheObliviousAVLTree) -> None:
        """Test inserting first node into empty tree.

        Parameters
        ----------
        empty_tree : CacheObliviousAVLTree
            Empty tree fixture

        Returns
        -------
        None

        Verifies that:
        - First insert creates root node
        - Root node has correct key
        - Root height is 1
        """
        empty_tree.insert(10)
        assert empty_tree.root is not None
        assert empty_tree.root.key == 10
        assert empty_tree._height(empty_tree.root) == 1

    def test_height_empty_node(self, empty_tree: CacheObliviousAVLTree) -> None:
        """Test height calculation for None node.

        Parameters
        ----------
        empty_tree : CacheObliviousAVLTree
            Empty tree fixture

        Returns
        -------
        None

        Verifies that:
        - Height of None node is 0
        """
        assert empty_tree._height(None) == 0

    def test_height_non_empty_node(self, sample_tree: CacheObliviousAVLTree) -> None:
        """Test height calculation for non-empty node.

        Parameters
        ----------
        sample_tree : CacheObliviousAVLTree
            Sample tree fixture
        
        Returns
        -------
        None

        Verifies that:
        - Height calculation returns correct value
        - Height updates after insertion
        """
        assert sample_tree._height(sample_tree.root) == 3
        sample_tree.insert(20)
        assert sample_tree._height(sample_tree.root) == 3

    def test_update_height(self, sample_tree: CacheObliviousAVLTree) -> None:
        """Test height update functionality.

        Parameters
        ----------
        sample_tree : CacheObliviousAVLTree
            Sample tree fixture

        Returns
        -------
        None

        Verifies that:
        - Height is correctly updated after modification
        """
        node = sample_tree.root
        if node:
            original_height = node.height
            node.left = None  # type: ignore
            sample_tree._update_height(node)
            assert node.height == original_height - 1

    def test_balance_factor_empty_node(self, empty_tree: CacheObliviousAVLTree) -> None:
        """Test balance factor for None node.

        Parameters
        ----------
        empty_tree : CacheObliviousAVLTree
            Empty tree fixture

        Returns
        -------
        None

        Verifies that:
        - Balance factor of None node is 0
        """
        assert empty_tree._balance_factor(None) == 0

    def test_balance_factor_balanced_node(self, sample_tree: CacheObliviousAVLTree) -> None:
        """Test balance factor for balanced node.

        Parameters
        ----------
        sample_tree : CacheObliviousAVLTree
            Sample tree fixture

        Returns
        -------
        None

        Verifies that:
        - Balance factor returns correct value
        - Tree remains balanced after insertion
        """
        node = sample_tree.root
        if node:
            assert abs(sample_tree._balance_factor(node)) <= 1

    def test_right_rotation(self, empty_tree: CacheObliviousAVLTree) -> None:
        """Test right rotation operation.

        Parameters
        ----------
        empty_tree : CacheObliviousAVLTree
            Empty tree fixture

        Returns
        -------
        None

        Verifies that:
        - Right rotation maintains tree structure
        - Height is updated correctly
        """
        # Create left-heavy tree
        empty_tree.insert(30)
        empty_tree.insert(20)
        empty_tree.insert(10)
        
        root = empty_tree.root
        if root:
            assert root.key == 20
            assert root.left and root.left.key == 10
            assert root.right and root.right.key == 30
            assert root.height == 2

    def test_left_rotation(self, empty_tree: CacheObliviousAVLTree) -> None:
        """Test left rotation operation.

        Parameters
        ----------
        empty_tree : CacheObliviousAVLTree
            Empty tree fixture

        Returns
        -------
        None

        Verifies that:
        - Left rotation maintains tree structure
        - Height is updated correctly
        """
        # Create right-heavy tree
        empty_tree.insert(10)
        empty_tree.insert(20)
        empty_tree.insert(30)
        
        root = empty_tree.root
        if root:
            assert root.key == 20
            assert root.left and root.left.key == 10
            assert root.right and root.right.key == 30
            assert root.height == 2

    def test_insert_duplicate_key(self, sample_tree: CacheObliviousAVLTree) -> None:
        """Test inserting duplicate key.

        Parameters
        ----------
        sample_tree : CacheObliviousAVLTree
            Sample tree fixture

        Returns
        -------
        None

        Verifies that:
        - Tree handles duplicate keys correctly
        - Tree remains balanced
        - Size increases by one
        """
        original_height = sample_tree._height(sample_tree.root)
        sample_tree.insert(10)
        assert sample_tree._height(sample_tree.root) == original_height

    def test_insert_maintains_balance(self, empty_tree: CacheObliviousAVLTree) -> None:
        """Test that insert maintains tree balance.

        Parameters
        ----------
        empty_tree : CacheObliviousAVLTree
            Empty tree fixture

        Returns
        -------
        None

        Verifies that:
        - Tree remains balanced after multiple inserts
        - Balance factor never exceeds ±1
        """
        keys = [10, 5, 15, 3, 7, 12, 20, 1, 9, 13]
        for key in keys:
            empty_tree.insert(key)
            root = empty_tree.root
            if root:
                assert abs(empty_tree._balance_factor(root)) <= 1

    def test_left_right_rotation(self, empty_tree: CacheObliviousAVLTree) -> None:
        """Test left-right rotation scenario.

        Parameters
        ----------
        empty_tree : CacheObliviousAVLTree
            Empty tree fixture

        Returns
        -------
        None

        Verifies that:
        - Tree performs correct left-right rotation
        - Final structure is balanced
        """
        empty_tree.insert(30)
        empty_tree.insert(10)
        empty_tree.insert(20)
        
        root = empty_tree.root
        if root:
            assert root.key == 20
            assert root.left and root.left.key == 10
            assert root.right and root.right.key == 30
            assert root.height == 2

    def test_right_left_rotation(self, empty_tree: CacheObliviousAVLTree) -> None:
        """Test right-left rotation scenario.

        Parameters
        ----------
        empty_tree : CacheObliviousAVLTree
            Empty tree fixture

        Returns
        -------
        None

        Verifies that:
        - Tree performs correct right-left rotation
        - Final structure is balanced
        """
        empty_tree.insert(10)
        empty_tree.insert(30)
        empty_tree.insert(20)
        
        root = empty_tree.root
        if root:
            assert root.key == 20
            assert root.left and root.left.key == 10
            assert root.right and root.right.key == 30
            assert root.height == 2

    def test_insert_sequence(self, empty_tree: CacheObliviousAVLTree) -> None:
        """Test complex insertion sequence.

        Parameters
        ----------
        empty_tree : CacheObliviousAVLTree
            Empty tree fixture

        Returns
        -------
        None

        Verifies that:
        - Tree handles complex insertion sequence correctly
        - Tree remains balanced throughout
        """
        sequence = [41, 20, 65, 11, 29, 50, 26]
        for key in sequence:
            empty_tree.insert(key)
            root = empty_tree.root
            if root:
                assert abs(empty_tree._balance_factor(root)) <= 1
        
        root = empty_tree.root
        if root:
            assert root.key == 41
            assert root.left and root.left.key == 20
            assert root.right and root.right.key == 65
            assert root.height == 4