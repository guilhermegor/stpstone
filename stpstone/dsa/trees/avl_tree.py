### AVL TREE ###
### REFERENCE: https://medium.com/pythoneers/master-avl-tree-in-python-7e756f72d07b ###


class CacheObliviousAVLNode:
    def __init__(self, key):
        self.key = key
        self.left = None
        self.right = None
        self.height = 1


class CacheObliviousAVLTree:
    """
    Cache-oblivious AVL trees optimize memory access patterns to improve performance across different
    levels of the memory hierarchy. By exploiting cache locality and minimizing cache misses,
    cache-oblivious AVL trees can achieve operations even without explicit knowledge og the cache
    size or cache line length
    """

    def __init__(self):
        self.root = None

    def _height(self, node):
        if not node:
            return 0
        return node.height

    def _update_height(self, node):
        node.height = 1 + max(self._height(node.left), self._height(node.right))

    def _balance_factor(self, node):
        """
        Compute the balance factor of the given node.
        The balance factor is defined as the difference in height between the left and right
        subtrees of the node.  A balance factor of zero indicates that the node is balanced.
        A positive balance factor indicates that the left subtree is higher than the right
        subtree, and a negative balance factor indicates that the right subtree is higher
        than the left subtree.
        Args:
            node: The node whose balance factor is to be computed.
        Returns:
            The balance factor of the given node.
        """
        if not node:
            return 0
        return self._height(node.left) - self._height(node.right)

    def _rotate_right(self, y):
        x = y.left
        T2 = x.right
        x.right = y
        y.left = T2
        y.height = 1 + max(self._height(y.left), self._height(y.right))
        x.height = 1 + max(self._height(x.left), self._height(x.right))
        return x

    def _rotate_left(self, x):
        y = x.right
        T2 = y.left
        y.left = x
        x.right = T2
        x.height = 1 + max(self._height(x.left), self._height(x.right))
        y.height = 1 + max(self._height(y.left), self._height(y.right))
        return y

    def _insert(self, root, key):
        if not root:
            return CacheObliviousAVLNode(key)
        elif key < root.key:
            root.left = self._insert(root.left, key)
        else:
            root.right = self._insert(root.right, key)
        root.height = 1 + max(self._height(root.left), self._height(root.right))
        balance = self._balance_factor(root)
        if balance > 1:
            if key < root.left.key:
                return self._rotate_right(root)
            else:
                root.left = self._rotate_left(root.left)
                return self._rotate_right(root)
        if balance < -1:
            if key > root.right.key:
                return self._rotate_left(root)
            else:
                root.right = self._rotate_right(root.right)
                return self._rotate_left(root)
        return root

    def insert(self, key):
        self.root = self._insert(self.root, key)
