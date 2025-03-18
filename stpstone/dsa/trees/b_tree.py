### CLASS B-TREE ###

from typing import List, Optional, Tuple


class Node:

    def __init__(self, bl_leaf:bool = False) -> None:
        """
        DOCSTRING: INITIALIZE A NODE FOR THE B-TREE
        INPUT: LEAF (BOOL): INDICATES IF THE NODE IS A LEAF
        OUTPUT: NONE
        """
        self.keys:List[int] = []
        self.children:List['Node'] = []
        self.bl_leaf:bool = bl_leaf


class BTree:

    def __init__(self, int_t:int) -> None:
        """
        REFERENCES:
            https://www.geeksforgeeks.org/b-tree-in-python/
            https://github.com/msambol/dsa/blob/master/trees/b_tree.py
            https://www.youtube.com/michaelsambol
        DOCSTRING: INITIALIZE A B-TREE WITH A GIVEN MINIMUM DEGREE
        INPUT: T (INT): MINIMUM DEGREE OF THE B-TREE (DEFINES THE RANGE OF CHILDREN FOR EACH NODE)
        OUTPUT: NONE
        """
        self.root:Node = Node(True)
        self.int_t:int = int_t

    def search(self, int_key:int, node:Optional[Node]=None) -> Optional[Tuple[Node, int]]:
        """
        DOCSTRING: SEARCH FOR A KEY IN THE B-TREE STARTING FROM A GIVEN NODE
        INPUT:
            - KEY (INT): THE KEY TO SEARCH FOR
            - NODE (OPTIONAL[NODE]): THE NODE TO START THE SEARCH FROM (DEFAULT IS THE ROOT)
        OUTPUT:
            OPTIONAL[TUPLE[NODE, INT]]: THE NODE AND INDEX OF THE KEY IF FOUND, OTHERWISE NONE
        """
        node = self.root if node is None else node
        int_i = 0
        while int_i < len(node.keys) and int_key > node.keys[int_i]:
            int_i += 1
        if int_i < len(node.keys) and int_key == node.keys[int_i]:
            return node, int_i
        elif node.bl_leaf:
            return None
        else:
            return self.search(int_key, node.children[int_i])

    def split_child(self, node_x:Node, int_i:int) -> None:
        """
        DOCSTRING: SPLIT A FULL CHILD NODE INTO TWO AND ADJUST THE PARENT NODE
        INPUT:
            - X (NODE): THE PARENT NODE
            - I (INT): THE INDEX OF THE CHILD TO SPLIT
        OUTPUT: NONE
        """
        # y is a full child of node_x
        node_y = node_x.children[int_i]
        # create a new node and add it to node_x's list of children
        node_z = Node(node_y.bl_leaf)
        node_x.children.insert(int_i + 1, node_z)
        # insert the median of the full child y into node_x
        node_x.keys.insert(int_i, node_y.keys[self.int_t - 1])
        # split apart y's keys into y & z
        node_z.keys = node_y.keys[self.int_t:(2 * self.int_t) - 1]
        node_y.keys = node_y.keys[0:self.int_t - 1]
        # if y is not a leaf, we reassign y's children to y & z
        if not node_y.bl_leaf:
            node_z.children = node_y.children[self.int_t:2 * self.int_t]
            node_y.children = node_y.children[0:self.int_t]

    def insert(self, int_k:int) -> None:
        """
        DOCSTRING: INSERT A KEY INTO THE B-TREE
        INPUT:
            - K (INT): THE KEY TO INSERT
        OUTPUT: NONE
        """
        # if root is full, create a new node - tree's height grows by 1
        if len(self.root.keys) == (2 * self.int_t) - 1:
            new_root = Node()
            self.root = new_root
            new_root.children.insert(0, self.root)
            self.split_child(new_root, 0)
            self.insert_non_full(new_root, int_k)
        else:
            self.insert_non_full(self.root, int_k)

    def insert_non_full(self, node_x:Node, int_k:int) -> None:
        """
        DOCSTRING: INSERT A KEY INTO A NODE THAT IS NOT FULL.
        INPUT:
            X (NODE): THE NODE TO INSERT THE KEY INTO.
            K (INT): THE KEY TO INSERT.
        OUTPUT:
            NONE
        """
        int_i = len(node_x.keys) - 1
        # find the correct spot in the leaf to insert the key
        if node_x.bl_leaf:
            #   placeholder
            node_x.keys.append(None)
            while int_i >= 0 and int_k < node_x.keys[int_i]:
                node_x.keys[int_i + 1] = node_x.keys[int_i]
                int_i -= 1
            node_x.keys[int_i + 1] = int_k
        # if not a leaf, find the correct subtree to insert the key
        else:
            while int_i >= 0 and int_k < node_x.keys[int_i]:
                int_i -= 1
            int_i += 1
            #   if child node is full, split it
            if len(node_x.children[int_i].keys) == (2 * self.int_t) - 1:
                self.split_child(node_x, int_i)
                if int_k > node_x.keys[int_i]:
                    int_i += 1
            self.insert_non_full(node_x.children[int_i], int_k)

    def delete(self, node_x:Node, int_k:int) -> None:
        """
        DOCSTRING: DELETE A KEY FROM THE B-TREE STARTING FROM A GIVEN NODE.
        INPUT:
            - X (NODE): THE NODE TO START THE DELETION FROM
            - K (INT): THE KEY TO DELETE
        OUTPUT:
            NONE
        """
        int_i = 0
        while int_i < len(node_x.keys) and int_k > node_x.keys[int_i]:
            int_i += 1
        if node_x.leaf:
            if int_i < len(node_x.keys) and node_x.keys[int_i] == int_k:
                node_x.keys.pop(int_i)
            return
        if int_i < len(node_x.keys) and node_x.keys[int_i] == int_k:
            return self.delete_internal_node(node_x, int_k, int_i)
        elif len(node_x.children[int_i].keys) >= self.self.t:
            self.delete(node_x.children[int_i], int_k)
        else:
            if int_i != 0 and int_i + 2 < len(node_x.children):
                if len(node_x.children[int_i - 1].keys) >= self.self.t:
                    self.delete_sibling(node_x, int_i, int_i - 1)
                elif len(node_x.children[int_i + 1].keys) >= self.self.t:
                    self.delete_sibling(node_x, int_i, int_i + 1)
                else:
                    self.delete_merge(node_x, int_i, int_i + 1)
            elif int_i == 0:
                if len(node_x.children[int_i + 1].keys) >= self.self.t:
                    self.delete_sibling(node_x, int_i, int_i + 1)
                else:
                    self.delete_merge(node_x, int_i, int_i + 1)
            elif int_i + 1 == len(node_x.children):
                if len(node_x.children[int_i - 1].keys) >= self.self.t:
                    self.delete_sibling(node_x, int_i, int_i - 1)
                else:
                    self.delete_merge(node_x, int_i, int_i - 1)
            self.delete(node_x.children[int_i], int_k)

    def delete_internal_node(self, node_x:Node, node_k:None, int_i:int) -> None:
        """
        DOCSTRING: DELETE INTERNAL NODE
        INPUTS:
            - X (NODE): THE NODE TO START THE DELETION FROM
            - K (INT): THE KEY TO DELETE
            - I (INT): THE INDEX OF THE KEY TO DELETE
        OUTPUTS:
        """
        if node_x.leaf:
            if node_x.keys[int_i] == node_k:
                node_x.keys.pop(int_i)
            return

        if len(node_x.children[int_i].keys) >= self.t:
            node_x.keys[int_i] = self.delete_predecessor(node_x.children[int_i])
            return
        elif len(node_x.children[int_i + 1].keys) >= self.t:
            node_x.keys[int_i] = self.delete_successor(node_x.children[int_i + 1])
            return
        else:
            self.delete_merge(node_x, int_i, int_i + 1)
            self.delete_internal_node(node_x.children[int_i], node_k, self.self.t - 1)

    def delete_predecessor(self, node_x:None) -> None:
        """
        DOCSTRING: DELETE PREDECESSOR
        INPUTS:
            - X (NODE): THE NODE TO START THE DELETION FROM
        OUTPUTS: NONE
        """
        if node_x.leaf:
            return node_x.keys.pop()
        n = len(node_x.keys) - 1
        if len(node_x.children[n].keys) >= self.self.t:
            self.delete_sibling(node_x, n + 1, n)
        else:
            self.delete_merge(node_x, n, n + 1)
        self.delete_predecessor(node_x.children[n])

    def delete_successor(self, node_x:Node) -> None:
        """
        DOCSTRING: DELETE SUCCESSOR
        INPUTS:
            - X (NODE): THE NODE TO START THE DELETION FROM
        OUTPUTS: NONE
        """
        if node_x.leaf:
            return node_x.keys.pop(0)
        if len(node_x.children[1].keys) >= self.self.t:
            self.delete_sibling(node_x, 0, 1)
        else:
            self.delete_merge(node_x, 0, 1)
        self.delete_successor(node_x.children[0])

    def delete_merge(self, node_x:Node, int_i:int, int_j:int) -> None:
        """
        DOCSTRING: MERGE TWO NODES
        INPUTS:
            - X (NODE): THE NODE TO START THE MERGE FROM
            - I (INT): THE INDEX OF THE NODE TO MERGE WITH
            - J (INT): THE INDEX OF THE NODE TO MERGE WITH
        OUTPUTS: NONE
        """
        cnode = node_x.children[int_i]
        if int_j > int_i:
            rsnode = node_x.children[int_j]
            cnode.keys.append(node_x.keys[int_i])
            for node_k in range(len(rsnode.keys)):
                cnode.keys.append(rsnode.keys[node_k])
                if len(rsnode.children) > 0:
                    cnode.children.append(rsnode.children[node_k])
            if len(rsnode.children) > 0:
                cnode.children.append(rsnode.children.pop())
            new = cnode
            node_x.keys.pop(int_i)
            node_x.children.pop(int_j)
        else:
            lsnode = node_x.children[int_j]
            lsnode.keys.append(node_x.keys[int_j])
            for int_i in range(len(cnode.keys)):
                lsnode.keys.append(cnode.keys[int_i])
                if len(lsnode.children) > 0:
                    lsnode.children.append(cnode.children[int_i])
            if len(lsnode.children) > 0:
                lsnode.children.append(cnode.children.pop())
            new = lsnode
            node_x.keys.pop(int_j)
            node_x.children.pop(int_i)
        if node_x == self.root and len(node_x.keys) == 0:
            self.root = new

    def delete_sibling(self, node_x:Node, int_i:int, int_j:int) -> None:
        """
        DOCSTRING: DELETE A SIBLING NODE
        INPUTS:
            - X (NODE): THE NODE TO START THE DELETION FROM
            - I (INT): THE INDEX OF THE KEY TO DELETE
            - J (INT): THE INDEX OF THE SIBLING NODE TO MERGE WITH
        OUTPUTS: NONE
        """
        cnode = node_x.children[int_i]
        if int_i < int_j:
            rsnode = node_x.children[int_j]
            cnode.keys.append(node_x.keys[int_i])
            node_x.keys[int_i] = rsnode.keys[0]
            if len(rsnode.children) > 0:
                cnode.children.append(rsnode.children[0])
                rsnode.children.pop(0)
            rsnode.keys.pop(0)
        else:
            lsnode = node_x.children[int_j]
            cnode.keys.insert(0, node_x.keys[int_i - 1])
            node_x.keys[int_i - 1] = lsnode.keys.pop()
            if len(lsnode.children) > 0:
                cnode.children.insert(0, lsnode.children.pop())

    def print_tree(self, node_x: Node, int_level:int = 0) -> None:
        """
        DOCSTRING: PRINT THE STRUCTURE OF THE B-TREE.
        INPUT:
            X (NODE): THE NODE TO START PRINTING FROM.
            LEVEL (INT): THE CURRENT LEVEL IN THE TREE.
        OUTPUT:
            NONE
        """
        print(f'Level {int_level}', end=": ")
        for int_i in node_x.keys:
            print(int_i, end=" ")
        print()
        int_level += 1
        if len(node_x.children) > 0:
            for child in node_x.children:
                self.print_tree(child, int_level)
