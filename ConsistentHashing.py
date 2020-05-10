import bisect
import mmh3


class CH:
    # init the consistent node ring
    def __init__(self, nodes, replication_factor=8):
        assert len(nodes) > 0
        self.nodes = nodes
        # maximum slot numbers
        self.M = pow(2, 32)
        # number of replica vnode
        self.rep = replication_factor
        # record the location of node as a hash code on the ring
        self.nodering = []
        # record the hash-node mapping
        self.nodehash = {}
        # initiate the ring
        for node in self.nodes:
            self.add_node(node)

    # add a node to the ring
    def add_node(self, node):
        # first get the hash by node name
        _hash = mmh3.hash(str(node).encode()) % self.M
        # add hash-node mapping to dict
        self.nodehash[_hash] = node
        # add ndoe to the ring
        self.nodering.append(_hash)
        # genereate virtual nodes
        for i in range(self.rep):
            # generate hash code of vnode by name
            v_hash = mmh3.hash((str(node) + f"#{i}").encode()) % self.M
            self.nodehash[v_hash] = node
            self.nodering.append(v_hash)
        # sort the ring
        self.nodering.sort()

    # remove a node
    def remove_node(self, node):
        # store node and relative vnode in a remove list
        rmlist = []
        # get hash of the node and vnode
        _hash = mmh3.hash(str(node).encode()) % self.M
        rmlist.append(self.nodering.append(_hash))
        for i in range(self.rep):
            v_hash = mmh3.hash((str(node) + f"#{i}").encode()) % self.M
            rmlist.append(v_hash)
        # find these node and remove them from ring and dict
        for each in rmlist:
            self.nodering.remove(each)
            self.nodehash.pop(each)

    # get the node index of a provided key, as well as the next node for data replication
    def get_node(self, key):
        # get key hash
        k_hash = mmh3.hash(key) % self.M
        # find the node by binary search
        n_i = bisect.bisect_left(self.nodering, k_hash) % len(self.nodering)
        node_list = [self.nodehash[self.nodering[n_i]]]
        n_nxt = (n_i + 1) % len(self.nodering)
        # avoid same data replication in same actual node
        while self.nodehash[self.nodering[n_i]] == self.nodehash[self.nodering[n_nxt]]:
            n_nxt = (n_nxt + 1) % len(self.nodering)
        node_list.append(self.nodehash[self.nodering[n_nxt]])
        return node_list

    # pring node ring and dict
    def check(self):
        print(self.nodering)
        print(self.nodehash)
