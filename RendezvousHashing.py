import mmh3
# node with their sockets
from server_config import NODES


class HRW:
    # creator of ring
    def __init__(self, nodes):
        assert len(nodes) > 0
        self.nodes = nodes

    # get node's socket by index
    def get_node(self, key_hex):
        # the md5 hash was digest by hex, so convert to int base on 16
        key = int(key_hex, 16)
        # get the node index by modulus
        node_index = key % len(self.nodes)
        # return the socket of the node
        return self.nodes[node_index]

    def hrw(self, key_hex):
        weights = [mmh3.hash(key_hex, i) for i in range(len(self.nodes))]
        node_index = weights.index(max(weights))
        return self.nodes[node_index]
