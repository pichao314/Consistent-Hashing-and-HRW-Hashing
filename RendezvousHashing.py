import mmh3


class HRW:
    # creator of ring
    def __init__(self, nodes):
        assert len(nodes) > 0
        self.nodes = nodes

    def get_node(self, key_hex):
        weights = [mmh3.hash(key_hex, i) for i in range(len(self.nodes))]
        node_index = weights.index(max(weights))
        return self.nodes[node_index],
