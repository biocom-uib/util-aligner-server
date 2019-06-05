

class Source(object):
    def __init__(self):
        pass


    async def get_network(self, net_desc):
        raise NotImplementedError()


    async def build_custom_network(self, net_desc):
        raise NotImplementedError()


    async def get_bitscore_matrix(self, net1, net2):
        raise NotImplementedError()


    async def get_ontology_mapping(self, species):
        raise NotImplementedError()
