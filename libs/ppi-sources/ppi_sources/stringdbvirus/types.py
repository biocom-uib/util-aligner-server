from ppi_sources.network import EdgeListNetwork, VirusHostNetwork


class StringDBVirusNetwork(EdgeListNetwork, VirusHostNetwork):
    def __init__(self, host_id, virus_id, edges_df):
        EdgeListNetwork.__init__(self, f'{host_id}-{virus_id}', edges_df)
        VirusHostNetwork.__init__(self, host_name=str(host_id), virus_name=str(virus_id))

        self.host_id = host_id
        self.virus_id = virus_id


    def is_host_vertex(self, v):
        return v['name'].startswith(f'{self.host_id}.')


    def is_virus_vertex(self, v):
        return v['name'].startswith(f'{self.virus_id}.')


    def get_details(self):
        details = {}
        details.update(super().get_details())

        details.update({
            'n_vert_host': self.host_net.igraph.vcount(),
            'n_edges_host': self.host_net.igraph.ecount(),
            'n_vert_virus': self.virus_net.igraph.vcount(),
            'n_edges_virus': self.virus_net.igraph.ecount(),

            'n_interactions': self.igraph.ecount()
                - self.host_net.igraph.ecount()
                - self.virus_net.igraph.ecount(),
        })

        return details

