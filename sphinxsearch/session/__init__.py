# -*- coding: utf-8 -*-


class SessionFactory(object):
    def __init__(self):
        pass

    def set_server(self, server):
        self.server = server

    def __call__(self):
        return Session(
            api=self.server.api,
            host=self.server.host,
            port=self.server.port)


class Session(object):
    def __init__(self, api, host='localhost', port=9312):
        self.api = api
        self.host = host
        self.port = port

    def run(self, query, *qs_list):
        sphinx = self.api.SphinxClient()
        sphinx._host = self.host
        sphinx._port = int(self.port)
        print(sphinx._port)

        sphinx.SetLimits(query.offset or 0, query.limit or 100,
                         query.max_matches or 10000, query.cutoff or 0)
        # sphinx.SetMatchMode(query.match_mode)
        # sphinx.SetSortMode(query.sort_mode)
        # sphinx.SetRankingMode(query.rank_mode)
        # sphinx.SetWeights(query.weights)
        # sphinx.SetFieldWeights(query.field_weights)
        sphinx.AddQuery(query.term, query.indexes_str[0])

        sphinx_result = sphinx.RunQueries()

        print(sphinx_result)
        print(sphinx.GetLastError())

        return [1, 2, 3]

    # def __enter__(self):
    #     pass

    # def __exit__(self, *args):
    #     self.close()

    # def close(self):
    #     self.conn.Close()
