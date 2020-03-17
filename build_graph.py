from firebase_admin import firestore
import re
from fuzzywuzzy import fuzz
import mwe_extractor
import time
from datetime import datetime
import networkx as nx
from networkx.algorithms import community


class Node:
    def __init__(self, name):
        self.num_id = 0
        self.name = name
        self.value = 1
        self.group = 0

    def increment(self):
        self.value += 1

    def to_dict(self):
        return {'id': self.name}

    def set_num_id(self, id):
        self.num_id = id

    def get_num_id(self):
        return self.num_id

    def get_id(self):
        return self.name

    def set_group(self, group):
        self.group = group


class Link:
    def __init__(self, id, fr, to, source):
        self.id = id
        self.fr = fr
        self.to = to
        self.source = source
        self.value = 1
        self.num_id = 0

    def increment(self):
        self.value += 1

    def to_dict(self):
        return {'id': self.id, 'source': self.fr, 'target': self.to, "type": self.source}

    def set_num_id(self, id):
        self.num_id = id

    def get_num_id(self):
        return self.num_id

    def get_id(self):
        return self.id


class GraphBuilder:

    def __init__(self):
        self.nodes = {}
        self.links = {}
        self.handles = set()
        self.fire_db = firestore.Client()
        self.normalized = {}

    # loads all articles for a given collection for above defined time period
    def get_articles(self, col, fr, to):
        art_stream = self.fire_db.collection('news').document('articles') \
            .collection(col).where("time_stamp", ">=", fr).where("time_stamp", "<=", to).stream()
        arts = []
        for art_ref in art_stream:
            art = art_ref.to_dict()
            arts.append(art)
        if len(arts) > 0:
            self.handles.add(col)
        return arts

    # iterates over the collections to load the articles
    def load_articles(self, fr, to):
        colls = self.fire_db.collection('news').document('articles').collections()
        ll = list(colls)
        all_articles = []
        for col in ll:
            my_articles = self.get_articles(col.id, fr, to)
            all_articles.extend(my_articles)
        return all_articles

    def build_graph(self, mwes, articles):
        for article in articles:
            mwes_in_article = self.match_mwes(article, mwes)
            # self.update_nodes(mwes_in_article)
            self.update_links(list(mwes_in_article), article['handle'])
        self.run_graph_analytics()

    def match_mwes(self, article, mwes):
        text = article['text']
        m_txt = re.sub(r'[^\sa-zA-Z0-9]', '', text).lower().strip()
        matches = set()
        for mwe in mwes['mwes']:
            if mwe in m_txt:
                matches.add(self.normalized[mwe])

        return matches

    def update_links(self, mwes_in_article, handle):
        for mwe in mwes_in_article:
            if mwe in self.nodes.keys():
                node = self.nodes[mwe]
                node.increment()
            else:
                node = Node(mwe)
                self.nodes[mwe] = node

        for i in range(0, len(mwes_in_article) - 1):
            for j in range(i + 1, len(mwes_in_article)):
                mwe1 = mwes_in_article[i]
                mwe2 = mwes_in_article[j]
                id = ''
                if mwe1 < mwe2:
                    id = mwe1 + "_" + mwe2 + "_" + handle
                else:
                    id = mwe2 + "_" + mwe1 + "_" + handle

                if id in self.links.keys():
                    l = self.links[id]
                    l.increment()
                else:
                    l = Link(id, mwe1, mwe2, handle)
                    self.links[id] = l

    def filter_unique(self, articles):

        unique = []
        unique.append(articles[0])
        for j in range(1, len(articles)):
            cand = articles[j]
            accept = True
            for art in unique:
                sim = fuzz.token_sort_ratio(art['title'], cand['title'])
                if sim > 90:
                    accept = False
                    break
            if accept:
                unique.append(cand)
        return unique

    def rm_inconsitent(self, articles):
        ret = []
        for art in articles:
            if 'title' in art.keys() and 'text' in art.keys():
                ret.append(art)

        return ret

    def run(self):
        # mwes = self.load_incr()
        # fr = mwes['from_date']
        # to = mwes['to_date']
        to = time.time()
        fr = to - 2 * 3600
        articles = self.load_articles(fr, to)
        articles = self.rm_inconsitent(articles)
        articles = self.filter_unique(articles)

        mwes = {'from_date': fr, 'to_date': to}
        my_mwes = mwe_extractor.extract_mwes(articles)

        raw_mwes = my_mwes
        mwes['mwes'] = [re.sub('[_]+', ' ', word) for word in raw_mwes]
        tmp = []
        for mwe in mwes['mwes']:
            if len(mwe.split()) >= 2:
                tmp.append(mwe)
        mwes['mwes'] = tmp
        self.normalize(tmp)
        from_date = str(datetime.fromtimestamp(fr))
        to_date = str(datetime.fromtimestamp(to))
        self.build_graph(mwes, articles)
        return {'nodes': [{'id': node.name, 'group': node.group, 'count': node.value} for node in self.nodes.values()],
                'links': [link.to_dict() for link in self.links.values()],
                'from_ts': fr, 'to_ts': to, 'from_date': from_date, 'to_date': to_date, 'sources': list(self.handles)}

    def run_graph_analytics(self):
        import matplotlib.pyplot as plt
        G = nx.Graph()

        nodes_dict = {}
        for k in self.nodes.keys():
            node = self.nodes[k]
            nodes_dict[k] = node
            # node.set_num_id(idx)
            # idx += 1
            # n_id_to_num_id[node.get_id()] = node.get_num_id()
            # n_num_id_to_node[node.get_num_id()] = node
            # G.add_node(node.get_num_id())

        l_id_to_num_id = {}
        l_num_id_to_link = {}
        idx = 0
        for k in self.links.keys():
            link = self.links[k]
            # link.set_num_id(idx)
            # idx += 1
            # l_id_to_num_id[link.get_id()] = link.get_num_id()
            # l_num_id_to_link[link.get_num_id()] = link
            # G.add_edge(n_id_to_num_id[link.fr],n_id_to_num_id[link.to])
            G.add_edge(link.fr, link.to)

        communities_generator = community.girvan_newman(G)
        top_level_communities = next(communities_generator)
        top_level_communities = next(communities_generator)
        top_level_communities = next(communities_generator)
        top_level_communities = next(communities_generator)
        top_level_communities = next(communities_generator)
        top_level_communities = next(communities_generator)
        # top_level_communities = next(communities_generator)
        # next_level_communities = next(communities_generator)
        # a = next(communities_generator)
        # b = next(communities_generator)
        # c = next(communities_generator)

        # d = next(communities_generator)
        group = 0
        for s in top_level_communities:
            for k in s:
                node = nodes_dict[k]
                node.set_group(group)
            group += 1

        # pos = nx.spring_layout(G)  # positions for all nodes
        #
        # # nodes
        # nx.draw_networkx_nodes(G, pos, nodelist=list(top_level_communities[0]), node_color='r', node_size=700)
        # nx.draw_networkx_nodes(G, pos, nodelist=list(top_level_communities[1]), node_color='b', node_size=700)
        # nx.draw_networkx_nodes(G, pos, nodelist=list(top_level_communities[2]), node_color='g', node_size=700)
        # # edges
        # el = [(u, v) for (u, v, d) in G.edges(data=True)]
        # nx.draw_networkx_edges(G, pos, edgelist=el,
        #                        width=6)
        #
        # # labels
        # nx.draw_networkx_labels(G, pos, font_size=20, font_family='sans-serif')
        # plt.axis('off')
        # plt.show()
        # pass

    def normalize(self, my_mwes):

        for mwe in my_mwes:
            self.normalized[mwe] = mwe
            for mwe2 in my_mwes:
                if mwe2 == mwe:
                    continue
                if mwe in mwe2:
                    self.normalized[mwe] = mwe2

        pass
