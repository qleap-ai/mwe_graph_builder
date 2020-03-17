from firebase_admin import firestore
import re
from fuzzywuzzy import fuzz
import mwe_extractor
import time
from datetime import datetime
import networkx as nx
from networkx.algorithms import community
import math


class Node:
    def __init__(self, name, article):
        self.num_id = 0
        self.name = name
        self.value = 1
        self.group = 0
        self.article = article
        self.centroid = False

    def increment(self):
        self.value += 1

    def set_value(self, v):
        self.value = v

    def set_centroid(self, c):
        self.centroid = c

    def is_centroid(self):
        return self.centroid

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

    def get_group(self):
        return self.group


class Link:
    def __init__(self, id, fr, to):
        self.id = id
        self.fr = fr
        self.to = to
        self.value = 1
        self.num_id = 0

    def increment(self):
        self.value += 1

    def to_dict(self):
        return {'id': self.id, 'source': self.fr.get_id(), 'target': self.to.get_id()}

    def set_num_id(self, id):
        self.num_id = id

    def get_num_id(self):
        return self.num_id

    def get_id(self):
        return self.id

    def weight(self):
        return self.value


class GraphBuilder:

    def __init__(self):
        self.nodes = {}
        self.links = []
        self.connectors = {}
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
            self.link_article(mwes_in_article, article)
        self.create_graph()
        self.run_graph_analytics()

    def match_mwes(self, article, mwes):
        text = article['text']
        m_txt = re.sub(r'[^\sa-zA-Z0-9]', '', text).lower().strip()
        matches = set()
        for mwe in mwes['mwes']:
            if mwe in m_txt:
                matches.add(self.normalized[mwe])

        return matches

    def link_article(self, mwes_in_article, article):
        node = Node(article['id'], article)
        self.nodes[node.name] = node
        self.connectors[node] = mwes_in_article

    def create_graph(self):
        node_list = [node for node in self.connectors.keys()]
        idx = 0
        for i in range(0, len(node_list) - 1):
            links_already = {}
            n1 = node_list[i]
            mwe1 = self.connectors[n1]
            for j in range(i + 1, len(node_list)):
                n2 = node_list[j]
                mwe2 = self.connectors[n1]
                for mwe in mwe1:
                    if mwe in mwe2:
                        k1 = n1.get_id() + "_" + n2.get_id()
                        k2 = n2.get_id() + "_" + n1.get_id()
                        if k1 in links_already.keys():
                            l1 = links_already[k1]
                            l1.increment()
                            l2 = links_already[k2]
                            l2.increment()
                        else:
                            l1 = Link(idx, n1, n2)
                            links_already[k1] = l1
                            self.links.append(l1)
                            idx += 1
                            l2 = Link(idx, n1, n2)
                            links_already[k2] = l2
                            self.links.append(l2)

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
        return {
            'nodes': [
                {'id': node.name, 'centroid': node.centroid, 'url': node.article['url'], 'title': node.article['title'],
                 'group': node.group,
                 'count': node.value} for
                node in self.nodes.values()],
            'links': [link.to_dict() for link in self.links],
            'from_ts': fr, 'to_ts': to, 'from_date': from_date, 'to_date': to_date, 'sources': list(self.handles)}

    def run_graph_analytics(self):
        import matplotlib.pyplot as plt
        G = nx.Graph()

        l_id_to_num_id = {}
        l_num_id_to_link = {}
        idx = 0
        for link in self.links:
            # link.set_num_id(idx)
            # idx += 1
            # l_id_to_num_id[link.get_id()] = link.get_num_id()
            # l_num_id_to_link[link.get_num_id()] = link
            # G.add_edge(n_id_to_num_id[link.fr],n_id_to_num_id[link.to])
            G.add_edge(link.fr, link.to, weight=link.weight())

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
        sz = 0
        bg = None
        for s in top_level_communities:
            if len(s) > sz:
                sz = len(s)
                bg = group
            for node in s:
                node.set_group(group)
            group += 1
            sub = G.subgraph(s)
            pr = nx.pagerank_numpy(sub, alpha=0.9, weight='weight')
            mx_rank = 0
            mx_node = None
            for k in pr.keys():
                v = pr[k]
                k.set_value(2 * math.exp(v))
                if v > mx_rank:
                    mx_rank = v
                    mx_node = k
                # k.set_value(10*)
            mx_node.set_centroid(True)

        new_links = []
        for link in self.links:
            # if link.fr.get_group() == bg or link.to.get_group() == bg:
            #     continue
            if link.fr.get_group() == link.to.get_group() or link.fr.is_centroid() or link.to.is_centroid():
                new_links.append(link)
            else:
                pass
        self.links = new_links
        #
        # pr = nx.pagerank_numpy(G, alpha=0.9,weight='weight')
        # for k in pr.keys():
        #     v = pr[k]
        #     k.set_value(10*math.exp(v))
        # k.set_value(10*)
        # nodes
        # pos = nx.spring_layout(G)  # positions for all nodes
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
