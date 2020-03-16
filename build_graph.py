
from firebase_admin import firestore
import re
from fuzzywuzzy import fuzz
import mwe_extractor
import time




class Node:
    def __init__(self, name):
        self.name = name
        self.value = 1

    def increment(self):
        self.value += 1

    def to_dict(self):
        return {'id': self.name}


class Link:
    def __init__(self, id, fr, to, source):
        self.id = id
        self.fr = fr
        self.to = to
        self.source = source
        self.value = 1

    def increment(self):
        self.value += 1

    def to_dict(self):
        return {'id': self.id, 'source': self.fr, 'target': self.to, "type": self.source}


class GraphBuilder:

    def __init__(self):
        self.nodes = {}
        self.links = {}
        self.handles = set()
        self.fire_db = firestore.Client()


    # loads all articles for a given collection for above defined time period
    def get_articles(self, col, fr, to):
        art_stream = self.fire_db.collection('news').document('articles') \
            .collection(col).where("time_stamp", ">=", fr).where("time_stamp", "<=", to).stream()
        arts = []
        for art_ref in art_stream:
            art = art_ref.to_dict()
            arts.append(art)
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

    def run(self):
        # mwes = self.load_incr()
        # fr = mwes['from_date']
        # to = mwes['to_date']
        to =time.time()
        fr = to-2*3600
        articles = self.load_articles(fr, to)
        articles = self.rm_inconsitent(articles)
        articles = self.filter_unique(articles)

        mwes = {'from_date':fr,'to_date':to}
        my_mwes = mwe_extractor.extract_mwes(articles)

        raw_mwes = my_mwes
        mwes['mwes'] = [re.sub('[_]+', ' ', word) for word in raw_mwes]
        tmp = []
        for mwe in mwes['mwes']:
            if len(mwe.split()) >= 2:
                tmp.append(mwe)
        mwes['mwes'] = tmp


        self.build_graph(mwes, articles)
        return {'nodes': [{'id': node.name, 'group': 1, 'count': node.value} for node in self.nodes.values()],
                'links': [link.to_dict() for link in self.links.values()]}

    def build_graph(self, mwes, articles):
        for article in articles:

            mwes_in_article = self.match_mwes(article, mwes)
            # self.update_nodes(mwes_in_article)
            self.update_links(list(mwes_in_article), article['handle'])
        pass

    def match_mwes(self, article, mwes):
        text = article['text']
        m_txt = re.sub(r'[^\sa-zA-Z0-9]', '', text).lower().strip()
        matches = set()
        for mwe in mwes['mwes']:
            if mwe in m_txt:
                matches.add(mwe)
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
        for j in range(1,len(articles)):
            cand = articles[j]
            accept = True
            for art in unique:
                sim = fuzz.token_sort_ratio(art['title'],cand['title'])
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


