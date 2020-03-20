from gensim.models import Phrases
from gensim.models.phrases import Phraser
import re
import codecs
from google.cloud import storage
import json

iterations = 2
accepted_connectors = ["of", "with", "without", "the", "a", "&"]


def load_stopwords():
    stopwords = []
    with codecs.open("resources/stopwords_SMART_curated.txt", encoding='utf_8') as f:
        for line in f:
            stopwords.append(line.rstrip())
    return stopwords


def convert_hyphens_to_underscore(sentence):
    processed = sentence.replace("-", "_")
    processed = re.sub("_+", "_", processed)
    return processed


def decompound_stopwords(token, stopwords):
    if token.count("_") >= 1:
        components = [word for word in token.split("_")]
        if components[0].lower() in stopwords or components[-1].lower() in stopwords:
            return components
        else:
            return token
    else:
        return token


def compound_without_stopwords(sentence, stopwords):
    decompounded = []
    for token in sentence.split(" "):
        catcher = decompound_stopwords(token, stopwords)
        if type(catcher) is list:
            decompounded.extend(catcher)
        else:
            decompounded.append(catcher)
    return decompounded


def decompound_non_word_chars(token):
    if token.count("_") >= 1:
        if re.search(r"[^\w&-]+", token):
            components = [word for word in token.split("_")]
            return components
        else:
            return token
    else:
        return token


def compound_without_non_word_chars(sentence):
    decompounded = []
    for token in sentence.split(" "):
        catcher = decompound_non_word_chars(token)
        if type(catcher) is list:
            decompounded.extend(catcher)
        else:
            decompounded.append(catcher)
    return decompounded


def decompound_digits(token):
    if token.count("_") >= 1:
        if re.search(r"\d", token):
            components = [word for word in token.split("_")]
            return components
        else:
            return token
    else:
        return token


def compound_without_digits(sentence):
    decompounded = []
    for token in sentence.split(" "):
        catcher = decompound_digits(token)
        if type(catcher) is list:
            decompounded.extend(catcher)
        else:
            decompounded.append(catcher)
    return decompounded


# Requires testing:
def build_mwe_set(articles):
    mwe_set = set([])
    for document in articles:
        text = ' '.join([document['title'], document["text"]])
        for word in text.split(" "):
            if "_" in word:
                mwe_set.add(word.lower())
    if len(mwe_set) == 0:
        return set()
    mwe_set.remove("_")
    return mwe_set


# Requires testing:
def train_mwe_model_from_json(articles):
    # if path.exists("./models/model"):
    #     phrases_model = SaveLoad.load("./models/model")
    # else:
    phrases_model = Phrases(common_terms=accepted_connectors,min_count=2)
    for document in articles:

        if 'text' in document.keys():
            text = document["text"]
            phrases_model.add_vocab([text.split(" ")])

        if 'title' in document.keys():
            title = document["title"]
            phrases_model.add_vocab([title.split(" ")])
    # phrases_model.save("./models/model")
    phraser_model = Phraser(phrases_model)
    return phraser_model


def compound(text, phraser_model, stopwords):
    mwe_text = u' '.join(phraser_model[text.split(" ")])
    mwe_text = convert_hyphens_to_underscore(mwe_text)
    mwe_text = u' '.join(compound_without_stopwords(mwe_text, stopwords))
    mwe_text = u' '.join(compound_without_non_word_chars(mwe_text))
    mwe_text = u' '.join(compound_without_digits(mwe_text))
    return mwe_text


# Requires testing:
def compound_mwe_to_json(articles):
    phraser_model = train_mwe_model_from_json(articles)
    stopwords = load_stopwords()
    documents = []
    for document in articles:
        if not 'text' in document.keys() or not 'title' in document.keys():
            continue
        text = compound(document['text'], phraser_model, stopwords)
        title = compound(document['title'], phraser_model, stopwords)
        new_document = {"text": text, "title": title}
        documents.append(new_document)
    return documents


# Requires testing:
def train_phrases_model(articles, iterations):
    for i in range(iterations):
        articles = compound_mwe_to_json(articles)
    return build_mwe_set(articles)


def load_mwes(filename):
    bucket_name = "semantic_features"
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob_name = "mwes/" + filename
    blob = bucket.blob(blob_name)
    dest = "/tmp/"+filename
    blob.download_to_filename(dest)

    with open(dest, encoding='utf8') as json_file:
        mwes = json.load(json_file)
    return set(mwes['mwes'])


def extract_mwes(articles):

    new_mwes=train_phrases_model(articles, 2)
    incr_mwes = load_mwes('incr_current.json')
    cum_mwes = load_mwes('cum_current.json')
    my_mwes = new_mwes - cum_mwes
    my_mwes = my_mwes.union(incr_mwes)
    return my_mwes
