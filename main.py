from build_graph import GraphBuilder
import json
from google.cloud import storage

bg = GraphBuilder()


def run(event, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         event (dict):  The dictionary with data specific to this type of
         event. The `data` field contains the PubsubMessage message. The
         `attributes` field will contain custom attributes if there are any.
         context (google.cloud.functions.Context): The Cloud Functions event
         metadata. The `event_id` field contains the Pub/Sub message ID. The
         `timestamp` field contains the publish time.
    """



    gb = GraphBuilder()
    graph = gb.run()
    graph['about'] = "When reading news online it is often the case that a main topic is covered thoroughly across " \
                     "many outlets. Sometimes it is difficult to find other relevant information apart of the main topic. " \
                     "This website shows central but well-distinct news headlines of the last two hours from various news sources.<br>" \
                     "The approach collects and clusters the news articles of the last two hours into different groups based on their " \
                     "semantic features. The displayed headlines correspond to the most central one per group. " \
                     "The size of the nodes indicate the centrality of the individual headline.<br>" \
                     "The content of this pages refreshes every hour. You just have to click the reload button. " \
                     "For question or comments please email me at gregor_AT_qleap_DOT_ai."
    tmp_file = "/tmp/tmp.json"
    with open(tmp_file, "w") as tmp:
        json.dump(graph, tmp)
    #
    bucket_name = "qleap.ai"
    storage_client = storage.Client()


    # rotate old
    for i in range(9, -1, -1):
        source_blob_name = "graph_data/mwe_graph_"+str(i)+".json"
        target_blob_name = "graph_data/mwe_graph_" + str(i+1) + ".json"

        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        new_blob = bucket.rename_blob(blob, target_blob_name)

    source_blob_name = "graph_data/mwe_graph_0.json"
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.upload_from_filename(tmp_file)


# run("","")