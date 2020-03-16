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
    tmp_file = "/tmp/tmp.json"
    with open(tmp_file, "w") as tmp:
        json.dump(graph, tmp)

    bucket_name = "qleap.ai"
    destination_blob_name = "graph_data/mwe_graph.json"
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(tmp_file)


