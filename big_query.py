from pprint import pprint

from googleapiclient import discovery

from oauth2client.client import GoogleCredentials

# Authentication is provided by the 'gcloud' tool when running locally
# and by built-in service accounts when running on GAE, GCE, or GKE.
# See https://developers.google.com/identity/protocols/application-default-credentials for more information.
credentials = GoogleCredentials.get_application_default()

# Construct the bigquery service object (version v2) for interacting
# with the API. You can browse other available API services and versions at
# https://developers.google.com/api-client-library/python/apis/
service = discovery.build('bigquery', 'v2', credentials=credentials)

def insert_quotes(source, quotes):

    def format_quotes_for_insert(quote):
        return {
            "json": {
                "name": quote[0],
                "quote": quote[1],
                "source": source,
                "captured_at": unicode(quote[2]),
            },
        }

    # * Project ID of the destination table.
    projectId = 'dataminor5'

    # * Dataset ID of the destination table.
    datasetId = 'currencies'

    # * Table ID of the destination table.
    tableId = 'quotes'

    body = {
      "kind": "bigquery#tableDataInsertAllRequest",
      "rows": map(format_quotes_for_insert, quotes),
    }

    request = service.tabledata().insertAll(projectId=projectId, datasetId=datasetId, tableId=tableId, body=body)
    response = request.execute()