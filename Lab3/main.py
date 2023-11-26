# Load libraries
from azure.storage.blob import BlobServiceClient
import pandas as pd
import io

# Define parameters
connectionString = "DefaultEndpointsProtocol=https;AccountName=lab3stoaccount;AccountKey=jQ+o/g/0QzQbvKe6ScSdArB+zSWFqZMeB0dxL+ivOY3y2xt64OINcSGegNXvWqaXA2Qx2UTGXoEO+AStXxeH4w==;EndpointSuffix=core.windows.net"
containerName = "stocks-data"
outputFileName	= "concatenated_stocks_data.csv"

# Establish connection with the blob storage account
blob_service_client = BlobServiceClient.from_connection_string(connectionString)
container_client = blob_service_client.get_container_client(containerName)

# List all blobs in the container
blobs = container_client.list_blobs()

dfs = []

def stock_name(company_name):
    if(company_name == 'AMAZON'): return 'AMZN'
    if(company_name == 'APPLE'): return 'AAPL'
    if(company_name == 'FACEBOOK'): return 'META'
    if(company_name == 'GOOGLE'): return 'GOOGL'
    if(company_name == 'MICROSOFT'): return 'MSFT'
    if(company_name == 'TESLA'): return 'TSLA'
    if(company_name == 'ZOOM'): return 'RATE'

for b in blobs:
    # Download the blob content
    blob_data = container_client.get_blob_client(b.name).download_blob().readall()
    
    # Convert the content to a DataFrame
    df = pd.read_csv(io.BytesIO(blob_data))

    # Add a new column "stock_name" with the value being the name of the file (excluding extension)
    df['stock_name'] = df.apply(lambda x : stock_name(x['company_name']) , axis = 1)
    
    # Append the DataFrame to the list
    dfs.append(df)

concatenated_df = pd.concat(dfs , ignore_index = True)

concatenated_df.to_csv(outputFileName , index = False)

# Upload the CSV file to Azure Blob Storage
with open(outputFileName , "rb") as data:
    container_client.upload_blob(name = outputFileName , data = data , overwrite = True)
