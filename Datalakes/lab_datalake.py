import os, random
from azure.identity import AzureCliCredential
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.storage import StorageManagementClient
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import DefaultAzureCredential
from azure.storage.filedatalake import DataLakeServiceClient

def create_blob(subscription_id):
    # Acquire a credential object using CLI-based authentication.
    credential = AzureCliCredential()

    # Retrieve subscription ID from environment variable.
    #subscription_id = 'c0d627a7-f6f0-4420-9111-e077bf02d304'
    # Obtain the management object for resources.
    resource_client = ResourceManagementClient(credential, subscription_id)

    # Constants we need in multiple places: the resource group name and the region
    # in which we provision resources. You can change these values however you want.
    RESOURCE_GROUP_NAME = "PythonAzure-Lab-Datalake"
    LOCATION = "centralus"

    # Step 1: Provision the resource group.

    rg_result = resource_client.resource_groups.create_or_update(RESOURCE_GROUP_NAME,
        { "location": LOCATION })

    print(f"Provisioned resource group {rg_result.name}")

    # For details on the previous code, see Example: Provision a resource group
    # at https://docs.microsoft.com/azure/developer/python/azure-sdk-example-resource-group


    # Step 2: Provision the storage account, starting with a management object.

    storage_client = StorageManagementClient(credential, subscription_id)

    # This example uses the CLI profile credentials because we assume the script
    # is being used to provision the resource in the same way the Azure CLI would be used.

    STORAGE_ACCOUNT_NAME = f"pythonazurestorage{random.randint(1,100000):05}"

    # You can replace the storage account here with any unique name. A random number is used
    # by default, but note that the name changes every time you run this script.
    # The name must be 3-24 lower case letters and numbers only.


    # Check if the account name is available. Storage account names must be unique across
    # Azure because they're used in URLs.
    availability_result = storage_client.storage_accounts.check_name_availability(
        { "name": STORAGE_ACCOUNT_NAME }
    )

    if not availability_result.name_available:
        print(f"Storage name {STORAGE_ACCOUNT_NAME} is already in use. Try another name.")
        exit()

    # The name is available, so provision the account
    poller = storage_client.storage_accounts.begin_create(RESOURCE_GROUP_NAME, STORAGE_ACCOUNT_NAME,
        {
            "location" : LOCATION,
            "kind": "StorageV2",
            "sku": {"name": "Standard_LRS"}
        }
    )

    # Long-running operations return a poller object; calling poller.result()
    # waits for completion.
    account_result = poller.result()
    print(f"Provisioned storage account {account_result.name}")


    # Step 3: Retrieve the account's primary access key and generate a connection string.
    keys = storage_client.storage_accounts.list_keys(RESOURCE_GROUP_NAME, STORAGE_ACCOUNT_NAME)

    print(f"Primary key for storage account: {keys.keys[0].value}")

    conn_string = f"DefaultEndpointsProtocol=https;EndpointSuffix=core.windows.net;AccountName={STORAGE_ACCOUNT_NAME};AccountKey={keys.keys[0].value}"

    # [END create_datalake_service_client]
 
    # Instantiate a DataLakeServiceClient Azure Identity credentials.
    # [START create_datalake_service_client_oauth]

    return {'storage_account_name' : STORAGE_ACCOUNT_NAME,
            'storage_account_key' : keys.keys[0].value}


SOURCE_DIRECTORY = '/Users/sarankansivananthan/Desktop/Datalakes/data' 

def upload_samples(filesystem_client):
    
    source_files = [f for f in os.listdir(SOURCE_DIRECTORY) if os.path.isfile(os.path.join(SOURCE_DIRECTORY, f)) and not f.startswith('.')]

    for source_file_name in source_files:
        # Construct the full path to the source file
        source_file_path = SOURCE_DIRECTORY + '/' + source_file_name

        # Create a file in the Data Lake with the same name as the source file
        file_client = filesystem_client.get_file_client(source_file_name)
        file_client.create_file()

        # Read the content of the source file and upload it to the Data Lake
        with open(source_file_path, 'rb') as source_file:
            file_content = source_file.read()
            file_client.append_data(data=file_content, offset=0, length=len(file_content))
            file_client.flush_data(len(file_content))

        # Get file properties
        properties = file_client.get_file_properties()
        print("Uploaded '{}' to Data Lake. File size: {} bytes".format(source_file_name, properties.size))


def run(account_name , account_key):

    # set up the service client with the credentials from the environment variables
    service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
        "https",
        account_name
    ), credential=account_key)

    # create the filesystem
    filesystem_client = service_client.create_file_system(file_system='data')

    # invoke the sample code
    try:
        upload_samples(filesystem_client)
    finally:
        # clean up the demo filesystem
        pass

if __name__ == '__main__':
    blob = create_blob('c0d627a7-f6f0-4420-9111-e077bf02d304')
    run(blob['storage_account_name'] , blob['storage_account_key'])
