# import modules
from google.ads.google_ads.client import GoogleAdsClient

# establish yaml file path
yaml_file_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'google-ads.yaml')
print(yaml_file_path)

# establish client
client = GoogleAdsClient.load_from_storage(path=yaml_file_path)

# establish customer id
customer_id = '6838368118'

# connect to GoogleAdsService
ga_service = client.get_service('GoogleAdsService', version='v4')

# create query
query = 'SELECT user_list.id, user_list.name, user_list.description FROM user_list'

# execute api call using created query
response = ga_service.search_stream(customer_id, query=query)

# print out response
try:
    for batch in response:
        for row in batch.results:
            print(f'User List with ID {row.user_list.id.value} and name '
                  f'"{row.user_list.name.value}" and description '
                  f'"{row.user_list.description}" was found.')
except GoogleAdsException as ex:
    print(f'Request with ID "{ex.request_id}" failed with status '
          f'"{ex.error.code().name}" and includes the following errors:')
    for error in ex.failure.errors:
        print(f'\tError with message "{error.message}".')
        if error.location:
            for field_path_element in error.location.field_path_elements:
                print(f'\t\tOn field: {field_path_element.field_name}')


