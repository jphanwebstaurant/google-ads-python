# add_user_list.py
import logging
import uuid
import os
from google.ads.google_ads.client import GoogleAdsClient
from google.ads.google_ads.errors import GoogleAdsException

# establish yaml file path
yaml_file_path = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'google-ads.yaml')
print(yaml_file_path)

# establish client
client = GoogleAdsClient.load_from_storage(path=yaml_file_path)

# log errors
logging.basicConfig(level=logging.INFO, format='[%(asctime)s - %(levelname)s] %(message).5000s')
logging.getLogger('google.ads.google_ads.client').setLevel(logging.DEBUG)

# establish customer id
customer_id = '6838368118'

# connect to user list service
user_list_service = client.get_service('UserListService', version='v4')

# create user list operation object
user_list_operation = client.get_type('UserListOperation', version='v4')

# establish object as create operation
user_list = user_list_operation.create

# create name of user list
user_list.name.value = 'Interplanetary Cruise Test %s' % uuid.uuid4()

# create user list description
user_list.description.value = 'Testing out the Google Ads API'

# execute api call to create user list
try:
    user_list_response = user_list_service.MutateUserLists(customer_id, [user_list_operation])
except GoogleAdsException as ex:
    print('Request with ID "%s" failed with status "%s" and includes the '
          'following errors:' % (ex.request_id, ex.error.code().name))
    for error in ex.failure.errors:
        print('\tError with message "%s".' % error.message)
        if error.location:
            for field_path_element in error.location.field_path_elements:
                print('\t\tOn field: %s' % field_path_element.field_name)