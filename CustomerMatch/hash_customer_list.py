# I need email, phone, first name and last name hashed
# Do not hash country or zip
# import packages
import hashlib

# TEST
# hashed_object = hashlib.sha256(b"Nobody inspects the spammish repetition").hexdigest()
# print(hashed_object)

# create hash function
def hash_me(text):
    return hashlib.sha256(text.encode('utf-8')).hexdigest()

# create copy of data
df_hashed = df_customer_list_narrow.copy()

# create list of rfm category names
rfm_hash_list =df_hashed['rfm_category'].unique().tolist()

# create a dataframe for each rfm category name
DataFrameDict = {elem: pd.DataFrame for elem in rfm_hash_list}

# create hashed version of columns
df_hashed['email_hash'] = df_hashed['Email'].apply(hash_me)
df_hashed['phone_hash'] = df_hashed['Phone'].apply(hash_me)
df_hashed['firstname_hash'] = df_hashed['First Name'].apply(hash_me)
df_hashed['lastname_hash'] = df_hashed['Last Name'].apply(hash_me)

# drop columns
df_hashed = df_hashed.drop(columns=['Email', 'First Name', 'Last Name', 'Phone'])

# rename columns
df_hashed.rename(columns={'email_hash': 'Email',
                            'firstname_hash': 'First Name',
                            'lastname_hash': 'Last Name',
                            'phone_hash': 'Phone'}, inplace=True)

# reorder columns for dictionary
df_hashed = df_hashed[['Email', 'First Name', 'Last Name', 'Country', 'Zip', 'Phone', 'rfm_category']]

# insert all data related to each dictionary name
for key in DataFrameDict.keys():
    DataFrameDict[key] = df_hashed.iloc[:, :-1][df_hashed.rfm_category == key]

# reorder columns and delete rfm category
df_hashed = df_hashed[['Email', 'First Name', 'Last Name', 'Country', 'Zip', 'Phone']]

# export each category to its own csv file
for rfm in rfm_hash_list:
    df_name = rfm
    filename = r'C:\Users\jphan\Repositories\CustomerMatching\hashed_csv_files\{}.csv'.format(rfm)
    DataFrameDict[df_name].to_csv(path_or_buf=filename, index=False)



########################################################################################################################





