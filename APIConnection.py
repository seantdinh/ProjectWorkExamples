import requests
import pandas as pd
import xml.etree.ElementTree as et
import xmltodict
import numpy

security_key_file = pd.read_excel('apikeys.xlsx', usecols=['SecurityKey'])
security_key_file = security_key_file.dropna()
master = []

for i, apikey in security_key_file.iterrows():
    print(apikey)
    r = requests.get('https://secure.networkmerchants.com/api/query.php',
                     params={
                         'security_key': apikey,
                         'condition': "complete",
                         'start_date': '20200904'
                     })


    json_data = xmltodict.parse(r.text, process_namespaces=True)
    new_json_data = {}
    for i, row in enumerate(json_data['nm_response']['transaction']):
        new_row = row
        if isinstance(row['action'], list):
            row_data = {}
            for j, action in enumerate(row['action']):
                for k, v in action.items():
                    row_data[f'{k}_{j}'] = v
        else:
            row_data = row['action']
        for k, v in row_data.items():
            new_row[k] = v
        new_json_data[i] = new_row
        df = pd.DataFrame.from_dict(new_json_data).T
        master.append(df)

temp = pd.concat(master)
df = temp
df['amount_0'] = df['amount_0'].astype(numpy.float).round(decimals=2)
df['date_0'] = pd.to_datetime(df['date_0'], format='%Y%m%d%H%M%S').dt.date
df_group = df.groupby(['username_0', 'processor_id', 'date_0', 'action_type_0']).sum()
df_group = df_group.loc[:, df_group.columns.isin(['date_0', 'action_type_0', 'amount_0', 'processor_id'])].unstack(3)
df_group.columns = df_group.columns.get_level_values(1)