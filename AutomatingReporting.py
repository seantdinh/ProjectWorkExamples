import pandas as pd
import numpy as np

pd.set_option('display.max_columns', 5000)
pd.set_option('display.width', 1000000)

month = '202011'


def split_network_and_clientsheets():
    df = pd.read_csv('data_files//NetworkCampaignAffiliate' + month + '.csv', usecols=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
    names = df['Network'].unique().tolist()
    df[['Client', 'Campaign']] = df["Campaign"].str.split(r" - ", 1, expand=True)
    df = df[~df["Campaign"].str.contains('- H')]
    client = df["Client"].unique().tolist()

    for myname in names:
        mydf = df.loc[df['Network'] == myname]
        mydf = mydf.sort_values('Cycle 1 Success Rate')
        mydf = mydf.dropna(axis=0, subset=['Affiliate'])
        mydf.loc['Total', :] = mydf.sum(numeric_only=True, axis=0)
        mydf = mydf[['Network', 'Campaign', 'Affiliate', 'Orders', 'Cycle 1 Success', 'Cycle 1 Success Rate', 'Client']]
        mydf.to_excel("output//network//"'Retention_' + myname + '_2020_09' + '.xlsx', index=False)
        writer = pd.ExcelWriter("output//network//"'Retention_' + myname + '_' + month + '.xlsx', engine='xlsxwriter')
        for myclient in client:
            df1 = mydf[mydf["Client"].str.contains(myclient, na=False)].copy()
            if not df1.empty:
                df1.loc['Total', :] = df1.sum(numeric_only=True, axis=0)
                df1.loc['Credit', :] = df1.style.format("${:,.2f}")
                print(df1)
                df1.to_excel(writer, sheet_name=myclient, index=False)
        writer.save()


def split_network():
    df = pd.read_csv('data_files//NetworkCampaignAffiliate' + month + '.csv', usecols=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
    names = df['Network'].unique().tolist()
    df[['Client', 'Campaign']] = df["Campaign"].str.split(r" - ", 1, expand=True)
    df = df[~df["Campaign"].str.contains('- H')]
    client = df["Client"].unique().tolist()

    for myname in names:
        mydf = df.loc[df['Network'] == myname]
        mydf = mydf.sort_values('Cycle 1 Success Rate')
        mydf = mydf.dropna(axis=0, subset=['Affiliate'])
        mydf = mydf[['Network', 'Campaign', 'Affiliate', 'Orders', 'Cycle 1 Success', 'Cycle 1 Success Rate', 'Client']]
        writer = pd.ExcelWriter("output//network//"'Retention_' + myname + '_' + month + '.xlsx', engine='xlsxwriter')
        mydf['Cycle 1 Success Rate'] = mydf['Cycle 1 Success Rate'] * .01
        mydf['Credit'] = np.where((mydf['Cycle 1 Success Rate'] < .38) & (mydf['Orders'] > 5),
                                  (((.40 * mydf['Orders']) - mydf['Cycle 1 Success']) * 36), False)
        mydf.loc['Total', :] = mydf.sum(numeric_only=True, axis=0)

        mydf.loc[12, 'Total'] = ((mydf['Cycle 1 Success'].sum() / mydf['Orders'].sum()))

        mydf.to_excel(writer, sheet_name=myname, index=False)
        workbook_2 = writer.book
        worksheet_2 = writer.sheets[myname]
        fmt_currency = workbook_2.add_format({"num_format": "$#,##0.00", "bold": False})
        fmt_rate = workbook_2.add_format({"num_format": "0.0%", "bold": False})
        worksheet_2.set_column("H:H", 10, fmt_currency)
        worksheet_2.set_column("I:I", 10, fmt_rate)
        worksheet_2.set_column("F:F", 10, fmt_rate)
        writer.save()


def split_network_one_workbook():
    df = pd.read_csv('data_files//NetworkCampaignAffiliate' + month + '.csv', usecols=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
    names = df['Network'].unique().tolist()
    df[['Client', 'Campaign']] = df["Campaign"].str.split(r" - ", 1, expand=True)
    client = df["Client"].unique().tolist()
    writer = pd.ExcelWriter("output//network//ARetention_Every_Network"'_' + month + '.xlsx', engine='xlsxwriter')

    for myname in names:
        mydf = df.loc[df['Network'] == myname]
        mydf = mydf.sort_values('Cycle 1 Success Rate')
        mydf = mydf.dropna(axis=0, subset=['Affiliate'])
        mydf.loc['Total', :] = mydf.sum(numeric_only=True, axis=0)
        mydf = mydf[['Network', 'Campaign', 'Affiliate', 'Orders', 'Cycle 1 Success', 'Cycle 1 Success Rate', 'Client']]
        mydf.loc['Total', :] = mydf.sum(numeric_only=True, axis=0)
        mydf.loc[12, 'Total'] = ((mydf['Cycle 1 Success'].sum() / mydf['Orders'].sum()) * 100).round(2)
        mydf['Credit'] = np.where(mydf['Cycle 1 Success Rate'] < 38,
                                  (((.40 * mydf['Orders']) - mydf['Cycle 1 Success']) * 36), False)

        mydf.to_excel(writer, sheet_name=myname, index=False)

    writer.save()


def split_client():
    df = pd.read_csv('data_files//NetworkCampaignAffiliate' + month + '.csv', usecols=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9])
    # names = df['Network'].unique().tolist()
    df[['Client', 'Campaign']] = df["Campaign"].str.split(r" - ", 1, expand=True)
    df = df[~df["Campaign"].str.contains('- H')]
    client = df["Client"].unique().tolist()
    writer = pd.ExcelWriter("output//network//ARetention_Every_Client"'_' + month + '.xlsx', engine='xlsxwriter')

    for myname in client:
        mydf = df.loc[df['Client'] == myname]
        mydf = mydf.sort_values('Cycle 1 Success Rate')
        mydf = mydf.dropna(axis=0, subset=['Affiliate'])
        mydf.loc['Total', :] = mydf.sum(numeric_only=True, axis=0)
        mydf = mydf[['Network', 'Campaign', 'Affiliate', 'Orders', 'Cycle 1 Success', 'Cycle 1 Success Rate', 'Client']]
        mydf.loc['Total', :] = mydf.sum(numeric_only=True, axis=0)
        mydf.loc[12, 'Total'] = ((mydf['Cycle 1 Success'].sum() / mydf['Orders'].sum()) * 100).round(2)
        mydf['Credit'] = np.where(mydf['Cycle 1 Success Rate'] < 38,
                                  (((.40 * mydf['Orders']) - mydf['Cycle 1 Success']) * 36), False)
        mydf.to_excel(writer, sheet_name=myname, index=False)

    writer.save()


def retention_product_category():
    month = '202011.csv'
    vaultx_data = pd.read_csv('data_files/NetworkCampaignAffiliate' + month, usecols=[1, 2, 3, 4, 5, 6, 7, 8, 9])
    vertical = pd.read_excel('table//Vaultx_ProductCategory.xlsx')

    # remove_scrub = vaultx_data[~vaultx_data["Campaign"].str.contains('- H')]

    merged_df = vaultx_data.merge(vertical, how='left', left_on=['Campaign'], right_on=['Campaign Name']).drop(
        "Campaign Name", axis=1)

    merged_df[['Client', 'Campaign']] = merged_df["Campaign"].str.split(r" - ", 1, expand=True)

    merged_df.drop(
        ['Affiliate', 'Cycle 1 Cancels', 'Cycle 1 Declines', 'Cycle 1 Recycling', 'Cycle 1 Success Rate', 'Campaign'],
        axis=1, inplace=True)

    retention = merged_df.groupby(['Product Category', 'Client']).sum()

    retention['calculated'] = (
                (retention['Cycle 1 Success'] / (retention['Orders'] - retention['Cycle 1 Pending'])) * 100).round(2)

    retention = retention['calculated']

    retention = retention.unstack('Client')

    retention = retention.fillna(0)

    retention = retention.astype(str) + '%'

    retention.to_csv('output//product//product_category_' + month)


def vaultx_monthly_sales_category():
    winauto_sales_draw = pd.read_excel(
        'C:\\Users\\Source Naturals\\Documents\\Shared\\VaultX\\Monthly Sales\\Monthly_Sales.xlsx', skiprows=1)

    category = pd.read_excel('table//Vaultx_ProductCategory.xlsx')

    winauto_sales_draw.rename(columns={winauto_sales_draw.iloc[1, 11]: "Date", 'Total': 'Step1'}, inplace=True)

    winauto_sales_draw = winauto_sales_draw[~winauto_sales_draw['Campaign'].isin(["Total:"])]

    winauto_sales_draw = winauto_sales_draw.merge(category, how='left', left_on=['Campaign'],
                                                  right_on=['Campaign Name']).drop("Campaign Name", axis=1)

    winauto_sales_draw[['Corporation', 'Campaign']] = winauto_sales_draw['Campaign'].str.split('-', n=1, expand=True)

    winauto_sales_draw.iloc[:, 11] = pd.DatetimeIndex(winauto_sales_draw.iloc[:, 11]).strftime("%Y-%m-%d")

    sales_grouped = winauto_sales_draw.groupby(['Date', 'Corporation', 'Product Category']).sum().sort_values(
        by=['Date'], ascending=False)

    sales_grouped = sales_grouped.loc[:, sales_grouped.columns.isin(['Step1'])].sort_values(by=['Date', 'Corporation'], ascending=False)


split_network()
split_client()
split_network_one_workbook()

affiliateScrub()
retention_product_category()
vaultx_monthly_sales_category()