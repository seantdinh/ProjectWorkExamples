import pandas as pd
import numpy as np
from config import engine2
import boto3
from io import BytesIO, StringIO
from datetime import datetime, timedelta, date
import warnings
import sys
from inspect import ismethod
import boto3
import io
from functools import reduce
import pymysql

def upload_to_s3(table, filename):
    excel_buffer = BytesIO()
    table['sales_id'].fillna("").to_excel(excel_buffer, engine='openpyxl')
    s3_resource = boto3.resource('s3')
    s3_resource.Object('elasticbeanstalk-us-west-2-271', 'testing/data/Output/' + filename) \
        .put(Body=excel_buffer.getvalue())


def upload_to_s3_no_table(table, filename):
    excel_buffer = BytesIO()
    table.fillna("").to_excel(excel_buffer, engine='openpyxl')
    s3_resource = boto3.resource('s3')
    s3_resource.Object('elasticbeanstalk-us-west-2-271', 'testing/data/Output/' + filename) \
        .put(Body=excel_buffer.getvalue())


def upload_to_s3_no_table_csv(table, filename):
    csv_buffer = StringIO()
    table.fillna(0).to_csv(csv_buffer)
    s3_resource = boto3.resource('s3')
    s3_resource.Object('elasticbeanstalk-us-west-2-271', 'testing/data/Output/' + filename) \
        .put(Body=csv_buffer.getvalue())


def upload_csv_to_s3(table, filename):
    csv_buffer = StringIO()
    table['sales_id'].fillna("").to_csv(csv_buffer)
    s3_resource = boto3.resource('s3')
    s3_resource.Object('elasticbeanstalk-us-west-2-271', 'testing/data/Output/' + filename) \
        .put(Body=csv_buffer.getvalue())


def upload_scrub_to_s3(table, filename):
    excel_buffer = BytesIO()
    table.fillna("").to_excel(excel_buffer)
    s3_resource = boto3.resource('s3')
    s3_resource.Object('elasticbeanstalk-us-west-2-271', 'testing/data/Output/' + filename) \
        .put(Body=excel_buffer.getvalue())


def add_upsell_take_rate(dataframe):
    dataframe[('sales_id', 'Upsell Take')] = ((dataframe[('sales_id', 'Step-2')] / dataframe
    [('sales_id', 'Step-1')]) * 100).round(2).astype(str) + '%'
    cats = ['Step-1', 'Step-2', 'Step-3', 'Step-4', 'Upsell Take', 'xAssign']
    lvl1 = dataframe.columns.levels[1]
    cati = pd.CategoricalIndex(
        lvl1,
        categories=cats,
        ordered=True
    )
    dataframe.columns.set_levels(
        cati, level=1, inplace=True
    )
    dataframe.sort_index(1, inplace=True)


def read_excel_boto(filename):
    session = boto3.session.Session()
    s3_client = session.client('s3')
    obj = s3_client.get_object(Bucket='elasticbeanstalk-us-west-2-271', Key='testing/data/Input/' + filename)
    data = obj['Body'].read()
    df = pd.read_excel(io.BytesIO(data), encoding='utf-8')
    return df.to_dict('records')


def job():
    print("Start Sales Number Job")
    date_yesterday = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-%d')
    date_today = datetime.strftime(datetime.now(), '%Y-%m-%d')
    current_month = datetime.strftime(datetime.now() - timedelta(1), '%Y-%m-01')
    previous_month = datetime.strftime(datetime.now() - pd.DateOffset(months=1), '%Y-%m-01')
    today = date.today()
    idx = (today.isoweekday() + 0) % 7
    last_week_sunday = today - timedelta(idx)
    this_week_sunday = today - timedelta(idx - 7)
    # pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 5000)
    pd.set_option('display.width', 1000000)
    # start_time = time.time()

    # connection to database - reading master_new_sales and purchase table
    conn2 = engine2.connect()
    master_new_sales = pd.read_sql_table('transactions', conn2, parse_dates=['date_created'])
    merge_network = pd.read_excel('s3://elasticbeanstalk-us-west-2-271/input_network.xlsx')
    purchase_data = pd.read_sql_table('purchases', conn2, parse_dates=['date_created'])
    sku_table_manually_updated = pd.read_excel('s3://elasticbeanstalk-us-west-2-271/Input/GroupingProductCategory.xlsx', sheet_name='Sku')
    conn2.close()
    engine2.dispose()

    # updating local files for testing
    # master_new_sales.to_csv('data//master_new_sales.csv')
    # merge_network.to_excel('data//input_network.xlsx')
    # purchase_data.to_csv('data//purchase_data.csv')
    # sku_table_manually_updated.to_excel('data//sku_table_manually_updated.xlsx')

    # local file testing
    # master_new_sales = pd.read_csv('data//master_new_sales.csv', parse_dates=['date_created'], low_memory=False)
    # purchase_data = pd.read_csv('data//purchase_data.csv', parse_dates=['date_created'], low_memory=False)
    # merge_network = pd.read_excel('data//input_network.xlsx')
    # sku_table_manually_updated = pd.read_excel('data//sku_table_manually_updated.xlsx')

    master_new_sales = master_new_sales.merge(merge_network, on='affiliate_id', how='left')
    master_new_sales = master_new_sales.merge(sku_table_manually_updated, on='product_name', how='left')

    master_new_sales["product_name"] = master_new_sales["product_name"].str.replace(r"\(.*?\)", "")
    master_new_sales['product_country'] = master_new_sales['country'] + ' -' + master_new_sales['product_name']

    # filtering data for new master_new_sales only - Master Data

    master_new_sales = master_new_sales.fillna('00')

    master_new_sales = master_new_sales[master_new_sales.response_type.isin(['SUCCESS']) &
                             master_new_sales.billing_cycle_number.isin([1]) &
                             master_new_sales.txn_type.isin(["SALE"]) &
                             ~master_new_sales.affiliate_id.isin( ['NULL', 'D858A67D', ' ']) &
                             ~master_new_sales.campaign_name.str.contains('Prepaid|Testing|Test|Skinny Matchi|Snap|MOTO|BPages', case=False, regex=True) &
                             ~master_new_sales.card_type.str.contains('TESTCARD', na=False)
                             ]
    # supress warning from making sub df from original df
    pd.options.mode.chained_assignment = None

    master_new_sales['client_id'] = np.where(master_new_sales['client_id'].astype(str).str.contains('1'),
                                             'Healthy Living',
                                             np.where(master_new_sales['client_id'].astype(str).str.contains('3'),
                                                      'Alpha Strategies',
                                                      np.where(
                                                          master_new_sales['client_id'].astype(str).str.contains('2'),
                                                          'Squid Ink', 'xAssignClient')))

    master_new_sales['Steps'] = \
        np.where(master_new_sales['product_name'].str.contains('Blocker|Keto Energy Enhancer|Vita C|Vitamin C|Keto Energy Enhanc|Keto Core Max - Garcinia',case=False, regex=True), 'Step-3',
        np.where(master_new_sales['product_sku'].str.contains('02'), 'Step-2',
        np.where(master_new_sales['product_sku'].str.contains('03', case=False, regex=True), 'Step-3',
        np.where(master_new_sales['product_sku'].str.contains('04', case=False, regex=True), 'Step-4',
        np.where(master_new_sales['product_sku'].str.contains('01', case=False, regex=True), 'Step-1', 'xAssign')))))

    master_new_sales['scrub'] = np.where(master_new_sales['affiliate_id'].astype(str)
                                         .str.contains('E19B5379|3FD93412|1ED5F3D5', regex=True), 'Scrubs', 'Actual')
    master_new_sales = master_new_sales[(master_new_sales['date_created'] > '2020-01-01')]

    master_new_sales = master_new_sales.loc[:,
                       master_new_sales.columns.isin(['client_id', 'date_created', 'country', 'Steps', 'scrub',
                                                       'Steps', 'network_name', 'sales_id', 'campaign_name',
                                                      'product_sku', 'product_name', 'mid_number', 'country'
                                                      , 'sub_affiliate_id_1', 'product_country', 'Product Category'])]
    master_new_sales['product_name'] = master_new_sales['product_name'].str.strip()

    brand_name = pd.read_excel('s3://elasticbeanstalk-us-west-2-271/data/Input/GroupingProductCategory.xlsx', sheet_name='Category')

    brand_name = brand_name.drop(['Client'], axis=1)
    master_new_sales = master_new_sales.merge(brand_name, how='left', left_on=['product_name'], right_on=['Product Name'])\

    master_new_sales = master_new_sales.drop(['Product Name'], axis=1)
    master_new_sales = master_new_sales.fillna('Unknown')
    master_new_sales["Product Country"] = master_new_sales["Brand Name"] + " - " + master_new_sales["country"]

    # Used for Local Testing
    # master_new_sales.to_csv('testing_master_scrub_after.csv')
    # master_new_sales = pd.read_csv('testing_master_scrub_after.csv', parse_dates=['date_created'], low_memory=False)

    # Run all methods in a class
    def call_all(obj, *args, **kwargs):
        for name in dir(obj):
            attribute = getattr(obj, name)
            if ismethod(attribute):
                attribute(*args, **kwargs)

    class Report:

        def __init__(self):
            pass

        # def sales_by_mid_and_corp(self):
            # new_sales_healthy_living = master_new_sales[master_new_sales.client_id.isin(['Healthy Living'])]
            # mid_info = pd.read_excel(
            #     's3://elasticbeanstalk-us-west-2-271/data/Input/Mids_Info.xlsx')
            # new_sales_healthy_living = new_sales_healthy_living.merge(mid_info, on='mid_number', how='left')
            # sales_by_mid = new_sales_healthy_living.groupby([
            #     pd.Grouper(key='client_id'),
            #     pd.Grouper(key='date_created', freq='MS'), 'Company name', 'mid_number', 'Steps']).count()
            #
            # sales_by_mid = sales_by_mid.unstack('Steps')
            #
            # # sum the totals
            # df1 = sales_by_mid.groupby(level=[0, 1, 2]).sum()
            # df2 = sales_by_mid.groupby(level=[0, 1]).sum()
            #
            # # sort the totals to have them filter correctly to be last
            # sales_by_mid['order'] = range(len(sales_by_mid))
            # df1['order'] = sales_by_mid.groupby(level=[0, 1, 2])['order'].last() + 0.1
            # df2['order'] = sales_by_mid.groupby(level=[0, 1])['order'].last() + 0.2
            #
            # # set the levels of the index because its missing it
            # df1 = df1.assign(lev4='Corporation Total').set_index('lev4', append=True)
            # df2 = df2.assign(lev3='Month Total', lev4='').set_index(['lev3', 'lev4'], append=True)
            #
            # sales_by_mid = pd.concat([sales_by_mid, df1, df2])
            # sales_by_mid = sales_by_mid.sort_values(by=['date_created'], ascending=False)
            # sales_by_mid = sales_by_mid.sort_values(by=['order'], ascending=False)
            # sales_by_mid = sales_by_mid.drop(['order'], axis=1)
            #
            # upload_to_s3(sales_by_mid, 'sales_by_mid.xlsx')
            #
            # sales_by_corp = new_sales_healthy_living.groupby([
            #     pd.Grouper(key='client_id'),
            #     pd.Grouper(key='date_created', freq='MS'), 'Company name', 'Steps']).count()
            # sales_by_corp = sales_by_corp[
            #     sales_by_corp.index.get_level_values('date_created').astype('datetime64[ns]').isin(
            #         [previous_month, current_month])]
            # sales_by_corp = sales_by_corp.unstack('Steps')
            # sales_by_corp = sales_by_corp['sales_id']
            # sales_by_corp = sales_by_corp.sort_values(['date_created', 'Step-1'], ascending=False)
            #
            # upload_to_s3_no_table(sales_by_corp, 'sales_by_corp.xlsx')

        def sales_by_months_by_vertical(self):
            sales_by_months_by_vertical = \
                master_new_sales.groupby([pd.Grouper(key='client_id'), pd.Grouper(key='date_created', freq='MS'),
                                          'Product Category', 'Steps']).count()

            sales_by_months_by_vertical = sales_by_months_by_vertical.unstack('Steps')

            df2 = sales_by_months_by_vertical.groupby(level=[0, 1]).sum()
            sales_by_months_by_vertical['order'] = range(len(sales_by_months_by_vertical))
            df2['order'] = sales_by_months_by_vertical.groupby(level=[0, 1])['order'].last() + 0.2
            df2 = df2.assign(lev3='Month Total', lev4='').set_index(['lev3', 'lev4'], append=True)

            sales_by_months_by_vertical = pd.concat([sales_by_months_by_vertical, df2])
            sales_by_months_by_vertical = sales_by_months_by_vertical.sort_values(by='order')
            sales_by_months_by_vertical = sales_by_months_by_vertical.drop(['order'], axis=1)

            sales_by_months_by_vertical = sales_by_months_by_vertical.sort_values(['client_id', 'date_created'], ascending=False)

            add_upsell_take_rate(sales_by_months_by_vertical)

            sales_by_months_by_vertical_recent_months = sales_by_months_by_vertical[sales_by_months_by_vertical
                .index.get_level_values('date_created').astype('datetime64[ns]').isin([current_month, previous_month])]

            upload_to_s3(sales_by_months_by_vertical_recent_months, 'sales_by_months_by_vertical_recent_months.xlsx')
            upload_to_s3(sales_by_months_by_vertical, 'sales_by_months_by_vertical.xlsx')

        def weekly_sales(self):
            sales_for_model = master_new_sales.groupby([
                pd.Grouper(key='client_id'),
                pd.Grouper(key='date_created', freq='W-SUN'), 'Steps']).count()

            sales_for_model = sales_for_model.unstack('Steps').sort_values(by=['client_id', 'date_created'],
                                                                           ascending=False)
            upload_csv_to_s3(sales_for_model, 'sales_numbers_weekly_model.csv')

        def weekly_sales_2_months_recent(self):
            sales_by_months = master_new_sales.groupby([
                pd.Grouper(key='client_id'),
                pd.Grouper(key='date_created', freq='MS'), 'Steps']).count()

            sales_by_months = sales_by_months.unstack('Steps').sort_values(by=['date_created'], ascending=False)

            add_upsell_take_rate(sales_by_months)

            sales_by_all_months = sales_by_months

            sales_by_recent_months = sales_by_months[sales_by_months
                .index.get_level_values('date_created').astype('datetime64[ns]').isin([current_month, previous_month])]

            upload_csv_to_s3(sales_by_recent_months, 'output_sales_by_recent_2_months.csv')
            upload_csv_to_s3(sales_by_all_months, 'sales_by_all_months.csv')

        def yesterday_sales(self):
            try:
                data_yesterday = master_new_sales[(master_new_sales['date_created'] >= date_yesterday) &
                                                  (master_new_sales['date_created'] <= date_today)]

                sales_yesterday = data_yesterday.groupby([
                    pd.Grouper(key='client_id'),
                    pd.Grouper(key='date_created', freq='D'), 'network_name','Brand Name', 'country', 'Steps']).count()

                sales_yesterday = sales_yesterday.unstack('Steps')
                # sum the totals
                df1 = sales_yesterday.groupby(level=[0, 1, 2]).sum()
                df2 = sales_yesterday.groupby(level=[0, 1]).sum()

                # sort the totals to have them filter correctly to be last
                sales_yesterday['order'] = range(len(sales_yesterday))
                df1['order'] = sales_yesterday.groupby(level=[0, 1, 2])['order'].last() + 0.1
                df2['order'] = sales_yesterday.groupby(level=[0, 1])['order'].last() + 0.2

                # set the levels of the index because its missing it
                df1 = df1.assign(lev1='Network Total',lev2='',lev3='').set_index(['lev1','lev2','lev3'], append=True)
                df2 = df2.assign(lev3='Client Total', lev4='', lev5='', lev6='').set_index(['lev3', 'lev4', 'lev5', 'lev6'], append=True)

                sales_yesterday = pd.concat([sales_yesterday, df1, df2])
                sales_yesterday = sales_yesterday.sort_values(by='order')
                sales_yesterday = sales_yesterday.drop(['order'], axis=1)

                add_upsell_take_rate(sales_yesterday)
                # sales_yesterday['sales_id'].to_excel('testing_steps.xlsx')
                upload_to_s3(sales_yesterday, 'output_sales_yesterday.xlsx')

            except Exception as e:
                print(e)

        def network_weekly(self):
            sales_by_network_weekly = master_new_sales.groupby([
                pd.Grouper(key='client_id'),
                pd.Grouper(key='date_created', freq='W-SUN'), 'network_name',
                'Product Country', 'Steps']).count()

            sales_by_network_weekly = sales_by_network_weekly.unstack('Steps').sort_values(
                by=['client_id', 'date_created',
                    'network_name', 'Product Country'], ascending=False)

            # sum the totals
            df1 = sales_by_network_weekly.groupby(level=[0, 1, 2]).sum()
            df2 = sales_by_network_weekly.groupby(level=[0, 1]).sum()
            df3 = sales_by_network_weekly.groupby(level=[0]).sum()

            # sort the totals to have them filter correctly to be last
            sales_by_network_weekly['order'] = range(len(sales_by_network_weekly))
            df1['order'] = sales_by_network_weekly.groupby(level=[0, 1, 2])['order'].last() + 0.1
            df2['order'] = sales_by_network_weekly.groupby(level=[0, 1])['order'].last() + 0.2
            df3['order'] = sales_by_network_weekly.groupby(level=[0])['order'].last() + 0.3

            # set the levels of the index because its missing it
            df1 = df1.assign(lev4='Network Total').set_index('lev4', append=True)
            df2 = df2.assign(lev3='Week Total', lev4='').set_index(['lev3', 'lev4'], append=True)
            df3 = df3.assign(lev2='', lev3='', lev1='Client Total').set_index(['lev2', 'lev3', 'lev1'], append=True)

            sales_by_network_weekly = pd.concat([sales_by_network_weekly, df1, df2, df3])
            sales_by_network_weekly = sales_by_network_weekly.sort_values(by='order')
            sales_by_network_weekly = sales_by_network_weekly.drop(['order'], axis=1)

            add_upsell_take_rate(sales_by_network_weekly)

            upload_to_s3(sales_by_network_weekly, 'output_sales_by_network_weekly.xlsx')

            # recent weeks
            sales_by_network_weekly_recent = sales_by_network_weekly[sales_by_network_weekly.index
                .get_level_values('date_created').astype('datetime64[ns]').isin([last_week_sunday, this_week_sunday])]

            upload_to_s3(sales_by_network_weekly_recent, 'output_sales_by_network_weekly_recent.xlsx')

        def invoice_checking(self):
            sales_by_network_weekly = master_new_sales.groupby([
                pd.Grouper(key='date_created', freq='W-SUN'), 'network_name',
                'Product Country', 'Steps']).count()

            sales_by_network_weekly = sales_by_network_weekly.unstack('Steps').sort_values(by=['date_created',
                                                                                               'network_name',
                                                                                               'Product Country'],
                                                                                           ascending=False)
            # sum the totals
            # df1 = sales_by_network_weekly.groupby(level=[0, 1, 2]).sum()
            df2 = sales_by_network_weekly.groupby(level=[0, 1]).sum()
            df3 = sales_by_network_weekly.groupby(level=[0]).sum()

            # sort the totals to have them filter correctly to be last
            sales_by_network_weekly['order'] = range(len(sales_by_network_weekly))
            # df1['order'] = sales_by_network_weekly.groupby(level=[0, 1, 2])['order'].last() + 0.1
            df2['order'] = sales_by_network_weekly.groupby(level=[0, 1])['order'].last() + 0.2
            df3['order'] = sales_by_network_weekly.groupby(level=[0])['order'].last() + 0.3

            # set the levels of the index because its missing it
            # df1 = df1.assign(lev4='Network Total').set_index('lev4', append=True)
            df2 = df2.assign(lev3='Network Total', lev4='').set_index(['lev3', 'lev4'], append=True)
            df3 = df3.assign(lev2='', lev3='Week Total', lev1='').set_index(['lev2', 'lev3', 'lev1'], append=True)

            sales_by_network_weekly = pd.concat([sales_by_network_weekly, df2, df3])
            sales_by_network_weekly = sales_by_network_weekly.sort_values(by='order')
            sales_by_network_weekly = sales_by_network_weekly.drop(['order'], axis=1)

            upload_to_s3(sales_by_network_weekly, 'output_sales_checking_invoices.xlsx')

        def network_month(self):
            sales_by_network_monthly = master_new_sales.groupby([
                pd.Grouper(key='client_id'),
                pd.Grouper(key='date_created', freq='MS'), 'network_name',
                'Product Country', 'Steps']).count()

            sales_by_network_monthly = sales_by_network_monthly.unstack('Steps').sort_values(
                by=['client_id', 'date_created',
                    'network_name', 'Product Country'], ascending=False)

            # sum the totals
            df1 = sales_by_network_monthly.groupby(level=[0, 1, 2]).sum()
            df2 = sales_by_network_monthly.groupby(level=[0, 1]).sum()
            df3 = sales_by_network_monthly.groupby(level=[0]).sum()

            # sort the totals to have them filter correctly to be last
            sales_by_network_monthly['order'] = range(len(sales_by_network_monthly))
            df1['order'] = sales_by_network_monthly.groupby(level=[0, 1, 2])['order'].last() + 0.1
            df2['order'] = sales_by_network_monthly.groupby(level=[0, 1])['order'].last() + 0.2
            df3['order'] = sales_by_network_monthly.groupby(level=[0])['order'].last() + 0.3

            # set the levels of the index because its missing it
            df1 = df1.assign(lev4='Network Total').set_index('lev4', append=True)
            df2 = df2.assign(lev3='Month Total', lev4='').set_index(['lev3', 'lev4'], append=True)
            df3 = df3.assign(lev2='', lev3='', lev1='Client Total').set_index(['lev2', 'lev3', 'lev1'], append=True)

            sales_by_network_monthly = pd.concat([sales_by_network_monthly, df1, df2, df3])
            sales_by_network_monthly = sales_by_network_monthly.sort_values(by='order')
            sales_by_network_monthly = sales_by_network_monthly.drop(['order'], axis=1)

            add_upsell_take_rate(sales_by_network_monthly)

            upload_to_s3(sales_by_network_monthly, 'output_sales_by_network_month.xlsx')

            # recent network months
            sales_by_network_recent_months = sales_by_network_monthly[sales_by_network_monthly
                .index.get_level_values('date_created').astype('datetime64[ns]').isin([previous_month, current_month])]

            upload_to_s3(sales_by_network_recent_months, 'sales_by_wc_recent_previous_month.xlsx')

        def affiliate_sales(self):
            sales_by_affiliate = master_new_sales.groupby([
                pd.Grouper(key='client_id'),
                pd.Grouper(key='date_created', freq='MS'), 'network_name', 'Product Country',
                'sub_affiliate_id_1', 'country', 'Steps']).count()

            sales_by_affiliate = sales_by_affiliate.unstack('Steps').sort_values(
                by=['client_id', 'date_created',
                    'network_name'], ascending=False)

            upload_to_s3_no_table_csv(sales_by_affiliate['sales_id'], 'sales_by_affiliate.csv')

        def scrub_total(self):

            cpa_table_manually_updated = pd.read_excel('s3://elasticbeanstalk-us-west-2-271/data/Input/manual_updated_cpa_networks.xlsx',  sheet_name='Edited')
            affiliatecpa_table_manually_updated = pd.read_excel('s3://elasticbeanstalk-us-west-2-271/data/Input/manual_updated_cpa_networks.xlsx', sheet_name='Affiliate CPA')

            sales_by_scrub = master_new_sales[(master_new_sales['date_created'] > '2019-12-31') & (master_new_sales['Steps'] != 'Step-3')]

            sales_by_scrub = sales_by_scrub.drop(columns=['sales_id', 'campaign_name', 'mid_number', 'product_name', 'product_country'], axis=1)

            sales_by_scrub.loc[:,'sub_affiliate_id_1'] = sales_by_scrub['sub_affiliate_id_1'].astype(str)
            affiliatecpa_table_manually_updated.loc[:,'Affiliate'] = affiliatecpa_table_manually_updated['Affiliate'].astype(str)


            sales_by_scrub = sales_by_scrub.merge(cpa_table_manually_updated, how='left',
                                                  left_on=['network_name', 'country', 'Steps', 'Product Category'],
                                                  right_on=['Network', 'Country', 'Steps', 'Product Category']).drop(['Network', 'Country'], axis=1)

            sales_by_scrub = sales_by_scrub.merge(affiliatecpa_table_manually_updated, how='left',
                                                  left_on=['network_name', 'country', 'Steps', 'sub_affiliate_id_1'],
                                                  right_on=['Network', 'Country', 'Steps', 'Affiliate']).drop(['Network', 'Country','Affiliate'], axis=1)

            sales_by_scrub.loc[sales_by_scrub['Affiliate CPA'].notnull(), 'CPA'] = sales_by_scrub['Affiliate CPA']

            sales_by_scrub = sales_by_scrub.drop('Affiliate CPA', axis=1)

            sales_by_scrub['product_sku'] = sales_by_scrub['product_sku'].replace(['KZW-03'], 'KZW-01')
            sales_by_scrub['product_sku'] = sales_by_scrub['product_sku'].replace(['KZW-04'], 'KZW-02')

            sales_by_scrub_count = sales_by_scrub.groupby([pd.Grouper(key='client_id'), 'country', 'scrub']).count()
            sales_by_scrub_sum_cpa = sales_by_scrub.groupby([pd.Grouper(key='client_id'), 'country', 'scrub']).sum()\
                .rename(columns={'CPA': 'CPA Sum'})

            report_cpa = reduce(lambda x, y: pd.merge(x, y, how='left',
                                                                  left_on=['client_id', 'country', 'scrub'],
                                                                  right_on=['client_id', 'country', 'scrub']),
                                            [sales_by_scrub_count, sales_by_scrub_sum_cpa])\
                .drop(['network_name', 'product_sku', 'sub_affiliate_id_1', 'Product Category', 'Brand Name', 'Product Country'], axis=1)

            report_cpa = report_cpa.groupby(level=[0, 1]).sum()
            report_cpa['Average CPA'] = (report_cpa['CPA Sum'] / report_cpa['Steps']).round()

            report_cpa = pd.pivot_table(report_cpa, values='Average CPA', index=['country'], columns=['client_id'], aggfunc=np.sum, fill_value=0)

            upload_to_s3_no_table(report_cpa, 'output_sales_by_scrub_total.xlsx')

            def monthly():
                sales_by_scrub_count = sales_by_scrub.groupby(
                    [pd.Grouper(key='client_id'), pd.Grouper(key='date_created', freq='MS'), 'country',
                     'scrub']).count()
                sales_by_scrub_sum_cpa = sales_by_scrub.groupby(
                    [pd.Grouper(key='client_id'), pd.Grouper(key='date_created', freq='MS'), 'country', 'scrub']).sum() \
                    .rename(columns={'CPA': 'CPA Sum'})

                report_cpa = reduce(lambda x, y: pd.merge(x, y, how='left',
                                                          left_on=['client_id', 'date_created', 'country', 'scrub'],
                                                          right_on=['client_id', 'date_created', 'country', 'scrub']),
                                    [sales_by_scrub_count, sales_by_scrub_sum_cpa]) \
                    .drop(['network_name', 'product_sku', 'sub_affiliate_id_1', 'Product Category', 'Brand Name',
                           'Product Country'], axis=1)

                report_cpa = report_cpa.groupby(level=[0, 1, 2]).sum()
                report_cpa['Average CPA'] = (report_cpa['CPA Sum'] / report_cpa['Steps']).round()

                report_cpa = pd.pivot_table(report_cpa, values='Average CPA', index=['date_created', 'country'],
                                            columns=['client_id'], aggfunc=np.sum,
                                            fill_value=0).sort_values('date_created', ascending=False)
                upload_scrub_to_s3(report_cpa, 'output_sales_by_scrub_monthly.xlsx')

            def weekly():
                sales_by_scrub_count = sales_by_scrub.groupby(
                    [pd.Grouper(key='client_id'), pd.Grouper(key='date_created', freq='W-SUN'), 'country',
                     'scrub']).count()
                sales_by_scrub_sum_cpa = sales_by_scrub.groupby(
                    [pd.Grouper(key='client_id'), pd.Grouper(key='date_created', freq='W-SUN'), 'country', 'scrub']).sum() \
                    .rename(columns={'CPA': 'CPA Sum'})

                report_cpa = reduce(lambda x, y: pd.merge(x, y, how='left',
                                                          left_on=['client_id', 'date_created', 'country', 'scrub'],
                                                          right_on=['client_id', 'date_created', 'country', 'scrub']),
                                    [sales_by_scrub_count, sales_by_scrub_sum_cpa]) \
                    .drop(['network_name', 'product_sku', 'sub_affiliate_id_1', 'Product Category', 'Brand Name',
                           'Product Country'], axis=1)

                report_cpa = report_cpa.groupby(level=[0, 1, 2]).sum()
                report_cpa['Average CPA'] = (report_cpa['CPA Sum'] / report_cpa['Steps']).round()

                report_cpa = pd.pivot_table(report_cpa, values='Average CPA', index=['date_created', 'country'],
                                            columns=['client_id'], aggfunc=np.sum,
                                            fill_value=0).sort_values('date_created', ascending=False)

                upload_scrub_to_s3(report_cpa, 'output_sales_by_scrub_weekly.xlsx')

            def yesterday():
                try:
                    sales_by_scrub = master_new_sales[(master_new_sales['date_created'] >= date_yesterday) &
                                                      (master_new_sales['date_created'] <= date_today)]

                    sales_by_scrub = sales_by_scrub.drop(
                        columns=['sales_id', 'campaign_name', 'mid_number', 'product_name', 'product_country'], axis=1)

                    sales_by_scrub.loc[:, 'sub_affiliate_id_1'] = sales_by_scrub['sub_affiliate_id_1'].astype(str)
                    affiliatecpa_table_manually_updated.loc[:, 'Affiliate'] = affiliatecpa_table_manually_updated[
                        'Affiliate'].astype(str)

                    sales_by_scrub = sales_by_scrub.merge(cpa_table_manually_updated, how='left',
                                                          left_on=['network_name', 'country', 'Steps',
                                                                   'Product Category'],
                                                          right_on=['Network', 'Country', 'Steps',
                                                                    'Product Category']).drop(['Network', 'Country'],
                                                                                              axis=1)

                    sales_by_scrub = sales_by_scrub.merge(affiliatecpa_table_manually_updated, how='left',
                                                          left_on=['network_name', 'country', 'Steps',
                                                                   'sub_affiliate_id_1'],
                                                          right_on=['Network', 'Country', 'Steps', 'Affiliate']).drop(
                        ['Network', 'Country', 'Affiliate'], axis=1)

                    sales_by_scrub.loc[sales_by_scrub['Affiliate CPA'].notnull(), 'CPA'] = sales_by_scrub[
                        'Affiliate CPA']

                    sales_by_scrub = sales_by_scrub.drop('Affiliate CPA', axis=1)

                    sales_by_scrub['product_sku'] = sales_by_scrub['product_sku'].replace(['KZW-03'], 'KZW-01')
                    sales_by_scrub['product_sku'] = sales_by_scrub['product_sku'].replace(['KZW-04'], 'KZW-02')

                    sales_by_scrub_count = sales_by_scrub.groupby([pd.Grouper(key='client_id'), 'country', 'scrub']).count()
                    sales_by_scrub_sum_cpa = sales_by_scrub.groupby([pd.Grouper(key='client_id'), 'country', 'scrub']).sum() \
                        .rename(columns={'CPA': 'CPA Sum'})

                    report_cpa = reduce(lambda x, y: pd.merge(x, y, how='left',
                                                              left_on=['client_id', 'country', 'scrub'],
                                                              right_on=['client_id', 'country', 'scrub']),
                                        [sales_by_scrub_count, sales_by_scrub_sum_cpa]) \
                        .drop(['network_name', 'product_sku', 'sub_affiliate_id_1', 'Product Category', 'Brand Name',
                               'Product Country'], axis=1)

                    report_cpa = report_cpa.groupby(level=[0, 1]).sum()
                    report_cpa['Average CPA'] = (report_cpa['CPA Sum'] / report_cpa['Steps']).round()

                    report_cpa = pd.pivot_table(report_cpa, values='Average CPA', index=['country'], columns=['client_id'],
                                                aggfunc=np.sum, fill_value=0)

                    upload_scrub_to_s3(report_cpa, 'cpa_yesterday.xlsx')

                except Exception as e:
                    print(e)

            monthly()
            weekly()
            yesterday()

        def country_month(self):
            sales_by_network_monthly = master_new_sales.groupby([
                pd.Grouper(key='client_id'),
                pd.Grouper(key='date_created', freq='MS'), 'country',
                'Product Country', 'Steps']).count()

            sales_by_network_monthly = sales_by_network_monthly.unstack('Steps').sort_values(
                by=['client_id', 'date_created',
                    'country', 'Product Country'], ascending=False)
            # sum the totals
            df1 = sales_by_network_monthly.groupby(level=[0, 1, 2]).sum()
            df2 = sales_by_network_monthly.groupby(level=[0, 1]).sum()
            df3 = sales_by_network_monthly.groupby(level=[0]).sum()

            # sort the totals to have them filter correctly to be last
            sales_by_network_monthly['order'] = range(len(sales_by_network_monthly))
            df1['order'] = sales_by_network_monthly.groupby(level=[0, 1, 2])['order'].last() + 0.1
            df2['order'] = sales_by_network_monthly.groupby(level=[0, 1])['order'].last() + 0.2
            df3['order'] = sales_by_network_monthly.groupby(level=[0])['order'].last() + 0.3

            # set the levels of the index because its missing it
            df1 = df1.assign(lev4='Network Total').set_index('lev4', append=True)
            df2 = df2.assign(lev3='Month Total', lev4='').set_index(['lev3', 'lev4'], append=True)
            df3 = df3.assign(lev2='', lev3='', lev1='Client Total').set_index(['lev2', 'lev3', 'lev1'], append=True)

            sales_by_network_monthly = pd.concat([sales_by_network_monthly, df1, df2, df3])
            sales_by_network_monthly = sales_by_network_monthly.sort_values(by='order')
            sales_by_network_monthly = sales_by_network_monthly.drop(['order'], axis=1)

            add_upsell_take_rate(sales_by_network_monthly)

            upload_to_s3(sales_by_network_monthly, 'output_sales_by_network_month.xlsx')

            # recent network months
            sales_by_network_recent_months = sales_by_network_monthly[sales_by_network_monthly
                .index.get_level_values('date_created').astype('datetime64[ns]').isin([previous_month, current_month])]

            upload_to_s3(sales_by_network_recent_months, 'sales_country_month.xlsx')


    report_1 = call_all(Report())

    print('Finished Sales Number Job')

job()
