from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html
from plotly import graph_objs as go
import dash
import pandas as pd
import dash_table
from server import app
import boto3
import io

pd.set_option('display.max_columns', 5000)
pd.set_option('display.width', 1000000)


def read_excel_boto(filename):
    session = boto3.session.Session()
    s3_client = session.client('s3')
    obj = s3_client.get_object(Bucket='elasticbeanstalk-us-west-2-271', Key='testing/data/Output/' + filename)
    data = obj['Body'].read()
    df = pd.read_excel(io.BytesIO(data))
    return df.to_dict('records')


def df_aws_s3_csv(filename):
    return pd.read_csv('s3://elasticbeanstalk-us-west-2-271/testing/data/Output/' + filename)


def df_aws_s3_excel(filename):
    return pd.read_excel('s3://elasticbeanstalk-us-west-2-271/testing/data/Output/' + filename)


def simple_table(df, update_callback):
    return html.Div([
                    html.Div([

                        dash_table.DataTable(
                            id=update_callback,
                            data=df.to_dict('records'),
                            filter_action='native',
                            columns=[{'id': c, 'name': c} for c in df.columns],
                            export_format='xlsx',
                            export_headers='display',
                            merge_duplicate_headers=True,
                            style_as_list_view=True),

                        dcc.Interval(
                            id='interval-component',
                            interval=432000000,  # in milliseconds
                            n_intervals=0),
                    ],

                        style={'width': '25%',
                               'display': 'inline',
                               'maxWidth': '40%',
                               'position': 'relative',
                               'top': '80px'

                               }),

                ])


def two_table(df, df2, update_callback, update_callback2):
    return html.Div([
                    html.Div([

                        dash_table.DataTable(
                            id=update_callback,
                            data=df.to_dict('records'),
                            filter_action='native',
                            columns=[{'id': c, 'name': c} for c in df.columns],
                            export_format='xlsx',
                            export_headers='display',
                            merge_duplicate_headers=True,
                            style_as_list_view=True, ),

                        dcc.Interval(
                            id='interval-component',
                            interval=432000000,  # in milliseconds
                            n_intervals=0),
                    ],
                        style={'width': '25%',
                               'display': 'inline',
                               'maxWidth': '40%',
                               'position': 'relative',
                               'top': '80px'

                               }),
            html.Br(),
            html.Div([html.H1('ALL MONTHS'),
                  dash_table.DataTable(
                      id=update_callback2,
                      data=df2.to_dict('records'),
                      filter_action='native',
                      columns=[{'id': c, 'name': c} for c in df2.columns],
                      export_format='xlsx',
                      export_headers='display',
                      merge_duplicate_headers=True,
                      style_as_list_view=True,

                  ),
                      dcc.Interval(
                          id='interval-component',
                          interval=432000000,  # in milliseconds
                          n_intervals=0),

                  ], style={'width': '25%',
                            'display': 'inline',
                            'maxWidth': '40%',
                            'position': 'relative',
                            'top': '60px'
                            }),

                ])


def yesterday_two_table(df, df2, update_callback, update_callback2):
    return html.Div([
                    html.Div([html.H1('Cost Per Sale'),

                        dash_table.DataTable(
                            id=update_callback,
                            data=df.to_dict('records'),
                            filter_action='native',
                            columns=[{'id': c, 'name': c} for c in df.columns],
                            export_format='xlsx',
                            export_headers='display',
                            merge_duplicate_headers=True,
                            style_as_list_view=True, ),

                        dcc.Interval(
                            id='interval-component',
                            interval=432000000,  # in milliseconds
                            n_intervals=0),
                    ],
                        style={'width': '25%',
                               'display': 'inline',
                               'maxWidth': '40%',
                               'position': 'relative',
                               'top': '80px'

                               }),
            html.Br(),
            html.Div([html.H1('Yesterday'),
                  dash_table.DataTable(
                      id=update_callback2,
                      data=df2.to_dict('records'),
                      filter_action='native',
                      columns=[{'id': c, 'name': c} for c in df2.columns],
                      export_format='xlsx',
                      export_headers='display',
                      merge_duplicate_headers=True,
                      style_as_list_view=True,

                  ),
                      dcc.Interval(
                          id='interval-component',
                          interval=432000000,  # in milliseconds
                          n_intervals=0),

                  ], style={'width': '25%',
                            'display': 'inline',
                            'maxWidth': '40%',
                            'position': 'relative',
                            'top': '60px'
                            }),

                ])


def three_table(df, df2, df3, update_callback, update_callback2, update_callback3):
    return html.Div([
        html.Div([

            dash_table.DataTable(
                id=update_callback,
                data=df.to_dict('records'),
                filter_action='native',
                columns=[{'id': c, 'name': c} for c in df.columns],
                export_format='xlsx',
                export_headers='display',
                merge_duplicate_headers=True,
                style_as_list_view=True, ),

            dcc.Interval(
                id='interval-component',
                interval=432000000,  # in milliseconds
                n_intervals=0),
        ],
            style={'width': '25%',
                   'display': 'inline',
                   'maxWidth': '40%',
                   'position': 'relative',
                   'top': '80px'

                   }),
        html.Br(),
        html.Div([html.H1('ALL WEEKS'),
                  dash_table.DataTable(
                      id=update_callback2,
                      data=df2.to_dict('records'),
                      filter_action='native',
                      columns=[{'id': c, 'name': c} for c in df2.columns],
                      export_format='xlsx',
                      export_headers='display',
                      merge_duplicate_headers=True,
                      style_as_list_view=True,

                  ),
                  dcc.Interval(
                      id='interval-component',
                      interval=432000000,  # in milliseconds
                      n_intervals=0),

                  ], style={'width': '25%',
                            'display': 'inline',
                            'maxWidth': '40%',
                            'position': 'relative',
                            'top': '60px'
                            }),
        html.Br(),
        html.Div([html.H1('ALL MONTHS'),
                  dash_table.DataTable(
                      id=update_callback3,
                      data=df3.to_dict('records'),
                      filter_action='native',
                      columns=[{'id': c, 'name': c} for c in df3.columns],
                      export_format='xlsx',
                      export_headers='display',
                      merge_duplicate_headers=True,
                      style_as_list_view=True,

                  ),
                  dcc.Interval(
                      id='interval-component',
                      interval=432000000,  # in milliseconds
                      n_intervals=0),

                  ], style={'width': '25%',
                            'display': 'inline',
                            'maxWidth': '40%',
                            'position': 'relative',
                            'top': '60px'
                            }),

    ]),


def four_table(df, df2, df3, update_callback, update_callback2, update_callback3, df4, update_callback5):
    return html.Div([
        html.Div([

            dash_table.DataTable(
                id=update_callback,
                data=df.to_dict('records'),
                filter_action='native',
                columns=[{'id': c, 'name': c} for c in df.columns],
                export_format='xlsx',
                export_headers='display',
                merge_duplicate_headers=True,
                style_as_list_view=True, ),

            dcc.Interval(
                id='interval-component',
                interval=432000000,  # in milliseconds
                n_intervals=0),
        ],
            style={'width': '25%',
                   'display': 'inline',
                   'maxWidth': '40%',
                   'position': 'relative',
                   'top': '80px'

                   }),
        html.Br(),
        html.Div([html.H1('Country'),
                  dash_table.DataTable(
                      id=update_callback5,
                      data=df4.to_dict('records'),
                      filter_action='native',
                      columns=[{'id': c, 'name': c} for c in df4.columns],
                      export_format='xlsx',
                      export_headers='display',
                      merge_duplicate_headers=True,
                      style_as_list_view=True,

                  ),
                  dcc.Interval(
                      id='interval-component',
                      interval=432000000,  # in milliseconds
                      n_intervals=0),

                  ], style={'width': '25%',
                            'display': 'inline',
                            'maxWidth': '40%',
                            'position': 'relative',
                            'top': '60px'
                            }),
        html.Br(),
        html.Div([html.H1('ALL WEEKS'),
                  dash_table.DataTable(
                      id=update_callback2,
                      data=df2.to_dict('records'),
                      filter_action='native',
                      columns=[{'id': c, 'name': c} for c in df2.columns],
                      export_format='xlsx',
                      export_headers='display',
                      merge_duplicate_headers=True,
                      style_as_list_view=True,

                  ),
                  dcc.Interval(
                      id='interval-component',
                      interval=432000000,  # in milliseconds
                      n_intervals=0),

                  ], style={'width': '25%',
                            'display': 'inline',
                            'maxWidth': '40%',
                            'position': 'relative',
                            'top': '60px'
                            }),
        html.Br(),
        html.Div([html.H1('ALL MONTHS'),
                  dash_table.DataTable(
                      id=update_callback3,
                      data=df3.to_dict('records'),
                      filter_action='native',
                      columns=[{'id': c, 'name': c} for c in df3.columns],
                      export_format='xlsx',
                      export_headers='display',
                      merge_duplicate_headers=True,
                      style_as_list_view=True,

                  ),
                  dcc.Interval(
                      id='interval-component',
                      interval=432000000,  # in milliseconds
                      n_intervals=0),

                  ], style={'width': '25%',
                            'display': 'inline',
                            'maxWidth': '40%',
                            'position': 'relative',
                            'top': '60px'
                            }),

    ]),


def yesterday_updated_usa(df, df2, df3, update_callback, update_callback2, update_callback3):
    return html.Div([
        html.Div([

            dash_table.DataTable(
                style_cell={
                    'whiteSpace': 'normal',
                    'height': 'auto',
                },
                style_data={
                    'whiteSpace': 'normal',
                    'height': 'auto',
                    'lineHeight': '15px'
                },
                id=update_callback,
                data=df.to_dict('records'),
                filter_action='native',
                sort_action='native',
                columns=[{'id': c, 'name': c} for c in df.columns],
                export_format='xlsx',
                export_headers='display',
                merge_duplicate_headers=True,
                # style_as_list_view=True,
            ),

            dcc.Interval(
                id='interval-component',
                interval=432000000,  # in milliseconds
                n_intervals=0),
        ],
            style={'width': '25%',
                   'display': 'inline',
                   'maxWidth': '40%',
                   'position': 'relative',
                   'top': '80px'

                   }),
        html.Br()
    ]),


def yesterday_international(df, df2, update_callback, update_callback2):
    return html.Div([
        html.Div([

            dash_table.DataTable(
                style_cell={
                    'whiteSpace': 'normal',
                    'height': 'auto',
                },
                style_data={
                    'whiteSpace': 'normal',
                    'height': 'auto',
                    'lineHeight': '15px'
                },
                id=update_callback,
                data=df.to_dict('records'),
                filter_action='native',
                sort_action='native',
                columns=[{'id': c, 'name': c} for c in df.columns],
                export_format='xlsx',
                export_headers='display',
                merge_duplicate_headers=True,
                # style_as_list_view=True,
            ),

            dcc.Interval(
                id='interval-component',
                interval=432000000,  # in milliseconds
                n_intervals=0),
        ],
            style={'width': '25%',
                   'display': 'inline',
                   'maxWidth': '40%',
                   'position': 'relative',
                   'top': '80px'

                   }),
        html.Br(),
        html.Div([html.H1('ALL WEEKS'),
                  dash_table.DataTable(
                      id=update_callback2,
                      data=df2.to_dict('records'),
                      filter_action='native',
                      columns=[{'id': c, 'name': c} for c in df2.columns],
                      export_format='xlsx',
                      export_headers='display',
                      merge_duplicate_headers=True,
                      style_as_list_view=True,

                  ),
                  dcc.Interval(
                      id='interval-component',
                      interval=432000000,  # in milliseconds
                      n_intervals=0),

                  ], style={'width': '25%',
                            'display': 'inline',
                            'maxWidth': '40%',
                            'position': 'relative',
                            'top': '60px'
                            }),
        html.Br(),

    ]),


df_sales_weekly_model = df_aws_s3_csv('sales_numbers_weekly_model.csv')
df_two_months_recent = df_aws_s3_csv('output_sales_by_recent_2_months.csv')
df_sales_all_months = df_aws_s3_csv('sales_by_all_months.csv')
df_affiliate = df_aws_s3_csv('sales_by_affiliate.csv')
df_sales_yesterday = df_aws_s3_excel('output_sales_yesterday.xlsx')
df_network = df_aws_s3_excel('output_sales_by_network_weekly.xlsx')
df_network_month = df_aws_s3_excel('output_sales_by_network_month.xlsx')
df_network_recent_month = df_aws_s3_excel('sales_by_wc_recent_previous_month.xlsx')
df_network_recent_week = df_aws_s3_excel('output_sales_by_network_weekly_recent.xlsx')
df_scrub_total = df_aws_s3_excel('output_sales_by_scrub_total.xlsx')
df_scrub_weekly = df_aws_s3_excel('output_sales_by_scrub_weekly.xlsx')
df_scrub_montly = df_aws_s3_excel('output_sales_by_scrub_monthly.xlsx')
df_cpa_yesterday = df_aws_s3_excel('cpa_yesterday.xlsx')
df_checking_invoices = df_aws_s3_excel('output_sales_checking_invoices.xlsx')
df_month_vertical = df_aws_s3_excel('sales_by_months_by_vertical_recent_months.xlsx')
df_sales_by_mid = df_aws_s3_excel('sales_by_mid.xlsx')
df_sales_by_corp = df_aws_s3_excel('sales_by_corp.xlsx')
country_month_sales = df_aws_s3_excel('sales_country_month.xlsx')
df_sales_vaultx = df_aws_s3_excel('sales_numbers_vaultx.xlsx')


sales_weekly_model = simple_table(df_sales_weekly_model, 'update_sales_numbers_weekly_model')
sales_by_mid = two_table(df_sales_by_corp, df_sales_by_mid, 'update_df_sales_by_corp', 'update_df_sales_by_mid')
network_weekly = simple_table(df_network_recent_week, 'update_df_network_recent_week')
network_month = two_table(df_network_recent_month, df_network_month,
                          'update_df_network_recent_month', 'update_network_month')
yesterday_layout = yesterday_updated_usa(df_sales_vaultx, df_cpa_yesterday, df_sales_yesterday, 'update_df_sales_vaultx', 'update_cpa_yesterday', 'update_yesterdaylayout' )
checking_invoices = simple_table(df_sales_yesterday,'update_checking_invoices')
months_recent = four_table(df_two_months_recent, df_month_vertical, df_sales_all_months,
                            'update_df_two_months_recent', 'update_df_month_vertical', 'update_df_sales_all_months', country_month_sales, 'update_country_month_sales')
scrub = three_table(df_scrub_total, df_scrub_weekly, df_scrub_montly, 'update_df_scrub_total',
                    'update_df_scrub_weekly', 'update_df_scrub_monthly')
affiliate = simple_table(df_affiliate, 'update_affiliate')

# updating tables every 12 hours
@app.callback([Output('update_yesterdaylayout', 'data'),
              Output('update_df_sales_vaultx', 'data')],
              [Input('interval-component', 'n_intervals')])
def update(n):
    return read_excel_boto('output_sales_yesterday.xlsx'), read_excel_boto('sales_numbers_vaultx.xlsx')


@app.callback(Output('update_cpa_yesterday', 'data'),
              [Input('interval-component', 'n_intervals')])
def update(n):
    return read_excel_boto('cpa_yesterday.xlsx')


@app.callback(Output('update_network_month', 'data'),
              [Input('interval-component', 'n_intervals')])
def update(n):
    return read_excel_boto('output_sales_by_network_month.xlsx')


@app.callback(Output('update_network_weekly', 'data'),
              [Input('interval-component', 'n_intervals')])
def update(n):
    return read_excel_boto('output_sales_by_network_weekly.xlsx')


@app.callback(Output('update_df_network_recent_month', 'data'),
              [Input('interval-component', 'n_intervals')])
def update(n):
    return read_excel_boto('sales_by_wc_recent_previous_month.xlsx')


@app.callback(Output('update_df_network_recent_week', 'data'),
              [Input('interval-component', 'n_intervals')])
def update(n):
    return read_excel_boto('output_sales_by_network_weekly_recent.xlsx')


@app.callback(Output('update_df_scrub_total', 'data'),
              [Input('interval-component', 'n_intervals')])
def update(n):
    return read_excel_boto('output_sales_by_scrub_total.xlsx')


@app.callback(Output('update_df_scrub_weekly', 'data'),
              [Input('interval-component', 'n_intervals')])
def update(n):
    return read_excel_boto('output_sales_by_scrub_weekly.xlsx')

@app.callback(Output('update_df_scrub_monthly', 'data'),
              [Input('interval-component', 'n_intervals')])
def update(n):
    return read_excel_boto('output_sales_by_scrub_monthly.xlsx')


@app.callback([Output('update_df_two_months_recent', 'data'),
               Output('update_country_month_sales', 'data')],
              [Input('interval-component', 'n_intervals')])
def update(n):
    df10 = df_aws_s3_csv('output_sales_by_recent_2_months.csv')
    return df10.to_dict('records'), read_excel_boto('sales_country_month.xlsx')


@app.callback(Output('update_df_sales_by_mid', 'data'),
              [Input('interval-component', 'n_intervals')])
def update(n):
    return read_excel_boto('sales_by_mid.xlsx')


@app.callback(Output('update_df_sales_by_corp', 'data'),
              [Input('interval-component', 'n_intervals')])
def update(n):
    return read_excel_boto('sales_by_corp.xlsx')

@app.callback(Output('update_sales_numbers_weekly_model', 'data'),
              [Input('interval-component', 'n_intervals')])
def update(n):
    df_sales_weekly_model = df_aws_s3_csv('sales_numbers_weekly_model.csv')
    return df_sales_weekly_model.to_dict('records')


@app.callback(Output('update_df_sales_all_months', 'data'),
              [Input('interval-component', 'n_intervals')])
def update(n):
    df_sales_weekly_model = df_aws_s3_csv('sales_by_all_months.csv')
    return df_sales_weekly_model.to_dict('records')


@app.callback(Output('updated_df_month_vertical', 'data'),
              [Input('interval-component', 'n_intervals')])
def update13(n):
    return read_excel_boto('sales_by_months_by_vertical_recent_months.xlsx')

@app.callback(Output('update_affiliate', 'data'),
              [Input('interval-component', 'n_intervals')])
def update(n):
    df = df_aws_s3_csv('sales_by_affiliate.csv')
    return df.to_dict('records')