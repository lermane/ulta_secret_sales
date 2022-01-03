import os
import pathlib

import dash
from dash.dependencies import Input, Output
from dash import dcc, html, dash_table
import pandas as pd

navy = '#06122F'
blue = '#0074D9'
gb_blue = '#004373'
red = '#C51E42'
light_grey = '#F6F6F6'
#=-----------------------

df = pd.read_csv('data/ss.csv')

app = dash.Dash(__name__)



app.layout = html.Div([

    html.Label('Select Team'),
    dcc.Dropdown(
        id='select_team',
        options=[{'label': i, 'value': i} for i in df['category_name'].unique()],
        value=[i for i in df['category_name']],
        searchable=True,
        style={'width':'75%'}
        ),
    dash_table.DataTable(
        id='general_stats',
        columns=[{"name": i, "id": i} for i in df.columns],
        data=df.to_dict('records'),
        style_header={
            'backgroundColor': blue,
            'fontWeight': 'bold',
            'color': 'white',
            'border': '0px'
        },
        style_cell={'padding': '5px', 'border': '0px', 'textAlign': 'center'},
        #style_data_conditional=[
        #{
        #    'if': {'row_index': 'odd'},
        #    'backgroundColor': light_grey,
        #    'if':{
        #        'filter_query': '{Team}= "TEAM SOLENT KESTRELS"' #filter to display our team
        #    },
        #    'backgroundColor': red,
        #    'color': 'white'
        #}
        #],
        #style_cell_conditional=[
        #{
        #'if': {'column_id': c},
        #    'textAlign': 'left'
        #} for c in ['Player', 'Team']
        #],
        style_as_list_view=True,
        page_action='native',
        fixed_rows={'headers':True},
        style_table={'height': '400px', 'overflowY': 'auto'},
        sort_action='native',
    )

])

@app.callback(
    [Output('general_stats', 'data')],
    [Input(component_id='select_team', component_property='value')]
)


def update_table(option_selected):     
    filtered_df = df[df.category_name == option_selected]      
    
    return[filtered_df.to_dict('records')]


if __name__ == '__main__':
    app.run_server(debug=True)