import dash
from dash import dcc, html, callback, Output, Input
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd


df = pd.read_parquet("./car_rental_analytics.parquet")
fig3 = fig4 = px.line(
    df.groupby('model')
    .mean(numeric_only=True)
    .reset_index()
    .sort_values('rating')
    , 'model', 'rating')


df_fig1 = (
    df.groupby('state_name').sum()
    .reset_index()
    .sort_values('rentertripstaken', ascending=False)
)
fig1 = px.bar(df_fig1, 'state_name','rentertripstaken')







app = dash.Dash(__name__)

# Define the external CSS stylesheet for the dark theme
external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

# Apply the external CSS stylesheet to the application
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
# Definir el diseño del dashboard
app.layout = html.Div(
    style={'display':'flex', 'flex-direction': 'column', 'align-items':'middle'},
    children=[
        html.H1('Informe car rental', style={'textAlign': 'center'}),
        html.Div(
            style={'display': 'grid', 'grid-template-columns': '40vw 40vw','grid-template-rows': 'auto auto', 'margin':'auto'},
            children=[
                html.Div(
                    children=[
                        html.H3('Cantidad de autos rentados según estado'),
                        dcc.Graph(
                            id='graph1',
                            figure=fig1
                        )
                    ],
                ),
                html.Div(
                    children=[
                        html.H3('10 modelos o marcas más rentadas'),
                        dcc.Dropdown(['make','model'], 'model', id='dropdown-selection'),
                        dcc.Graph(id='plot2')
                    ],
                ),          
        
                html.Div(
                    children=[
                        html.H3('Rating promedio según marca por estados'),
                        dcc.Dropdown(df.state_name.unique(), 'Washington', id='dropdown-estados'),
                        dcc.Graph(id='plot3')
                    ],
                ),
                html.Div(
                    children=[
                        html.H3("Trabajo final bootcamp data engenieering Edvai"),
                        html.H4("Escuela de datos vivos"),
                        html.P("Este es el final del pipeline que empieza creando automatizaciones de ingesta, transformación y carga de datos en un data warehouse")
                    ],
                )
            ],
        ),
            
    ],
    className='dashboard'
)


@callback(
    Output('plot2', 'figure'),
    Input('dropdown-selection', 'value')
)
def plot_2(vaulue):
    df_fig2 = (
    df.groupby(vaulue).sum()
    .reset_index()
    .sort_values('rentertripstaken', ascending=False)
    .head(10)
)
    return px.pie(df_fig2, vaulue, 'rentertripstaken', hole=0.5)


@callback(
    Output('plot3', 'figure'),
    Input('dropdown-estados', 'value')
)
def rating_por_estados(value:str):
    return px.line(
        df.query("state_name == @value")
        .groupby('make')
        .mean(numeric_only=True)
        .reset_index()
        .sort_values('rating')
        , 'make', 'rating')


if __name__ == '__main__':
    app.run_server(debug=True)