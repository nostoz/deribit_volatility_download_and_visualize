import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from influxdb_wrapper import InfluxDBWrapper
from utils import *
from dateutil import parser as timeparser

# Replace 'your_influxdb_config.json' with the path to your configuration file
config = read_json('config.json')
wrapper = InfluxDBWrapper(config['database']['url'], config['database']['token'], config['database']['org'], 30_000)

st.set_page_config(layout="wide")
st.title('Volatility Dashboard')

col1, col2, col3 = st.columns(3)

# Define function to fetch volatility data for a given query
def fetch_volatility_data(query_func, **kwargs):
    try:
        data = query_func( **kwargs)
        return data
    except Exception as e:
        st.error(f"Error fetching data: {e}")
        return None

obs_time = '2023-07-26T06:05:00Z'
field = 'mid_iv'
vol_surface = fetch_volatility_data(wrapper.get_vol_surface_for_obs_time, bucket='eth_vol_surfaces', 
                                   measurement='volatility', obs_time=obs_time, field=field)
# display from expiry T+1
surface_display = vol_surface.iloc[1:]
# reverse rows and columns
surface_display = surface_display.iloc[::-1]
surface_display = surface_display[vol_surface.columns[::-1]]
fig = go.Figure(data=[go.Surface(x=surface_display.columns,
                                    y=surface_display.index,
                                    z=surface_display.values,
                                    contours = {
                                "x": {"show": True, "start": 0, "end": 20, "size": 1, "color":"gray"},
                                "y": {"show": True, "start": 0, "end": 10, "size": 1, "color":"gray"}},
                                colorscale='rdylbu',
                                reversescale=True)])

fig.update_layout(
    title=f'Volatility Surface on {timeparser.parse(obs_time).strftime("%Y-%m-%d %H:%M")}',
    # width=800,  
    # height=600,  
    scene=dict(
        xaxis=dict(
            tickmode='array',  
            tickvals=list(range(len(surface_display.columns))),  
            ticktext=surface_display.columns, 
            title='Delta'
        ),
        yaxis=dict(
            tickmode='array',  
            tickvals=list(range(len(surface_display.index))),
            ticktext=surface_display.index, 
            title='Expiry',
            type = 'category'
        ),
        zaxis=dict(title=field)
    
    )
)
with col1:
    st.plotly_chart(fig, theme="streamlit", use_container_width=True)



vol_smiles = wrapper.get_vol_surface_for_obs_time('eth_vol_surfaces', 'volatility', obs_time, field)
maturities = vol_smiles.index.to_list()
traces = []

for idx, maturity in enumerate(maturities):
    vol_data = vol_smiles.loc[[maturity]]

    if (idx in [1,3,7]):
        visible = True
    else:
        visible='legendonly'

    trace = go.Scatter(
        x=vol_data.columns,  
        y=vol_data.values[0],   
        mode='lines+markers',
        name=maturity,
        visible=visible
    )

    traces.append(trace)

min_vol = vol_surface.min().min() - 2
max_vol = vol_surface.max().max() + 2

layout = go.Layout(
    title=f'Smiles on {timeparser.parse(obs_time).strftime("%Y-%m-%d %H:%M")}',
    xaxis=dict(title='Delta'),
    yaxis=dict(title='Implied Volatility', range=[min_vol, max_vol]),
    # height = 800,
    # width = 1000
)
fig_smile = go.Figure(data=traces, layout=layout)
with col2:
    st.plotly_chart(fig_smile, theme="streamlit", use_container_width=True)



vol_surface = wrapper.get_vol_surface_for_obs_time('eth_vol_surfaces', 'volatility', obs_time, field)

deltas = vol_surface.columns.to_list()
traces = []

initial_display = [2,4,9,14,16]

for idx, delta in enumerate(deltas):
    vol_data = vol_surface[delta]

    if idx in initial_display:
        visible = True
    else:
        visible='legendonly'

    trace = go.Scatter(
        x=vol_data.index,  # Deltas as x-axis
        y=vol_data.values,   # Implied vols as y-axis
        mode='lines+markers',
        name=delta,
        visible=visible
    )

    traces.append(trace)

min_vol = vol_surface.iloc[:,initial_display].min().min() - 5
max_vol = vol_surface.iloc[:,initial_display].max().max() + 5

layout = go.Layout(
    title=f'Vol term structure on {timeparser.parse(obs_time).strftime("%Y-%m-%d %H:%M")}',
    xaxis=dict(title='Expiry'),
    yaxis=dict(title='Implied Volatility', range=[min_vol, max_vol]),
    height = 800,
    width = 1200,
    updatemenus=[
        dict(
            type="buttons",
            # direction="right",
            active=0,
            buttons=[
                dict(label="Expiries as Categories",
                        method="relayout",
                        args=[{"xaxis.type": "category"}]),
                dict(label="Expiries as Dates",
                        method="relayout",
                        args=[{"xaxis.type": "date"}])
            ]
        )
    ]
)

fig = go.Figure(data=traces, layout=layout)
fig.update_xaxes(type='category')
with col3:
    st.plotly_chart(fig, theme="streamlit", use_container_width=True)