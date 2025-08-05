import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime
import pydeck as pdk
from geopy.geocoders import Nominatim
from sqlalchemy import create_engine
import psycopg2

engine = create_engine("postgresql+psycopg2://postgres:281299@localhost:5432/phone_pe")

# Read CSVs
df_agg_trans = pd.read_csv("data/agg_transactions.csv")
df_agg_user_data = pd.read_csv("data/agg_users.csv")
df_map_ins_data = pd.read_csv("data/map_insurance.csv")
df_map_ins_hover_data = pd.read_csv("data/maphover_insurance.csv")
df_map_trans_data = pd.read_csv("data/map_transaction.csv")
df_map_user_data = pd.read_csv("data/map_user.csv")
df_top_ins_data = pd.read_csv("data/top_insurance.csv")
df_top_trans_data = pd.read_csv("data/top_transaction.csv")
df_top_user_data = pd.read_csv("data/top_user.csv")

# Sidebar menu
genre = st.sidebar.radio("Select", ["Dashboard", "Business Case Study"], index=None)

#Home Page
if genre is None:
    st.markdown("### 📱Welcome to PhonePe Transaction Insights")
    st.write("""Explore India's digital payment trends with real-time data from PhonePe Pulse.
    Track transactions, categories, user adoption, and more — state by state, quarter by quarter.
    """)
    st.markdown('--------')
    st.subheader("🔍 What You Can Explore")
    st.markdown("""
                - **Transaction Analysis:** Volume and value trends by state, category, and time.
                - **User Behavior:** Device types, app adoption trends, and registration growth.
                - **Insurance Insights:** Regional insurance penetration and premium patterns.
                - **Top Performers:** States/districts with the highest transactions and users.""")


# Metrics block
def metric(df_agg_trans):
    st.markdown('-----')    

    a, b, c = st.columns(3)
    d, e, f = st.columns(3)

    a.metric('Total Categories', df_agg_trans['payment_category'].nunique(),border=True)
    b.metric('Total Transactions', f"{round(df_agg_trans['count'].sum() / 1000000000, 2)}L",border=True)
    c.metric('Total Amount', f"{round(df_agg_trans['amount'].sum() / 1000000000000, 2)}Cr",border=True)
    d.metric('Total States', df_agg_trans['state'].nunique(),border=True)
    e.metric('Average Transactions', f"{round(df_agg_trans['count'].mean()/ 1000000, 2)}L",border=True)
    f.metric('Average Amount', f"{round(df_agg_trans['amount'].mean()/ 1000000000, 2)}Cr",border=True)

    st.markdown('-----')

# Map
def map(df_agg_trans):
    st.title('Map')
    df_agg_trans = df_agg_trans[['state', 'amount', 'lat', 'lon', 'count']]

    st.pydeck_chart(
        pdk.Deck(
            map_style=None,
            initial_view_state=pdk.ViewState(
                latitude=df_agg_trans['lat'].mean(),
                longitude=df_agg_trans['lon'].mean(),
                zoom=4,
                pitch=25,
            ),
            layers=[
                pdk.Layer(
                    "HexagonLayer",
                    data=df_agg_trans,
                    get_position="[lon, lat]",
                    get_elevation="count",
                    radius=20000,
                    elevation_scale=400,
                    elevation_range=[0, 1000],
                    pickable=True,
                    extruded=True,
                    auto_highlight=True,
                    coverage=1
                ),
                pdk.Layer(
                    "ScatterplotLayer",
                    data=df_agg_trans,
                    get_position="[lon, lat]",
                    get_color="[200, 30, 0, 160]",
                    get_radius=5000,
                    
                ),
            ],
            tooltip={
                "html": "<b>State:</b> {state}<br/><b>Amount:</b> ₹{amount}",
                "style": {
                    "backgroundColor": "steelblue",
                    "color": "white"
                }}
        )
    )

#Dashboard
def main():
    st.title("PhonePe Pulse Analysis Dashboard")

    year = st.sidebar.multiselect('Select Year', df_agg_trans['year'].unique(),default=df_agg_trans['year'].unique())
    quarter = st.sidebar.multiselect('Select Quarter', df_agg_trans['quarter'].unique(), default=df_agg_trans['quarter'].unique())
    payment_category = st.sidebar.multiselect('Select Category', df_agg_trans['payment_category'].unique(), default=df_agg_trans['payment_category'].unique())

    df_select = df_agg_trans[
        (df_agg_trans['year'].isin(year)) &
        (df_agg_trans['quarter'].isin(quarter)) &
        (df_agg_trans['payment_category'].isin(payment_category))
    ]
    metric(df_select)
    map(df_select)
    st.write('----')

#1. Decoding Transaction Dynamics on PhonePe
def run_query_transaction(question1):
    queries = {
        "Total transactions per state": """
            SELECT state, SUM(count) as total_transactions 
            FROM agg_transaction_data 
            GROUP BY state 
            ORDER BY total_transactions DESC;
        """,
        "Quarterly trend of total transactions": """
            SELECT year, quarter, SUM(count) as total 
            FROM agg_transaction_data 
            GROUP BY year, quarter 
            ORDER BY year, quarter;
        """,
        "Top payment categories by amount": """
            SELECT payment_category, SUM(amount) as total_amount 
            FROM agg_transaction_data 
            GROUP BY payment_category 
            ORDER BY total_amount DESC 
            LIMIT 10;
        """,
        "States with decline in transaction count": """
            SELECT state, year, quarter, SUM(count) as total
            FROM agg_transaction_data 
            GROUP BY state, year, quarter 
            ORDER BY state, year, quarter;
        """,
        "State vs category matrix": """
            SELECT state, payment_category, SUM(amount) as total 
            FROM agg_transaction_data 
            GROUP BY state, payment_category;
        """
    }
    return pd.read_sql(queries[question1], engine)

#Device Dominance and User Engagement Analysis
def run_query_device(question2):
    queries = {
        "Top 5 device brands by registrations": """
            SELECT brand, SUM(registered_users) AS users 
            FROM agg_user_data 
            GROUP BY brand 
            ORDER BY users DESC
        """,
        "Device-wise User Trend Over Time": """
            SELECT brand,year,quarter,SUM(registered_users) AS total_registered_users
            FROM agg_user_data
            GROUP BY brand, year, quarter
            ORDER BY brand, year, quarter;
        """,
        "Underutilized device brands": """
            SELECT brand, SUM(registered_users) AS users
            FROM agg_user_data 
            GROUP BY brand;
        """,
        "State-wise device preference": """
            SELECT state, brand
            FROM agg_user_data 
            GROUP BY state,brand;
        """,
        "Device trend across quarters": """
            SELECT brand,year, quarter, SUM(registered_users) AS total_users 
            FROM agg_user_data 
            GROUP BY year, quarter,brand;
        """
    }
    return pd.read_sql(queries[question2], engine)

#Insurance Penetration and Growth Potential Analysis
def run_query_insurance(question3):
    queries = {
        "Top states by insurance amount": """
            SELECT state, SUM(amount) AS total 
            FROM agg_insurance_data 
            GROUP BY state 
            ORDER BY total DESC;
        """,
        "Quarterly insurance growth trend": """
            SELECT year, quarter, SUM(count) AS total 
            FROM agg_insurance_data 
            GROUP BY year, quarter 
            ORDER BY year, quarter;
        """,
        "Seasonality in Insurance Uptake": """
            SELECT quarter, SUM(count) AS total_policies 
            FROM agg_insurance_data 
            GROUP BY quarter 
            ORDER BY quarter;
        """,
        "Compare insurance vs transactions": """
            SELECT state, district, SUM(amount) AS amount 
            FROM map_ins_data 
            GROUP BY state, district;
        """,
        "Untapped insurance states": """
            SELECT state, AVG(amount/count) AS avg_size 
            FROM agg_insurance_data 
            GROUP BY state;
        """
    }
    return pd.read_sql(queries[question3], engine)

#Transaction Analysis Across States and Districts
def run_query_state_district(question4):
    queries = {
        "Top 10 districts by amount": """
            SELECT state, SUM(d_amount) AS total_amount 
            FROM top_trans_data 
            GROUP BY state 
            ORDER BY total_amount DESC 
            LIMIT 10;
        """,
        "District-wise heatmap": """
            SELECT district_name, SUM(d_count) AS total_count 
            FROM top_trans_data 
            GROUP BY district_name 
            ORDER BY total_count DESC 
            LIMIT 10;
        """,
        "State-wise transaction comparison": """
            SELECT state, SUM(amount) AS total 
            FROM map_trans_data 
            GROUP BY state;
        """,
        "District contribution to state total": """
            SELECT pincode, SUM(p_amount) AS total 
            FROM top_trans_data 
            GROUP BY pincode;
        """,
        "Pincode-wise breakdown": """
            SELECT pincode,year, quarter, SUM(p_count) 
            FROM top_trans_data 
            GROUP BY pincode, year, quarter;
        """
    }
    return pd.read_sql(queries[question4], engine)

#User Registration Analysis
def run_query_user(question5):
    queries = {
        "Top states with highest amount of registrations": """
            SELECT state,year,SUM(d_regusers) AS TOTAL
            FROM top_user_data 
            GROUP BY state,year
            ORDER BY TOTAL DESC
            LIMIT 10;
        """,
        "Quarterly new users trend": """
            SELECT pincode, SUM(p_regusers) AS TOTAL
            FROM top_user_data 
            GROUP BY pincode 
            ORDER BY 2 DESC 
            LIMIT 10;
        """,
        "Districts with high registrations": """
            SELECT district_name,year, SUM(d_regusers) AS TOTAL
            FROM top_user_data 
            GROUP BY district_name,year 
            ORDER BY TOTAL DESC;
        """,
        "Compare registrations and transactions": """
            SELECT district_name, AVG(registered_users) AS avg_users 
            FROM map_user_data 
            GROUP BY district_name;
        """,
        "Pincode-wise registration density": """
            SELECT pincode, SUM(p_regusers) AS total_users
            FROM top_user_data
            GROUP BY pincode
            ORDER BY total_users DESC 
            LIMIT 20;
        """
    }
    return pd.read_sql(queries[question5], engine)

case_study_questions = {
    "1. Transaction Dynamics": [
        "Total transactions per state",
        "Quarterly trend of total transactions",
        "Top payment categories by amount",
        "States with decline in transaction count",
        "State vs category matrix"
    ],
    "2. Device Engagement": [
        "Top 5 device brands by registrations",
        "Device-wise User Trend Over Time",
        "Underutilized device brands",
        "State-wise device preference",
        "Device trend across quarters"
    ],
    "3. Insurance Analysis": [
        "Top states by insurance amount",
        "Quarterly insurance growth trend",
        "Seasonality in Insurance Uptake",
        "Compare insurance vs transactions",
        "Untapped insurance states"
    ],
    "4. State & District Transactions": [
        "Top 10 districts by amount",
        "District-wise heatmap",
        "State-wise transaction comparison",
        "District contribution to state total",
        "Pincode-wise breakdown"
    ],
    "5. User Registration Trends": [
        "Top states with highest amount of registrations",
        "Quarterly new users trend",
        "Districts with high registrations",
        "Compare registrations and transactions",
        "Pincode-wise registration density"
    ]
}

def display_chart1(question1, df):
    st.markdown(f"#### 📊 {question1}")
    
    if question1 == "Total transactions per state":
        fig = px.bar(df, x='state', y='total_transactions')
    elif question1 == "Quarterly trend of total transactions":
        fig = px.line(df, x='quarter', y='total', color='year', markers=True)
    elif question1 == "Top payment categories by amount":
        fig = px.pie(df, names='payment_category', values='total_amount')
    elif question1 == "States with decline in transaction count":
        fig = px.line(df, x='quarter', y='total', color='state')
    elif question1 == "State vs category matrix":
        fig = px.density_heatmap(df, x='state', y='payment_category', z='total')
    else:
        fig = px.bar(df)  # fallback
    st.plotly_chart(fig, use_container_width=True)

def display_chart2(question2, df):
    st.markdown(f"#### 📊 {question2}")
    
    if question2 == "Top 5 device brands by registrations":
        fig = px.bar(df, x='brand', y='users')
    elif question2 == "Device-wise User Trend Over Time":
        fig = px.histogram(df,x='brand',y='year',color='total_registered_users')
    elif question2 == "Underutilized device brands":
        fig = px.line(df, x='brand', y='users')
    elif question2 == "State-wise device preference":
        fig = px.bar(df, x='brand', y='state')
    elif question2 == "Device trend across quarters":
        fig = px.pie(df, names='brand', values='quarter', color='year')
    else:
        fig = px.bar(df)  # fallback
    st.plotly_chart(fig, use_container_width=True)

def display_chart3(question3, df):
    st.markdown(f"#### 📊 {question3}")
    
    if question3 == "Top states by insurance amount":
        fig = px.bar(df, x='state', y='total')
    elif question3 == "Quarterly insurance growth trend":
        fig = px.line(df, x='quarter', y='total', color='year', markers=True)
    elif question3 == "Seasonality in Insurance Uptake":
        fig = px.bar(df, x='quarter', y='total_policies', text='total_policies')
    elif question3 == "Compare insurance vs transactions":
        fig = px.line(df, x='district', y='amount', color='state')
    elif question3 == "Untapped insurance states":
        fig = px.bar(df, x='state', y='avg_size')
    else:
        fig = px.bar(df)  # fallback
    st.plotly_chart(fig, use_container_width=True)

def display_chart4(question4, df):
    st.markdown(f"#### 📊 {question4}")
    
    if question4 == "Top 10 districts by amount":
        fig = px.bar(df, x='state', y='total_amount')
    elif question4 == "District-wise heatmap":
        fig = px.density_heatmap(df, x='district_name', y='total_count',z=None)
    elif question4 == "State-wise transaction comparison":
        fig = px.bar(df, x='state', y='total',color='total')
    elif question4 == "District contribution to state total":
        fig = px.histogram(df, x='pincode', y='total')
    elif question4 == "Pincode-wise breakdown":
        fig = px.pie(df,names='year',values='quarter',color="pincode")
    else:
        fig = px.bar(df)  # fallback
    st.plotly_chart(fig, use_container_width=True)

def display_chart5(question5, df):
    st.markdown(f"#### 📊 {question5}")
    
    if question5 == "Top states with highest amount of registrations":
        fig = px.line(df, x='total', y='state',color='year')
    elif question5 == "Quarterly new users trend":
        fig = px.bar(df, x='pincode', y='total')
    elif question5 == "Districts with high registrations":
        fig = px.pie(df, names='year',values='total',color='district_name')
    elif question5 == "Compare registrations and transactions":
        fig = px.line(df, x='avg_users', y='district_name')
    elif question5 == "Pincode-wise registration density":
        fig = px.density_heatmap(df, x='pincode', y='total_users')
    else:
        fig = px.bar(df)  # fallback
    st.plotly_chart(fig, use_container_width=True)

def case_study():
    st.title("List of Business Case Studies")
    st.write('--------')

    selected_case = st.sidebar.radio("Choose a Case Study", list(case_study_questions.keys()))
    questions = case_study_questions[selected_case]

    selected_q= st.selectbox("Select a Question", questions)

    if selected_case == "1. Transaction Dynamics":
        df = run_query_transaction(selected_q)
        display_chart1(selected_q, df)

    elif selected_case == "2. Device Engagement":
        df = run_query_device(selected_q)
        display_chart2(selected_q, df)

    elif selected_case == "3. Insurance Analysis":
        df = run_query_insurance(selected_q)
        display_chart3(selected_q, df)

    elif selected_case == "4. State & District Transactions":
        df = run_query_state_district(selected_q)
        display_chart4(selected_q, df)

    elif selected_case == "5. User Registration Trends":
        df = run_query_user(selected_q)
        display_chart5(selected_q, df)

# Run dashboard only if selected
if genre == "Dashboard":
    main()

if genre == "Business Case Study":
    case_study()
    

