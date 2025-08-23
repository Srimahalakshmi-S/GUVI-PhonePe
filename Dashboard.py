import os
import streamlit as st
import pandas as pd
import plotly.express as px
import requests 
import difflib
import numpy as np
from datetime import datetime
import pydeck as pdk
import plotly.graph_objects as go
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# Read CSVs
df_agg_trans = pd.read_csv("data/agg_transactions.csv")
df_agg_ins=pd.read_csv("data/agg_insurance.csv")
df_agg_user_data = pd.read_csv("data/agg_users.csv")
df_map_ins_data = pd.read_csv("data/map_insurance.csv")
df_map_ins_hover_data = pd.read_csv("data/maphover_insurance.csv")
df_map_trans_data = pd.read_csv("data/map_transaction.csv")
df_map_user_data = pd.read_csv("data/map_user.csv")
df_top_ins_data = pd.read_csv("data/top_insurance.csv")
df_top_trans_data = pd.read_csv("data/top_transaction.csv")
df_top_user_data = pd.read_csv("data/top_user.csv")

# Sidebar menu
genre = st.sidebar.radio("Select", ["🏠 Home","📍 Dashboard", "📊 Business Case Study"], index=0)

#Home Page
if genre == "🏠 Home":
    st.markdown("### 📱 Welcome to PhonePe Transaction Insights")
    st.markdown('----')
    st.write("""Explore India's digital payment trends with real-time data from PhonePe.
    Track transactions, categories, user adoption, and more — state by state, quarter by quarter.
    """)
    st.markdown('--------')
    st.markdown("### 🔍 What you can explore here")
    st.markdown("""
    - **📊 Transaction Overview**: Analyze state-wise transaction volumes, values, and payment categories using aggregated data.  
    - **👥 User Insights**: Discover how registered users and app engagement vary across states and quarters.  
    - **🛡️ Insurance Trends**: Visualize regional insurance adoption and premium flow across time.  
    - **📍 Top Regions**: Identify the top-performing states, districts, and pincodes by transaction volume and user registrations.  
    - **🗺️ Geographic Visualization**: Explore India with interactive maps showing real-time transaction and user activity.  
    - **📈 Growth Over Time**: Track quarterly and yearly growth across transactions, users, and insurance categories.  
    - **💼 Payment Categories**: Dive into specific categories like peer-to-peer transfers, merchant payments, and bill payments.
    """)

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
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    geojson = requests.get(url).json()

    # Extract official state names from GeoJSON
    geo_states = [feature["properties"]["ST_NM"] for feature in geojson["features"]]

    # Function to match CSV state to closest GeoJSON state
    def match_state(state):
        state= state.replace("-", " ").replace("&", "and").title()
        best_match = difflib.get_close_matches(state, geo_states, n=1, cutoff=0.6)
        return best_match[0] if best_match else None

    # Apply matching
    df_agg_trans["state"] = df_agg_trans["state"].apply(match_state)

    # Aggregate transaction amounts at state level
    df_state = df_agg_trans.groupby("state", as_index=False)["amount"].sum()

    # Choropleth map
    fig = px.choropleth(
        df_state,
        geojson=geojson,
        featureidkey='properties.ST_NM',
        locations='state',
        color='amount',
        color_continuous_scale='earth',
        height=800,  # Bigger
        width=1100   # Wider
    )

    # Fix map scale and center (no cutting)
    fig.update_geos(
        visible=False,
        projection_type="mercator",
        projection_scale=5,  # zoom level (try 4–6)
        center={"lat": 22, "lon": 80}  # keep India centered
)
    fig.update_geos(fitbounds="locations", visible=False)
    st.markdown("## 🌍 Digital Transactions Across Indian States")
    st.plotly_chart(fig, use_container_width=True)


# Dashboard
def main():
    st.title("📊 PhonePe Analysis Dashboard")

    year = st.sidebar.multiselect('Select Year', df_agg_trans['year'].unique(),default=df_agg_trans['year'].unique())
    quarter = st.sidebar.multiselect('Select Quarter', df_agg_trans['quarter'].unique(), default=df_agg_trans['quarter'].unique())
    payment_category = st.sidebar.multiselect('Select Category', df_agg_trans['payment_category'].unique(), default=df_agg_trans['payment_category'].unique())

    df_select = df_agg_trans[
        (df_agg_trans['year'].isin(year)) &
        (df_agg_trans['quarter'].isin(quarter)) &
        (df_agg_trans['payment_category'].isin(payment_category))
    ]
    
    # Metrics first
    metric(df_select)

    # Map after metrics
    map(df_select)

    st.write('----')

    # Category wise Table
    st.markdown("## 🗂️ Category-wise Transaction Count")
    st.write('----')
    category_summary = (
        df_select.groupby("payment_category")["count"].sum().reset_index()
        .rename(columns={"payment_category": "Payment Category", "count": "Total Transactions"})
        .sort_values("Total Transactions", ascending=False)
    )
    category_summary["Total Transactions"]=category_summary["Total Transactions"].apply(lambda x: f"₹{x:,}")
    st.dataframe(category_summary,hide_index=True)

@st.cache_data
def run_query(query):
    return pd.read_sql(query, engine)

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
        "States with rise in transaction count": """
            SELECT state, year, quarter, SUM(count) as total
            FROM agg_transaction_data 
            GROUP BY state, year, quarter 
            ORDER BY total;
        """,
        "State vs Payment Category matrix": """
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
            SELECT state, brand, SUM(registered_users) AS total_users
            FROM agg_user_data
            GROUP BY state,brand
            ORDER BY total_users DESC
            LIMIT 50;
        """,
        "Device trend across quarters": """
            SELECT quarter,brand,year,SUM(registered_users)as total_users
            FROM agg_user_data
            GROUP BY year, brand,quarter
            ORDER BY total_users DESC;
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
        "Average Insurance Amount by State": """
            SELECT state, AVG(amount) AS avg_insurance
            FROM agg_insurance_data
            GROUP BY state
            ORDER BY avg_insurance DESC;
        """,
        "Year-wise Share of Total Insurance": """
            SELECT year, SUM(amount) AS total_insurance
            FROM agg_insurance_data
            GROUP BY year
            ORDER BY total_insurance ASC,year
            LIMIT 10;
        """
    }
    return pd.read_sql(queries[question3], engine)

#Transaction Analysis Across States and Districts
def run_query_state_district(question4):
    queries = {
        "Top 10 districts by Transaction amount": """
            SELECT district_name, SUM(d_amount) AS total_amount 
            FROM top_trans_data 
            GROUP BY district_name 
            ORDER BY total_amount DESC 
            LIMIT 10;
        """,
        "Year-wise transaction Analysis": """
            SELECT year,state_name, SUM(d_count) AS total_count 
            FROM top_trans_data 
            GROUP BY state_name,year 
            ORDER BY total_count DESC 
            LIMIT 10;
        """,
        "State-wise transaction comparison": """
            SELECT state, SUM(amount) AS total 
            FROM map_trans_data 
            GROUP BY state 
            ORDER BY total DESC;
        """,
        "Quarter-wise Transaction Trends": """
            SELECT year, quarter,SUM(d_amount) AS total_amount
            FROM top_trans_data
            GROUP BY year, quarter
            ORDER BY year, quarter;
        """,
        "Average Transaction analysis": """
            SELECT district_name, SUM(d_amount)/SUM(d_count) AS avg_txn_size
            FROM top_trans_data
            GROUP BY district_name
            ORDER BY avg_txn_size DESC
            LIMIT 10;
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
            SELECT quarter,pincode, SUM(p_regusers) AS TOTAL
            FROM top_user_data 
            GROUP BY pincode,quarter
            ORDER BY TOTAL DESC 
            LIMIT 10;
        """,
        "Districts with high registrations": """
            SELECT district_name, SUM(d_regusers) AS TOTAL
            FROM top_user_data 
            GROUP BY district_name
            ORDER BY TOTAL DESC
            LIMIT 10;
        """,
        "Compare the state registrations": """
            SELECT state,SUM(registered_users) AS total
            FROM map_user_data 
            GROUP BY state
            ORDER BY total
            LIMIT 10;
        """,
        "Pincode registration density": """
            SELECT year, SUM(p_regusers) AS total_users
            FROM top_user_data
            GROUP BY year
            ORDER BY total_users DESC;
        """
    }
    return pd.read_sql(queries[question5], engine)

case_study_questions = {
    "1. Decoding Transaction Dynamics on PhonePe": [
        "Total transactions per state",
        "Quarterly trend of total transactions",
        "Top payment categories by amount",
        "States with rise in transaction count",
        "State vs Payment Category matrix"
    ],
    "2. Device Dominance and User Engagement Analysis": [
        "Top 5 device brands by registrations",
        "Device-wise User Trend Over Time",
        "Underutilized device brands",
        "State-wise device preference",
        "Device trend across quarters"
    ],
    "3. Insurance Penetration and Growth Potential Analysis": [
        "Top states by insurance amount",
        "Quarterly insurance growth trend",
        "Seasonality in Insurance Uptake",
        "Average Insurance Amount by State",
        "Year-wise Share of Total Insurance"
    ],
    "4. Transaction Analysis Across States and Districts": [
        "Top 10 districts by Transaction amount",
        "Year-wise transaction Analysis",
        "State-wise transaction comparison",
        "Quarter-wise Transaction Trends",
        "Average Transaction analysis"
    ],
    "5. User Registration Analysis": [
        "Top states with highest amount of registrations",
        "Quarterly new users trend",
        "Districts with high registrations",
        "Compare the state registrations",
        "Pincode registration density"
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
    elif question1 == "States with rise in transaction count":
        fig = px.bar(df, x='quarter', y='total', color='state')
    elif question1 == "State vs Payment Category matrix":
        fig = px.density_heatmap(df, x='state', y='payment_category', z='total',color_continuous_scale=px.colors.diverging.balance_r)
    else:
        fig = px.bar(df)  # fallback
    st.plotly_chart(fig, use_container_width=True)

def display_chart2(question2, df):
    st.markdown(f"#### 📊 {question2}")
    
    if question2 == "Top 5 device brands by registrations":
        fig = px.bar(df, x='brand', y='users')
    elif question2 == "Device-wise User Trend Over Time":
        fig = px.density_heatmap(df,x='brand',y='year',z='total_registered_users',color_continuous_scale=px.colors.diverging.Picnic)
    elif question2 == "Underutilized device brands":
        fig = px.line(df, x='brand', y='users')
    elif question2 == "State-wise device preference":
        fig = px.bar(df, x='state',y='total_users',color='brand',color_discrete_sequence=px.colors.qualitative.Set3)
    elif question2 == "Device trend across quarters":
        fig = px.bar(df,x='brand',y='year',color='quarter')
    else:
        fig = px.bar(df)  # fallback
    st.plotly_chart(fig, use_container_width=True)

def display_chart3(question3, df):
    st.markdown(f"#### 📊 {question3}")
    
    if question3 == "Top states by insurance amount":
        fig = px.bar(df, x='state', y='total',color_discrete_sequence=px.colors.qualitative.Pastel)
    elif question3 == "Quarterly insurance growth trend":
        fig = px.line(df, x='quarter', y='total', color='year', markers=True)
    elif question3 == "Seasonality in Insurance Uptake":
        fig = px.bar(df, x='quarter', y='total_policies', text='total_policies',color_discrete_sequence=px.colors.qualitative.Vivid)
    elif question3 == "Average Insurance Amount by State":
        fig = px.bar(df, x="state", y="avg_insurance",title="Average Insurance Amount by State",color="avg_insurance",color_continuous_scale="Blues")
    elif question3 == "Year-wise Share of Total Insurance":
        fig = px.pie(df,names="year",values="total_insurance",color_discrete_sequence=px.colors.qualitative.Dark2)
    else:
        fig = px.bar(df)  # fallback
    st.plotly_chart(fig, use_container_width=True)

def display_chart4(question4, df):
    st.markdown(f"#### 📊 {question4}")
    
    if question4 == "Top 10 districts by Transaction amount":
        fig = px.bar(df, x='district_name', y='total_amount')
    elif question4 == "Year-wise transaction Analysis":
        fig = px.line(df, x='year', y='total_count')
    elif question4 == "State-wise transaction comparison":
        fig = px.bar(df, x='state', y='total',color_discrete_sequence=px.colors.qualitative.Vivid)
    elif question4 == "Quarter-wise Transaction Trends":
        fig=px.area(df,x='quarter',y='total_amount',color='year',markers=True,color_discrete_sequence=px.colors.qualitative.D3_r)
    elif question4 == "Average Transaction analysis":
        fig = px.bar(df,y="avg_txn_size",x="district_name",color_discrete_sequence=px.colors.qualitative.Alphabet)
    else:
        fig = px.bar(df)  # fallback
    st.plotly_chart(fig, use_container_width=True)

def display_chart5(question5, df):
    st.markdown(f"#### 📊 {question5}")
    
    if question5 == "Top states with highest amount of registrations":
        fig = px.bar(df, x='state',y='total',color='year')
    elif question5 == "Quarterly new users trend":
        fig = px.bar(df, x='quarter', y='total')
    elif question5 == "Districts with high registrations":
        fig = px.pie(df, values='total',names='district_name')
    elif question5 == "Compare the state registrations":
        fig = px.line(df, x='state', y='total')
    elif question5 == "Pincode registration density":
        fig = px.line(df, x='year', y='total_users')
    else:
        fig = px.bar(df)  # fallback
    st.plotly_chart(fig, use_container_width=True)

def case_study():
    st.title("List of Business Case Studies")
    st.write('--------')

    selected_case = st.sidebar.radio("Choose a Case Study", list(case_study_questions.keys()))
    questions = case_study_questions[selected_case]

    selected_q= st.selectbox("Select a Question", questions)

    if selected_case == "1. Decoding Transaction Dynamics on PhonePe":
        df = run_query_transaction(selected_q)
        display_chart1(selected_q, df)

    elif selected_case == "2. Device Dominance and User Engagement Analysis":
        df = run_query_device(selected_q)
        display_chart2(selected_q, df)

    elif selected_case == "3. Insurance Penetration and Growth Potential Analysis":
        df = run_query_insurance(selected_q)
        display_chart3(selected_q, df)

    elif selected_case == "4. Transaction Analysis Across States and Districts":
        df = run_query_state_district(selected_q)
        display_chart4(selected_q, df)

    elif selected_case == "5. User Registration Analysis":
        df = run_query_user(selected_q)
        display_chart5(selected_q, df)

# Run dashboard only if selected
if genre == "📍 Dashboard":

    main()

if genre == "📊 Business Case Study":

    case_study()
    

