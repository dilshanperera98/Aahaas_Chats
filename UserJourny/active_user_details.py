from sqlalchemy import create_engine
import pandas as pd
import re
 
# -------------------------------
# Database connection
# -------------------------------
#database_url = 'mysql+pymysql://appletech:VRgRfgLb3ytX@aahaas-prod-db3.c8usp2ko6qdq.ap-southeast-1.rds.amazonaws.com:3306/production_test2'
database_url = 'mysql+pymysql://appletech:jkhgfy748*&Hjdlid@aahaas-prod-database-4.crgimm6mohf1.ap-southeast-1.rds.amazonaws.com:3306/production_live1'
 
# SQL query
query = """
SELECT user_id, route_name, calculated_duration, app_version, entered_at
FROM navigation_journey
WHERE user_id NOT IN (
    630,4030,4133,4241,10916314,10916975,41,46,288,404,992,993,994,995,
    1046,1047,1048,1049,1050,1053,1058,1092,1093,1104,1147,1205,1210,
    1222,1311,1431,1445,1624,1627,1665,4217,4263,4286,4289,4321,4345,
    4349,4376,4379,4392,4402,4403,4424,4435,4436,4437,4561,4790,7494,
    8114,8115,8116,8117,8118,8120,8492,10911003,10911017,10914122,10916975,
    1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,22,27,30,46,166,167,168,171,172,173,174,175,177,184,205,208,241,247,
3965,3966,3971,10910540
)
"""
 
# -------------------------------
# Connect and fetch data
# -------------------------------
engine = create_engine(database_url)
df = pd.read_sql(query, engine)
print("Database connected successfully. Rows fetched:", len(df))
 
# -------------------------------
# Convert entered_at to datetime and extract day
# -------------------------------
df['entered_at'] = pd.to_datetime(df['entered_at'])
df['day'] = df['entered_at'].dt.date
 
# -------------------------------
# Convert calculated_duration ("X min Y sec") to total seconds
# -------------------------------
def duration_to_seconds(duration_str):
    match = re.match(r'(?:(\d+)\s*min)?\s*(?:(\d+)\s*sec)?', str(duration_str))
    if match:
        minutes = int(match.group(1)) if match.group(1) else 0
        seconds = int(match.group(2)) if match.group(2) else 0
        return minutes * 60 + seconds
    return 0
 
df['calculated_duration_sec'] = df['calculated_duration'].apply(duration_to_seconds)
 
# -------------------------------
# Ask user to enter a specific date
# -------------------------------
input_date = input("Enter the Date (YYYY-MM-DD): ").strip()
try:
    specific_date = pd.to_datetime(input_date).date()
except:
    print("Invalid date format. Please use YYYY-MM-DD")
    exit()
 
# Filter data for the specific date
df_date = df[df['day'] == specific_date]
 
if df_date.empty:
    print(f"No data found for {specific_date}")
else:
    # -------------------------------
    # 01. Daily Active Users
    # -------------------------------
    daily_active_users = df_date['user_id'].nunique()
    print(f"\nDaily Active Users on {specific_date}: {daily_active_users}")
 
    # -------------------------------
    # 02. Average Session Duration Per User
    # -------------------------------
    # Total duration per user
    user_day_duration = df_date.groupby('user_id')['calculated_duration_sec'].sum().reset_index()
 
    # Average session duration per user
    avg_duration_sec = user_day_duration['calculated_duration_sec'].mean()
 
    # Convert seconds to minutes & seconds (rounded)
    avg_duration_str = f"{int(avg_duration_sec // 60)}m {round(avg_duration_sec % 60)}s"
    print(f"Average Session Duration Per User on {specific_date}: {avg_duration_str}")