from sqlalchemy import create_engine

import pandas as pd

import re

import os
 
# -------------------------------

# Database connection

# -------------------------------

# Using production_live1 database

database_url = 'mysql+pymysql://appletech:jkhgfy748*&Hjdlid@aahaas-prod-database-4.crgimm6mohf1.ap-southeast-1.rds.amazonaws.com:3306/production_live1'

engine = create_engine(database_url)
 
# -------------------------------

# Ask user to enter a specific date

# -------------------------------

input_date = input("Enter the Date (YYYY-MM-DD): ").strip()

try:

    specific_date = pd.to_datetime(input_date).date()

except:

    print("Invalid date format. Please use YYYY-MM-DD")

    exit()
 
# -------------------------------

# SQL Query - Fixed to use production_live1

# -------------------------------

query = f"""

SELECT

    nl.user_id,

    nl.app_version,

    JSON_UNQUOTE(JSON_EXTRACT(nl.device_info, '$.system_name')) AS system_name,

    j.route_name,

    TIMESTAMPDIFF(SECOND, j.entered_at, j.exited_at) AS duration_sec

FROM production_live1.navigation_logs nl

JOIN JSON_TABLE(

    nl.navigation_journey,

    '$[*]' COLUMNS (

        route_name   VARCHAR(255) PATH '$.route_name',

        entered_at   DATETIME PATH '$.entered_at',

        exited_at    DATETIME PATH '$.exited_at'

    )

) AS j

ON TRUE

WHERE DATE(j.entered_at) = '{specific_date}' and user_id NOT IN (

    630,4030,4133,4241,10916314,10916975,41,46,288,404,992,993,994,995,

    1046,1047,1048,1049,1050,1053,1058,1092,1093,1104,1147,1205,1210,

    1222,1311,1431,1445,1624,1627,1665,4217,4263,4286,4289,4321,4345,

    4349,4376,4379,4392,4402,4403,4424,4435,4436,4437,4561,4790,7494,

    8114,8115,8116,8117,8118,8120,8492,10911003,10911017,10914122,10916975,

    1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,22,27,30,46,166,167,168,171,172,173,174,175,177,184,205,208,241,247,

    3965,3966,3971,10910540

)

ORDER BY nl.user_id, j.entered_at

"""
 
# -------------------------------

# Fetch data with error handling

# -------------------------------

try:

    df = pd.read_sql(query, engine)

    if df.empty:

        print(f"No data found for {specific_date}")

        exit()

    print(f"Found {len(df)} records for {specific_date}")

except Exception as e:

    print(f"Error executing query: {e}")

    # Let's check what tables are available

    try:

        tables_query = "SHOW TABLES"

        tables_df = pd.read_sql(tables_query, engine)

        print("Available tables in the database:")

        print(tables_df)

    except Exception as table_error:

        print(f"Could not list tables: {table_error}")

    exit()
 
# -------------------------------

# Convert duration seconds to Xm Ys

# -------------------------------

def format_duration(seconds):

    if pd.isna(seconds) or seconds < 0:

        return "0m 0s"

    minutes = int(seconds // 60)

    sec = int(seconds % 60)

    return f"{minutes}m {sec}s"
 
df['calculated_duration'] = df['duration_sec'].apply(format_duration)

df.drop('duration_sec', axis=1, inplace=True)
 
# -------------------------------

# Save to Excel

# -------------------------------

output_dir = r'/Users/dilshanperera/Desktop/User Journy/user_journey_2025-09-22.csv'

try:

    os.makedirs(output_dir, exist_ok=True)

except Exception as e:

    print(f"Could not create directory: {e}")

    output_dir = os.getcwd()  # Use current directory as fallback
 
output_file = os.path.join(output_dir, f"user_journey_{specific_date}.xlsx")
 
# Save Excel with grouping for better readability

try:

    with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:

        df.to_excel(writer, index=False, sheet_name='UserJourney')

        # Get the workbook and worksheet objects

        workbook = writer.book

        worksheet = writer.sheets['UserJourney']

        # Add some formatting

        header_format = workbook.add_format({

            'bold': True,

            'text_wrap': True,

            'valign': 'top',

            'fg_color': '#D7E4BC',

            'border': 1

        })

        # Write the column headers with formatting

        for col_num, value in enumerate(df.columns.values):

            worksheet.write(0, col_num, value, header_format)

        # Auto-adjust column widths

        for i, col in enumerate(df.columns):

            max_length = max(df[col].astype(str).map(len).max(), len(col)) + 2

            worksheet.set_column(i, i, min(max_length, 50))

    print(f"Report exported successfully: {output_file}")

except Exception as e:

    print(f"Error saving Excel file: {e}")

    # Fallback to CSV

    csv_file = os.path.join(output_dir, f"user_journey_{specific_date}.csv")

    df.to_csv(csv_file, index=False)

    print(f"Saved as CSV instead: {csv_file}")
 
# Display summary statistics

print(f"\n=== Summary ===")

print(f"Total records: {len(df)}")

print(f"Unique users: {df['user_id'].nunique()}")

print(f"Unique routes: {df['route_name'].nunique()}")

print(f"Date range: {specific_date}")
 
# Close database connection

engine.dispose()
 