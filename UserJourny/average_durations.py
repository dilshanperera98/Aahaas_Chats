import pandas as pd

import re
 
# -------------------------------

# Load the Excel

# -------------------------------

input_file = r"C:\Users\Dilshan\Desktop\User Journy\user_journey_2025-09-21.xlsx"  # <-- update date accordingly

df = pd.read_excel(input_file)
 
# -------------------------------

# Convert 'calculated_duration' to seconds

# -------------------------------

def duration_to_seconds(duration_str):

    match = re.match(r"(\d+)m (\d+)s", str(duration_str))

    if match:

        minutes = int(match.group(1))

        seconds = int(match.group(2))

        return minutes * 60 + seconds

    return 0
 
df['duration_sec'] = df['calculated_duration'].apply(duration_to_seconds)
 
# -------------------------------

# Categorize route_name into groups

# -------------------------------

def categorize_route(route):

    if route == "Home":

        return "Home Page"

    elif "Lifestyle" in route:

        return "Lifestyle Page"

    elif route in ["EssentialDetailsMeta"]:

        return "Essentials Page"

    elif "Hotel" in route:

        return "Hotels Page"

    elif route in [

        "FlightSearch",

        "FlightsMainPageMeta",

        "FlightsDetailsPage",

        "FlightsPassengerDetails",

        "FlightsBookingReviewMeta",

        "PaymentsSuccessfulFlights"

    ]:

        return "Flights Page"

    else:

        return None  # ignore unrelated pages
 
df['page_category'] = df['route_name'].apply(categorize_route)
 
# Keep only relevant pages

df = df[df['page_category'].notna()]
 
# -------------------------------

# Calculate Metrics

# -------------------------------

# 1. Average session duration

avg_durations = df.groupby('page_category')['duration_sec'].mean().round(2)
 
# 2. Unique user counts

unique_users = df.groupby('page_category')['user_id'].nunique()
 
# Convert average duration back to Xm Ys

def format_duration(seconds):

    minutes = int(seconds // 60)

    sec = int(seconds % 60)

    return f"{minutes}m {sec}s"
 
avg_durations_formatted = avg_durations.apply(format_duration)
 
# -------------------------------

# Combine Results

# -------------------------------

summary = pd.DataFrame({

    "Average Duration": avg_durations_formatted,

    "Unique Users": unique_users

})
 
print("Summary Report:\n")

print(summary)
 
# -------------------------------

# Save Results to Excel (new sheet)

# -------------------------------

output_file = input_file  # save into same file

with pd.ExcelWriter(output_file, engine='openpyxl', mode='a', if_sheet_exists='replace') as writer:

    summary.to_excel(writer, sheet_name="Summary")
 
print(f"\nSummary sheet added to: {output_file}")

 