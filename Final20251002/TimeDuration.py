import pandas as pd
from datetime import datetime
import os

# ------------------ File Paths ------------------
#file_path = os.path.expanduser("~/Desktop/firebase_path/customer_wise_chat_export_2025-10-07.xlsx")
#output_txt_path = os.path.expanduser("~/Desktop/firebase_path/response_time_summary.txt")

file_path = os.path.expanduser("~/Desktop/Chat_Report/Final20251002/exports/customer_wise_chat_export_2025-11-02.xlsx")
output_txt_path = os.path.expanduser("~/Desktop/Chat_Report/Final20251002/exports/response_time_summary.txt")

# ------------------ Load the Excel File ------------------
try:
    df = pd.read_excel(file_path)
except Exception as e:
    print(f"❌ Error loading Excel file: {e}")
    exit()

# Ensure 'createdAt' is in datetime format
df['createdAt'] = pd.to_datetime(df['createdAt'], errors='coerce')

# Drop rows with invalid 'createdAt' values
df = df.dropna(subset=['createdAt'])

# Standardize 'type' column to handle case variations
df['type'] = df['type'].str.lower()

# Sort by customer_id and createdAt for chronological processing
df = df.sort_values(['customer_id', 'createdAt'])

# ------------------ Initialize Date Statistics ------------------
date_stats = {}

# Group by customer_id
grouped = df.groupby('customer_id')

# ------------------ Calculate Response Times ------------------
for customer_id, group in grouped:
    # Filter customer and admin messages
    customer_msgs = group[group['type'] == 'customer']
    admin_msgs = group[group['type'] == 'admin']

    for i in range(len(customer_msgs)):
        cust_msg = customer_msgs.iloc[i]
        cust_time = cust_msg['createdAt']
        date_str = cust_time.strftime('%Y-%m-%d')

        # Find the first admin reply after the customer message
        subsequent_admin = admin_msgs[admin_msgs['createdAt'] > cust_time]

        if not subsequent_admin.empty:
            first_admin_reply = subsequent_admin.iloc[0]
            reply_time = first_admin_reply['createdAt']
            response_duration = (reply_time - cust_time).total_seconds()

            # Initialize stats for the date if not already present
            if date_str not in date_stats:
                date_stats[date_str] = {
                    'total_responses': 0,
                    '0-10_sec': 0,
                    '10-30_sec': 0,
                    '30+_sec': 0
                }

            # Update stats
            date_stats[date_str]['total_responses'] += 1
            if response_duration <= 10:
                date_stats[date_str]['0-10_sec'] += 1
            elif 10 < response_duration <= 30:
                date_stats[date_str]['10-30_sec'] += 1
            else:
                date_stats[date_str]['30+_sec'] += 1

# ------------------ Calculate Totals ------------------
total_responses = sum(stats['total_responses'] for stats in date_stats.values())
total_0_10 = sum(stats['0-10_sec'] for stats in date_stats.values())
total_10_30 = sum(stats['10-30_sec'] for stats in date_stats.values())
total_30_plus = sum(stats['30+_sec'] for stats in date_stats.values())

# ------------------ Create Output Summary ------------------
output_lines = []

# Generate per-date statistics
for date in sorted(date_stats.keys()):
    stats = date_stats[date]
    output_lines.append(
        f"📅 Date: {date}\n"
        f"   ➤ Total Admin Responses: {stats['total_responses']}\n"
        f"   ➤ 0-10 Sec: {stats['0-10_sec']} ({(stats['0-10_sec'] / stats['total_responses'] * 100):.2f}%)\n"
        f"   ➤ 10-30 Sec: {stats['10-30_sec']} ({(stats['10-30_sec'] / stats['total_responses'] * 100):.2f}%)\n"
        f"   ➤ 30+ Sec: {stats['30+_sec']} ({(stats['30+_sec'] / stats['total_responses'] * 100):.2f}%)\n"
    )

# Add overall totals
output_lines.append(
    "\n📊 Overall Totals:\n"
    f"   ➤ Total Admin Responses: {total_responses}\n"
    f"   ➤ 0-10 Sec: {total_0_10} ({(total_0_10 / total_responses * 100):.2f}%)\n"
    f"   ➤ 10-30 Sec: {total_10_30} ({(total_10_30 / total_responses * 100):.2f}%)\n"
    f"   ➤ 30+ Sec: {total_30_plus} ({(total_30_plus / total_responses * 100):.2f}%)"
)

output_text = "\n".join(output_lines)

# ------------------ Print & Save Summary ------------------
print(output_text)

try:
    with open(output_txt_path, 'w', encoding='utf-8') as f:
        f.write(output_text)
    print(f"\n✅ Detailed summary saved to: {output_txt_path}")
except Exception as e:
    print(f"❌ Error saving summary: {e}")

# ------------------ Troubleshooting ------------------
if not date_stats:
    print("\n⚠️ No response times calculated. Possible reasons:")
    print("1. No customer or admin messages found in the data.")
    print("2. 'type' column may not contain 'customer' or 'admin' values.")
    print("3. 'createdAt' timestamps may be invalid or missing.")
    print("4. Ensure the Excel file contains data for the expected dates.")