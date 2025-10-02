import firebase_admin
from firebase_admin import credentials, firestore
import pandas as pd
from datetime import datetime
import pytz
import os
from collections import defaultdict

# ------------------ Excluded UIDs ------------------
EXCLUDED_UIDS = {
    '630','4030','4133','4241','10916314','10916975','41','46','288','404',
    '992','993','994','995','1046','1047','1048','1049','1050','1053','1058',
    '1092','1093','1104','1147','1205','1210','1222','1311','1431','1445',
    '1624','1627','1665','4217','4263','4286','4289','4321','4345','4349',
    '4376','4379','4392','4402','4403','4424','4435','4436','4437','4561',
    '4790','7494','8114','8115','8116','8117','8118','8120','8492','10911003',
    '10911017','10914122','10916975','1','2','3','4','5','6','7','8','9','10',
    '11','12','13','14','15','16','17','18','19','20','22','27','30','46',
    '166','167','168','171','172','173','174','175','177','184','205','208',
    '241','247','3965','3966','3971','10910540','458','601','1646','3967',
    '4417','10913942','655','4032','4231','674','624','289'
}

# ------------------ Firebase Initialization ------------------
try:
    json_path = os.path.expanduser("~/Desktop/firebaselogion/aahaas-bb222-firebase-adminsdk-go844-e44a0c6797.json")
    if not firebase_admin._apps:
        cred = credentials.Certificate(json_path)
        firebase_admin.initialize_app(cred)

    db = firestore.client()
    _ = list(db.collections())
    print("✅ Successfully connected to Firebase Firestore!")

except Exception as e:
    print("❌ Failed to connect to Firebase.")
    print(f"Error: {e}")
    exit()

# ------------------ Configuration ------------------
local_tz = pytz.timezone("Asia/Colombo")
export_path = os.path.expanduser("~/Desktop/firebase_path")
os.makedirs(export_path, exist_ok=True)

chat_root = db.collection("chat-updated").document("chats")

# ------------------ Load and Process Chat Data ------------------
print("\n📥 Starting to process chat data...")

all_messages = []
customer_count = 0

try:
    customer_collections = chat_root.collections()
    
    for customer_col in customer_collections:
        customer_id = customer_col.id
        customer_count += 1
        
        chat_docs = customer_col.stream()

        for chat_doc in chat_docs:
            chat_data = chat_doc.to_dict()
            if not chat_data:
                continue

            created_at = chat_data.get("createdAt")
            role = chat_data.get("role")
            text = chat_data.get("text")
            uid = chat_data.get("uid")
            name = chat_data.get("name", "Unknown")

            if not created_at or not role or not text:
                continue

            # Convert to local timezone
            if isinstance(created_at, datetime):
                created_at = created_at.replace(tzinfo=pytz.UTC).astimezone(local_tz)
            else:
                continue

            # Store UID as string
            uid_str = str(uid) if uid else None

            all_messages.append({
                "customer_id": customer_id,
                "customer_name": name if role == "Customer" else "",
                "admin_name": name if role == "Admin" else "",
                "chat_doc_id": chat_doc.id,
                "role": role,
                "text": text,
                "createdAt": created_at,
                "uid": uid_str
            })

    print(f"📊 Total messages collected: {len(all_messages)}")

except Exception as e:
    print(f"❌ Error loading chat data: {e}")
    exit()

# ------------------ Export All Chats to Excel ------------------
if all_messages:
    df_all = pd.DataFrame(all_messages)
    df_all['createdAt_str'] = df_all['createdAt'].apply(
        lambda dt: dt.strftime("%Y-%m-%d %I:%M:%S %p") if isinstance(dt, datetime) else None
    )
    
    # Sort first, then prepare export columns
    df_all_sorted = df_all.sort_values(['customer_id', 'createdAt'], ascending=[True, True])
    
    # Reorder columns for export
    export_cols = ["customer_id", "customer_name", "chat_doc_id", "role", "text", "createdAt_str", "admin_name", "uid"]
    df_export = df_all_sorted[[col for col in export_cols if col in df_all_sorted.columns]]
    
    all_chats_file = os.path.join(export_path, "all_chats_export.xlsx")
    df_export.to_excel(all_chats_file, index=False, engine='openpyxl')
    print(f"✅ All chats exported to: {all_chats_file}")

# ------------------ Calculate Response Times ------------------
print("\n📊 Calculating response times (excluding specified UIDs)...")

# Sort messages by customer and time
df_sorted = pd.DataFrame(all_messages).sort_values(['customer_id', 'createdAt'])

# Group by customer
response_data = []
filtered_count = 0
total_valid_responses = 0
unique_customers = set()

for customer_id, group in df_sorted.groupby('customer_id'):
    messages = group.to_dict('records')
    
    # Track unmatched customer messages
    pending_customer_msgs = []
    
    for msg in messages:
        role = msg['role'].lower()
        uid = msg['uid']
        
        if role == 'customer':
            # Check if UID should be excluded
            if uid and uid in EXCLUDED_UIDS:
                filtered_count += 1
                continue  # Skip this customer message
            
            # Add to unique customers (non-excluded)
            if uid:
                unique_customers.add(uid)
            
            # Add to pending messages
            pending_customer_msgs.append(msg)
        
        elif role == 'admin':
            # Match with earliest pending customer message
            if pending_customer_msgs:
                customer_msg = pending_customer_msgs.pop(0)
                
                response_time_sec = (msg['createdAt'] - customer_msg['createdAt']).total_seconds()
                date_str = customer_msg['createdAt'].date()
                
                response_data.append({
                    'date': date_str,
                    'customer_id': customer_id,
                    'customer_uid': customer_msg['uid'],
                    'customer_msg_time': customer_msg['createdAt'],
                    'customer_msg_text': customer_msg['text'],
                    'customer_msg_id': customer_msg['chat_doc_id'],
                    'admin_msg_time': msg['createdAt'],
                    'admin_msg_text': msg['text'],
                    'admin_msg_id': msg['chat_doc_id'],
                    'admin_name': msg['admin_name'],
                    'response_time_sec': response_time_sec,
                    'response_time_min': response_time_sec / 60
                })
                
                total_valid_responses += 1

print(f"\n🔍 Filtered out {filtered_count} customer messages with excluded UIDs")
print(f"📊 Total valid response pairs: {total_valid_responses}")
print(f"👥 Unique customers (non-excluded UIDs): {len(unique_customers)}")

# ------------------ Create Response Time DataFrame ------------------
if response_data:
    df_responses = pd.DataFrame(response_data)
    
    # Export detailed response times
    response_file = os.path.join(export_path, "response_times_detailed.xlsx")
    df_responses.to_excel(response_file, index=False, engine='openpyxl')
    print(f"✅ Detailed response times exported to: {response_file}")
    
    # ------------------ Date-wise Summary Statistics ------------------
    date_stats = []
    
    for date, date_group in df_responses.groupby('date'):
        times = date_group['response_time_sec'].values
        
        stats = {
            'date': date,
            'total_responses': len(times),
            'min_time_sec': times.min(),
            'max_time_sec': times.max(),
            'avg_time_sec': times.mean(),
            'median_time_sec': pd.Series(times).median(),
            'min_time_min': times.min() / 60,
            'max_time_min': times.max() / 60,
            'avg_time_min': times.mean() / 60,
            '0-10_sec': sum(1 for t in times if t <= 10),
            '10-30_sec': sum(1 for t in times if 10 < t <= 30),
            '30-60_sec': sum(1 for t in times if 30 < t <= 60),
            '60+_sec': sum(1 for t in times if t > 60)
        }
        date_stats.append(stats)
    
    df_stats = pd.DataFrame(date_stats).sort_values('date')
    
    # Export summary statistics
    summary_file = os.path.join(export_path, "response_times_summary.xlsx")
    df_stats.to_excel(summary_file, index=False, engine='openpyxl')
    print(f"✅ Summary statistics exported to: {summary_file}")
    
    # Print summary
    print("\n" + "="*80)
    print("📊 RESPONSE TIME SUMMARY (Excluding specified UIDs)")
    print("="*80)
    
    for _, row in df_stats.iterrows():
        print(f"\n📅 Date: {row['date']}")
        print(f"   Total Responses: {row['total_responses']}")
        print(f"   Min: {row['min_time_sec']:.2f}s ({row['min_time_min']:.2f}m)")
        print(f"   Max: {row['max_time_sec']:.2f}s ({row['max_time_min']:.2f}m)")
        print(f"   Avg: {row['avg_time_sec']:.2f}s ({row['avg_time_min']:.2f}m)")
        print(f"   Median: {row['median_time_sec']:.2f}s")
        print(f"   Distribution: 0-10s: {row['0-10_sec']} | 10-30s: {row['10-30_sec']} | 30-60s: {row['30-60_sec']} | 60+s: {row['60+_sec']}")
    
    # Overall totals
    print("\n" + "="*80)
    print("📊 OVERALL TOTALS")
    print("="*80)
    print(f"Total Responses: {df_stats['total_responses'].sum()}")
    print(f"0-10 seconds: {df_stats['0-10_sec'].sum()}")
    print(f"10-30 seconds: {df_stats['10-30_sec'].sum()}")
    print(f"30-60 seconds: {df_stats['30-60_sec'].sum()}")
    print(f"60+ seconds: {df_stats['60+_sec'].sum()}")
    
    # ------------------ Customer-wise Summary ------------------
    customer_stats = []
    
    for customer_id, cust_group in df_responses.groupby('customer_id'):
        times = cust_group['response_time_sec'].values
        
        customer_stats.append({
            'customer_id': customer_id,
            'total_interactions': len(times),
            'min_response_sec': times.min(),
            'max_response_sec': times.max(),
            'avg_response_sec': times.mean(),
            'min_response_min': times.min() / 60,
            'max_response_min': times.max() / 60,
            'avg_response_min': times.mean() / 60
        })
    
    df_customer_stats = pd.DataFrame(customer_stats)
    customer_stats_file = os.path.join(export_path, "customer_wise_response_times.xlsx")
    df_customer_stats.to_excel(customer_stats_file, index=False, engine='openpyxl')
    print(f"✅ Customer-wise statistics exported to: {customer_stats_file}")
    
    # ------------------ Search Specific Date ------------------
    print("\n" + "="*80)
    print("🔍 SEARCH RESPONSE TIME DETAIL BY DATE")
    print("="*80)
    
    search_date_input = input("\nEnter the date to search (YYYY-MM-DD) or press Enter to skip: ")
    
    if search_date_input.strip():
        try:
            search_date = datetime.strptime(search_date_input, "%Y-%m-%d").date()
            
            date_responses = df_responses[df_responses['date'] == search_date]
            
            if len(date_responses) > 0:
                max_response = date_responses.loc[date_responses['response_time_sec'].idxmax()]
                min_response = date_responses.loc[date_responses['response_time_sec'].idxmin()]
                
                print(f"\n📊 Response Time Details for {search_date}:")
                print(f"  📈 Total Responses: {len(date_responses)}")
                print(f"  🔺 Max Response Time: {max_response['response_time_sec']:.2f}s ({max_response['response_time_min']:.2f}m)")
                print(f"  🔻 Min Response Time: {min_response['response_time_sec']:.2f}s ({min_response['response_time_min']:.2f}m)")
                print(f"  🧮 Avg Response Time: {date_responses['response_time_sec'].mean():.2f}s ({date_responses['response_time_min'].mean():.2f}m)")
                
                print(f"\n👤 LONGEST Response - Customer ID: {max_response['customer_id']}")
                print(f"📨 Customer Message:")
                print(f"  Chat ID: {max_response['customer_msg_id']}")
                print(f"  UID: {max_response['customer_uid']}")
                print(f"  Time: {max_response['customer_msg_time'].strftime('%Y-%m-%d %I:%M:%S %p')}")
                print(f"  Text: {max_response['customer_msg_text'][:100]}...")
                print(f"\n✅ Admin Response:")
                print(f"  Chat ID: {max_response['admin_msg_id']}")
                print(f"  Admin: {max_response['admin_name']}")
                print(f"  Time: {max_response['admin_msg_time'].strftime('%Y-%m-%d %I:%M:%S %p')}")
                print(f"  Text: {max_response['admin_msg_text'][:100]}...")
            else:
                print(f"\n⚠️ No response data found for date: {search_date}")
        
        except ValueError:
            print("\n❌ Invalid date format. Please use YYYY-MM-DD.")

else:
    print("\n⚠️ No valid response pairs found.")

print("\n✅ Processing complete!")