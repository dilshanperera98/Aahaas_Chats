# %% [markdown]
# All Chats Exports As Excel with UID and Date Filter

# %%
import firebase_admin
from firebase_admin import credentials, firestore
import pandas as pd
from datetime import datetime
import pytz
import os

# ------------------ Firebase Initialization ------------------
if not firebase_admin._apps:
    cred_path = os.path.expanduser("~/Desktop/firebaselogion/aahaas-bb222-firebase-adminsdk-go844-e44a0c6797.json")
    cred = credentials.Certificate(cred_path)
    firebase_admin.initialize_app(cred)

db = firestore.client() 

# ------------------ Date Filter Input ------------------
filter_date_input = input("📅 Enter date to filter (YYYY-MM-DD) or press Enter for all dates: ").strip()

filter_date = None
if filter_date_input:
    try:
        filter_date = datetime.strptime(filter_date_input, "%Y-%m-%d").date()
        print(f"✅ Filtering data for: {filter_date}")
    except ValueError:
        print("❌ Invalid date format. Processing all dates.")
else:
    print("✅ Processing all dates")

# ------------------ Output Path ------------------
export_path = os.path.expanduser("~/Desktop/firebase_path")
os.makedirs(export_path, exist_ok=True)

if filter_date:
    output_file = os.path.join(export_path, f"customer_wise_chat_export_{filter_date}.xlsx")
else:
    output_file = os.path.join(export_path, "customer_wise_chat_export_all_dates.xlsx")

# ------------------ Timezone ------------------
local_tz = pytz.timezone("Asia/Colombo")

# ------------------ References ------------------
chat_root = db.collection("chat-updated").document("chats")

# ------------------ Collect All Messages ------------------
all_messages = []
customer_count = 0

print("🔍 Starting to process customers...")

try:
    customer_collections = chat_root.collections()
    customer_collections_list = list(customer_collections)

    if not customer_collections_list:
        print("No customer collections found under chat root document")
    else:
        print(f"✅ Found {len(customer_collections_list)} customer collections")

    for customer_col in customer_collections_list:
        customer_id = customer_col.id
        customer_count += 1
        print(f"\n🔄 Processing customer {customer_count}: {customer_id}")
        
        chat_docs = list(customer_col.stream())
        print(f"   📄 Found {len(chat_docs)} chat documents")

        for chat_doc in chat_docs:
            chat_doc_id = chat_doc.id
            chat_data = chat_doc.to_dict()

            if not chat_data:
                print(f"   ⚠️ No data in chat document: {chat_doc_id}")
                continue

            print(f"   📝 Processing chat document: {chat_doc_id}")
            role = chat_data.get("role")
            text = chat_data.get("text")
            created_at = chat_data.get("createdAt")
            name = chat_data.get("name", "Unknown")
            uid = chat_data.get("uid", "")  # Get UID field

            print(f"      Message: role={role}, name={name}, uid={uid}, has_text={bool(text)}")

            if not role or not text:
                print(f"      ⚠️ Skipping - missing role or text")
                continue

            if isinstance(created_at, datetime):
                created_at = created_at.replace(tzinfo=pytz.UTC).astimezone(local_tz)
            else:
                print(f"      ⚠️ Invalid createdAt format: {created_at}")
                continue

            # Apply date filter if specified
            if filter_date:
                message_date = created_at.date()
                if message_date != filter_date:
                    print(f"      ⏭️ Skipping - date {message_date} doesn't match filter {filter_date}")
                    continue

            customer_name = name if role == "Customer" else ""
            admin_name = name if role == "Admin" else ""
            # Only include UID if role is Customer
            customer_uid = uid if role == "Customer" else ""

            all_messages.append({
                "customer_id": customer_id,
                "uid": customer_uid,
                "customer_name": customer_name,
                "chat_doc_id": chat_doc_id,
                "type": role,
                "message": text,
                "createdAt": created_at,
                "admin_name": admin_name
            }) 

except Exception as e:
    print(f"❌ Error during processing: {e}")
    import traceback
    traceback.print_exc()

print(f"\n📊 Total messages collected: {len(all_messages)}")

# ------------------ Create DataFrame ------------------
if all_messages:
    df = pd.DataFrame(all_messages)
    df['createdAt'] = df['createdAt'].apply(lambda dt: dt.strftime("%Y-%m-%d %I:%M:%S %p") if isinstance(dt, datetime) else None)
    
    columns_order = ["customer_id", "uid", "customer_name", "chat_doc_id", "type", "message", "createdAt", "admin_name"]
    df = df[[col for col in columns_order if col in df.columns]]
    
    df = df.sort_values(['customer_id', 'createdAt'], ascending=[True, True])
    df.to_excel(output_file, index=False, engine='openpyxl')

    print(f"\n✅ Export complete. File saved at:\n{output_file}")
    print(f"📈 Total rows exported: {len(df)}")

    print(f"\n📊 Summary:")
    print(f"   Total customers: {df['customer_id'].nunique()}")
    print(f"   Total customer messages: {len(df[df['type'] == 'Customer'])}")
    print(f"   Total admin messages: {len(df[df['type'] == 'Admin'])}")

    print("\n📋 Sample data:") 
    print(df.head(10))
else:
    print(f"\n⚠️ No messages found to export.")
    print("\n🔍 Troubleshooting suggestions:")
    print("1. Check if the Firestore path 'chat-updated/chats' is correct") 
    print("2. Verify your Firebase credentials have read access")
    print("3. Check if customer collections exist under the 'chats' document")
    print("4. Verify message documents contain 'role', 'text', and 'createdAt' fields")
    if filter_date:
        print(f"5. Check if there are any messages for the date: {filter_date}")


# %% [markdown]
# Duration Wise Chat Reports

# %%
import pandas as pd
from datetime import datetime
import os

# ------------------ File Paths ------------------
if filter_date:
    file_path = os.path.join(export_path, f"customer_wise_chat_export_{filter_date}.xlsx")
    output_txt_path = os.path.join(export_path, f"response_time_summary_{filter_date}.txt")
else:
    file_path = os.path.join(export_path, "customer_wise_chat_export_all_dates.xlsx")
    output_txt_path = os.path.join(export_path, "response_time_summary.txt")

# Check if file exists
if not os.path.exists(file_path):
    print(f"❌ File not found: {file_path}")
    print("⚠️ Skipping response time analysis")
else:
    # ------------------ Load the Excel File ------------------
    df = pd.read_excel(file_path)

    # Convert 'createdAt' to datetime
    df['createdAt'] = pd.to_datetime(df['createdAt'])

    # Sort by customer_id and createdAt
    df = df.sort_values(['customer_id', 'createdAt'])

    # ------------------ Initialize Date Statistics ------------------
    date_stats = {}

    # Group by customer_id
    grouped = df.groupby('customer_id')

    for customer_id, group in grouped:
        customer_msgs = group[group['type'] == 'Customer']
        admin_msgs = group[group['type'] == 'Admin']

        for i in range(len(customer_msgs)):
            cust_msg = customer_msgs.iloc[i]
            cust_time = cust_msg['createdAt']
            date_str = cust_time.strftime('%Y-%m-%d')

            # Find first admin reply after the customer message
            subsequent_admin = admin_msgs[admin_msgs['createdAt'] > cust_time]

            if not subsequent_admin.empty:
                first_admin_reply = subsequent_admin.iloc[0]
                reply_time = first_admin_reply['createdAt']
                response_duration = (reply_time - cust_time).total_seconds()

                if date_str not in date_stats:
                    date_stats[date_str] = {
                        'total_responses': 0,
                        '0-10_sec': 0,
                        '10-30_sec': 0,
                        '30+_sec': 0
                    }

                date_stats[date_str]['total_responses'] += 1

                if response_duration <= 10:
                    date_stats[date_str]['0-10_sec'] += 1
                elif 10 < response_duration <= 30:
                    date_stats[date_str]['10-30_sec'] += 1
                else:
                    date_stats[date_str]['30+_sec'] += 1

    # ------------------ Totals ------------------
    total_responses = sum(stats['total_responses'] for stats in date_stats.values())
    total_0_10 = sum(stats['0-10_sec'] for stats in date_stats.values())
    total_10_30 = sum(stats['10-30_sec'] for stats in date_stats.values())
    total_30_plus = sum(stats['30+_sec'] for stats in date_stats.values())

    # ------------------ Create Output Summary ------------------
    output_lines = []

    if filter_date:
        output_lines.append(f"🎯 Filtered Report for Date: {filter_date}\n")
    
    for date in sorted(date_stats.keys()):
        stats = date_stats[date]
        output_lines.append(
            f"📅 Date: {date}\n"
            f"   ➤ Total Admin Responses: {stats['total_responses']}\n"
            f"   ➤ 0-10 Sec: {stats['0-10_sec']}\n"
            f"   ➤ 10-30 Sec: {stats['10-30_sec']}\n"
            f"   ➤ 30+ Sec: {stats['30+_sec']}\n"
        )

    # Add overall totals
    output_lines.append(
        "\n📊 Overall Totals:\n"
        f"   ➤ Total Admin Responses: {total_responses}\n"
        f"   ➤ 0-10 Sec: {total_0_10}\n"
        f"   ➤ 10-30 Sec: {total_10_30}\n"
        f"   ➤ 30+ Sec: {total_30_plus}"
    )

    output_text = "\n".join(output_lines)

    # ------------------ Print & Save Summary ------------------
    print("\n" + "="*50)
    print(output_text)
    print("="*50)

    with open(output_txt_path, 'w') as f:
        f.write(output_text)

    print(f"\n✅ Detailed summary saved to: {output_txt_path}")


# %%