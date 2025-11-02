# %% [markdown]
# All Chats Exports As Excel (with UID filtering)

# %%
import firebase_admin
from firebase_admin import credentials, firestore
import pandas as pd
from datetime import datetime
import pytz
import os

# ------------------ Date Filter Input ------------------
# Enter the date you want to filter (format: YYYY-MM-DD)
FILTER_DATE = input("📅 Enter the date to filter (YYYY-MM-DD) or press Enter for all dates: ").strip()

if FILTER_DATE:
    try:
        filter_date_obj = datetime.strptime(FILTER_DATE, "%Y-%m-%d").date()
        print(f"✅ Filtering data for date: {FILTER_DATE}")
    except ValueError:
        print("❌ Invalid date format. Please use YYYY-MM-DD format.")
        exit()
else:
    filter_date_obj = None
    print("✅ Processing all dates")

# ------------------ Ignore UID List ------------------
IGNORE_UIDS = {
    '630', '4030', '4133', '4241', '10916314', '10916975', '41', '46', '288', '404', 
    '992', '993', '994', '995', '1046', '1047', '1048', '1049', '1050', '1053', '1058', 
    '1092', '1093', '1104', '1147', '1205', '1210', '1222', '1311', '1431', '1445', 
    '1624', '1627', '1665', '4217', '4263', '4286', '4289', '4321', '4345', '4349', 
    '4376', '4379', '4392', '4402', '4403', '4424', '4435', '4436', '4437', '4561', 
    '4790', '7494', '8114', '8115', '8116', '8117', '8118', '8120', '8492', '10911003', 
    '10911017', '10914122', '10916975', '1', '2', '3', '4', '5', '6', '7', '8', '9', 
    '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '22', '27', '30', 
    '46', '166', '167', '168', '171', '172', '173', '174', '175', '177', '184', '205', 
    '208', '241', '247', '3965', '3966', '3971', '10910540', '458', '601', '1646', 
    '3967', '4417', '10913942', '655', '4032', '4231', '674', '624','289','608','4220','3759'
}

print(f"🚫 Ignoring {len(IGNORE_UIDS)} customer UIDs")

# ------------------ Firebase Initialization ------------------
if not firebase_admin._apps:
    cred_path = os.path.expanduser("~/Desktop/firebaselogion/aahaas-bb222-firebase-adminsdk-go844-e44a0c6797.json")
    cred = credentials.Certificate(cred_path)
    firebase_admin.initialize_app(cred)

db = firestore.client() 

# ------------------ Output Path ------------------
export_path = os.path.expanduser("~/Desktop/firebase_path")
os.makedirs(export_path, exist_ok=True)

# Create filename based on filter date
if filter_date_obj:
    output_file = os.path.join(export_path, f"customer_wise_chat_export_{FILTER_DATE}.xlsx")
else:
    output_file = os.path.join(export_path, "customer_wise_chat_export_all_dates.xlsx")

# ------------------ Timezone ------------------
local_tz = pytz.timezone("Asia/Colombo")

# ------------------ References ------------------
chat_root = db.collection("chat-updated").document("chats")

# ------------------ Collect All Messages ------------------
all_messages = []
customer_count = 0
ignored_customer_count = 0

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
        
        # Check if customer_id is in the ignore list
        if customer_id in IGNORE_UIDS:
            ignored_customer_count += 1
            print(f"\n🚫 Skipping customer {customer_count}: {customer_id} (in ignore list)")
            continue
        
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
            uid = chat_data.get("uid", "")

            print(f"      Message: role={role}, name={name}, uid={uid}, has_text={bool(text)}")

            if not role or not text:
                print(f"      ⚠️ Skipping - missing role or text")
                continue

            if isinstance(created_at, datetime):
                created_at = created_at.replace(tzinfo=pytz.UTC).astimezone(local_tz)
            else:
                print(f"      ⚠️ Invalid createdAt format: {created_at}")
                continue

            # Check if message matches the filter date (if specified)
            if filter_date_obj:
                message_date = created_at.date()
                if message_date != filter_date_obj:
                    print(f"      ⏭️ Skipping - date {message_date} doesn't match filter {filter_date_obj}")
                    continue

            customer_name = name if role == "Customer" else ""
            admin_name = name if role == "Admin" else ""

            all_messages.append({
                "uid": uid,
                "customer_id": customer_id,
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
print(f"🚫 Total customers ignored: {ignored_customer_count}")
print(f"✅ Total customers processed: {customer_count - ignored_customer_count}")

# ------------------ Create DataFrame ------------------
if all_messages:
    df = pd.DataFrame(all_messages)
    df['createdAt'] = df['createdAt'].apply(lambda dt: dt.strftime("%Y-%m-%d %I:%M:%S %p") if isinstance(dt, datetime) else None)
    
    columns_order = ["uid", "customer_id", "customer_name", "chat_doc_id", "type", "message", "createdAt", "admin_name"]
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
    print("5. Check if all customers are in the ignore list")


# %% [markdown]
# Duration Wise Chat Reports

# %%
import pandas as pd
from datetime import datetime
import os

# ------------------ File Paths ------------------
file_path = os.path.expanduser("~/Desktop/firebase_path/customer_wise_chat_export_all_dates.xlsx")
output_txt_path = os.path.expanduser("~/Desktop/firebase_path/response_time_summary.txt")

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
print(output_text)

with open(output_txt_path, 'w') as f:
    f.write(output_text)

print(f"\n✅ Detailed summary saved to: {output_txt_path}")


# %%


