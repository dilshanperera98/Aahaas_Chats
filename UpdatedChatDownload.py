import firebase_admin
from firebase_admin import credentials, firestore
import pandas as pd
from datetime import datetime
import pytz
import os

# ------------------ Excluded UIDs ------------------
EXCLUDED_UIDS = {
    '630', '4030', '4133', '4241', '10916314', '10916975', '41', '46', '288', '404', '992', '993', '994', '995',
    '1046', '1047', '1048', '1049', '1050', '1053', '1058', '1092', '1093', '1104', '1147', '1205', '1210',
    '1222', '1311', '1431', '1445', '1624', '1627', '1665', '4217', '4263', '4286', '4289', '4321', '4345',
    '4349', '4376', '4379', '4392', '4402', '4403', '4424', '4435', '4436', '4437', '4561', '4790', '7494',
    '8114', '8115', '8116', '8117', '8118', '8120', '8492', '10911003', '10911017', '10914122', '10916975',
    '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20',
    '22', '27', '30', '46', '166', '167', '168', '171', '172', '173', '174', '175', '177', '184', '205', '208', '241',
    '247', '3965', '3966', '3971', '10910540', '458', '601', '1646', '3967', '4417', '10913942', '655', '4032', '4231',
    '674', '624'
}

# ------------------ Firebase Initialization ------------------
if not firebase_admin._apps:
    cred_path = os.path.expanduser("~/Desktop/firebaselogion/aahaas-bb222-firebase-adminsdk-go844-e44a0c6797.json")
    cred = credentials.Certificate(cred_path)
    firebase_admin.initialize_app(cred)

db = firestore.client()

# ------------------ Timezone ------------------
local_tz = pytz.timezone("Asia/Colombo")

# ------------------ Prompt for Date ------------------
while True:
    search_date_input = input("\nEnter the date to search (YYYY-MM-DD): ")
    try:
        search_date = datetime.strptime(search_date_input, "%Y-%m-%d").date()
        break
    except ValueError:
        print("\n❌ Invalid date format. Please use YYYY-MM-DD.")

# ------------------ Output Path ------------------
export_path = os.path.expanduser("~/Desktop/firebase_path")
os.makedirs(export_path, exist_ok=True)
output_file = os.path.join(export_path, f"customer_wise_chat_export_{search_date_input}.xlsx")
output_txt_path = os.path.join(export_path, f"response_time_summary_{search_date_input}.txt")

# ------------------ References ------------------
chat_root = db.collection("chat-updated").document("chats")

# ------------------ Collect Messages for Specified Date ------------------
all_messages = []
customer_count = 0
filtered_count = 0
skipped_admin_count = 0

print(f"\n🔍 Starting to process customers for date {search_date_input}...")

try:
    customer_collections = chat_root.collections()
    customer_collections_list = list(customer_collections)

    if not customer_collections_list:
        print("No customer collections found under chat root document")
    else:
        print(f"✅ Found {len(customer_collections_list)} customer collections")

    valid_customer_msgs = {}  # Track valid customer messages per customer_id

    for customer_col in customer_collections:
        customer_id = customer_col.id
        customer_count += 1
        print(f"\n🔄 Processing customer {customer_count}: {customer_id}")

        chat_docs = list(customer_col.stream())
        print(f"   📄 Found {len(chat_docs)} chat documents")

        valid_customer_msgs[customer_id] = []  # Initialize list for valid customer messages

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
            uid = chat_data.get("uid")

            print(f"      Message: role={role}, name={name}, has_text={bool(text)}, uid={uid}")

            if not role or not text:
                print(f"      ⚠️ Skipping - missing role or text")
                continue

            if isinstance(created_at, datetime):
                created_at = created_at.replace(tzinfo=pytz.UTC).astimezone(local_tz)
            else:
                print(f"      ⚠️ Invalid createdAt format: {created_at}")
                continue

            # Filter by date
            if created_at.date() != search_date:
                print(f"      ⚠️ Skipping - message date {created_at.date()} does not match {search_date}")
                continue

            # Handle customer messages
            if role.lower() == "customer":
                if uid and str(uid) in EXCLUDED_UIDS:
                    print(f"      🚫 Skipping customer message - UID {uid} in EXCLUDED_UIDS")
                    filtered_count += 1
                    continue
                valid_customer_msgs[customer_id].append({
                    "createdAt": created_at,
                    "text": text,
                    "chat_doc_id": chat_doc_id,
                    "name": name,
                    "uid": str(uid) if uid else None
                })
                all_messages.append({
                    "customer_id": customer_id,
                    "customer_name": name,
                    "chat_doc_id": chat_doc_id,
                    "type": role,
                    "message": text,
                    "createdAt": created_at,
                    "admin_name": ""
                })
            # Handle admin messages
            elif role.lower() == "admin":
                # Check if there's a valid customer message to pair with
                matched = False
                for cust_msg in valid_customer_msgs[customer_id]:
                    if created_at > cust_msg["createdAt"]:
                        all_messages.append({
                            "customer_id": customer_id,
                            "customer_name": "",
                            "chat_doc_id": chat_doc_id,
                            "type": role,
                            "message": text,
                            "createdAt": created_at,
                            "admin_name": name
                        })
                        matched = True
                        break
                if not matched:
                    print(f"      🚫 Skipping admin message - no valid customer message to pair with")
                    skipped_admin_count += 1

except Exception as e:
    print(f"❌ Error during processing: {e}")
    import traceback
    traceback.print_exc()

print(f"\n📊 Total messages collected: {len(all_messages)}")
print(f"🔍 Filtered out {filtered_count} customer messages with excluded UIDs")
print(f"🚫 Skipped {skipped_admin_count} admin messages due to no valid customer match")

# ------------------ Create DataFrame ------------------
if all_messages:
    df = pd.DataFrame(all_messages)
    df['createdAt'] = df['createdAt'].apply(lambda dt: dt.strftime("%Y-%m-%d %I:%M:%S %p") if isinstance(dt, datetime) else None)
    
    columns_order = ["customer_id", "customer_name", "chat_doc_id", "type", "message", "createdAt", "admin_name"]
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
    print(f"\n⚠️ No messages found to export for date {search_date_input}.")
    print("\n🔍 Troubleshooting suggestions:")
    print("1. Check if the Firestore path 'chat-updated/chats' is correct")
    print("2. Verify your Firebase credentials have read access")
    print("3. Check if customer collections exist under the 'chats' document")
    print("4. Verify message documents contain 'role', 'text', and 'createdAt' fields")
    print(f"5. Ensure there are messages for the date {search_date_input}")

# ------------------ Duration Wise Chat Reports ------------------
if all_messages:
    print("\n" + "="*60)
    print(f"🔍 RESPONSE TIME SUMMARY FOR {search_date_input}")
    print("="*60)

    # Convert 'createdAt' to datetime for analysis
    df['createdAt'] = pd.to_datetime(df['createdAt'])

    # Sort by customer_id and createdAt
    df = df.sort_values(['customer_id', 'createdAt'])

    # ------------------ Initialize Date Statistics ------------------
    date_stats = {search_date_input: {
        'total_responses': 0,
        '0-10_sec': 0,
        '10-30_sec': 0,
        '30+_sec': 0
    }}

    # Group by customer_id
    grouped = df.groupby('customer_id')

    for customer_id, group in grouped:
        customer_msgs = group[group['type'] == 'Customer']
        admin_msgs = group[group['type'] == 'Admin']

        for i in range(len(customer_msgs)):
            cust_msg = customer_msgs.iloc[i]
            cust_time = cust_msg['createdAt']

            # Find first admin reply after the customer message
            subsequent_admin = admin_msgs[admin_msgs['createdAt'] > cust_time]

            if not subsequent_admin.empty:
                first_admin_reply = subsequent_admin.iloc[0]
                reply_time = first_admin_reply['createdAt']
                response_duration = (reply_time - cust_time).total_seconds()

                date_stats[search_date_input]['total_responses'] += 1

                if response_duration <= 10:
                    date_stats[search_date_input]['0-10_sec'] += 1
                elif 10 < response_duration <= 30:
                    date_stats[search_date_input]['10-30_sec'] += 1
                else:
                    date_stats[search_date_input]['30+_sec'] += 1

    # ------------------ Create Output Summary ------------------
    output_lines = []

    stats = date_stats[search_date_input]
    output_lines.append(
        f"📅 Date: {search_date_input}\n"
        f"   ➤ Total Admin Responses: {stats['total_responses']}\n"
        f"   ➤ 0-10 Sec: {stats['0-10_sec']}\n"
        f"   ➤ 10-30 Sec: {stats['10-30_sec']}\n"
        f"   ➤ 30+ Sec: {stats['30+_sec']}\n"
    )

    output_text = "\n".join(output_lines)

    # ------------------ Print & Save Summary ------------------
    print(output_text)

    with open(output_txt_path, 'w') as f:
        f.write(output_text)

    print(f"\n✅ Detailed summary saved to: {output_txt_path}")
else:
    print(f"\n⚠️ No response time summary generated for {search_date_input} due to no valid messages.")