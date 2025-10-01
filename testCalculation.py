#!pip install firebase-admin

import firebase_admin
from firebase_admin import credentials, firestore
import os
from datetime import datetime, timedelta
from collections import defaultdict, deque
import pytz

# ------------------ Excluded UIDs ------------------
EXCLUDED_UIDS = {
    '1222', '1311', '1431', '1445', '1624', '1627', '1665', '4217', '4263', '4286', 
    '4289', '4321', '4345', '4349', '4376', '4379', '4392', '4402', '4403', '4424', 
    '4435', '4436', '4437', '4561', '4790', '7494', '8114', '8115', '8116', '8117', 
    '8118', '8120', '8492', '10911003', '10911017', '10914122', '10916975',
    '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', 
    '15', '16', '17', '18', '19', '20', '22', '27', '30', '46', '166', '167', 
    '168', '171', '172', '173', '174', '175', '177', '184', '205', '208', '241', 
    '247', '3965', '3966', '3971', '10910540', '458', '601', '1646', '3967', 
    '4417', '10913942', '655', '4032', '4231', '674', '624'
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

# ------------------ Timezone ------------------
local_tz = pytz.timezone("Asia/Colombo")
chat_root = db.collection("chat-updated").document("chats")
response_details = defaultdict(list)
all_customer_chats = defaultdict(list)
unique_uids_by_date = defaultdict(set)  # Track unique UIDs per date

# ------------------ Load Chat Data ------------------
print("\n📥 Starting to process chat data...")

try:
    customer_collections = chat_root.collections()
    for customer_col in customer_collections:
        customer_id = customer_col.id
        chat_docs = customer_col.stream()

        for chat_doc in chat_docs:
            chat_data = chat_doc.to_dict()
            if not chat_data:
                continue

            created_at = chat_data.get("createdAt")
            role = chat_data.get("role")
            text = chat_data.get("text")
            uid = chat_data.get("uid")  # Get UID from message

            if not created_at or not role or not text:
                continue

            created_at = created_at.astimezone(local_tz)

            all_customer_chats[customer_id].append({
                "createdAt": created_at,
                "role": role,
                "text": text,
                "chat_id": chat_doc.id,
                "uid": str(uid) if uid else None  # Store UID as string
            })

            # Track unique UIDs for customer messages by date
            if role == "Customer" and uid:
                uid_str = str(uid)
                if uid_str not in EXCLUDED_UIDS:
                    msg_date = created_at.date()
                    unique_uids_by_date[msg_date].add(uid_str)

except Exception as e:
    print(f"❌ Error loading chat data: {e}")
    exit()

# ------------------ Group Chats into Sessions ------------------
def split_into_sessions(messages, gap_minutes=180):
    messages.sort(key=lambda x: x["createdAt"])
    sessions = []
    current_session = [messages[0]]

    for i in range(1, len(messages)):
        time_diff = (messages[i]["createdAt"] - messages[i - 1]["createdAt"]).total_seconds() / 60
        if time_diff > gap_minutes or messages[i]["createdAt"].date() != messages[i - 1]["createdAt"].date():
            sessions.append(current_session)
            current_session = [messages[i]]
        else:
            current_session.append(messages[i])

    sessions.append(current_session)
    return sessions

# ------------------ Process All Sessions and Match Admin Replies ------------------
filtered_count = 0
total_count = 0

for customer_id, messages in all_customer_chats.items():
    if not messages:
        continue

    sessions = split_into_sessions(messages)
    unmatched_customers = deque()

    # Flatten all sessions and process chronologically
    flat_messages = [msg for session in sessions for msg in session]
    flat_messages.sort(key=lambda x: x["createdAt"])

    for msg in flat_messages:
        role = msg["role"].lower()
        if role == "customer":
            # Check if UID should be excluded
            msg_uid = msg.get("uid")
            if msg_uid and str(msg_uid) in EXCLUDED_UIDS:
                filtered_count += 1
                continue  # Skip this customer message
            
            unmatched_customers.append(msg)

        elif role == "admin":
            # Match with the earliest unmatched customer message
            while unmatched_customers:
                customer_msg = unmatched_customers.popleft()

                if msg["createdAt"] > customer_msg["createdAt"]:
                    total_count += 1
                    response_time = (msg["createdAt"] - customer_msg["createdAt"]).total_seconds()
                    response_date = customer_msg["createdAt"].date()

                    response_details[response_date].append({
                        "customer_id": customer_id,
                        "response_time": response_time,
                        "customer_msg": {
                            "createdAt": customer_msg["createdAt"],
                            "text": customer_msg["text"],
                            "chat_id": customer_msg["chat_id"],
                            "uid": customer_msg.get("uid")
                        },
                        "admin_msg": {
                            "createdAt": msg["createdAt"],
                            "text": msg["text"],
                            "chat_id": msg["chat_id"]
                        }
                    })
                    break  # Exit after finding one match

print(f"\n🔍 Filtered out {filtered_count} customer messages with excluded UIDs")
print(f"📊 Total response pairs calculated: {total_count}")

# ------------------ Analyze a Specific Date -------------------
print("\n" + "="*60)
print("🔍 SEARCH RESPONSE TIME DETAIL BY DATE")
print("="*60)

search_date_input = input("\nEnter the date to search (YYYY-MM-DD): ")

try:
    search_date = datetime.strptime(search_date_input, "%Y-%m-%d").date()

    if search_date in response_details:
        responses = response_details[search_date]

        if not responses:
            print(f"\n📭 No response records found on {search_date}.")
        else:
            response_times = [r["response_time"] for r in responses]
            max_response = max(responses, key=lambda x: x["response_time"])
            min_response = min(responses, key=lambda x: x["response_time"])
            avg_response = sum(response_times) / len(response_times)

            # Get unique customers for this specific date
            unique_customers_today = len(unique_uids_by_date.get(search_date, set()))

            print(f"\n📊 Response Time Stats for {search_date} (Excluded UIDs filtered):")
            print(f"  👥 Unique Customers: {unique_customers_today}")
            print(f"  📈 Total Responses: {len(responses)}")
            print(f"  🔺 Max Response Time: {max_response['response_time']:.2f} sec ({max_response['response_time']/60:.2f} min)")
            print(f"  🔻 Min Response Time: {min(response_times):.2f} sec ({min(response_times)/60:.2f} min)")
            print(f"  🧮 Avg Response Time: {avg_response:.2f} sec ({avg_response/60:.2f} min)")

            print(f"\n👤 Max Time Customer ID: {max_response['customer_id']}")
            print(f"📨 Customer Message:")
            print(f"  Chat ID : {max_response['customer_msg']['chat_id']}")
            print(f"  UID     : {max_response['customer_msg'].get('uid', 'N/A')}")
            print(f"  Time    : {max_response['customer_msg']['createdAt'].strftime('%Y-%m-%d %I:%M:%S %p')}")
            print(f"  Text    : {max_response['customer_msg']['text']}")

            print(f"\n✅ Admin Response:")
            print(f"  Chat ID : {max_response['admin_msg']['chat_id']}")
            print(f"  Time    : {max_response['admin_msg']['createdAt'].strftime('%Y-%m-%d %I:%M:%S %p')}")
            print(f"  Text    : {max_response['admin_msg']['text']}")

            # Show unique UIDs for this date if needed
            print(f"\n📋 Unique UIDs on {search_date}: {len(unique_uids_by_date.get(search_date, set()))}")
            
    else:
        # Check if we have unique customer data for this date even without responses
        unique_customers_today = len(unique_uids_by_date.get(search_date, set()))
        if unique_customers_today > 0:
            print(f"\n⚠️ No response data found for date: {search_date}")
            print(f"👥 But found {unique_customers_today} unique customers who sent messages on this date")
        else:
            print(f"\n⚠️ No data found for date: {search_date}")

except ValueError:
    print("\n❌ Invalid date format. Please use YYYY-MM-DD.")

# ------------------ Show Date-wise Unique Customer Summary ------------------
print("\n" + "="*60)
print("📅 DATE-WISE UNIQUE CUSTOMER SUMMARY")
print("="*60)

if unique_uids_by_date:
    print(f"\n{'Date':<15} {'Unique Customers':<20}")
    print("-" * 35)
    for date in sorted(unique_uids_by_date.keys()):
        unique_count = len(unique_uids_by_date[date])
        print(f"{date} {unique_count:>18}")
    
    print("-" * 35)
    print(f"{'TOTAL':<15} {len(all_unique_uids):>18}")
else:
    print("\n⚠️ No unique customer data found.")