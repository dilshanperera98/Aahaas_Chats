# %%
#!pip install firebase-admin

import firebase_admin
from firebase_admin import credentials, firestore
import os
from datetime import datetime, timedelta
from collections import defaultdict, deque
import pytz

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

            if not created_at or not role or not text:
                continue

            created_at = created_at.astimezone(local_tz)

            all_customer_chats[customer_id].append({
                "createdAt": created_at,
                "role": role,
                "text": text,
                "chat_id": chat_doc.id
            })

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
            unmatched_customers.append(msg)

        elif role == "admin":
            # Match with the earliest unmatched customer message
            while unmatched_customers:
                customer_msg = unmatched_customers.popleft()

                if msg["createdAt"] > customer_msg["createdAt"]:
                    response_time = (msg["createdAt"] - customer_msg["createdAt"]).total_seconds()
                    response_date = customer_msg["createdAt"].date()

                    response_details[response_date].append({
                        "customer_id": customer_id,
                        "response_time": response_time,
                        "customer_msg": {
                            "createdAt": customer_msg["createdAt"],
                            "text": customer_msg["text"],
                            "chat_id": customer_msg["chat_id"]
                        },
                        "admin_msg": {
                            "createdAt": msg["createdAt"],
                            "text": msg["text"],
                            "chat_id": msg["chat_id"]
                        }
                    })
                    break  # Exit after finding one match

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

            print(f"\n📊 Response Time Stats for {search_date}:")
            print(f"  🔺 Max Response Time: {max_response['response_time']:.2f} sec ({max_response['response_time']/60:.2f} min)")
            print(f"  🔻 Min Response Time: {min(response_times):.2f} sec ({min(response_times)/60:.2f} min)")
            print(f"  🧮 Avg Response Time: {avg_response:.2f} sec ({avg_response/60:.2f} min)")

            print(f"\n👤 Max Time Customer ID: {max_response['customer_id']}")
            print(f"📨 Customer Message:")
            print(f"  Chat ID : {max_response['customer_msg']['chat_id']}")
            print(f"  Time    : {max_response['customer_msg']['createdAt'].strftime('%Y-%m-%d %I:%M:%S %p')}")
            print(f"  Text    : {max_response['customer_msg']['text']}")

            print(f"\n✅ Admin Response:")
            print(f"  Chat ID : {max_response['admin_msg']['chat_id']}")
            print(f"  Time    : {max_response['admin_msg']['createdAt'].strftime('%Y-%m-%d %I:%M:%S %p')}")
            print(f"  Text    : {max_response['admin_msg']['text']}")
    else:
        print(f"\n⚠️ No response data found for date: {search_date}")

except ValueError:
    print("\n❌ Invalid date format. Please use YYYY-MM-DD.")


# %%


# %% [markdown]
# UPDATED CODE

# %%
import firebase_admin
from firebase_admin import credentials, firestore
import os
from datetime import datetime
from collections import defaultdict, deque
import pytz

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

# ------------------ Get Input Date ------------------
print("\n" + "=" * 60)
print("🔍 SEARCH RESPONSE TIME DETAIL BY DATE")
print("=" * 60)
search_date_input = input("\nEnter the date to search (YYYY-MM-DD): ")

try:
    search_date = datetime.strptime(search_date_input, "%Y-%m-%d").date()
except ValueError:
    print("\n❌ Invalid date format. Please use YYYY-MM-DD.")
    exit()

# ------------------ Load and Filter Chat Data by Date ------------------
print("\n📥 Filtering chat data for date:", search_date)
all_customer_chats = defaultdict(list)

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

            if not created_at or not role or not text:
                continue

            created_at = created_at.astimezone(local_tz)
            if created_at.date() != search_date:
                continue

            all_customer_chats[customer_id].append({
                "createdAt": created_at,
                "role": role.lower(),
                "text": text,
                "chat_id": chat_doc.id
            })

except Exception as e:
    print(f"❌ Error loading chat data: {e}")
    exit()

# ------------------ Calculate Response Times by Customer ------------------
print(f"\n📊 Analyzing Response Times for {search_date}...\n")
response_summary = {}
all_response_times = []

for customer_id, messages in all_customer_chats.items():
    if not messages:
        continue

    messages.sort(key=lambda x: x["createdAt"])
    unmatched_customers = deque()
    response_times = []
    response_records = []

    for msg in messages:
        role = msg["role"]
        if role == "customer":
            unmatched_customers.append(msg)
        elif role == "admin":
            while unmatched_customers:
                customer_msg = unmatched_customers.popleft()
                if msg["createdAt"] > customer_msg["createdAt"]:
                    response_time = (msg["createdAt"] - customer_msg["createdAt"]).total_seconds()
                    response_times.append(response_time)
                    all_response_times.append(response_time)
                    response_records.append({
                        "response_time": response_time,
                        "customer_msg": customer_msg,
                        "admin_msg": msg
                    })
                    break

    if response_times:
        max_response = max(response_records, key=lambda x: x["response_time"])
        min_response = min(response_times)
        avg_response = sum(response_times) / len(response_times)

        response_summary[customer_id] = {
            "max_response": max_response,
            "min_response_time": min_response,
            "avg_response_time": avg_response
        }

# ------------------ Print Per-Customer Summary ------------------
if response_summary:
    for customer_id, summary in response_summary.items():
        print(f"👤 Customer ID: {customer_id}")
        print(f"  🔺 Max Response Time: {summary['max_response']['response_time']:.2f} sec ({summary['max_response']['response_time']/60:.2f} min)")
        print(f"  🔻 Min Response Time: {summary['min_response_time']:.2f} sec ({summary['min_response_time']/60:.2f} min)")
        print(f"  🧮 Avg Response Time: {summary['avg_response_time']:.2f} sec ({summary['avg_response_time']/60:.2f} min)")

        cm = summary["max_response"]["customer_msg"]
        am = summary["max_response"]["admin_msg"]

        print(f"\n  📨 Customer Message:")
        print(f"    Chat ID : {cm['chat_id']}")
        print(f"    Time    : {cm['createdAt'].strftime('%Y-%m-%d %I:%M:%S %p')}")
        print(f"    Text    : {cm['text']}")

        print(f"\n  ✅ Admin Response:")
        print(f"    Chat ID : {am['chat_id']}")
        print(f"    Time    : {am['createdAt'].strftime('%Y-%m-%d %I:%M:%S %p')}")
        print(f"    Text    : {am['text']}")
        print("-" * 60)
else:
    print(f"📭 No response data found for {search_date}.")

# ------------------ Print Overall Summary ------------------
if all_response_times:
    total_max = max(all_response_times)
    total_min = min(all_response_times)
    total_avg = sum(all_response_times) / len(all_response_times)

    print("\n" + "=" * 60)
    print("📈 OVERALL RESPONSE TIME SUMMARY")
    print("=" * 60)
    print(f"  🔺 Max Response Time: {total_max:.2f} sec ({total_max/60:.2f} min)")
    print(f"  🔻 Min Response Time: {total_min:.2f} sec ({total_min/60:.2f} min)")
    print(f"  🧮 Avg Response Time: {total_avg:.2f} sec ({total_avg/60:.2f} min)")
else:
    print("\n📭 No overall response time data found.")


# %%



