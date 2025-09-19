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

# ------------------ Output Path ------------------
export_path = os.path.expanduser("~/Desktop/firebase_path")
os.makedirs(export_path, exist_ok=True)
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
        print(f"✅Found {len(customer_collections_list)} customer collections")

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

            print(f"      Message: role={role}, name={name}, has_text={bool(text)}")

            if not role or not text:
                print(f"      ⚠️ Skipping - missing role or text")
                continue

            if isinstance(created_at, datetime):
                created_at = created_at.replace(tzinfo=pytz.UTC).astimezone(local_tz)
            else:
                print(f"      ⚠️ Invalid createdAt format: {created_at}")
                continue

            customer_name = name if role == "Customer" else ""
            admin_name = name if role == "Admin" else ""

            all_messages.append({
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
    print(f"\n⚠️ No messages found to export.")
    print("\n🔍 Troubleshooting suggestions:")
    print("1. Check if the Firestore path 'chat-updated/chats' is correct") 
    print("2. Verify your Firebase credentials have read access")
    print("3. Check if customer collections exist under the 'chats' document")
    print("4. Verify message documents contain 'role', 'text', and 'createdAt' fields")


update set where inner 