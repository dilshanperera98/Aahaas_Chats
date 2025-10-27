import firebase_admin
from firebase_admin import credentials, firestore
import pandas as pd
from datetime import datetime
import pytz
import os

# ------------------ Excluded UIDs ------------------
EXCLUDED_UIDS = {
    '630','4030','4133','4241','10916314','10916975','41','46','288','404','608'
    '992','993','994','995','1046','1047','1048','1049','1050','1053','1058',
    '1092','1093','1104','1147','1205','1210','1222','1311','1431','1445',
    '1624','1627','1665','4217','4263','4286','4289','4321','4345','4349',
    '4376','4379','4392','4402','4403','4424','4435','4436','4437','4561',
    '4790','7494','8114','8115','8116','8117','8118','8120','8492','10911003',
    '10911017','10914122','10916975','1','2','3','4','5','6','7','8','9','10',
    '11','12','13','14','15','16','17','18','19','20','22','27','30','46',
    '166','167','168','171','172','173','174','175','177','184','205','208',
    '241','247','3965','3966','3971','10910540','458','601','1646','3967',
    '4417','10913942','655','4032','4231','674','624','289','663','608','669','10914638','4218',
    '1634','398','1070','10914649','10914654','671','555','10910395','4200'
}

# ------------------ Firebase Initialization ------------------
if not firebase_admin._apps:
    cred_path = os.path.expanduser("~/Desktop/firebaselogion/aahaas-bb222-firebase-adminsdk-go844-e44a0c6797.json")
    cred = credentials.Certificate(cred_path)
    firebase_admin.initialize_app(cred)

db = firestore.client()

# ------------------ Timezone ------------------
local_tz = pytz.timezone("Asia/Colombo")

# ------------------ Get Date Input ------------------
search_date_input = input("\nEnter the date to download chats (YYYY-MM-DD): ")
try:
    search_date = datetime.strptime(search_date_input, "%Y-%m-%d").date()
except ValueError:
    print("\n❌ Invalid date format. Please use YYYY-MM-DD.")
    exit()

# ------------------ Output Path ------------------
#export_path = os.path.expanduser("~/Desktop/firebase_path")
export_path = os.path.expanduser("~/Desktop/Chat_Report/Final20251002/exports")

os.makedirs(export_path, exist_ok=True)
output_file = os.path.join(export_path, f"customer_wise_chat_export_{search_date}.xlsx")

# ------------------ References ------------------
chat_root = db.collection("chat-updated").document("chats")

# ------------------ Collect All Messages ------------------
all_messages = []
customer_count = 0
filtered_customer_ids = set()

print(f"\n🔍 Starting to process customers for {search_date}...")

try:
    customer_collections = chat_root.collections()
    customer_collections_list = list(customer_collections)

    if not customer_collections_list:
        print("No customer collections found under chat root document")
    else:
        print(f"✅ Found {len(customer_collections_list)} customer collections")

    # First pass: Identify customer_ids with excluded UIDs
    for customer_col in customer_collections_list:
        customer_id = customer_col.id
        chat_docs = list(customer_col.stream())
        
        for chat_doc in chat_docs:
            chat_data = chat_doc.to_dict()
            if not chat_data:
                continue
            
            role = chat_data.get("role")
            uid = str(chat_data.get("uid", "")) if chat_data.get("uid") else ""
            created_at = chat_data.get("createdAt")
            
            if not role or not created_at:
                continue
                
            if isinstance(created_at, datetime):
                created_at = created_at.replace(tzinfo=pytz.UTC).astimezone(local_tz)
                if created_at.date() != search_date:
                    continue
            else:
                continue
            
            # If customer message has excluded UID, mark customer_id for exclusion
            if role.lower() == "customer" and uid in EXCLUDED_UIDS:
                filtered_customer_ids.add(customer_id)
                break  # No need to check further messages for this customer_id

    print(f"\n🔍 Excluded {len(filtered_customer_ids)} customer IDs due to excluded UIDs")

    # Second pass: Collect messages for non-excluded customer_ids
    for customer_col in customer_collections_list:
        customer_id = customer_col.id
        if customer_id in filtered_customer_ids:
            continue  # Skip excluded customer_ids
        
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
            uid = str(chat_data.get("uid", "")) if chat_data.get("uid") else ""

            print(f"      Message: role={role}, name={name}, uid={uid}, has_text={bool(text)}")

            if not role or not text:
                print(f"      ⚠️ Skipping - missing role or text")
                continue

            if isinstance(created_at, datetime):
                created_at = created_at.replace(tzinfo=pytz.UTC).astimezone(local_tz)
                if created_at.date() != search_date:
                    print(f"      ⚠️ Skipping - message date {created_at.date()} does not match {search_date}")
                    continue
            else:
                print(f"      ⚠️ Invalid createdAt format: {created_at}")
                continue

            customer_name = name if role.lower() == "customer" else ""
            admin_name = name if role.lower() == "admin" else ""

            all_messages.append({
                "customer_id": customer_id,
                "customer_name": customer_name,
                "chat_doc_id": chat_doc_id,
                "type": role,
                "message": text,
                "createdAt": created_at,
                "admin_name": admin_name,
                "uid": uid
            }) 

except Exception as e:
    print(f"❌ Error during processing: {e}")
    import traceback
    traceback.print_exc()
    exit()

print(f"\n📊 Total messages collected: {len(all_messages)}")

# ------------------ Create DataFrame ------------------
if all_messages:
    df = pd.DataFrame(all_messages)
    # Convert timezone-aware datetimes to timezone-naive for Excel compatibility
    df['createdAt'] = df['createdAt'].apply(lambda dt: dt.replace(tzinfo=None) if isinstance(dt, datetime) else None)
    
    columns_order = ["customer_id", "customer_name", "chat_doc_id", "type", "message", "createdAt", "admin_name", "uid"]
    df = df[[col for col in columns_order if col in df.columns]]
    
    df = df.sort_values(['customer_id', 'createdAt'], ascending=[True, True])
    df.to_excel(output_file, index=False, engine='openpyxl')

    print(f"\n✅ Export complete. File saved at:\n{output_file}")
    print(f"📈 Total rows exported: {len(df)}")

    print(f"\n📊 Summary:")
    print(f"   Total customers: {df['customer_id'].nunique()}")
    print(f"   Total customer messages: {len(df[df['type'].str.lower() == 'customer'])}")
    print(f"   Total admin messages: {len(df[df['type'].str.lower() == 'admin'])}")

    print("\n📋 Sample data:") 
    print(df.head(10))
else:
    print(f"\n⚠️ No messages found to export for {search_date}.")
    print("\n🔍 Troubleshooting suggestions:")
    print("1. Check if the Firestore path 'chat-updated/chats' is correct") 
    print("2. Verify your Firebase credentials have read access")
    print("3. Check if customer collections exist under the 'chats' document")
    print("4. Verify message documents contain 'role', 'text', 'createdAt', and 'uid' fields")