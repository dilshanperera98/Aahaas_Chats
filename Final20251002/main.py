import pandas as pd
from datetime import datetime, timedelta
import pytz
import os
from collections import defaultdict

# ------------------ File Paths ------------------
def get_file_path(search_date):
    return os.path.expanduser(f"~/Desktop/firebase_path/customer_wise_chat_export_{search_date}.xlsx")

output_txt_path = os.path.expanduser("~/Desktop/firebase_path/response_time_summary_corrected.txt")
response_file = os.path.expanduser("~/Desktop/firebase_path/response_times_detailed.xlsx")

# ------------------ Split Chats into Sessions ------------------
def split_into_sessions(messages, gap_minutes=180):
    messages = messages.sort_values('createdAt')
    sessions = []
    current_session = [messages.iloc[0]]
    
    for i in range(1, len(messages)):
        time_diff = (messages.iloc[i]['createdAt'] - messages.iloc[i-1]['createdAt']).total_seconds() / 60
        if time_diff > gap_minutes or messages.iloc[i]['createdAt'].date() != messages.iloc[i-1]['createdAt'].date():
            sessions.append(pd.DataFrame(current_session))
            current_session = [messages.iloc[i]]
        else:
            current_session.append(messages.iloc[i])
    
    sessions.append(pd.DataFrame(current_session))
    return sessions

# ------------------ Calculate Response Times ------------------
def calculate_response_times(df, search_date):
    response_details = defaultdict(list)
    total_count = 0
    
    # Filter by date
    df['date'] = df['createdAt'].dt.date
    df_date = df[df['date'] == search_date].copy()
    
    if df_date.empty:
        return None, 0, set()
    
    # Group by customer_id
    grouped = df_date.groupby('customer_id')
    unique_customer_uids = set()
    
    for customer_id, group in grouped:
        # Add UID to unique_customer_uids if available
        customer_msgs = group[group['type'].str.lower() == 'customer']
        if not customer_msgs.empty and 'uid' in customer_msgs.columns:
            uids = customer_msgs['uid'].dropna().astype(str)
            unique_customer_uids.update(uids)
        
        # Split into sessions
        sessions = split_into_sessions(group, gap_minutes=180)
        
        for session in sessions:
            customer_msgs = session[session['type'].str.lower() == 'customer']
            admin_msgs = session[session['type'].str.lower() == 'admin']
            
            unmatched_customers = []
            
            # Process messages in chronological order
            for _, msg in session.sort_values('createdAt').iterrows():
                role = msg['type'].lower()
                
                if role == 'customer':
                    unmatched_customers.append(msg)
                elif role == 'admin' and unmatched_customers:
                    # Match with the earliest unmatched customer message
                    customer_msg = unmatched_customers.pop(0)
                    response_time = (msg['createdAt'] - customer_msg['createdAt']).total_seconds()
                    response_date = customer_msg['createdAt'].date()
                    
                    total_count += 1
                    response_details[response_date].append({
                        'customer_id': customer_id,
                        'customer_name': customer_msg.get('customer_name', 'Unknown'),
                        'response_time': response_time,
                        'customer_msg': {
                            'createdAt': customer_msg['createdAt'],
                            'text': customer_msg['message'],
                            'chat_id': customer_msg['chat_doc_id'],
                            'uid': customer_msg.get('uid', 'N/A')
                        },
                        'admin_msg': {
                            'createdAt': msg['createdAt'],
                            'text': msg['message'],
                            'chat_id': msg['chat_doc_id'],
                            'admin_name': msg.get('admin_name', 'Unknown')
                        }
                    })
    
    return response_details, total_count, unique_customer_uids

# ------------------ Save Detailed Response Times to Excel ------------------
def save_response_details(response_details, search_date):
    if not response_details or search_date not in response_details:
        return None
    
    responses = response_details[search_date]
    response_data = []
    
    for r in responses:
        response_data.append({
            'customer_id': r['customer_id'],
            'customer_name': r['customer_name'],
            'response_time_sec': r['response_time'],
            'response_time_min': r['response_time'] / 60,
            'customer_chat_id': r['customer_msg']['chat_id'],
            'customer_uid': r['customer_msg']['uid'],
            'customer_message': r['customer_msg']['text'],
            'customer_time': r['customer_msg']['createdAt'].replace(tzinfo=None),
            'admin_chat_id': r['admin_msg']['chat_id'],
            'admin_message': r['admin_msg']['text'],
            'admin_time': r['admin_msg']['createdAt'].replace(tzinfo=None),
            'admin_name': r['admin_msg']['admin_name']
        })
    
    df_responses = pd.DataFrame(response_data)
    return df_responses

# ------------------ Main Processing ------------------
def main():
    # Get user input for date
    search_date_input = input("\nEnter the date to analyze (YYYY-MM-DD): ")
    try:
        search_date = datetime.strptime(search_date_input, "%Y-%m-%d").date()
    except ValueError:
        print("\n❌ Invalid date format. Please use YYYY-MM-DD.")
        return
    
    # Load the Excel file
    file_path = get_file_path(search_date)
    try:
        df = pd.read_excel(file_path)
        df['createdAt'] = pd.to_datetime(df['createdAt'])
        df = df.sort_values(['customer_id', 'createdAt'])
    except Exception as e:
        print(f"❌ Error loading Excel file: {e}")
        print(f"\n🔍 Ensure the file {file_path} exists and contains valid data.")
        return
    
    # Calculate response times
    response_details, total_count, unique_customer_uids = calculate_response_times(df, search_date)
    
    # Save detailed response times to Excel
    df_responses = save_response_details(response_details, search_date)
    if df_responses is not None:
        try:
            df_responses.to_excel(response_file, index=False, engine='openpyxl')
            print(f"\n✅ Detailed response times saved to: {response_file}")
        except Exception as e:
            print(f"❌ Error saving response times to Excel: {e}")
    
    # Output results
    output_lines = []
    output_lines.append("="*60)
    output_lines.append(f"🔍 RESPONSE TIME SUMMARY FOR {search_date}")
    output_lines.append("="*60)
    output_lines.append(f"\n📊 Total response pairs calculated: {total_count}")
    output_lines.append(f"👥 Unique Customers: {len(unique_customer_uids)}")
    
    if response_details and search_date in response_details:
        responses = response_details[search_date]
        response_times = [r['response_time'] for r in responses]
        
        if response_times:
            max_response = max(responses, key=lambda x: x['response_time'])
            min_response = min(responses, key=lambda x: x['response_time'])
            avg_response = sum(response_times) / len(response_times)
            
            output_lines.append(f"\n📊 Response Time Stats for {search_date}:")
            output_lines.append(f"  📈 Total Responses: {len(responses)}")
            output_lines.append(f"  🔺 Max Response Time: {max_response['response_time']:.2f} sec ({max_response['response_time']/60:.2f} min)")
            output_lines.append(f"  🔻 Min Response Time: {min(response_times):.2f} sec ({min(response_times)/60:.2f} min)")
            output_lines.append(f"  🧮 Avg Response Time: {avg_response:.2f} sec ({avg_response/60:.2f} min)")
            
            output_lines.append(f"\n👤 Max Time Customer ID: {max_response['customer_id']}")
            output_lines.append(f"📨 Customer Message:")
            output_lines.append(f"  Chat ID : {max_response['customer_msg']['chat_id']}")
            output_lines.append(f"  UID     : {max_response['customer_msg']['uid']}")
            output_lines.append(f"  Time    : {max_response['customer_msg']['createdAt'].strftime('%Y-%m-%d %I:%M:%S %p')}")
            output_lines.append(f"  Text    : {max_response['customer_msg']['text']}")
            
            output_lines.append(f"\n✅ Admin Response:")
            output_lines.append(f"  Chat ID : {max_response['admin_msg']['chat_id']}")
            output_lines.append(f"  Time    : {max_response['admin_msg']['createdAt'].strftime('%Y-%m-%d %I:%M:%S %p')}")
            output_lines.append(f"  Text    : {max_response['admin_msg']['text']}")
            output_lines.append(f"  Admin   : {max_response['admin_msg']['admin_name']}")
        else:
            output_lines.append(f"\n📭 No response records found on {search_date}.")
    else:
        output_lines.append(f"\n⚠️ No response data found for date: {search_date}")
    
    # Print and save output
    output_text = "\n".join(output_lines)
    print(output_text)
    
    with open(output_txt_path, 'w') as f:
        f.write(output_text)
    
    print(f"\n✅ Detailed summary saved to: {output_txt_path}")

if __name__ == "__main__":
    main()