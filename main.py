import argparse
from datetime import datetime
import pandas as pd
from pymongo import MongoClient

def clean_collections(collection1, collection2):
    
    bad_data_ids = set()

    # Clean collection1
    for document in collection1.find():
        if is_bad_data(document):
            bad_data_ids.add(document["_id"])

    # Clean collection2
    for document in collection2.find():
        if is_bad_data(document):
            bad_data_ids.add(document["_id"])

    # Remove bad data documents
    for bad_id in bad_data_ids:
        collection1.delete_one({"_id": bad_id})
        collection2.delete_one({"_id": bad_id})


    print("Collections cleaned successfully.")


# Function to validate and filter data (example implementation)
def is_bad_data(document):
    # Define a list of required fields
    required_fields = ["Test #", "Build #", "Category", "Test Case", "Expected Result", "Actual Result", "Repeatable?", "Blocker?", "Test Owner"]

    # Check if any required field is missing
    for field in required_fields:
        if field not in document:
            return True  # Document is bad if any required field is missing
    return False  # Document is not bad if all required fields are present

def insert_data_into_collection(db, collection_name, filename, collection1, collection2):
    collection = db[collection_name]

    if filename.endswith('.csv'):
        # Read CSV file with the appropriate encoding
        data = pd.read_csv(filename, encoding='ISO-8859-1')
    elif filename.endswith('.xlsx'):
        # Read Excel file
        data = pd.read_excel(filename)
        data = pd.read_excel(filename).dropna()
    else:
        print("Unsupported file format. Please provide a CSV or Excel file.")
        return

    # Insert data into MongoDB collection
    collection.insert_many(data.to_dict('records'))
    print("Data inserted into MongoDB collection successfully.")

def list_entries_by_user(db, user_id):
    collection1 = db["Collection1"]
    collection2 = db["Collection2"]

    # List entries for the specific user from both collections
    entries = []
    for collection_name, collection in [("Collection1", collection1), ("Collection2", collection2)]:
        for document in collection.find({"Test Owner": user_id}):
            entries.append(document)

    return entries

def count_blocker_and_repeater_bugs(db):
    collection1 = db["Collection1"]
    collection2 = db["Collection2"]

    blocker_entries = []
    repeater_entries = []

    # Find and collect blocker and repeater entries
    for collection in [collection1, collection2]:
        for document in collection.find():
            if document.get("Blocker?") == "Yes":
                blocker_entries.append(document)
            if document.get("Repeatable?") == "Yes":
                repeater_entries.append(document)

    return blocker_entries, repeater_entries

def find_reports_on_build(db, build_date):
    collection1 = db["Collection1"]
    collection2 = db["Collection2"]

    # Convert input date to a datetime object
    formatted_build_date = datetime.strptime(build_date, "%m/%d/%Y").date()

    # Retrieve all reports on the specified build date from both collections (no duplicates)
    reports = []
    for collection in [collection1, collection2]:
        for document in collection.find():
            # Check if the "Build #" field is a datetime object and compare the date part
            if isinstance(document.get("Build #"), datetime) and document.get("Build #").date() == formatted_build_date:
                reports.append(document)

    return reports

def get_documents_for_test_cases(db):
    collection2 = db["Collection2"]

    # Retrieve all documents from collection 2
    documents = list(collection2.find())

    # Get indices for first, middle, and last documents
    first_index = 0
    middle_index = len(documents) // 2
    last_index = len(documents) - 1

    # Extract first, middle, and last documents
    first_document = documents[first_index]
    middle_document = documents[middle_index]
    last_document = documents[last_index]

    return first_document, middle_document, last_document

def main():
    parser = argparse.ArgumentParser(description="Database Answers")

    # Add arguments
    parser.add_argument("--collection", choices=["Collection1", "Collection2"], help="Name of the collection to insert data into")
    parser.add_argument("--insert", metavar="CSV_FILENAME", help="CSV file containing data to insert into the collection")
    parser.add_argument("--user", metavar="USER_ID", help="User ID to list entries for")
    parser.add_argument("--dbanswers", action="store_true", help="Display repeatable, blocker, and reports on build date")
    parser.add_argument("--build_date", metavar="DATE", help="Find reports on the specified build date (format: MM/DD/YYYY)")
    
    args = parser.parse_args()

    # Connect to MongoDB
    client = MongoClient("mongodb://localhost:27017/")
    db = client["your_database"]
    collection1 = db["Collection1"] 
    collection2 = db["Collection2"]
    if args.collection and args.insert:
        insert_data_into_collection(db, args.collection, args.insert, collection1, collection2)
        clean_collections(collection1, collection2)
    elif args.user:
        user_entries = list_entries_by_user(db, args.user)
        df_user = pd.DataFrame(user_entries)
        df_user.to_csv(f"{args.user}.csv", index=False)
        print(f"Total entries for user '{args.user}': {len(user_entries)}")
    elif args.dbanswers:
        blocker_entries, repeater_entries = count_blocker_and_repeater_bugs(db)
        df_blockers = pd.DataFrame(blocker_entries)
        df_blockers.to_csv("blocker_entries.csv", index=False)
        print("Blocker entries exported to blocker_entries.csv")
        df_repeaters = pd.DataFrame(repeater_entries)
        df_repeaters.to_csv("repeater_entries.csv", index=False)
        print("Repeater entries exported to repeater_entries.csv")
        first_document, middle_document, last_document = get_documents_for_test_cases(db)
        print("First document:", first_document)
        print("Middle document:", middle_document)
        print("Last document:", last_document)
    elif args.build_date:
        reports_on_build = find_reports_on_build(db, args.build_date)
        df_reports = pd.DataFrame(reports_on_build)
        df_reports.to_csv(f"reports_on_{args.build_date.replace('/', '-')}.csv", index=False)
        print(f"Reports on build date ({args.build_date}) exported to reports_on_{args.build_date.replace('/', '-')}.csv")
    
    
    # Close MongoDB connection
    client.close()

if __name__ == "__main__":
    main()
