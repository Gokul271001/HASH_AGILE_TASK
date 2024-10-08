from elasticsearch import Elasticsearch, NotFoundError

import pandas as pd

# Connect to Elasticsearch
es = Elasticsearch(
    ["https://localhost:9200"],  
    basic_auth=("elastic", "gokul@2710"),
    verify_certs=False 
)


try:
    if es.ping():
        print("Elasticsearch is running")
    else:
        print("Elasticsearch is not running or cannot be reached")
except Exception as e:
    print(f"Error while checking Elasticsearch: {e}")

index_settings = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 1
    },
    "mappings": {
        "properties": {
            "Employee ID": {"type": "keyword"},
            "Department": {"type": "keyword"},  
            "Gender": {"type": "keyword"},
            
        }
    }
}

def createCollection(collection_name):
    # Ensure the index name is lowercase
    collection_name = collection_name.lower()
    try:
        response = es.indices.create(index=collection_name, body=index_settings, ignore=400)
        print(f"Collection '{collection_name}' creation response: {response}")
    except Exception as e:
        print(f"Error while creating collection: {e}")

def get_document_count(collection_name):
    collection_name = collection_name.lower()
    count = es.count(index=collection_name)
    print(f"Document count for collection '{collection_name}': {count['count']}")

# Check counts for both collections
def getEmpCount(collection_name):
    try:
        response = es.count(index=collection_name)
        return response['count']
    except Exception as e:
        print(f"Error while getting employee count from '{collection_name}': {e}")
        return 0

def indexData(collection_name):
    collection_name = collection_name.lower()
    try:
        # Read CSV file
        df = pd.read_csv("Employee_data_new.csv", encoding='latin1') 
        df = df.fillna("Unknown")  # Fill NaN values with "Unknown"
        
        # Iterate through DataFrame rows and index documents
        for _, row in df.iterrows():
            doc = {
                "Employee ID": row['Employee ID'],  
                "Department": row['Department'],    
                "Gender": row['Gender']             
            }
            es.index(index=collection_name, id=row['Employee ID'], body=doc)
        print(f"Data indexed successfully in collection '{collection_name}'")
    except Exception as e:
        print(f"Error while indexing data: {e}")

def delEmpById(collection_name, emp_id):
    collection_name = collection_name.lower()
    try:
        # Check if the document exists
        doc = es.get(index=collection_name, id=emp_id)
        print(f"Document found: {doc['_source']}")
        
        # Attempt to delete the document
        es.delete(index=collection_name, id=emp_id)
        print(f"Employee with ID '{emp_id}' deleted from collection '{collection_name}'")
    except NotFoundError:
        print(f"Error: Employee with ID '{emp_id}' not found in collection '{collection_name}'.")
    except Exception as e:
        print(f"Error while deleting employee by ID: {e}")

def searchByColumn(collection_name, column_name, value):
    collection_name = collection_name.lower()
    query = {
        "query": {
            "match": {
                column_name: value
            }
        }
    }
    try:
        result = es.search(index=collection_name, body=query)
        print(f"Search results for '{value}' in column '{column_name}':")
        for hit in result['hits']['hits']:
            print(hit["_source"])
    except Exception as e:
        print(f"Error while searching by column: {e}")

def get_sample_document(collection_name):
    collection_name = collection_name.lower()
    response = es.search(index=collection_name, body={
        "size": 1  
    })
    
    if response['hits']['hits']:
        print(f"Sample document from '{collection_name}': {response['hits']['hits'][0]['_source']}")
    else:
        print(f"No documents found in '{collection_name}'.")

def get_index_mapping(collection_name):
    collection_name = collection_name.lower()
    mapping = es.indices.get_mapping(index=collection_name)
    print(f"Mapping for '{collection_name}': {mapping}")

def delete_collection(collection_name):
    collection_name = collection_name.lower()
    try:
        es.indices.delete(index=collection_name)
        print(f"Collection '{collection_name}' deleted.")
    except Exception as e:
        print(f"Error while deleting collection: {e}")



def getDepFacet(collection_name):
    collection_name = collection_name.lower()
    try:
       
        response = es.search(index=collection_name, body={
            "size": 0,  
            "aggs": {
                "departments": {
                    "terms": {
                        "field": "Department",  
                        "size": 10  
                    }
                }
            }
        })
        
        # Output the results
        print(f"Department facets for collection '{collection_name}':")
        for bucket in response['aggregations']['departments']['buckets']:
            print(f"{bucket['key']}: {bucket['doc_count']}")

    except Exception as e:
        print(f"Error while retrieving department facets: {e}")

# Function Executions
v_nameCollection = 'hash_gokulram'
v_phoneCollection = 'hash_9448'

createCollection(v_nameCollection)
createCollection(v_phoneCollection)

# Get sample documents for both collections
# get_sample_document('hash_gokulram')
# get_sample_document('hash_9448')

# get_index_mapping('hash_gokulram')
# get_index_mapping('hash_9448')

get_document_count('hash_gokulram')
get_document_count('hash_9448')

# delete_collection('hash_gokulram')
# delete_collection('hash_9448')

# Index data for both collections
indexData(v_nameCollection)  
indexData(v_phoneCollection)  

print("Employee count after indexing:", getEmpCount(v_nameCollection))
print("Employee count after indexing:", getEmpCount(v_phoneCollection))

# # Example deletions and searches
delEmpById(v_nameCollection, 'E02003')  
print("Employee count after deletion:", getEmpCount(v_nameCollection))

searchByColumn(v_nameCollection, 'Department', 'IT')  
searchByColumn(v_nameCollection, 'Gender', 'Male')     
searchByColumn(v_phoneCollection, 'Department', 'IT')  
searchByColumn(v_nameCollection, 'Employee ID', 'E02003')    

getDepFacet(v_nameCollection)  
getDepFacet(v_phoneCollection)  
