import pysolr
import pandas as pd
import requests

def createCollection(collection_name):
    """Create Solr collection."""
    print(f"Collection '{collection_name}' must be created manually through Solr Admin UI or CLI.")

def indexData(collection_name, exclude_column):
    """Index data from CSV to Solr, excluding a specified column."""
    solr_instance = pysolr.Solr(f"http://localhost:8989/solr/{collection_name}")
    try:
       
        df = pd.read_csv("Employee_data_new.csv", encoding='latin1')
        df = df.fillna("Unknown")  

        # Remove the excluded column
        if exclude_column in df.columns:
            df = df.drop(columns=[exclude_column])
        
        documents = []
        for _, row in df.iterrows():
            doc = {
                "id": row['Employee ID'],
                "Department": row.ge('Department',[]),
                "Gender": row.get('Gender',[])
            }
            documents.append(doc)

        # Index documents in Solr
        solr_instance.add(documents)
        solr_instance.commit()
        print(f"Data indexed successfully in collection '{collection_name}', excluding column '{exclude_column}'")
    except Exception as e:
        print(f"Error while indexing data to Solr: {e}")


def searchByColumn(collection_name, column_name, column_value):
    """Search for records in Solr by column."""
    solr_instance = pysolr.Solr(f"http://localhost:8989/solr/{collection_name}")
    try:
        query = f"{column_name}:{column_value}"
        results = solr_instance.search(query)
        print(f"Search results for '{column_value}' in column '{column_name}':")
        for result in results:
            print(result)
    except Exception as e:
        print(f"Error while searching in Solr: {e}")

def deleteCore(core_name):
    """Delete a Solr core."""
    solr_url = f"http://localhost:8989/solr/admin/cores?action=UNLOAD&core={core_name}&deleteIndex=true"
    
    try:
        response = requests.get(solr_url)
        response_data = response.json()

        if response_data.get("responseHeader", {}).get("status") == 0:
            print(f"Core '{core_name}' deleted successfully.")
        else:
            print(f"Error deleting core '{core_name}': {response_data.get('error', {}).get('msg', 'Unknown error')}")
    except Exception as e:
        print(f"Exception occurred while deleting core '{core_name}': {e}")



# Function to retrieve the total number of documents (employees) in the specified collection
def getEmpCount(collection_name):
    """Get the count of employees in Solr."""
    solr_instance = pysolr.Solr(f"http://localhost:8989/solr/{collection_name}")
    try:
        results = solr_instance.search('*:*', rows=0)  # Count without fetching documents
        print(f"Employee count for collection '{collection_name}': {results.hits}")
    except Exception as e:
        print(f"Error while counting employees in Solr: {e}")

# Function to delete a specific employee by their ID
def delEmpById(collection_name, employee_id):
    """Delete an employee from Solr by Employee ID."""
    solr_instance = pysolr.Solr(f"http://localhost:8989/solr/{collection_name}")
    try:
        solr_instance.delete(id=employee_id)
        solr_instance.commit()
        print(f"Employee with ID '{employee_id}' deleted from collection '{collection_name}'")
    except Exception as e:
        print(f"Error while deleting employee in Solr: {e}")

# Function to get facet counts of employees grouped by the 'Department' field
def getDepFacet(collection_name):
    """Retrieve employee count by department in Solr."""
    solr_instance = pysolr.Solr(f"http://localhost:8989/solr/{collection_name}")
    try:
        facet_query = {
    'facet': 'true',
    'facet.field': 'Department',
    'rows': 0, 
    'facet.mincount': 1,  
    'facet.limit': 10
}
        response = solr_instance.search('*:*', **facet_query)
        facets = response.facets['facet_fields']['Department']
        print(f"Department facets for collection '{collection_name}':")
        for i in range(0, len(facets), 2):
            print(f"{facets[i]}: {facets[i+1]}")
    except Exception as e:
        print(f"Error while retrieving department facets in Solr: {e}")

# Main code execution
if __name__ == "__main__":
    # Collection names
    v_nameCollection = "hash_gokulram"
    v_phoneCollection = "hash_9448"

    # Step 1: Create Collections (Reminder: This must be done manually in Solr)
    createCollection(v_nameCollection)
    createCollection(v_phoneCollection)
    # deleteCore(v_nameCollection)
    # deleteCore(v_phoneCollection)


    getEmpCount(v_nameCollection)

    indexData(v_nameCollection, 'Department')

    indexData(v_phoneCollection, 'Gender')

    
    getEmpCount(v_nameCollection)

 
    delEmpById(v_nameCollection, 'E02003')


    getEmpCount(v_nameCollection)

   
    searchByColumn(v_nameCollection, 'Department', 'IT')

    searchByColumn(v_nameCollection, 'Gender', 'Male')

    
    searchByColumn(v_phoneCollection, 'Department', 'IT')

  
    getDepFacet(v_nameCollection)


    getDepFacet(v_phoneCollection)
