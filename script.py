from inverted_index import index_docs
import lucene
from search import extract_results
from search import *
import pandas as pd
import os

# # Start indexing
# index_docs(txt_directory, index_writer)

# index_writer.close()

"""
Extracts a batch of queries and their relevant documents from the dataset.
Arguments:
    first_row (int): The starting index for extracting queries from the dataset.
    batch_size (int): The number of queries to extract in the current batch.
    retrieve_docs (bool): Flag indicating whether to retrieve relevant documents for each query.
Returns:
    dict: A dictionary where keys are query IDs and values are dictionaries containing the query text and relevant documents.
"""
def extract_docs_queries(first_row, batch_size, retrieve_docs = False):
    queries_relevant_text = {}
    batch =  queries.iloc[first_row: first_row + batch_size]

    for _, j in batch.iterrows():
        id = j['Query number']
        query_text = j['Query']
        if(retrieve_docs == True):
            relevant_docs = queries_results[queries_results['Query_number'] == id]['doc_number'].astype(str).tolist()
            queries_relevant_text[id] = {
                'query' : query_text, 
                'relevant_docs': relevant_docs }
        else:
            queries_relevant_text[id] = {'query' : query_text}
        
    return queries_relevant_text


def initialize():
    # output = 'results/assignment2_results.csv'
    # output = 'results/assignment1_results.csv'
    output = 'results/results.csv'
    file_exists = os.path.isfile(output)

    first_row = 0
    batch_size = 99999999
    # k = [1, 3, 5, 10]
    k = [3, 10]
    header_written = not file_exists

    num_query = first_row + 1
    
    # Calculate MAP MAR values (Following 2 lines of code should be uncommented for calculation)
    #    
    # query_docs = extract_docs_queries(first_row, batch_size, retrieve_docs=True)
    # process_map_mar(query_docs, num_query, k, output, header_written)

    #############################################################################################

    # Extract results of queries (Following 2 lines of code should be uncommented for calculation)
    # 
    query_docs = extract_docs_queries(first_row, batch_size, retrieve_docs=False)
    extract_results(query_docs, num_query, output, header_written)


queries = pd.read_csv('chosen_queries.csv', sep=',', on_bad_lines='skip')
initialize()