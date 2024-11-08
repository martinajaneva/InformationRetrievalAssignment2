from inverted_index import index_docs
from org.apache.lucene.store import FSDirectory
from java.nio.file import Paths
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.analysis.core import SimpleAnalyzer, WhitespaceAnalyzer
from org.apache.lucene.analysis.en import EnglishAnalyzer
from org.apache.lucene.index import IndexWriter, IndexWriterConfig
from retrieve_map_mar import process_map_mar
import lucene
from search import extract_results
from search import *
import pandas as pd
import os


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
    output = 'results/assignment2_results.csv'
    # output = 'results/assignment2_whitespaceanalyzer_results.csv'
    # output = 'results/assignment2_simpleanalyzer_results.csv'
    # output = 'results/assignment2_englishanalyzer_results.csv'
    # output = 'results/assignment1_results.csv'
    # output = 'results/results.csv'
    file_exists = os.path.isfile(output)

    first_row = 0
    batch_size = 99999999
    k = [1, 3, 5, 10]
    # k = [3, 10]
    header_written = not file_exists

    num_query = first_row + 1
    
    # Calculate MAP MAR values (Following 2 lines of code should be uncommented for calculation)
    #    
    query_docs = extract_docs_queries(first_row, batch_size, retrieve_docs=True)
    process_map_mar(analyzer, query_docs, num_query, k, output, index_directory, header_written)

    #############################################################################################

    # Extract results of queries (Following 2 lines of code should be uncommented for calculation)
    # 
    # query_docs = extract_docs_queries(first_row, batch_size, retrieve_docs=False)
    # extract_results(query_docs, num_query, output, header_written)


lucene.initVM(vmargs=['-Djava.awt.headless=true'])
index_directory = FSDirectory.open(Paths.get("index"))
analyzer = StandardAnalyzer()
# analyzer = WhitespaceAnalyzer()
# analyzer = SimpleAnalyzer()
# analyzer = EnglishAnalyzer()

index_config = IndexWriterConfig(analyzer)
index_config.setRAMBufferSizeMB(256.0)
index_writer = IndexWriter(index_directory, index_config)

# ===================================================
# STEP 1: Indexing the files
# ===================================================
# Index the documents of the given directory
print("Indexing the documents...")

txt_directory = "full_docs"
index_docs(txt_directory, index_writer)
index_writer.close()

print("Done indexing the documents!")

# ===================================================
# STEP 2: Evaluating a specific dataset
# ===================================================
# # For all the small queries
# queries = pd.read_excel('small_queries/dev_small_queries.xlsx')
# queries_results = pd.read_csv('small_queries/dev_query_results_small.csv')

# For all the dev queries (~5K queries)
queries = pd.read_table('large_queries/dev_queries.tsv')
queries_results = pd.read_csv('large_queries/dev_query_results.csv')

# # For selected queries (310 queries)
# queries = pd.read_csv('large_queries/selected_queries.csv')
# queries_results = pd.read_csv('large_queries/selected_queries_results.csv')

# queries = pd.read_csv('chosen_queries.csv', sep=',', on_bad_lines='skip')
initialize()