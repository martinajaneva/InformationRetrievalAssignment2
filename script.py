from inverted_index import index_docs
import lucene
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.store import FSDirectory
from java.nio.file import Paths

lucene.initVM(vmargs=['-Djava.awt.headless=true'])
index_directory = FSDirectory.open(Paths.get("index"))
standard_analyzer = StandardAnalyzer()
index_config = IndexWriterConfig(standard_analyzer)
index_config.setRAMBufferSizeMB(256.0)
index_writer = IndexWriter(index_directory, index_config)

txt_directory = "full_docs"

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