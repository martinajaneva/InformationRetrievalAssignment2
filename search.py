import lucene
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.store import FSDirectory
from java.nio.file import Paths
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.index import IndexWriter, IndexWriterConfig, DirectoryReader
from org.apache.lucene.search import IndexSearcher


# lucene.initVM(vmargs=['-Djava.awt.headless=true'])
# index_directory = FSDirectory.open(Paths.get("index"))
# standard_analyzer = StandardAnalyzer()
# index_config = IndexWriterConfig(standard_analyzer)
# index_config.setRAMBufferSizeMB(256.0)
# index_writer = IndexWriter(index_directory, index_config)

"""
Parses and processes a query to a format that is suitable for searching.
Arguments:
    query (str): The raw query that is being processed.
    analyzer (Analyzer): Text analyzer used to process the query string.
Returns:
    Query: A Lucene Query object that is the processed output of the raw string/
"""
def process_query(query, analyzer):
    return QueryParser("content", analyzer).parse(QueryParser.escape(query))


"""
Searches through the index to match documents to a query string.
Arguments:
    analyzer (obj): Contains the specified analyzer method.
    query (str): The given query that is being matched.
    index_directory (str): The directory where the index files are located.
    num_results (int): Maximum number of files to return, default is 10.
Returns:
    index_results: Searched index results tuple where each tuple contains document_cleaned, the ID of the document and the score.
"""
def search_index(analyzer, query, index_directory, num_results=10):
    index_results = []

    index_search = IndexSearcher(DirectoryReader.open(index_directory))
    matches = index_search.search(process_query(query, analyzer), num_results).scoreDocs

    for i in matches:
        found_doc = index_search.doc(i.doc).get("doc_id")
        clean_doc = found_doc.replace('output_', '').replace('.txt', '')
        index_results.append((clean_doc, i.score))
    return index_results


"""
Process a set of queries and save the top 10 results for each query to a CSV file.
Arguments:
    query_docs (dict): A dictionary where keys are query identifiers and values are dictionaries containing the query details.
    num_query (int): A counter for the number of queries processed, used for logging.
    header_written (bool): A flag indicating whether the CSV header has been written.
Returns:
    None: The function appends results to a CSV file and does not return a value.
"""
def extract_results(query_docs, num_query, output, header_written):
    
    for query, i in query_docs.items():
        detail = i['query']
        print(f"Processing {num_query} query - {detail}")
        num_query += 1
        result = search_index(detail, index_directory)
        documents = [term[0] for term in result[:10]]

        results = pd.DataFrame({'Query_number': [query] * len(documents),
                                'doc_number': documents})
        # print(results)
        results.to_csv(output, mode='a', index=False, header=header_written)
        header_written = False