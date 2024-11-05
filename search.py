import lucene
from org.apache.lucene.queryparser.classic import QueryParser

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
    query (str): The given query that is being matched.
    index_directory (str): The directory where the index files are located.
    num_results (int): Maximum number of files to return, default is 10.
Returns:
    index_results: Searched index results tuple where each tuple contains document_cleaned, the ID of the document and the score.
"""
def search_index(query, index_directory, num_results=10):
    index_results = []

    index_search = IndexSearcher(DirectoryReader.open(index_directory))
    matches = index_search.search(process_query(query), num_results).scoreDocs

    for i in matches:
        found_doc = index_search.doc(i.doc).get("doc_id")
        clean_doc = found_doc.replace('output_', '').replace('.txt', '')
        index_results.append((clean_doc, i.score))
    return index_results