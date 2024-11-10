from average_functions import avg_precision_at_k, avg_recall_at_k
from search import *
import pandas as pd

"""
Calculate Mean Average Precision (MAP) and Mean Average Recall (MAR) at specified ranks (k) for a given set of query results.
References: https://sdsawtelle.github.io/blog/output/mean-average-precision-MAP-for-recommender-systems.html
Arguments:
    k_values (list): A list of rank positions (k) at which MAP and MAR are calculated.
    result (list): A list of tuples (document, similarity score), sorted by highest to lowest similarity.
    docs (set): A set of relevant documents for the query.
    results (DataFrame): A pandas DataFrame to store the MAP and MAR values.
Returns:
    None: The function updates the results DataFrame with MAP and MAR values at specified k.
"""
def find_map_mar(k_values, result, docs, results):
    for k in k_values:
        map_k = avg_precision_at_k(result, docs, k)
        map = "map_" + str(k) 
        results.loc[:, map] = round(map_k, 3)
        print(f"MAP@{k}: {map_k:.3f}")

        mar_k = avg_recall_at_k(result, docs, k) 
        mar = "mar_" + str(k)
        results.loc[:, mar] = round(mar_k, 3)
        print(f"MAR@{k}: {mar_k:.3f}")


"""
Process queries to compute and save Mean Average Precision (MAP) and Mean Average Recall (MAR) for each query based on the retrieved results.
References: https://www.evidentlyai.com/ranking-metrics/mean-average-precision-map
Arguments:
    analyzer (obj): Contains the specified analyzer method.
    query_docs (dict): A dictionary where keys are query identifiers and values are dictionaries containing the query details.
    num_query (int): A counter for the number of queries processed.
    k (list): A list of rank positions (k) for which MAP and MAR are calculated.
    output (str): The path to the output CSV file where results will be saved.
    file_exists (bool): A flag indicating whether the output file already exists to manage header writing.
Returns:
    None: The function appends the computed MAP and MAR results to the specified CSV file.
"""
def process_map_mar(analyzer, query_docs, num_query, k, output, index_directory, header_written):
    for query, i in query_docs.items():
        detail = i['query']
        print(f"Processing {num_query} query - {detail}")
        result = search_index(analyzer, detail, index_directory)
        docs = i['relevant_docs']

        print(f"Extracted query results {num_query}")
        num_query += 1

        results = pd.DataFrame(data = [{'num_query': query, 'text_query': detail}])
        
        find_map_mar(k, result, docs, results)
        results.to_csv(output, mode='a', index=False, header=header_written)
        header_written=False
