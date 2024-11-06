import os
import lucene
from org.apache.lucene.document import Document, Field, TextField
from concurrent.futures import ThreadPoolExecutor, as_completed

"""
Adds a single document to the Lucene index, attaching the current thread to the JVM.
Arguments:
    file_name (str): The name of the file being indexed.
    path (str): The full path to the file.
    index_writer (IndexWriter): The Lucene IndexWriter instance used to add documents to the index.    
Return:
    Adds the document to the index
"""
def add_doc_to_index(file_name, path, index_writer):
    lucene.getVMEnv().attachCurrentThread()
    with open(path, "r", encoding="utf-8") as file:
        content = file.read()
        doc = Document()
        doc.add(Field("id", file_name, TextField.TYPE_STORED))
        doc.add(Field("content", content, TextField.TYPE_STORED))
        index_writer.addDocument(doc)

"""
Indexes all .txt files in the specified directory using multithreading.
Arguments:
    direc_path (str): The path to the directory containing files to index.
    index_writer (IndexWriter): The Lucene IndexWriter instance used to add documents to the index.
    batch_limit (int): The number of files to process in each batch before committing changes.
Returns:
    Commits documents to the index in batches.
"""
def index_docs(direc_path, index_writer,batch_limit=1000):
    with ThreadPoolExecutor() as pool:
        futures = []
        
        for file_name in os.listdir(direc_path):
            if file_name.endswith(".txt"):
                file_path = os.path.join(direc_path, file_name)
                futures.append(pool.submit(add_doc_to_index, file_name, file_path))
            
            if len(futures) % batch_limit == 0:
                for future in as_completed(futures):
                    future.result()
                index_writer.commit()
                futures = []
        
        for future in as_completed(futures):
            future.result()
        index_writer.commit()