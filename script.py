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

