"""
Script ini merupakan script untuk melakukan wordcount dengan pyspark
"""

# Import library yang diperlukan
from pyspark import SparkContext, SparkConf
import shutil

# Buat konfigurasi Spark
spark = SparkConf().setAppName("WordCount")
sc = SparkContext(conf=spark)

# Baca teks dari file input
input_file = "gs://pub/shakespeare/rose.txt"   # Ganti dengan lokasi file input Anda
text_file = sc.textFile(input_file)

# Split setiap baris menjadi kata-kata dan hitung frekuensi setiap kata
word_counts = text_file.flatMap(lambda line: line.split(" ")) \
                       .map(lambda word: (word, 1)) \
                       .reduceByKey(lambda x, y: x + y)

# Path file untuk outputnya
output_file = "gs://dev-fiber-399503/wordcount_output" 

# Menghapus folder 'Output' jika sudah ada agar fungsi menyimpan filenya tidak eror
shutil.rmtree(output_file, ignore_errors=True)

# Simpan hasil wordcount ke file output
word_counts.saveAsTextFile(output_file)

# Preview outputnya
output_view = word_counts.collect()
for (word, count) in output_view:
    print("%s: %i" % (word, count))

# Stop konteks Spark
sc.stop()
