/home/huan1531/spark-3.5.3-bin-hadoop3-scala2.13/bin/spark-submit \
  --class "SimpleApp" \
  --master local[1] \
  target/scala-2.13/simple-project_2.13-1.0.jar




rm -rf /home/huan1531/lr-spark/data/output/*.parquet && \
/home/huan1531/spark-3.5.3-bin-hadoop3-scala2.13/bin/spark-submit \
  --class "BuiltinUpper" \
  --master local[1] \
  --driver-memory 6g \
  target/scala-2.13/simple-project_2.13-1.0.jar large_string_1row_50col_10m


rm -rf /home/huan1531/lr-spark/data/output/*.parquet && \
/home/huan1531/spark-3.5.3-bin-hadoop3-scala2.13/bin/spark-submit \
  --class "UDFUpper" \
  --master local[1] \
  --driver-memory 6g \
  target/scala-2.13/simple-project_2.13-1.0.jar large_string_1row_50col_10m




rm -rf /home/huan1531/lr-spark/data/output/*.parquet && \
/home/huan1531/spark-3.5.3-bin-hadoop3-scala2.13/bin/spark-submit \
  --class "BuiltinLength" \
  --master local[1] \
  --driver-memory 6g \
  target/scala-2.13/simple-project_2.13-1.0.jar large_string_1row_1col_500m


rm -rf /home/huan1531/lr-spark/data/output/*.parquet && \
/home/huan1531/spark-3.5.3-bin-hadoop3-scala2.13/bin/spark-submit \
  --class "UDFLength" \
  --master local[1] \
  --driver-memory 6g \
  target/scala-2.13/simple-project_2.13-1.0.jar large_string_1row_1col_500m


