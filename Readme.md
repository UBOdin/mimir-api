## GeoSpark Support

Until the spark 3.0 version of GeoSpark is released we need to install it locally before running mimir-api

```bash
git clone https://github.com/mrb24/GeoSpark.git
cd GeoSpark
git checkout spark-3.0
mvn clean install -DskipTests
```
