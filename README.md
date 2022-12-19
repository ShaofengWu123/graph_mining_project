# Directory Structure
- LPA: contains source code and necessary configurations for LPA application.
    - src: source code
    - data: datsets
    - target: compilation targets
    - config.json: configuration for Spark and the algorithm
- InfoMap: contains source code and necessary configurations for InfoMap application.

# Compile
Go to one of the project directory, and type
```bash
sbt package
```
A target .jar file will be in `target/scala-[version]/`.

# Run
```bash
spark-submit --master local[4] target/scala-[version]/[application name].jar
```