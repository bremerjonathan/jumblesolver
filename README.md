1. Install sbt

2. Run 'sbt package' in project root

3. Find the generated jar in target folder (e.g. target/scala-2.11/jumblesolver_2.11-0.1.jar)

4. As an example, to run the application locally in client mode where the input is 'input/puzzle1.csv' and the output is 'output', execute the command:

    spark-submit --class com.ibm.cio.JumbleSolver --master local --deploy-mode client --name jumblesolver --conf "spark.app.id=jumblesolver" jumblesolver_2.11-0.1.jar input/puzzle1.csv output

5. Multiple csv input files:

    spark-submit --class com.ibm.cio.JumbleSolver --master local --deploy-mode client --name jumblesolver --conf "spark.app.id=jumblesolver" jumblesolver_2.11-0.1.jar "input/*.csv" output