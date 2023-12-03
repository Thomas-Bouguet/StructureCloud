// Pour tout mettre sur un file.

import org.apache.spark.sql.{SparkSession, functions => F}

var old_data_path = "C:/Thomas/Etudes/ESILV/A9/Structure_de_donnees_cloud/Projet/Report_4/startingTables/"
var new_data_path = "C:/Thomas/Etudes/ESILV/A9/Structure_de_donnees_cloud/Projet/Report_4/denTables/"

// Create a Spark session
val spark = SparkSession.builder.appName("Denormalization")
  .config("spark.driver.memory", "8g")
  .config("spark.executor.memory", "8g")
  .getOrCreate()

// Load the CSV data into DataFrames
val moviesDF = spark.read.csv(old_data_path + "movies.csv").toDF("id","name","year","rank")
val oldMoviesGenresDF = spark.read.csv(old_data_path + "movies_genres.csv").toDF("movie_id","genre")
val oldMoviesDirectorsDF = spark.read.csv(old_data_path + "movies_directors.csv").toDF("director_id","movie_id")
val oldRolesDF = spark.read.csv(old_data_path + "roles.csv").toDF("actor_id","movie_id","role")

val directorsDF = spark.read.csv(old_data_path + "directors.csv").toDF("id", "first_name", "last_name")
val oldDirectorsGenresDF = spark.read.csv(old_data_path + "directors_genres.csv").toDF("director_id","genre","prob")

val actorsDF = spark.read.csv(old_data_path + "actors.csv").toDF("id","first_name","last_name","gender")

// Denormalize movies.list_genres
val denormalizedMoviesDF = moviesDF
  .join(oldMoviesGenresDF, moviesDF("id") === oldMoviesGenresDF("movie_id"), "left_outer")
  .groupBy("id", "name", "year", "rank")
  .agg(F.collect_list("genre").alias("list_genres"))

// Denormalize movies.list_directors_id
val denormalizedMoviesWithDirectorsDF = denormalizedMoviesDF
  .join(oldMoviesDirectorsDF, denormalizedMoviesDF("id") === oldMoviesDirectorsDF("movie_id"), "left_outer")
  .groupBy("id", "name", "year", "rank", "list_genres")
  .agg(F.collect_list("director_id").alias("list_directors_id"))

// Denormalize movies.den_nb_actors
val denormalizedMoviesWithActorsDF = denormalizedMoviesWithDirectorsDF
  .join(oldRolesDF, denormalizedMoviesWithDirectorsDF("id") === oldRolesDF("movie_id"), "left_outer")
  .groupBy("id", "name", "year", "rank", "list_genres", "list_directors_id")
  .agg(F.size(F.collect_list("actor_id")).alias("den_nb_actors"))

// Denormalize directors.list_movies_id
val denormalizedDirectorsWithMoviesDF = directorsDF
  .join(oldMoviesDirectorsDF, directorsDF("id") === oldMoviesDirectorsDF("director_id"),"left_outer")
  .groupBy("id","first_name","last_name")
  .agg(F.collect_list("movie_id").alias("list_movies_id"))

// Denormalize directors.dict_genres_den_prob
val denormalizedDirectorsDF = denormalizedDirectorsWithMoviesDF
  .join(oldDirectorsGenresDF, denormalizedDirectorsWithMoviesDF("id") === oldDirectorsGenresDF("director_id"), "left_outer")
  .groupBy("id", "first_name", "last_name", "list_movies_id")
  .agg(
    F.map_from_entries(
      F.collect_list(
        F.when(
          F.col("genre").isNotNull && F.col("prob").isNotNull,
          F.struct("genre", "prob")
        )
      )
    ).alias("dict_genres_den_prob")
  )

// Denormalize actors.list_movies_id
val denormalizedActorsDF = actorsDF
  .join(oldRolesDF, actorsDF("id") === oldRolesDF("actor_id"), "left_outer")
  .groupBy("id", "first_name", "last_name")
  .agg(F.collect_list("movie_id").alias("list_movies_id"))

// Save the denormalized data to a JSON file
denormalizedActorsDF.write.mode("overwrite").json(new_data_path + "denormalized_actors")

// Save the denormalized data to a JSON file
denormalizedDirectorsDF.write
  .mode("overwrite") // Use "overwrite" or "append" based on your requirement
  .json(new_data_path + "denormalized_directors")

// Save the denormalized data to a JSON file
denormalizedMoviesWithActorsDF.write
  .mode("overwrite") // Use "overwrite" or "append" based on your requirement
  .json(new_data_path + "denormalized_movies")

// Stop the Spark session
spark.stop()