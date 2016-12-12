import java.io.{File, FileReader, PrintWriter}

import au.com.bytecode.opencsv.CSVReader
import org.apache.commons.io.FileUtils
import org.apache.log4j.Logger
import org.specs2.mutable.Specification

import scala.collection.JavaConverters._
import scala.util.Properties

/**
  * Created by tomoya.igarashi on 2016/12/07.
  */
class ImportMovieLensToDBSpec extends Specification {
  val logger = Logger.getLogger(this.getClass)

  """Import MovieLens to Database""".stripMargin >> {
    val databaseName = "recommender_development"
    """ratings""".stripMargin >> pending {
      val filePath = "src/test/resources/movie_ratings.csv"
      val reader = new CSVReader(new FileReader(filePath))
      val xs = Iterator.continually(reader.readNext).takeWhile(_ != null).map(_.toList).map {
        case userId :: movieId :: rating :: _ => Some((userId, movieId, rating))
        case _ => None
      }.toList.collect { case Some((userId, movieId, rating)) =>
        val values = s"""($userId, $movieId, $rating)"""
        s"INSERT INTO $databaseName.ratings (user_id, movie_id, rating) VALUES $values;"
      }.tail
      val pw = new PrintWriter("insert_to_ratings.sql")
      val sqls = (List(s"TRUNCATE $databaseName.ratings;") ++ xs).mkString(Properties.lineSeparator)
      pw.write(sqls)
      pw.close
      true must_== true
    }
    """movies""".stripMargin >> pending {
      val filePath = "src/test/resources/movies.csv"
      val reader = new CSVReader(new FileReader(filePath))
      val xs = Iterator.continually(reader.readNext).takeWhile(_ != null).map(_.toList).map {
        case movieId :: title :: genres :: _ => Some((movieId, title, genres))
        case _ => None
      }.toList.collect { case Some((movieId, title, genres)) =>
        val title2 = title.replaceAll("'", "''")
        val values = s"""($movieId, '$title2', '$genres')"""
        s"INSERT INTO $databaseName.movies (movie_id, title, genres) VALUES $values;"
      }.tail
      val pw = new PrintWriter("insert_to_movies.sql")
      val sqls = (List(s"TRUNCATE $databaseName.movies;") ++ xs).mkString(Properties.lineSeparator)
      pw.write(sqls)
      pw.close
      true must_== true
    }
    """links""".stripMargin >> {
      val filePath = "src/test/resources/links.csv"
      val reader = new CSVReader(new FileReader(filePath))
      val xs = Iterator.continually(reader.readNext).takeWhile(_ != null).map(_.toList).map {
        case movieId :: imdbId :: tmdbId :: _ if tmdbId.isEmpty => Some((movieId, imdbId, "NULL"))
        case movieId :: imdbId :: tmdbId :: _  => Some((movieId, imdbId, tmdbId))
        case _ => None
      }.toList.collect { case Some((movieId, imdbId, tmdbId)) =>
        val values = s"""($movieId, '$imdbId', $tmdbId)"""
        s"INSERT INTO $databaseName.links (movie_id, imdb_id, tmdb_id) VALUES $values;"
      }.tail
      val pw = new PrintWriter("insert_to_links.sql")
      val sqls = (List(s"TRUNCATE $databaseName.links;") ++ xs).mkString(Properties.lineSeparator)
      pw.write(sqls)
      pw.close
      true must_== true
    }
  }
}
