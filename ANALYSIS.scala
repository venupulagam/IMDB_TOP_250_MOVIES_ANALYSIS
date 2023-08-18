import org.apache.spark.SparkContext
import java.io._
import breeze.plot._
import breeze.linalg._


object ANALYSIS {

  val sc = new SparkContext("local", "ANALYSIS")
  val path = "C://Users//Venu Pulagam//Desktop//Top Indian Movies.csv"
  var RDD = sc.textFile(path)
  val Head = sc.parallelize(RDD.take(1))
  RDD = RDD.subtract(Head)

  def perc_films_per_year() : (Array[String], Array[String], Array[String]) ={

    val len = RDD.count()
    val YEAR_NAME_0 = RDD.map(line => {
      val field = line.split(",")
      (field(1), field(0))
    })

    val YEAR_NAME_1 = YEAR_NAME_0.groupByKey().sortByKey(false)

    val YEAR_NAME_2 = YEAR_NAME_1.map { case (x, y) => (x, y.size) }
    val YEAR_NAME = YEAR_NAME_2.map { case (x, y) => (x, y.toFloat * 100 / len) }

    val year = YEAR_NAME.map { case (x, y) => x.toString }.collect()
    val perc = YEAR_NAME.map { case (x, y) => y.toString }.collect()
    var num = YEAR_NAME_2.map { case (x, y) => y.toString }.collect()


    return (year, num, perc)
  }

  def actor_films_count () : (Array[String], Array[Int], String) = {
    val COUNT_ACTOR_0 = RDD.map(line => {
      val field = line.split(",")
      (field(5), field(1))
    }).groupByKey().sortByKey(false)

    val COUNT_ACTOR = COUNT_ACTOR_0.map { case (x, y) => (x, y.size) }

    val actor = COUNT_ACTOR.map { case (x, y) => x.toString }.collect()
    val count = COUNT_ACTOR.map { case (x, y) => y.toInt }.collect()
    val max = count.max

    val ind = count.toList.indexOf(max)
    val cont = actor(ind)

    return (actor, count, cont)
  }

  def director_films_count () : (Array[String], Array[String], String) = {
    val COUNT_DIR_0 = RDD.map(line => {
      val field = line.split(",")
      (field(6), field(1))
    }).groupByKey().sortByKey(false)

    val COUNT_DIR = COUNT_DIR_0.map { case (x, y) => (x, y.size) }

    val director = COUNT_DIR.map { case (x, y) => x.toString }.collect()
    val countdir = COUNT_DIR.map { case (x, y) => y.toString }.collect()
    val max_dir = countdir.max

    val ind_dir = countdir.toList.indexOf(max_dir)
    val cont_dir = director(ind_dir)

    return (director, countdir, cont_dir)
  }

  def pop_gen () : (Array[Int], Array[String], String) = {
    val POP_GEN_0 = RDD.map(line => {
      val field = line.split(",")
      (field(7), field(4).toInt)
    }).groupByKey().sortByKey(false).map { case (x, y) => (x, y.sum / y.size) }

    val POP = POP_GEN_0.map { case (x, y) => y }.collect()
    val GEN = POP_GEN_0.map { case (x, y) => x }.collect()
    val max_pop = POP.max
    val pop_gen = GEN(POP.toList.indexOf(max_pop))

  return (POP, GEN, pop_gen)
  }

  def made_gen () : (Array[Int], Array[String], String) = {
    val MADE_GEN_0 = RDD.map(line => {
      val field = line.split(",")
      (field(7), field(4).toInt)
    }).groupByKey.sortByKey(false).map { case (x, y) => (x, y.size) }

    val MADE = MADE_GEN_0.map { case (x, y) => y }.collect()
    val MGEN = MADE_GEN_0.map { case (x, y) => x }.collect()

    val max_made = MADE.max
    val made_gen = MGEN(MADE.toList.indexOf(max_made))

    return (MADE, MGEN, made_gen)
  }

  def succ_dir () : (Array[String], Array[Double], String) = {
    val SUCC_DIR_0 = RDD.map(line => {
      val field = line.split(",")
      (field(6), field(2).toFloat)
    }).groupByKey().sortByKey(false).map { case (x, y) => (x, y.sum / y.size) }


    val direc = SUCC_DIR_0.map { case (x, y) => x }.collect()
    val rate = SUCC_DIR_0.map { case (x, y) => "%.1f".format(y).toDouble }.collect()
    val max_rate = rate.max

    val ind_rate = rate.toList.indexOf(max_rate)
    val succ_dir = direc(ind_rate)

    return (direc, rate, succ_dir)
  }

  def most_watched_film_inyear () : (Array[Int], Array[Int], Array[String], String, String) ={
    val MOST_FILM = RDD.map(line => {
      val field = line.split(",")
      (field(1).toInt, (field(3).toInt, field(0).toString))
    }).groupByKey.sortByKey(false)

    val MOST_FILM_2 = MOST_FILM.mapValues { line => line.max }

    val y0 = MOST_FILM_2.map { case (x, (y, z)) => x }.collect()
    val c0 = MOST_FILM_2.map { case (x, (y, z)) => y }.collect()
    val n0 = MOST_FILM_2.map { case (x, (y, z)) => z }.collect()

    val c0_ind = n0(c0.toList.indexOf(c0.max))
    val c1_ind = n0(c0.toList.indexOf(c0.min))

    return (y0, c0, n0, c0_ind, c1_ind)
  }

  def avg_views_per_year () : (Array[Int], Array[Int], Int, Int) = {
    val AVG_VIEWS = RDD.map(line => {
      val field = line.split(",")
      (field(1).toInt, field(3).toInt)
    }).groupByKey.sortByKey(false)

    val AVG = AVG_VIEWS.map { case (y, z) => (y, z.sum / z.size) }

    val avg = AVG.map { case (x, y) => y }.collect()
    val y = AVG.map { case (x, y) => x }.collect()

    val cy = y(avg.toList.indexOf(avg.max))
    val dy = y(avg.toList.indexOf(avg.min))

    return (avg, y, cy, dy)
  }

  def most_made_genre_inyear () : (Array[String], Array[Int], Array[String]) = {
    val DIR_GEN = RDD.map(line => {
      val field = line.split(",")
      (field(1), field(7))
    }).groupByKey.sortByKey(false)

    val TUP = DIR_GEN.flatMapValues(line => line).map(x => (x, 1)).groupByKey()

    val TUPS = TUP.map { case ((x, y), z) => ((x, y), z.size) }.map { case ((x, y), z) => (x, (z, y)) }.groupByKey().sortByKey(false).mapValues(x => x.max)

    val y1 = TUPS.map { case (x, (y, z)) => x }.collect()
    val c1 = TUPS.map { case (x, (y, z)) => y }.collect()
    val g1 = TUPS.map { case (x, (y, z)) => z }.collect()

    return (y1,c1,g1)
  }

  def year_lang_number () : (Array[Int], Array[String], Array[Int]) = {
    val LANG_FILM = RDD.map(line => {
      val field = line.split(",")
      (field(1).toInt, field(8))
    }).groupByKey.sortByKey(false).flatMapValues(x => x).map { case (x, y) => ((x, y), 1) }.groupByKey().sortByKey(false).mapValues(x => x.size)


    val lang_year = LANG_FILM.map { case ((x, y), z) => x }.collect()
    val lang_lang = LANG_FILM.map { case ((x, y), z) => y }.collect()
    val lang_num = LANG_FILM.map { case ((x, y), z) => z }.collect()

    return (lang_year, lang_lang, lang_num)
  }

  def most_lang () : (Array[Int], Array[Int], Array[String]) = {
    val most_lang = RDD.map(line => {
      val field = line.split(",")
      (field(1).toInt, (field(3).toInt, field(8)))
    }).groupByKey.sortByKey(false).mapValues { x => x.max }

    val my = most_lang.map { case (x, (y, z)) => x }.collect()
    val mv = most_lang.map { case (x, (y, z)) => y }.collect()
    val mg = most_lang.map { case (x, (y, z)) => z }.collect()

    return (my, mv, mg)
  }

  def hits_flops_lang () : (Array[String], Array[String], Array[Int]) = {
    val TitlenRating = RDD.map(line => {
      val field = line.split(",")
      (field(0), field(2).toDouble, field(8))
    })
    val RATING = TitlenRating.map { case (x, y, z) => y }

    val avgRating = RATING.sum / RATING.count

    val STATUS = TitlenRating.filter { case (x, y, z) => y >= avgRating }.map { case (x, y, z) => (x, "HIT")
                 } ++ TitlenRating.filter { case (x, y, z) => y < avgRating }.map { case (x, y, z) => (x, "FLOP")}

    val LANG = TitlenRating.map { case (x, y, z) => (x, z) }
    val LANG1 = LANG.join(STATUS)

    val LANG_STATUS = LANG1.map { case (x, y) => ((y, 1)) }.reduceByKey(_ + _).sortByKey(false)

    val lang = LANG_STATUS.map{ case((x,y),z) => x}.collect()
    val hitflop = LANG_STATUS.map{ case((x,y),z) => y}.collect()
    val count = LANG_STATUS.map{ case((x,y),z) => z}.collect()

    return (lang, hitflop, count)
  }

  def main(args: Array[String]) : Unit = {

    val (year1, num1, perc1) = perc_films_per_year()
    val (actor2, count2, cont2) = actor_films_count()
    val (director3, count3, cont3) = director_films_count()
    val (pop4, gen4, pop_gen4) = pop_gen()
    val (made5, mgen5, made_gen5) = made_gen()
    val (direc6, rate6, succ_dir6) = succ_dir()
    val (year7, count7, name7, most7, least7) = most_watched_film_inyear()
    val (avg8, year8, best_year8, worst_year8) = avg_views_per_year()
    val (year9, count9, genre9) = most_made_genre_inyear()
    val (year10, lang10, count10) = year_lang_number()
    val (most_year11, most_views11, most_genre11) = most_lang()
    val (lang12, hitflop12, count12) = hits_flops_lang()

    val file = new File("C://Users//Venu Pulagam//Desktop//Analysis.txt")
    val writer = new PrintWriter(file)

    writer.write("ANALYSIS ON IMDB TOP 250 MOVIES DATASET : \n \n \n")


    writer.write("PERCENTAGE NO. OF FILMS IN AN YEAR : \n \n")

    writer.write("YEAR     |   % FILMS    |     NO. OF FILMS \n")
    writer.write("------------------------------------------ \n")

    for (x <- 0 to year1.length - 1) {
      writer.write(year1(x).toString + "     |     " + perc1(x).toString + "%" + "     |     " + num1(x))
      writer.write("\n")
    }

    // we can use foreach for printing the values but the required format of printing the results requires 3 foreach,
    // so here we used only one for loop instead of 3 foreach


    writer.write("\nNO. OF FILMS AN ACTOR ACTED IN (1955-2022) : \n \n")

    writer.write("NO. OF FILMS |      ACTOR \n")
    writer.write("--------------------------------------------- \n")

    for (x <- 0 to actor2.length - 1) {
      if (count2(x).toString.length > 1) {
        writer.write("     " + count2(x).toString + "      |  " + actor2(x))
      }
      else {
        writer.write("     " + count2(x).toString + "       |  " + actor2(x))
      }
      writer.write("\n")
    }

    writer.write("\nHIGHEST CONTRIBUTING ACTOR (1955-2022) : " + cont2)


    writer.write("\n \nNO. OF FILMS OF A DIRECTOR (1955-2022) : \n \n")

    writer.write("NO. OF FILMS |      DIRECTOR \n")
    writer.write("--------------------------------------------- \n")

    for (x <- 0 to director3.length - 1) {
      writer.write("     " + count3(x).toString + "       |  " + director3(x))
      writer.write("\n")
    }

    writer.write("\n \nHIGHEST CONTRIBUTING DIRECTOR (1955-2022) : " + cont3)


    writer.write("\n \nGENRES AND THEIR POPULARITY : \n \n")

    writer.write("POPULARITY |   GENRE \n")
    writer.write("----------------------- \n")

    for (x <- 0 to pop4.length - 1) {
      writer.write("   " + pop4(x).toString + "   |  " + gen4(x))
      writer.write("\n")
    }

    writer.write("\n \nMOST POPULAR GENRE (1955-2022) : " + pop_gen4)


    writer.write("\n \nGENRES AND THEIR PRODUCTIVITY : \n \n")

    writer.write(" FILMS MADE  |  GENRE \n")
    writer.write("----------------------- \n")

    for (x <- 0 to made5.length - 1) {
      if (made5(x).toString.length > 1) {
        writer.write("     " + made5(x).toString + "      |  " + mgen5(x))
      }
      else {
        writer.write("     " + made5(x).toString + "       |  " + mgen5(x))
      }
      writer.write("\n")
    }

    writer.write("\n \nMOST MADE GENRE (1955-2022) : " + made_gen5)


    writer.write("\n \nDIRECTORS AND THEIR AVERAGE FILMOGRAPHY RATING : \n \n")

    writer.write("   AVG RATING  |   DIRECTOR \n")
    writer.write("--------------------------- \n")

    for (x <- 0 to direc6.length - 1) {
      writer.write("     " + rate6(x).toString + "       |  " + direc6(x))
      writer.write("\n")
    }

    writer.write("\n \nMOST SUCCESSFUL DIRECTOR (1955-2022) : " + succ_dir6)


    writer.write("\n\nMOST WATCHED FILM IN AN YEAR : \n \n")

    writer.write("YEAR     |     VIEWS     |     FILM NAME     \n")
    writer.write("------------------------------------------ \n")

    for (x <- 0 to year7.length - 1) {
      if (count7(x).toString.length == 4) {
        writer.write(year7(x).toString + "     |     " + count7(x).toString + "      | " + name7(x))
      }
      else if (count7(x).toString.length == 6) {
        writer.write(year7(x).toString + "     |     " + count7(x).toString + "    | " + name7(x))
      }
      else {
        writer.write(year7(x).toString + "     |     " + count7(x).toString + "     | " + name7(x))
      }
      writer.write("\n")
    }


    writer.write("\n \nMOST WATCHED FILM OF ALL TIMES (1955-2022) : " + most7)
    writer.write("\n \nLEAST WATCHED FILM OF ALL TIMES (1955-2022) : " + least7)


    writer.write("\n\nAVERAGE VIEWS OF A FILM IN AN YEAR : \n \n")

    writer.write("  YEAR  |  AVG VIEWS  \n")
    writer.write("--------------------- \n")

    for (x <- 0 to year8.length - 1) {
      writer.write("  " + year8(x).toString + "  |  " + avg8(x))
      writer.write("\n")
    }

    writer.write("\n \nBEST YEAR FOR FILMMAKERS (1955-2022) : " + best_year8)
    writer.write("\n \nWORST YEAR FOR FILMMAKERS (1955-2022) : " + worst_year8)


    writer.write("\n\nMOST NUMBER OF FILMS OF A GENRE RELEASED IN AN YEAR : \n \n")

    writer.write("  YEAR   |NO.OF FILMS|   GENRE  \n")
    writer.write("------------------------------------------ \n")

    for (x <- 0 to year9.length - 1) {
      writer.write("  " + year9(x).toString + "   |     " + count9(x).toString + "     | " + genre9(x))
      writer.write("\n")
    }


    writer.write("\n\nNUMBER OF FILMS OF A LANGUAGE RELEASED IN AN YEAR : \n \n")

    writer.write("  YEAR   |NO.OF FILMS| LANGUAGE \n")
    writer.write("------------------------------------------ \n")

    for (x <- 0 to year10.length - 1) {
      writer.write("  " + year10(x).toString + "   |     " + count10(x).toString + "     | " + lang10(x))
      writer.write("\n")
    }


    writer.write("\n\nMOST WATCHED LANGUAGE IN AN YEAR : \n \n")

    writer.write("  YEAR     |     VIEWS     | LANGUAGE   \n")
    writer.write("---------------------------------------- \n")

    for (x <- 0 to most_year11.length - 1) {
      if (most_views11(x).toString.length == 4) {
        writer.write("  " + most_year11(x).toString + "     |     " + most_views11(x).toString + "      | " + most_genre11(x))
      }
      else if (most_views11(x).toString.length == 6) {
        writer.write("  " + most_year11(x).toString + "     |     " + most_views11(x).toString + "    | " + most_genre11(x))
      }
      else {
        writer.write("  " + most_year11(x).toString + "     |     " + most_views11(x).toString + "     | " + most_genre11(x))
      }
      writer.write("\n")
    }


    writer.write("STATUS AND COUNT OF FILMS OF A LANGUAGE (HIT/FLOP) : \n \n")

    writer.write("  HIT/FLOP  | NO. OF FILMS |  LANGUAGE \n")
    writer.write("------------------------------------------ \n")

    for (x <- 0 to lang12.length - 1) {
      writer.write("  " + hitflop12(x).toString + "     |     " + count12(x).toString + "     |     " + lang12(x))
      writer.write("\n")
    }


    writer.close()


  }

}
