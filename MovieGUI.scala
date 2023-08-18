import scala.swing.MenuBar.NoMenuBar.{listenTo, reactions}
import scala.swing._
import scala.swing.event._
import java.awt.{BorderLayout, Font, FontMetrics, Graphics2D, Image}
import java.io._
import org.apache.spark.SparkContext
import java.awt.Color
import javax.swing.ImageIcon

object MovieGUI {

  def getlist(year: Int, lang: String, genre: String): Array[String] = {
    val sc = new SparkContext("local", "MovieGUI")

    var RDD = sc.textFile("C://Users//Venu Pulagam//Desktop//Top Indian Movies.csv")
    val Head = sc.parallelize(RDD.take(1))
    RDD = RDD.subtract(Head)

    val app = RDD.map(line => {
      val field = line.split(",")
      ((field(1).toInt, field(8), field(7)), field(0))
    }).groupByKey.sortByKey(false)

    val app_ylg = app.map { case ((x, y, z), a) => (x, y, z) }.collect()
    val app_name = app.map { case ((x, y, z), a) => a.toArray }.collect()

    val input = (year, lang, genre)

    if (app_ylg.contains(input)) {
      val o1 = app_ylg.toList.indexOf(input)
      val out = app_name(o1)
      return out
    }
    else {
      var out = Array("Oops !", "No films found")
      return out
    }
  }


  def main(args: Array[String]): Unit = {
    val genreComboBox = new ComboBox(List("War-film", "Thriller", "SuperHero", "Sci-Fi",
      "Romance", "Horror", "Historic", "Drama", "Dark Comedy", "Crime", "Comedy", "Biopic", "Anthology", "Action"))
    val yearTextField = new TextField(15)
    val languageComboBox = new ComboBox(List("Telugu", "Tamil", "Hindi", "English", "Malayalam", "Kannada", "Bengali"))
    val outputTextField = new TextArea(3, 25)

    val submitButton = new Button("Submit")
    val font = new Font("Comic Sans MS", Font.PLAIN, 18)
    val outfont = new Font("Courier New", Font.BOLD, 24)
    val labelFont = new Font("Ink Free", Font.BOLD, 20)

    genreComboBox.font = font
    yearTextField.font = font
    languageComboBox.font = font
    outputTextField.font = outfont
    submitButton.font = font

    val mainPanel = new GridBagPanel {
      val c = new Constraints
      c.insets = new Insets(10, 10, 10, 10)
      c.anchor = GridBagPanel.Anchor.Center
      c.fill = GridBagPanel.Fill.None
      c.weightx = 1.0
      c.weighty = 1.0

      val yearLabel = new Label("Year of Release")
      yearLabel.font = labelFont
      yearLabel.foreground = Color.WHITE
      layout (yearLabel) = c
      c.gridy = 1


      layout(yearTextField) = c
      c.gridy = 2


      val langLabel = new Label("Language of the Film")
      langLabel.font = labelFont
      langLabel.foreground = Color.WHITE
      layout (langLabel) = c
      c.gridy = 3

      layout(languageComboBox) = c
      c.gridy = 4

      val genLabel = new Label("Genre of the Film")
      genLabel.font = labelFont
      genLabel.foreground = Color.WHITE
      layout (genLabel) = c
      c.gridy = 5

      layout(genreComboBox) = c
      c.gridy = 6

      val outLabel = new Label("Output - Film name")
      outLabel.font = labelFont
      outLabel.foreground = Color.WHITE
      layout(outLabel) = c
      c.gridy = 7
      c.gridwidth = 10

      layout(outputTextField) = c

      c.gridy = 8
      c.fill = GridBagPanel.Fill.None
      layout(submitButton) = c

      override protected def paintComponent(g: Graphics2D): Unit = {
        super.paintComponent(g)
        val backgroundImage = new ImageIcon("C://Users//Venu Pulagam//Desktop//WRITTEN & DIRECTED BY.jpg").getImage
        g.drawImage(backgroundImage, 0, 0, null)

      }
    }


    val frame = new MainFrame {
      title = "Movie GUI"
      contents = new BorderPanel {

        layout(mainPanel) = BorderPanel.Position.Center
      }
      pack()
      centerOnScreen()
      // maximize()

    }

      listenTo(submitButton)
      reactions += {
        case ButtonClicked(`submitButton`) =>
          val genre = genreComboBox.selection.item
          val year = yearTextField.text
          val language = languageComboBox.selection.item
          val output = getlist(year.toInt, language, genre).mkString("\n")
          outputTextField.text = s"$output"


      }

    frame.visible = true
  }
}