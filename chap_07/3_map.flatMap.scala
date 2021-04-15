val fruits = Seq("apple", "banana", "orange")
fruits.map(_.toUpperCase).foreach(println)
fruits.flatMap(_.toUpperCase).foreach(println)

// :paste
// -------------------------------------------------------
def toInt(s: String): Option[Int] = {
    try {
        Some(Integer.parseInt(s.trim))
    } catch {
        case e: Exception => None
    }
}
// Ctrl+D
// -------------------------------------------------------

val strings = Seq("1", "2", "foo", "3", "bar")
strings.map(toInt)
strings.flatMap(toInt)
strings.flatMap(toInt).sum
