package reireiei.tookitaki

case class KBoxConfigCmd(inputDir:String = "./input", outputDir:String = "./output")
object KBoxConfigCmd {
  val parser = new scopt.OptionParser[KBoxConfigCmd]("kbox") {
    opt[String]('i',"input").action((x,c) =>
    c.copy(inputDir = x))

    opt[String]('o',"output").action((x,c) =>
    c.copy(outputDir = x))
  }
}
