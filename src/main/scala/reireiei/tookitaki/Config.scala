package reireiei.tookitaki

import com.typesafe.config.ConfigFactory

object Config {
  val config = ConfigFactory.load()
  val appName = config.getString("appName")

}
