package kafkaDemo

import scalikejdbc.ConnectionPool

object SetupJdbc {
  def apply(driver: String, host: String, user: String, password: String): Unit = {
    Class.forName(driver)
    ConnectionPool.singleton(host, user, password)
  }
}
