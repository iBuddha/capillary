package sqlite

import java.sql.DriverManager

/**
 * Created by xhuang on 12/3/14.
 */
object DbCreation extends App{
  Class.forName("org.sqlite.JDBC")
  val c = DriverManager.getConnection("jdbc:sqlite:/Users/xhuang/test.db")
  c.close()
}
