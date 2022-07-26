import sbt._

object Dependencies {
  val sparkVersion = "3.1.0"

  val core = "org.apache.spark" %% "spark-core" % sparkVersion
  val sql = "org.apache.spark" %% "spark-sql" % sparkVersion

  val all: Seq[ModuleID] = Seq(core, sql)
}
