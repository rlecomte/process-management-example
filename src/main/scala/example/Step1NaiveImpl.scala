package example

import zio._

object Step1NaiveImpl extends App {
  case class ZookeeperConfig(port: Int)

  case class KafkaConfig(port: Int)

  case class SchemaRegistryConfig(port: Int)

  case class StackConfig(
      zookeeperPort: Int,
      kafkaPort: Int,
      registryPort: Int
  )

  def startStack: IO[Throwable, StackConfig] = for {
    zookeeperConfig <- startZookeeper
    kafkaConfig <- startKafka(zookeeperConfig)
    registryConfig <- startSchemaRegistry(kafkaConfig)
  } yield StackConfig(
    zookeeperConfig.port,
    kafkaConfig.port,
    registryConfig.port
  )

  def startZookeeper: IO[Throwable, ZookeeperConfig] =
    IO(println("Start Zookeeper.")).as(ZookeeperConfig(port = 2181))

  def startKafka(zookeeperConfig: ZookeeperConfig): IO[Throwable, KafkaConfig] =
    IO(println("Start Kafka.")).as(KafkaConfig(port = 9092))

  def startSchemaRegistry(kafkaConfig: KafkaConfig): IO[Throwable, SchemaRegistryConfig] =
    IO(println("Start Schema registry.")).as(SchemaRegistryConfig(port = 8081))

  def run(args: List[String]): URIO[ZEnv, ExitCode] =
    Step1NaiveImpl.startStack.flatMap(conf => IO(println(s"Stack configuration: $conf"))).orDie.as(ExitCode.success)
}
