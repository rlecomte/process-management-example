package example

import zio._

object Step2ProcessManagement extends App {
  case class ProcessHandler(stopProcess: UIO[Unit])

  case class ZookeeperConfig(port: Int, handler: ProcessHandler)

  case class KafkaConfig(port: Int, handler: ProcessHandler)

  case class SchemaRegistryConfig(port: Int, handler: ProcessHandler)

  case class StackConfig(
      zookeeperPort: Int,
      kafkaPort: Int,
      registryPort: Int
  )

  def runStack: Managed[Throwable, StackConfig] = for {
    zookeeperConfig <- runZookeeper
    kafkaConfig <- runKafka(zookeeperConfig)
    registryConfig <- runSchemaRegistry(kafkaConfig)
  } yield StackConfig(
    zookeeperConfig.port,
    kafkaConfig.port,
    registryConfig.port
  )

  def runZookeeper: Managed[Throwable, ZookeeperConfig] =
    Managed.make(startZookeeper)(_.handler.stopProcess)

  def startZookeeper: IO[Throwable, ZookeeperConfig] =
    IO(println("Start Zookeeper.")).as(
      ZookeeperConfig(
        port = 2181,
        handler = ProcessHandler(IO.succeed(println("Stop Zookeeper.")))
      )
    )

  def runKafka(zookeeperConfig: ZookeeperConfig): Managed[Throwable, KafkaConfig] =
    Managed.make(startKafka(zookeeperConfig))(_.handler.stopProcess)

  def startKafka(zookeeperConfig: ZookeeperConfig): IO[Throwable, KafkaConfig] =
    IO(println("Start Kafka.")).as(
      KafkaConfig(
        port = 9092,
        handler = ProcessHandler(IO.succeed(println("Stop Kafka.")))
      )
    )

  def runSchemaRegistry(kafkaConfig: KafkaConfig): Managed[Throwable, SchemaRegistryConfig] =
    Managed.make(startSchemaRegistry(kafkaConfig))(_.handler.stopProcess)

  def startSchemaRegistry(kafkaConfig: KafkaConfig): IO[Throwable, SchemaRegistryConfig] =
    IO(println("Start Schema registry.")).as(
      SchemaRegistryConfig(
        port = 8081,
        handler = ProcessHandler(IO.succeed(println("Stop Schema Registry.")))
      )
    )

  def run(args: List[String]): URIO[ZEnv, ExitCode] =
    Step2ProcessManagement.runStack
      .use { conf =>
        IO(println(s"Stack configuration: $conf"))
      }
      .orDie
      .as(ExitCode.success)
}
