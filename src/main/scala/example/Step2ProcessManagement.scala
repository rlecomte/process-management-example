package example

import zio._

object Step2ProcessManagement extends App {
  case class ZookeeperConfig(port: Int, stopProcess: UIO[Unit])

  case class KafkaConfig(port: Int, stopProcess: UIO[Unit])

  case class SchemaRegistryConfig(port: Int, stopProcess: UIO[Unit])

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
    Managed.make(startZookeeper)(_.stopProcess)

  def startZookeeper: IO[Throwable, ZookeeperConfig] = {
    val startProcess: IO[Throwable, Unit] = IO(println("Start Zookeeper."))
    val stopProcess: UIO[Unit] = IO.succeed(println("Stop Zookeeper."))

    startProcess.as(ZookeeperConfig(port = 2181, stopProcess))
  }

  def runKafka(zookeeperConfig: ZookeeperConfig): Managed[Throwable, KafkaConfig] =
    Managed.make(startKafka(zookeeperConfig))(_.stopProcess)

  def startKafka(zookeeperConfig: ZookeeperConfig): IO[Throwable, KafkaConfig] = {
    val startProcess: IO[Throwable, Unit] = IO(println("Start Kafka."))
    val stopProcess: UIO[Unit] = IO.succeed(println("Stop Kafka."))

    startProcess.as(KafkaConfig(port = 9092, stopProcess))
  }

  def runSchemaRegistry(kafkaConfig: KafkaConfig): Managed[Throwable, SchemaRegistryConfig] =
    Managed.make(startSchemaRegistry(kafkaConfig))(_.stopProcess)

  def startSchemaRegistry(kafkaConfig: KafkaConfig): IO[Throwable, SchemaRegistryConfig] = {
    val startProcess: IO[Throwable, Unit] = IO(println("Start Schema Registry."))
    val stopProcess: UIO[Unit] = IO.succeed(println("Stop Schema Registry."))

    startProcess.as(SchemaRegistryConfig(port = 8081, stopProcess))
  }

  def run(args: List[String]): URIO[ZEnv, ExitCode] =
    Step2ProcessManagement.runStack
      .use(conf => IO(println(s"Stack configuration: $conf")))
      .orDie
      .as(ExitCode.success)
}
