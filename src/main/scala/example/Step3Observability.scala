package example

import zio._
import zio.stream._
import zio.duration.Duration

object Step3Observability extends App {
  case class ShutdownSignal(promise: Promise[Nothing, Unit]) {
    val shutdown: UIO[Boolean] = promise.succeed(())
    val await: UIO[Unit] = promise.await
  }

  object ShutdownSignal {
    def make: UIO[ShutdownSignal] = Promise.make.map(ShutdownSignal.apply)
  }

  case class ProcessHandler(stopProcess: UIO[Unit])

  sealed trait Event
  object StackStarting extends Event
  case class StackStarted(config: StackConfig) extends Event
  object StackStopping extends Event
  object StackStopped extends Event
  object ZookeeperStarting extends Event
  object ZookeeperStarted extends Event
  object ZookeeperStopping extends Event
  object ZookeeperStopped extends Event
  object KafkaStarting extends Event
  object KafkaStarted extends Event
  object KafkaStopping extends Event
  object KafkaStopped extends Event
  object SchemaRegistryStarting extends Event
  object SchemaRegistryStarted extends Event
  object SchemaRegistryStopping extends Event
  object SchemaRegistryStopped extends Event

  case class ZookeeperConfig(port: Int, handler: ProcessHandler)

  case class KafkaConfig(port: Int, handler: ProcessHandler)

  case class SchemaRegistryConfig(port: Int, handler: ProcessHandler)

  case class StackConfig(
      zookeeperPort: Int,
      kafkaPort: Int,
      registryPort: Int
  )

  def runStack(shutdownSignal: ShutdownSignal): Stream[Throwable, Event] = runZookeeperStream(shutdownSignal)

  def runZookeeperStream(shutdownSignal: ShutdownSignal): Stream[Throwable, Event] =
    ZStream(StackStarting) ++ ZStream(ZookeeperStarting) ++ ZStream.unwrapManaged(runZookeeper.map { zookeeperConfig =>
      ZStream(ZookeeperStarted) ++
        runKafkaStream(shutdownSignal, zookeeperConfig) ++
        ZStream(ZookeeperStopping) ++
        ZStream.fromEffect(zookeeperConfig.handler.stopProcess).as(ZookeeperStopped) ++
        ZStream(StackStopped)
    })

  def runKafkaStream(shutdownSignal: ShutdownSignal, zookeeperConfig: ZookeeperConfig): Stream[Throwable, Event] =
    ZStream(KafkaStarting) ++ ZStream.unwrapManaged(runKafka(zookeeperConfig).map { kafkaConfig =>
      ZStream(KafkaStarted) ++
        runSchemaRegistryStream(shutdownSignal, zookeeperConfig, kafkaConfig) ++
        ZStream(KafkaStopping) ++
        ZStream.fromEffect(kafkaConfig.handler.stopProcess).as(KafkaStopped)
    })

  def runSchemaRegistryStream(
      shutdownSignal: ShutdownSignal,
      zookeeperConfig: ZookeeperConfig,
      kafkaConfig: KafkaConfig
  ): Stream[Throwable, Event] =
    ZStream(SchemaRegistryStarting) ++ ZStream.unwrapManaged(runSchemaRegistry(kafkaConfig).map { registryConfig =>
      ZStream(SchemaRegistryStarted) ++
        ZStream(
          StackStarted(
            StackConfig(
              zookeeperConfig.port,
              kafkaConfig.port,
              registryConfig.port
            )
          )
        ) ++
        ZStream.fromEffect(shutdownSignal.await).as(StackStopping) ++
        ZStream(SchemaRegistryStopping) ++
        ZStream.fromEffect(registryConfig.handler.stopProcess).as(SchemaRegistryStopped)
    })

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

  def run(args: List[String]): URIO[ZEnv, ExitCode] = for {
    signal <- ShutdownSignal.make
    processFiber <- Step3Observability
      .runStack(signal)
      .foreach(event => IO(println(s"Stack event received > $event")))
      .fork
    _ <- ZIO.sleep(Duration.fromMillis(5000)) *> signal.shutdown
    _ <- processFiber.join.orDie
  } yield ExitCode.success
}
