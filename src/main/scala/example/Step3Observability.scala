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

  sealed trait Event
  object StackStarting extends Event
  case class StackStarted(config: Step3Observability.StackConfig) extends Event
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

  case class ZookeeperConfig(port: Int, stopProcess: UIO[Unit])

  case class KafkaConfig(port: Int, stopProcess: UIO[Unit])

  case class SchemaRegistryConfig(port: Int, stopProcess: UIO[Unit])

  case class StackConfig(
      zookeeperPort: Int,
      kafkaPort: Int,
      registryPort: Int
  )

  def runStack(shutdownSignal: ShutdownSignal): Stream[Throwable, Event] =
    ZStream(StackStarting) ++ runZookeeperStream { zookeeperConfig =>
      runKafkaStream(
        zookeeperConfig,
        { kafkaConfig =>
          runSchemaRegistryStream(
            kafkaConfig,
            { registryConfig =>
              ZStream(
                StackStarted(
                  StackConfig(
                    zookeeperConfig.port,
                    kafkaConfig.port,
                    registryConfig.port
                  )
                )
              ) ++ ZStream.fromEffect(shutdownSignal.await).as(StackStopping)
            }
          )
        }
      )
    } ++ ZStream(StackStopped)

  def runZookeeperStream(next: ZookeeperConfig => Stream[Throwable, Event]): Stream[Throwable, Event] =
    ZStream(ZookeeperStarting) ++ ZStream.unwrapManaged(runZookeeper.map { zookeeperConfig =>
      ZStream(ZookeeperStarted) ++
        next(zookeeperConfig) ++
        ZStream(ZookeeperStopping) ++
        ZStream.fromEffect(zookeeperConfig.stopProcess).as(ZookeeperStopped)
    })

  def runKafkaStream(
      zookeeperConfig: ZookeeperConfig,
      next: KafkaConfig => Stream[Throwable, Event]
  ): Stream[Throwable, Event] =
    ZStream(KafkaStarting) ++ ZStream.unwrapManaged(runKafka(zookeeperConfig).map { kafkaConfig =>
      ZStream(KafkaStarted) ++
        next(kafkaConfig) ++
        ZStream(KafkaStopping) ++
        ZStream.fromEffect(kafkaConfig.stopProcess).as(KafkaStopped)
    })

  def runSchemaRegistryStream(
      kafkaConfig: KafkaConfig,
      next: SchemaRegistryConfig => Stream[Throwable, Event]
  ): Stream[Throwable, Event] =
    ZStream(SchemaRegistryStarting) ++ ZStream.unwrapManaged(runSchemaRegistry(kafkaConfig).map { registryConfig =>
      ZStream(SchemaRegistryStarted) ++
        next(registryConfig) ++
        ZStream(SchemaRegistryStopping) ++
        ZStream.fromEffect(registryConfig.stopProcess).as(SchemaRegistryStopped)
    })

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
