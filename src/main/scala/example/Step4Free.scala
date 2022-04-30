package example

import zio._
import zio.stream._
import zio.duration.Duration

object Step4Free extends App {
  case class Step[A](
      process: Managed[Throwable, (ProcessHandler, A)],
      startingEvent: Event,
      startedEvent: Event,
      stoppingEvent: Event,
      stoppedEvent: Event
  )

  object Step {
    implicit val functorStep: Functor[Step] = new Functor[Step] {
      def map[A, B](fa: Step[A])(f: A => B): Step[B] = fa.copy(process = fa.process.map { case (handler, a) =>
        (handler, f(a))
      })
    }

    def runStream(program: Free[Step, StackConfig])(signal: ShutdownSignal): Stream[Throwable, Event] = {

      def go(program: Free[Step, StackConfig]): Stream[Throwable, Event] = {
        val pureStep: StackConfig => Stream[Throwable, Event] = { case stackConfig =>
          ZStream(StackStarted(stackConfig)) ++ ZStream.fromEffect(signal.await).as(StackStopping)
        }

        val suspendStep: Step[Free[Step, StackConfig]] => Stream[Throwable, Event] = {
          case Step(process, startingEvent, startedvent, stoppingEvent, stoppedEvent) =>
            ZStream(startingEvent) ++ ZStream.unwrapManaged(process.map { case (handler, nextStep) =>
              ZStream(startedvent) ++
                go(nextStep) ++
                ZStream(stoppingEvent) ++
                ZStream
                  .fromEffect(handler.stopProcess)
                  .as(stoppedEvent)
            })
        }

        Free.fold(program)(
          pure = pureStep,
          suspend = suspendStep
        )
      }

      ZStream(StackStarting) ++ go(program) ++ ZStream(StackStopped)
    }
  }

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

  case class ZookeeperConfig(port: Int)

  case class KafkaConfig(port: Int)

  case class SchemaRegistryConfig(port: Int)

  case class StackConfig(
      zookeeperPort: Int,
      kafkaPort: Int,
      registryPort: Int
  )

  def runStack(signal: ShutdownSignal): Stream[Throwable, Event] = Step.runStream(
    for {
      zookeeperConfig <- runZookeeper
      kafkaConfig <- runKafka(zookeeperConfig)
      registryConfig <- runSchemaRegistry(kafkaConfig)
    } yield StackConfig(
      zookeeperConfig.port,
      kafkaConfig.port,
      registryConfig.port
    )
  )(signal)

  def runZookeeper: Free[Step, ZookeeperConfig] = Free.liftF(
    Step(
      process = Managed.make(startZookeeper)(_._1.stopProcess),
      startingEvent = ZookeeperStarting,
      startedEvent = ZookeeperStarted,
      stoppingEvent = ZookeeperStopping,
      stoppedEvent = ZookeeperStopped
    )
  )

  def startZookeeper: IO[Throwable, (ProcessHandler, ZookeeperConfig)] =
    IO(println("Start Zookeeper.")).as(
      (
        ProcessHandler(IO.succeed(println("Stop Zookeeper."))),
        ZookeeperConfig(port = 2181)
      )
    )

  def runKafka(zookeeperConfig: ZookeeperConfig): Free[Step, KafkaConfig] = Free.liftF(
    Step(
      process = Managed.make(startKafka(zookeeperConfig))(_._1.stopProcess),
      startingEvent = KafkaStarting,
      startedEvent = KafkaStarted,
      stoppingEvent = KafkaStopping,
      stoppedEvent = KafkaStopped
    )
  )

  def startKafka(zookeeperConfig: ZookeeperConfig): IO[Throwable, (ProcessHandler, KafkaConfig)] =
    IO(println("Start Kafka.")).as(
      (
        ProcessHandler(IO.succeed(println("Stop Kafka."))),
        KafkaConfig(port = 9092)
      )
    )

  def runSchemaRegistry(kafkaConfig: KafkaConfig): Free[Step, SchemaRegistryConfig] = Free.liftF(
    Step(
      process = Managed.make(startSchemaRegistry(kafkaConfig))(_._1.stopProcess),
      startingEvent = SchemaRegistryStarting,
      startedEvent = SchemaRegistryStarted,
      stoppingEvent = SchemaRegistryStopping,
      stoppedEvent = SchemaRegistryStopped
    )
  )

  def startSchemaRegistry(kafkaConfig: KafkaConfig): IO[Throwable, (ProcessHandler, SchemaRegistryConfig)] =
    IO(println("Start Schema registry.")).as(
      ProcessHandler(IO.succeed(println("Stop Schema Registry."))),
      SchemaRegistryConfig(port = 8081)
    )

  def run(args: List[String]): URIO[ZEnv, ExitCode] = for {
    signal <- ShutdownSignal.make
    processFiber <- Step4Free
      .runStack(signal)
      .foreach(event => IO(println(s"Stack event received > $event")))
      .fork
    _ <- ZIO.sleep(Duration.fromMillis(5000)) *> signal.shutdown
    _ <- processFiber.join.orDie
  } yield ExitCode.success
}
