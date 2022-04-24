package example

trait Functor[F[_]] {
  def map[A, B](fa: F[A])(f: A => B): F[B]
}

trait Monad[F[_]] extends Functor[F] {
  def pure[A](value: A): F[A]

  def map[A, B](fa: F[A])(f: A => B): F[B] = flatMap(fa)(a => pure(f(a)))

  def flatMap[A, B](fa: F[A])(f: A => F[B]): F[B]
}

sealed trait Free[F[_], A]
object Free {
  case class Pure[F[_], A](value: A) extends Free[F, A]
  case class Suspend[F[_], A](next: F[Free[F, A]]) extends Free[F, A]

  def liftF[F[_], A](op: F[A])(implicit F: Functor[F]): Free[F, A] = Suspend(F.map(op)(Pure(_)))

  def fold[F[_], A, B](program: Free[F, A])(pure: A => B, suspend: F[Free[F, A]] => B): B = program match {
    case Pure(value)   => pure(value)
    case Suspend(next) => suspend(next)
  }

  implicit def monadFree[F[_]](implicit F: Functor[F]): Monad[Free[F, *]] = new Monad[Free[F, *]] {
    def pure[A](value: A): Free[F, A] = Pure(value)

    def flatMap[A, B](fa: Free[F, A])(f: A => Free[F, B]): Free[F, B] = fa match {
      case Pure(value)   => f(value)
      case Suspend(next) => Suspend(F.map(next)(subFree => flatMap(subFree)(f)))
    }
  }

  implicit class FreeOps[F[_], A](free: Free[F, A])(implicit F: Functor[F]) {
    def map[B](f: A => B): Free[F, B] = monadFree.map(free)(f)
    def flatMap[B](f: A => Free[F, B]): Free[F, B] = monadFree.flatMap(free)(f)
  }
}
