package commons

import scala.language.reflectiveCalls

trait Stoppable {
  type T = { def stop(): Unit }
  def using[A <: T, B](resource: A)(f: A => B) = try {
    f(resource)
  } finally {
    resource.stop()
  }
}
