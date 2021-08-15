package dev.chopsticks

package object stream {
  type UAkkaFlow[-In, +Out, +Mat] = ZAkkaFlow[Any, Nothing, In, Out, Mat]
  type UAkkaSource[+Out, +Mat] = ZAkkaSource[Any, Nothing, Out, Mat]
}
