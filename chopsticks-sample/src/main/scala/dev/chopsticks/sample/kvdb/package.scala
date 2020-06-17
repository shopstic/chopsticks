package dev.chopsticks.sample

import zio.Has

package object kvdb {
  type SampleDbEnv = Has[SampleDb.Db]
}
