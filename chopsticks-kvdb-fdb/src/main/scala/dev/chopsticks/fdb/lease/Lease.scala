package dev.chopsticks.fdb.lease

import java.time.Instant
import java.util.UUID

import eu.timepit.refined.types.all.NonNegInt

object LeaseKey {
  def apply(lease: Lease): LeaseKey = LeaseKey(lease.partitionNumber)
}
final case class LeaseKey(partitionNumber: Int)

final case class Lease(
  partitionNumber: Int,
  owner: Option[NonNegInt],
  refreshedAt: Instant,
  assigneeWaitingFrom: Option[Instant],
  // we need it to verify that the same replica does not hold multiple leases for one partition:
  // first that was expired and second that appeared
  acquisitionId: UUID
) {
  def withRefreshedAt(newRefreshedAt: Instant): Lease = copy(refreshedAt = newRefreshedAt)
}
