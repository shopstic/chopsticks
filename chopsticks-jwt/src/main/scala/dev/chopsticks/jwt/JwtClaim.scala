package dev.chopsticks.jwt

final case class JwtClaim[C](
  content: C,
  issuer: Option[String] = None,
  subject: Option[String] = None,
  audience: Option[Set[String]] = None,
  expiration: Option[Long] = None,
  notBefore: Option[Long] = None,
  issuedAt: Option[Long] = None,
  jwtId: Option[String] = None
)
