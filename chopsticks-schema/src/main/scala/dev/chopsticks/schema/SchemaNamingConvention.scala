package dev.chopsticks.schema

import zio.Chunk

trait SchemaNamingConvention:
  def toTokens(value: String): Chunk[String]
  def fromTokens(tokens: Chunk[String]): String

object SchemaNamingConvention:
  object CapitalizedWordsNamingConvention:
    private val Empty = Chunk("")

  trait CapitalizedWordsNamingConvention extends SchemaNamingConvention:
    @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
    override def toTokens(s: String): Chunk[String] =
      if (s.isEmpty)
        // to maintain compatibility with PureConfig's tokenizer
        CapitalizedWordsNamingConvention.Empty
      else
        val buffer = Chunk.newBuilder[String]
        val currentTokenBuilder = new StringBuilder
        val noneCtx = 0
        val lowerCaseCtx = 1
        val upperCaseCtx = 2
        val notLetterCtx = 3
        var ctx = noneCtx
        val _ =
          val c = s.charAt(0)
          if ('a' <= c && c <= 'z') ctx = lowerCaseCtx
          else if ('A' <= c && c <= 'Z') ctx = upperCaseCtx
          else ctx = notLetterCtx
          currentTokenBuilder.append(c.toLower)
        var i = 1
        while i < s.length do
          val c = s.charAt(i)
          if ('a' <= c && c <= 'z')
            if (ctx == upperCaseCtx && currentTokenBuilder.length > 1)
              val previousChar = currentTokenBuilder.charAt(currentTokenBuilder.length - 1)
              currentTokenBuilder.deleteCharAt(currentTokenBuilder.length - 1)
              buffer += currentTokenBuilder.result()
              currentTokenBuilder.clear()
              currentTokenBuilder.append(previousChar)
            ctx = lowerCaseCtx
          else if ('A' <= c && c <= 'Z')
            if (ctx != upperCaseCtx)
              buffer += currentTokenBuilder.result()
              currentTokenBuilder.clear()
            ctx = upperCaseCtx
          else
            if (ctx != notLetterCtx)
              buffer += currentTokenBuilder.result()
              currentTokenBuilder.clear()
            ctx = notLetterCtx
          currentTokenBuilder.append(c.toLower)
          i += 1
        end while
        buffer += currentTokenBuilder.result()
        buffer.result()
  end CapitalizedWordsNamingConvention

  // Naming conventions below are mostly copied from PureConfig:

  object CamelCase extends CapitalizedWordsNamingConvention:
    override def fromTokens(l: Chunk[String]): String =
      l.length match
        case 0 => ""
        case 1 => l(0).toLowerCase
        case _ => l(0).toLowerCase + l.iterator.drop(1).map(_.capitalize).mkString

  object PascalCase extends CapitalizedWordsNamingConvention:
    override def fromTokens(l: Chunk[String]): String = l.iterator.map(_.capitalize).mkString

  class StringDelimitedNamingConvention(d: String) extends SchemaNamingConvention {
    def toTokens(s: String): Chunk[String] =
      Chunk.fromIterator(s.split(d).iterator.map(_.toLowerCase))

    def fromTokens(l: Chunk[String]): String =
      l.map(_.toLowerCase).mkString(d)
  }

  object KebabCase extends StringDelimitedNamingConvention("-")
  object SnakeCase extends StringDelimitedNamingConvention("_")

  object ScreamingSnakeCase extends SchemaNamingConvention:
    override def toTokens(value: String): Chunk[String] = SnakeCase.toTokens(value)
    override def fromTokens(tokens: Chunk[String]): String = tokens.iterator.map(_.toUpperCase).mkString("_")

end SchemaNamingConvention
