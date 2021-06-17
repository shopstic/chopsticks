package dev.chopsticks.util.config

import pureconfig.{CamelCase, NamingConvention, PascalCase}

object PureconfigFastCapitalizedWordsNamingConvention {
  private val Empty = List("")
}

trait PureconfigFastCapitalizedWordsNamingConvention extends NamingConvention {
  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def toTokens(s: String): Seq[String] = {
    if (s.isEmpty) {
      // to maintain compatibility with PureConfig's tokenizer
      PureconfigFastCapitalizedWordsNamingConvention.Empty
    }
    else {
      val buffer = List.newBuilder[String]
      val currentTokenBuilder = new StringBuilder
      val noneCtx = 0
      val lowerCaseCtx = 1
      val upperCaseCtx = 2
      val notLetterCtx = 3
      var ctx = noneCtx
      val _ = {
        val c = s.charAt(0)
        if ('a' <= c && c <= 'z') ctx = lowerCaseCtx
        else if ('A' <= c && c <= 'Z') ctx = upperCaseCtx
        else ctx = notLetterCtx
        currentTokenBuilder.append(c.toLower)
      }
      var i = 1
      while (i < s.length) {
        val c = s.charAt(i)
        if ('a' <= c && c <= 'z') {
          if (ctx == upperCaseCtx && currentTokenBuilder.length > 1) {
            val previousChar = currentTokenBuilder.charAt(currentTokenBuilder.length - 1)
            currentTokenBuilder.deleteCharAt(currentTokenBuilder.length - 1)
            buffer += currentTokenBuilder.result()
            currentTokenBuilder.clear()
            currentTokenBuilder.append(previousChar)
          }
          ctx = lowerCaseCtx
        }
        else if ('A' <= c && c <= 'Z') {
          if (ctx != upperCaseCtx) {
            buffer += currentTokenBuilder.result()
            currentTokenBuilder.clear()
          }
          ctx = upperCaseCtx
        }
        else {
          if (ctx != notLetterCtx) {
            buffer += currentTokenBuilder.result()
            currentTokenBuilder.clear()
          }
          ctx = notLetterCtx
        }
        currentTokenBuilder.append(c.toLower)
        i += 1
      }
      buffer += currentTokenBuilder.result()
      buffer.result()
    }
  }
}

object PureconfigFastCamelCaseNamingConvention extends PureconfigFastCapitalizedWordsNamingConvention {
  override def fromTokens(l: Seq[String]) = CamelCase.fromTokens(l)
}

object PureconfigFastPascalCaseNamingConvention extends PureconfigFastCapitalizedWordsNamingConvention {
  def fromTokens(l: Seq[String]): String = PascalCase.fromTokens(l)
}
