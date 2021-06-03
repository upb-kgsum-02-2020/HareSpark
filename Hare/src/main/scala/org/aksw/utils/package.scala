package org.aksw

import org.apache.commons.text.StringEscapeUtils
import org.apache.commons.text.StringEscapeUtils.ESCAPE_JAVA

package object utils {
  def escapeLiteralQuotes(str: String): String =
    StringEscapeUtils
      .builder(ESCAPE_JAVA)
      .append("\"")
      .escape(str.stripPrefix("\"").stripSuffix("\""))
      .append("\"")
      .toString

  def escapeLiteralWithLang(str: String): String = {
    // RFC 3066: language tags
    // ?= lookbehind to preserve @en, @ja, etc.
    val literalArray = str.split("(?=@[A-z]{2}$)", 2)
    s"${escapeLiteralQuotes(literalArray(0))}${literalArray(1)}"
  }

  def escapeObject(str: String, format: Boolean = false): String =
    if (str.startsWith("\"")) {
      val literalArray = str.split("\\^\\^http", 2)
      if (literalArray.length > 1) {
        val tail = if (format) s"<http${literalArray(1)}>" else s"http${literalArray(1)}"
        s"${escapeLiteralQuotes(literalArray(0))}^^$tail"
      } else {
        escapeLiteralWithLang(str)
      }
    } else {
      if (format) s"<$str>" else str
    }

  def escapeEntity(str: String, format: Boolean = false): String =
    if (str.startsWith("@")) {
      if (format)
        s"<${str.substring(1)}>"
      else
        str
    } else {
      escapeObject(str, format)
    }

  def escapeTriple(str: String, format: Boolean = false): String = {
    val Array(a, b, c) = str.split(" ", 3)

    val o = escapeObject(c, format)

    if (format) s"<$a> <${b.substring(1)}> $o ." else s"$a $b $o"
  }
}
