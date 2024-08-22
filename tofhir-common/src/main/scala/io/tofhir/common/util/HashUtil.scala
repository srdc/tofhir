package io.tofhir.common.util

import java.security.MessageDigest

/**
 * Utility class providing hash functions
 */
object HashUtil {

  /**
   * Given a text input, return MD5 hash representation as text
   *
   * @param text
   * @return
   */
  def md5Hash(text: String): String = {
    // Get an instance of the MD5 MessageDigest
    val md = MessageDigest.getInstance("MD5")

    // Convert the input string to bytes and compute the hash
    val hashBytes = md.digest(text.getBytes)

    // Convert the byte array to a hexadecimal string
    hashBytes.map("%02x".format(_)).mkString
  }

}
