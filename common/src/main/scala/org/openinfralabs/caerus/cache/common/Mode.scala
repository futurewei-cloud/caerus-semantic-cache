package org.openinfralabs.caerus.cache.common

object Mode extends Enumeration {
  type Mode = Value
  val NO_CACHE, MANUAL_WRITE, MANUAL_EVICTION, FULLY_AUTOMATIC = Value
}
