package org.openinfralabs.caerus.cache.common

object Tier extends Enumeration {
  type Tier = Value
  val COMPUTE_MEMORY, COMPUTE_DISK, STORAGE_MEMORY, STORAGE_DISK = Value
}
