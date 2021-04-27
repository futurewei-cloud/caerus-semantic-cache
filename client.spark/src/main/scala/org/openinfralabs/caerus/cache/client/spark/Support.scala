package org.openinfralabs.caerus.cache.client.spark

private[caerus] sealed trait Tree[+T]
private[caerus] case class Support[T](support: T, children: Seq[Support[T]]) extends Tree[T]
