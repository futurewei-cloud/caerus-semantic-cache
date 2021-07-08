package org.openinfralabs.caerus.cache.client

import org.openinfralabs.caerus.cache.common.Candidate

/**
 * Size estimator for Spark's Semantic Cache Client. SamplingSizeEstimator should create relatively small samples based
 * on a customized value (@ratio). Then it should instantiate the candidate for this sample.
 *
 * The writeSizeInfo should be derived from the size of the specific candidate instantiation
 * ((sample candidate size/sample source input size) * real source input size).
 *
 * The readSizeInfo should be derived differently for each technique.
 *
 * For repartitioning, the SamplingSizeEstimator should observe how many partitions are used for a specific query and
 * return:
 * (nrPartitionsUsed/nrPartitions) * real source input size
 * For example if you have repartitions for temperature [0-9], [10,19], [20-29] and a query asks for an estimation for
 * temperatures in [5,10]. Then there should be only two partitions selected. So, the answer should be:
 * 2/3*real source input size
 *
 * For file-skipping, the SamplingSizeEstimator should observe how many partitions are used for a specific query and
 * return:
 * (nrPartitionsUsed/nrPartitions) * real source input size
 * For example if you have files for temperature [0-15], [8,20], [12-30] and a query asks for an estimation for
 * temperatures in [5,10]. Then there should be only two files selected. So, the answer should be:
 * 2/3*real source input size
 *
 * For caching, the SamplingSizeEstimator should return the same value as the writeSizeInfo no matter what is the query.
 */
class SamplingSizeEstimator(private val ratio: Float) extends SizeEstimator {
  /**
   * Estimate and update read and write sizes for specific candidate.
   *
   * @param candidate Candidate to estimate and updates sizes for.
   */
  override def estimateSize(candidate: Candidate): Unit = ???
}
