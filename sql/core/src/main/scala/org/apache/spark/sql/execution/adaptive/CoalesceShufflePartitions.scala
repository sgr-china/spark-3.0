/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.adaptive

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf

/**
 * A rule to coalesce the shuffle partitions based on the map output statistics, which can
 * avoid many small reduce tasks that hurt performance.
 */
case class CoalesceShufflePartitions(session: SparkSession) extends Rule[SparkPlan] {
  import CoalesceShufflePartitions._
  private def conf = session.sessionState.conf

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.coalesceShufflePartitionsEnabled) {
      return plan
    }
    if (!plan.collectLeaves().forall(_.isInstanceOf[QueryStageExec])
        || plan.find(_.isInstanceOf[CustomShuffleReaderExec]).isDefined) {
      // If not all leaf nodes are query stages, it's not safe to reduce the number of
      // shuffle partitions, because we may break the assumption that all children of a spark plan
      // have same number of output partitions. 如果不是所有的叶节点都是查询阶段，那么减少shuffle分区的数量是不安全的
      // 因为我们可能会打破spark计划的所有子节点具有相同数量的输出分区的假设。
      return plan
    }

    def collectShuffleStages(plan: SparkPlan): Seq[ShuffleQueryStageExec] = plan match {
      case stage: ShuffleQueryStageExec => Seq(stage)
      case _ => plan.children.flatMap(collectShuffleStages)
    }

    val shuffleStages = collectShuffleStages(plan)
    // ShuffleExchanges introduced by repartition do not support changing the number of partitions.
    // 重分区引入的ShuffleExchanges不支持更改分区数量。只有在所有ShuffleExchanges都支持的情况下，我们才会在这个阶段更改分区的数量。
    // We change the number of partitions in the stage only if all the ShuffleExchanges support it.
    // 如用户repartition操作将不会分区合并
    if (!shuffleStages.forall(_.shuffle.canChangeNumPartitions)) {
      plan
    } else {
      // `ShuffleQueryStageExec#mapStats` returns None when the input RDD has 0 partitions,
      // we should skip it when calculating the `partitionStartIndices`. 当输入RDD有0个分区时
      // ShuffleQueryStageExecmapStats '返回None，我们应该在计算' partitionStartIndices '时跳过它。
      val validMetrics = shuffleStages.flatMap(_.mapStats)

      // We may have different pre-shuffle partition numbers, don't reduce shuffle partition number
      // in that case. For example when we union fully aggregated data (data is arranged to a single
      // partition) and a result of a SortMergeJoin (multiple partitions).
      // 如果多个task进行shuffle，且task有不同的分区数的话，我们可能有不同的预洗牌分区号，在这种情况下不要减少洗牌分区号。
      // 例如，当我们将完全聚合的数据(数据被安排到单个分区)和SortMergeJoin的结果(多个分区)结合在一起时。
      val distinctNumPreShufflePartitions: Seq[Int] =
        validMetrics.map(stats => stats.bytesByPartitionId.length).distinct
      if (validMetrics.nonEmpty && distinctNumPreShufflePartitions.length == 1) {
        // We fall back to Spark default parallelism if the minimum number of coalesced partitions
        // is not set, so to avoid perf regressions compared to no coalescing. 如果没有设置合并分区的最小数量，
        // 我们将回到Spark默认并行度，以避免与没有合并相比的性能退化
        val minPartitionNum = conf.getConf(SQLConf.COALESCE_PARTITIONS_MIN_PARTITION_NUM)
          .getOrElse(session.sparkContext.defaultParallelism)
        val partitionSpecs = ShufflePartitionsUtil.coalescePartitions(
          validMetrics.toArray,
          advisoryTargetSize = conf.getConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES),
          minNumPartitions = minPartitionNum)
        // This transformation adds new nodes, so we must use `transformUp` here.
        val stageIds = shuffleStages.map(_.id).toSet
        plan.transformUp {
          // even for shuffle exchange whose input RDD has 0 partition, we should still update its
          // `partitionStartIndices`, so that all the leaf shuffles in a stage have the same
          // number of output partitions.
          case stage: ShuffleQueryStageExec if stageIds.contains(stage.id) =>
            CustomShuffleReaderExec(stage, partitionSpecs, COALESCED_SHUFFLE_READER_DESCRIPTION)
        }
      } else {
        plan
      }
    }
  }
}

object CoalesceShufflePartitions {
  val COALESCED_SHUFFLE_READER_DESCRIPTION = "coalesced"
}
