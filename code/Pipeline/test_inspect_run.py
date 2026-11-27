from collections import Counter
import unittest
from unittest.mock import Mock, patch

from inspect_run import LiveMetrics, ProgressSampler, UNAVAILABLE
from kafka_io import INPUT_PARTITIONS


def offset_line(partition, offset, *, job="job", topic="trading-test", subtask=0):
    return (
        "flink_taskmanager_job_task_operator_KafkaSourceReader_topic_partition_currentOffset"
        f'{{job_id="{job}",topic="{topic}",partition="{partition}",subtask_index="{subtask}"}} {offset}'
    )


class LiveMetricsTest(unittest.TestCase):
    def test_reads_only_the_selected_job_and_topic_and_requires_all_counter_subtasks(self):
        lines = [
            offset_line(0, "-1.0"),
            offset_line(1, "1.2e5", subtask=1),
            offset_line(0, "100.0", job="other"),
            offset_line(0, "100.0", topic="other"),
            offset_line(2, "NaN"),
            'flink_taskmanager_job_task_operator_trading_raw_received{job_id="job",subtask_index="0"} 12.0',
            'flink_taskmanager_job_task_operator_trading_raw_received{job_id="job",subtask_index="1"} 10.0',
            'flink_taskmanager_job_task_operator_trading_price_accepted{job_id="job",subtask_index="0"} 8.0',
        ]
        for subtask, value in enumerate((1_636_000_000_000, 1_637_000_000_000)):
            lines.append(
                'flink_taskmanager_job_task_operator_currentOutputWatermark{job_id="job",'
                f'operator_name="price-windows-ema-crossover",subtask_index="{subtask}"}} {value}'
            )
        self.assertEqual(LiveMetrics("job", "trading-test", 2).parse(lines),
                         ({0: 0, 1: 120001}, {"raw_received": 22}, 1_636_000_000_000))

    def test_duplicate_partition_is_unavailable(self):
        lines = [offset_line(0, "5.0"), offset_line(0, "4.0", subtask=1)]
        self.assertEqual(LiveMetrics("job", "trading-test", 2).parse(lines)[0], {})


class ProgressSamplerTest(unittest.TestCase):
    def setUp(self):
        with patch("inspect_run.AdminClient"):
            self.sampler = ProgressSampler("trading-test")
        self.metrics = Mock()
        self.results = Mock(counts=Counter())

    def set_offsets(self, current, end):
        self.metrics.sample.return_value = (dict.fromkeys(range(INPUT_PARTITIONS), current), {}, None)
        futures = {partition: Mock() for partition in self.sampler.requested}
        for future in futures.values():
            future.result.return_value.offset = end
        self.sampler.admin.list_offsets.return_value = futures

    def test_live_lag_grows_and_drains(self):
        for current, end, lag in [(15, 15, 0), (15, 20, 40), (16, 30, 112), (30, 30, 0)]:
            self.set_offsets(current, end)
            sample = self.sampler.sample(self.metrics, self.results)
            self.assertEqual(sample["lag_records"], lag)
            self.assertEqual(sample["lag_reason"], "none")
            self.assertLessEqual(sample["monotonic_ns"], sample["kafka_sample_ns"])

    def test_failed_scrape_discards_previous_offsets(self):
        self.set_offsets(15, 20)
        self.assertEqual(self.sampler.sample(self.metrics, self.results)["lag_records"], 40)
        self.metrics.sample.side_effect = TimeoutError("scrape timed out")
        sample = self.sampler.sample(self.metrics, self.results)
        self.assertEqual(sample["lag_records"], UNAVAILABLE)
        self.assertEqual(sample["lag_reason"], "flink_metrics_unavailable")
        self.assertEqual(sample["partitions"], "0/8")

    def test_missing_partition_and_failed_broker_query_are_unavailable(self):
        self.set_offsets(15, 20)
        self.metrics.sample.return_value[0].pop(0)
        sample = self.sampler.sample(self.metrics, self.results)
        self.assertEqual((sample["lag_records"], sample["lag_reason"]),
                         (UNAVAILABLE, "flink_partitions_missing"))
        self.set_offsets(15, 20)
        self.sampler.admin.list_offsets.side_effect = TimeoutError("broker timed out")
        with patch("inspect_run.AdminClient"):
            sample = self.sampler.sample(self.metrics, self.results)
        self.assertEqual((sample["lag_records"], sample["lag_reason"]),
                         (UNAVAILABLE, "kafka_offsets_unavailable"))


if __name__ == "__main__":
    unittest.main()
