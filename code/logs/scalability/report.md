# Flink CPU scalability

26 November 2026 · WSL2 · AMD Ryzen 5 3600

The eight runs replayed the first million rows from each of the November 8, 9, and 10 trading files. They used JVM windows, eight Kafka input partitions, the same subscriptions, a five-second watermark delay, and checkpoints disabled. The producer used WSL CPU IDs `8,9`, and Kafka used `10,11`. Worker CPU affinity and subtask count changed together. The worker used distinct physical cores through the four-CPU setting. The six-CPU setting included two sibling threads. Each setting was measured once. [Host and code revision](host-20261126-103711.json)

| Target rows/s | Worker CPUs and subtasks | Kafka acked rows/s | Flink rows/s during publishing | Peak sampled lag (records) | EMA p95 (s) | Drain (s) | Mean worker CPU (cores) | Peak worker RSS (MiB) |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 30,000 | [1](final-20261126-103711-scaling-30000-jvm-p1/) | 29,999.1 | 29,888.8 | 136,102 | 5.935 | 1.631 | 0.857 | 813.8 |
| 30,000 | [2](final-20261126-103711-scaling-30000-jvm-p2/) | 29,998.9 | 29,860.9 | 22,413 | 0.917 | 1.429 | 1.106 | 1,117.9 |
| 30,000 | [4](final-20261126-103711-scaling-30000-jvm-p4/) | 29,998.8 | 29,834.2 | 1,880 | 0.799 | 1.291 | 1.289 | 1,485.8 |
| 30,000 | [6](final-20261126-103711-scaling-30000-jvm-p6/) | 29,999.2 | 29,751.9 | 2,501 | 0.855 | 1.344 | 1.333 | 2,021.7 |
| 35,000 | [1](final-20261126-103711-scaling-35000-jvm-p1/) | 34,708.8 | 27,674.8 | 594,263 | 21.042 | 16.975 | 0.892 | 825.1 |
| 35,000 | [2](final-20261126-103711-scaling-35000-jvm-p2/) | 34,999.1 | 34,876.3 | 28,308 | 0.938 | 0.349 | 1.121 | 1,155.2 |
| 35,000 | [4](final-20261126-103711-scaling-35000-jvm-p4/) | 32,335.1 | 32,193.9 | 23,881 | 1.001 | 1.809 | 1.400 | 1,585.4 |
| 35,000 | [6](final-20261126-103711-scaling-35000-jvm-p6/) | 33,154.2 | 33,015.1 | 12,492 | 0.947 | 0.346 | 1.538 | 2,040.3 |

At the 30,000 target, all four configurations received about 30,000 rows/s. Moving from one to four CPUs and subtasks reduced peak lag from 136,102 to 1,880 records and EMA p95 from 5.935 to 0.799 seconds. The six-CPU setting used about 536 MiB more peak worker RSS, with peak lag of 2,501 and EMA p95 of 0.855 seconds.

At the 35,000 target, moving from one to two CPUs and subtasks increased processing during publishing by 26.0% at similar achieved input. Peak lag fell from 594,263 to 28,308 records. The four- and six-CPU settings received about 32,335 and 33,154 rows/s. Their results show how Flink performed at those delivered rates, with the throughput effect of adding CPUs at a matched 35,000 rows/s still unmeasured. All eight runs processed 3,000,000 rows and reached zero lag. [Per-run measurements](summary.csv)

The producer and worker counters use the same monotonic clock. The processing rate uses the worker counter interpolated at producer completion. Lag is sampled from live source and Kafka end offsets and includes progress markers. The early and late lag medians use the first and last thirds of samples from 10% to 95% of publishing. Drain ends at the first post-publish sample with zero lag and matching acknowledged and processed totals. EMA p95 uses the nearest rank across 403 window-to-visible observations per run. CPU is the average number of logical CPUs used during publishing. Peak RSS is the maximum sampled sum across worker processes.

## Input distribution

The common slice contained 1,686,062 eligible trades across 3,127 symbols on FR, NL, and ETR. The ten busiest symbols accounted for 15,822 trades, or 0.94% of eligible trades. [Exchange totals](trades-by-exchange.csv) · [Ranked symbols](trades-by-symbol.csv)
