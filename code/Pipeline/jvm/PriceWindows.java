package ssdm;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/** Maintains each symbol's window closes and EMAs in Flink's JVM keyed state. */
public final class PriceWindows extends KeyedProcessFunction<String, Row, String> {
    private static final long WINDOW_MS = 300_000;
    private static final Map<String, ZoneId> ZONES = Map.of(
            "FR", ZoneId.of("Europe/Paris"), "NL", ZoneId.of("Europe/Amsterdam"),
            "ETR", ZoneId.of("Europe/Berlin"));
    private static final OutputTag<String> ADVISORIES = new OutputTag<>("advisories", Types.STRING);
    private static final OutputTag<String> DIAGNOSTICS = new OutputTag<>("diagnostics", Types.STRING);
    private final String runId;
    private final Set<String> subscriptions;
    private final int diagnosticCap;
    private transient MapState<Long, Tuple5<Long, Integer, Long, Double, String>> windows;
    private transient ValueState<Tuple2<Double, Double>> previousEmas;
    private transient ObjectMapper json;
    private transient Counter accepted, late, completed, ema, buy, sell, sampled, suppressed;
    private transient Map<String, Counter> acceptedByExchange;
    private transient int lateSamples;

    private PriceWindows(String runId, Set<String> subscriptions, int diagnosticCap) {
        this.runId = runId;
        this.subscriptions = subscriptions;
        this.diagnosticCap = diagnosticCap;
    }

    public static SingleOutputStreamOperator<String> apply(
            DataStream<Row> prices, String runId, String[] subscriptions, int diagnosticCap) {
        Set<String> subscribed = subscriptions == null ? null : new HashSet<>(Arrays.asList(subscriptions));
        return prices.keyBy(row -> row.<String>getFieldAs(0))
                .process(new PriceWindows(runId, subscribed, diagnosticCap), Types.STRING);
    }

    @Override
    public void open(OpenContext context) {
        windows = getRuntimeContext().getMapState(new MapStateDescriptor<>(
                "open-price-windows", Types.LONG,
                Types.TUPLE(Types.LONG, Types.INT, Types.LONG, Types.DOUBLE, Types.STRING)));
        previousEmas = getRuntimeContext().getState(new ValueStateDescriptor<>(
                "previous-emas", Types.TUPLE(Types.DOUBLE, Types.DOUBLE)));
        json = new ObjectMapper();
        MetricGroup metrics = getRuntimeContext().getMetricGroup().addGroup("trading");
        accepted = metrics.counter("price_accepted");
        late = metrics.counter("price_late");
        completed = metrics.counter("price_windows_completed");
        ema = metrics.counter("ema_published");
        buy = metrics.counter("buy_published");
        sell = metrics.counter("sell_published");
        sampled = metrics.counter("diagnostics_sampled_price_late");
        suppressed = metrics.counter("diagnostics_suppressed_price_late");
        acceptedByExchange = new HashMap<>();
        for (String exchange : ZONES.keySet()) {
            acceptedByExchange.put(exchange, metrics.counter("price_accepted_" + exchange));
        }
    }

    @Override
    public void processElement(Row row, Context context, Collector<String> output) throws Exception {
        long tradeUs = row.getFieldAs(1);
        long end = (tradeUs / 1000 / WINDOW_MS + 1) * WINDOW_MS;
        long watermark = context.timerService().currentWatermark();
        if (watermark >= end) {
            late.inc();
            if (lateSamples >= diagnosticCap) {
                suppressed.inc();
                return;
            }
            lateSamples++;
            sampled.inc();
            context.output(DIAGNOSTICS, json.createObjectNode()
                    .put("kind", "price_late").put("reason", "window_already_final")
                    .put("symbol", context.getCurrentKey()).put("trade_us", tradeUs)
                    .put("source_index", (int) row.getFieldAs(2)).put("source_row", (long) row.getFieldAs(3))
                    .put("last", (double) row.getFieldAs(4)).put("source_file", (String) row.getFieldAs(5))
                    .put("window_end_ms", end).put("watermark_ms", watermark).toString());
            return;
        }

        accepted.inc();
        acceptedByExchange.get(exchange(context.getCurrentKey())).inc();
        Tuple5<Long, Integer, Long, Double, String> candidate = windows.get(end);
        int sourceIndex = row.getFieldAs(2);
        long sourceRow = row.getFieldAs(3);
        if (candidate == null) {
            context.timerService().registerEventTimeTimer(end);
        }
        if (candidate == null || isLater(tradeUs, sourceIndex, sourceRow, candidate)) {
            windows.put(end, Tuple5.of(tradeUs, sourceIndex, sourceRow, row.getFieldAs(4), row.getFieldAs(5)));
        }
    }

    private static boolean isLater(
            long tradeUs, int sourceIndex, long sourceRow, Tuple5<Long, Integer, Long, Double, String> close) {
        int byTime = Long.compare(tradeUs, close.f0);
        if (byTime != 0) {
            return byTime > 0;
        }
        int byFile = Integer.compare(sourceIndex, close.f1);
        return byFile > 0 || (byFile == 0 && sourceRow > close.f2);
    }

    @Override
    public void onTimer(long end, OnTimerContext context, Collector<String> output) throws Exception {
        Tuple5<Long, Integer, Long, Double, String> close = windows.get(end);
        windows.remove(end);
        Tuple2<Double, Double> previous = previousEmas.value();
        double previous38 = previous == null ? 0 : previous.f0;
        double previous100 = previous == null ? 0 : previous.f1;
        double fast = ema(close.f3, previous38, 38);
        double slow = ema(close.f3, previous100, 100);
        previousEmas.update(Tuple2.of(fast, slow));
        completed.inc();

        String symbol = context.getCurrentKey();
        if (subscriptions != null && !subscriptions.contains(symbol)) {
            return;
        }
        ZoneId zone = ZONES.get(exchange(symbol));
        String id = runId + ":" + symbol + ":" + end;
        ObjectNode result = json.createObjectNode().put("run_id", runId).put("symbol", symbol)
                .put("window_start", windowTime(end - WINDOW_MS, zone)).put("window_end", windowTime(end, zone))
                .put("window_end_ms", end).put("close", close.f3).put("ema38", fast).put("ema100", slow);
        ema.inc();
        output.collect(result.deepCopy().put("kind", "ema").put("result_id", id + ":ema")
                .put("trade_us", close.f0).put("source_index", close.f1)
                .put("source_row", close.f2).put("source_file", close.f4).toString());
        if (previous38 <= previous100 && fast > slow) {
            buy.inc();
            advice(context, result, id + ":buy", "BUY", "bullish_crossover", previous38, previous100);
        } else if (previous38 >= previous100 && fast < slow) {
            sell.inc();
            advice(context, result, id + ":sell", "SELL", "bearish_crossover", previous38, previous100);
        }
    }

    private static void advice(OnTimerContext context, ObjectNode result, String identity, String side,
            String reason, double previous38, double previous100) {
        context.output(ADVISORIES, result.put("kind", "advisory").put("result_id", identity)
                .put("advice", side).put("reason", reason)
                .put("previous_ema38", previous38).put("previous_ema100", previous100).toString());
    }

    private static String exchange(String symbol) {
        return symbol.substring(symbol.lastIndexOf('.') + 1);
    }

    private static double ema(double close, double previous, int period) {
        double alpha = 2.0 / (period + 1);
        return alpha * close + (1 - alpha) * previous;
    }

    private static String windowTime(long timestampMs, ZoneId zone) {
        return Instant.ofEpochMilli(timestampMs).atZone(zone).format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
    }
}
