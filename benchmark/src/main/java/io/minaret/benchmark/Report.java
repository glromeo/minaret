package io.minaret.benchmark;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Locale;

/**
 * One run's throughput and latency distribution, printed to stdout and appended as a CSV row.
 *
 * The CSV columns are fixed, so rows from different runs can be read side by side.
 */
record Report(Options.Mode mode, int messages, Duration duration, double throughput,
              double min, double mean, double p50, double p95, double p99, double max) {

    private static final String HEADER = "timestamp,mode,totalMessages,totalDurationSec,throughputPerSec,minMs,meanMs,p50Ms,p95Ms,p99Ms,maxMs";

    static Report of(Options.Mode mode, double[] latenciesMs, Duration duration) {
        double[] sorted = latenciesMs.clone();
        Arrays.sort(sorted);
        int n = sorted.length;

        return new Report(mode, n, duration, n / (duration.toNanos() / 1e9),
                sorted[0], Arrays.stream(sorted).average().orElse(0),
                percentile(sorted, 0.50), percentile(sorted, 0.95), percentile(sorted, 0.99), sorted[n - 1]);
    }

    private static double percentile(double[] sorted, double p) {
        return sorted[Math.min((int) (sorted.length * p), sorted.length - 1)];
    }

    void print(PrintStream out) {
        out.println("Benchmark results (" + mode.name().toLowerCase(Locale.ROOT) + ")");
        out.println("------------------");
        out.printf(Locale.ROOT, "Total messages   : %,d%n", messages);
        out.printf(Locale.ROOT, "Total duration   : %.2f s%n", duration.toNanos() / 1e9);
        out.printf(Locale.ROOT, "Throughput       : %,.0f msg/s%n", throughput);
        out.printf(Locale.ROOT, "Latency min/mean : %.2f / %.2f ms%n", min, mean);
        out.printf(Locale.ROOT, "Latency p50/p95  : %.2f / %.2f ms%n", p50, p95);
        out.printf(Locale.ROOT, "Latency p99/max  : %.2f / %.2f ms%n", p99, max);
    }

    void appendTo(Path path) throws IOException {
        Path parent = path.toAbsolutePath().getParent();
        if (parent != null) Files.createDirectories(parent);

        String row = String.format(Locale.ROOT, "%s,%s,%d,%.3f,%.2f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f%n",
                Instant.now(), mode.name().toLowerCase(Locale.ROOT), messages, duration.toNanos() / 1e9, throughput,
                min, mean, p50, p95, p99, max);
        String text = Files.exists(path) ? row : HEADER + System.lineSeparator() + row;

        Files.writeString(path, text, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

}
