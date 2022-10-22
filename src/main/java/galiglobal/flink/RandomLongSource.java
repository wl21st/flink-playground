package galiglobal.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.SplittableRandom;

public class RandomLongSource extends RichParallelSourceFunction<Long> {

    private static final long DURATION_MS = 60000L;

    private volatile boolean cancelled = false;
    private transient SplittableRandom random;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        random = new SplittableRandom();
    }

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        long startTime = System.currentTimeMillis();
        long now = startTime;
        while (!cancelled) {
            Long nextLong = random.nextLong();
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(nextLong);
            }

            if (now - startTime > DURATION_MS) {
                break;
            }
            now = System.currentTimeMillis();
        }
    }

    @Override
    public void cancel() {
        cancelled = true;
    }
}
