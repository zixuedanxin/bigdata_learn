package com.donews.data;


import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
/**
 * Created by reynold on 16-6-22.
 *
 */
public class Counter {
    private Logger LOG = LoggerFactory.getLogger(Counter.class);
    AtomicLong messages = new AtomicLong(0L);
    AtomicLong bytes = new AtomicLong(0L);
    private long start = System.currentTimeMillis();

    private void reset() {
        messages.set(0L);
        bytes.set(0L);
        start = System.currentTimeMillis();
    }

    public void start(Vertx vertx) {
        LOG.info("start Counter");
        long delay = Configuration.conf.getDuration("server.counter.delay", TimeUnit.MILLISECONDS);
        vertx.setPeriodic(delay, h -> {
            long time = System.currentTimeMillis() - start;
            double rps = messages.get() * 1000.0 / time;
            double mbps = (bytes.get() * 1000.0 / 1024.0 / 1024.0) / time;
            Runtime runtime = Runtime.getRuntime();
            double totalMem = runtime.totalMemory() * 1.0 / 1024 / 1024;
            double maxMem = runtime.maxMemory() * 1.0 / 1024 / 1024;
            double freeMem = runtime.freeMemory() * 1.0 / 1024 / 1024;
            LOG.info("{0}:Message/S, {1}:MBytes/S", rps, mbps);
            LOG.info("totalMem:{0}MB maxMem:{1}MB freeMem:{2}MB", totalMem, maxMem, freeMem);
            reset();
        });
    }


}