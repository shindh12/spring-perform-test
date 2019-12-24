package com.sql.exec.perform;

import com.google.common.base.Function;
import com.google.common.base.Stopwatch;
import com.sun.management.OperatingSystemMXBean;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import javax.management.MBeanServerConnection;

/**
 * Basically a {@link Runnable} with an Integer input.
 */
public abstract class PerformanceTestBase implements Function<Integer, Void>, AutoCloseable {

    private Stopwatch watch = Stopwatch.createUnstarted();

    private MBeanServerConnection mbsc = ManagementFactory.getPlatformMBeanServer();

    private OperatingSystemMXBean osMBean;

    private long nanoBefore;
    private long cpuBefore;
    private long elapsedNano;
    private long elapsedCpu;


    public Void apply(Integer input) {
        run(input);
        return null;
    }

    public void initialize() {
        try {
            osMBean = ManagementFactory.newPlatformMXBeanProxy(
                mbsc, ManagementFactory.OPERATING_SYSTEM_MXBEAN_NAME, OperatingSystemMXBean.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        watch.reset();
        init();
    }

    public abstract void init();

    public abstract void run(int input);

    public abstract void close();

    String getName() {
        return getClass().getSimpleName();
    }

    Stopwatch getWatch() {
        return watch;
    }

    void startCpu() {
        this.nanoBefore = System.nanoTime();
        this.cpuBefore = osMBean.getProcessCpuTime();
    }
    void stopCpu() {
        long cpuAfter = osMBean.getProcessCpuTime();
        long nanoAfter = System.nanoTime();

        if (nanoAfter > this.nanoBefore) {
            this.elapsedNano += (nanoAfter - this.nanoBefore);
            this.elapsedCpu += (cpuAfter - this.cpuBefore);
        }
    }

    double getCpuUsage() {
        return (elapsedCpu * 100.0) / elapsedNano;
    }
}
