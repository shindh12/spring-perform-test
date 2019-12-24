package com.sql.exec.perform;

/**
 * Date: 2019-12-18
 */
public class PerformResult {
    private String name;
    private long millis;
    private double cpuUsage;

    public PerformResult() {
    }

    public PerformResult(String name, long millis, double cpuUsage) {
        this.name = name;
        this.millis = millis;
        this.cpuUsage = cpuUsage;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getMillis() {
        return millis;
    }

    public void setMillis(long millis) {
        this.millis = millis;
    }

    public double getCpuUsage() {
        return cpuUsage;
    }

    public void setCpuUsage(double cpuUsage) {
        this.cpuUsage = cpuUsage;
    }
}
