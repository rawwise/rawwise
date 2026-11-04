package io.github.rawwise.backend.server.job;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;

public class JobEventLog {
    private final ConcurrentLinkedDeque<JobEvent> events = new ConcurrentLinkedDeque<>();
    private final int maxCapacity;

    public JobEventLog() {
        this(10000);
    }

    public JobEventLog(int maxCapacity) {
        this.maxCapacity = maxCapacity;
    }

    public void emit(JobEvent event) {
        events.addLast(event);
        while (events.size() > maxCapacity) {
            events.pollFirst();
        }
    }

    public List<JobEvent> queryByJobId(String jobId) {
        return events.stream().filter(e -> e.jobId().equals(jobId)).collect(Collectors.toList());
    }

    public List<JobEvent> queryByType(JobEvent.JobEventType type, long sinceTimestamp) {
        return events.stream()
                .filter(e -> e.type() == type && e.timestamp() >= sinceTimestamp)
                .collect(Collectors.toList());
    }

    public List<JobEvent> recentEvents(int limit) {
        List<JobEvent> result = new ArrayList<>();
        for (JobEvent event : events.reversed()) {
            if (result.size() >= limit) break;
            result.add(event);
        }
        return result;
    }

    public int size() {
        return events.size();
    }

    public void clear() {
        events.clear();
    }
}
