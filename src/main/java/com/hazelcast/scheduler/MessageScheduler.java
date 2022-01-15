package com.hazelcast.scheduler;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.lock.FencedLock;
import com.hazelcast.map.IMap;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

@Component
public class MessageScheduler {

    private final IMap<String, LocalDateTime> timeMap;

    private final HazelcastInstance hazelcastInstance;

    public MessageScheduler(IMap<String, LocalDateTime> timeMap, HazelcastInstance hazelcastInstance) {
        this.timeMap = timeMap;
        this.hazelcastInstance = hazelcastInstance;
    }


    /**
     * The first solution
     */
    @Scheduled(cron = "0/5 * * * * *")
    public void showMessage() {
        FencedLock lock = hazelcastInstance.getCPSubsystem().getLock("showMessageLock");
        if (lock.tryLock()) {
            try {
                LocalDateTime date = timeMap.get("message");
                if (date == null || LocalDateTime.now().isAfter(date)) {
                    System.out.println("I started at " + LocalDateTime.now());
                    timeMap.put("message", LocalDateTime.now().plusSeconds(4));
                }
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * The second solution
     */
    @Scheduled(cron = "0/5 * * * * *")
    public void showMessageWithThreadSleep() throws InterruptedException {
        FencedLock lock = hazelcastInstance.getCPSubsystem().getLock("showMessageLock");
        if (lock.tryLock()) {
            try {
                TimeUnit.MILLISECONDS.sleep(4000);
                System.out.println("I started at " + LocalDateTime.now());
            } finally {
                lock.unlock();
            }
        }
    }
}