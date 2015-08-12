/**
 * Copyright (C) 2015 Deveryware S.A. All Rights Reserved.
 */
package com.github.fakemongo;

import com.github.fakemongo.junit.FongoRule;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import java.lang.management.ManagementFactory;
import java.lang.management.MonitorInfo;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.assertj.core.api.Assertions;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

/**
 * see #132
 */
public class DeadlockIT {

  @Rule
  public FongoRule fongoRule = new FongoRule();

  @Rule
  public Timeout timeout = new Timeout(4, TimeUnit.SECONDS);

  @Test(expected = TimeoutException.class)
  public void deadLockTest() throws InterruptedException, TimeoutException, ExecutionException {
    final Mongo client = fongoRule.getMongo();
    final String databaseName = "test";

    final CyclicBarrier barrier = new CyclicBarrier(3);
    final ExecutorService service = Executors.newFixedThreadPool(3);
    final AtomicBoolean terminated = new AtomicBoolean(false);
    try {

      service.submit(new Callable<Void>() {
                       @Override
                       public Void call() throws Exception {
                         barrier.await();

                         while (!Thread.currentThread().isInterrupted() && !terminated.get()) {
                           DBCollection collection = client.getDB(databaseName).getCollection("test");

                           DBObject object = new BasicDBObject();
                           object.put("name", String.valueOf(System.currentTimeMillis()));

                           collection.insert(object);
                         }
                         return null;
                       }
                     }
      );

      service.submit(new Callable<Void>() {
                       @Override
                       public Void call() throws Exception {
                         barrier.await();

                         while (!Thread.currentThread().isInterrupted() && !terminated.get()) {
                           client.dropDatabase(databaseName);
                         }
                         return null;
                       }
                     }
      );

      final Future<Void> submit = service.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          barrier.await();

          while (!Thread.currentThread().isInterrupted() && !terminated.get()) {
            ThreadMXBean tmx = ManagementFactory.getThreadMXBean();
            long[] ids = tmx.findDeadlockedThreads();

            if (ids != null) {
              System.err.println("Deadlock found: ");
              ThreadInfo[] infos = tmx.getThreadInfo(ids, true, true);
              for (ThreadInfo ti : infos) {
                for (StackTraceElement stackTraceElement : ti.getStackTrace()) {
                  System.err.println(stackTraceElement);
                }

                System.err.println("Monitor: ");
                for (MonitorInfo monitorInfo : ti.getLockedMonitors()) {
                  System.err.println(monitorInfo.getLockedStackFrame());
                }
                System.err.println();
              }
            }

            Assertions.assertThat(ids).as("Deadlock detected").isNullOrEmpty();

            TimeUnit.MILLISECONDS.sleep(10);
          }
          return null;
        }
      });


      try {
        submit.get(3, TimeUnit.SECONDS);
      } catch (ExecutionException e) {
      }

    } finally {
      terminated.set(true);
      service.shutdown();
      service.awaitTermination(10, TimeUnit.MILLISECONDS);
    }
  }

}