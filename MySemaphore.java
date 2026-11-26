import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

public class MySemaphore {

    private final AtomicInteger permits;
    private final Queue<Thread> waitingThreads =
            new ConcurrentLinkedQueue<>();

    public MySemaphore(int permits) {
        this.permits = new AtomicInteger(permits);
    }

    public void acquire(int n) {
        Thread current = Thread.currentThread();

        while (true) {
            int p = permits.get();

            if (p >= n && permits.compareAndSet(p, p - n)) {
                return;
            }

            waitingThreads.add(current);
            LockSupport.park();

            waitingThreads.remove(current);
        }
    }

    public void release(int n) {
        permits.addAndGet(n);

        Thread waiting = waitingThreads.poll();

        if (waiting != null) {
            LockSupport.unpark(waiting);
        }
    }
}


class Main {

    public static void main(String[] args) {
        
        MySemaphore semaphore = new MySemaphore(2);

        for (int i = 1; i <= 5; i++) {

            final int number = i;

            new Thread(() -> {

                System.out.println(
                        "Поток " + number + " ожидает разрешение"
                );

                semaphore.acquire(1);

                try {
                    System.out.println(
                            "Поток " + number + " начал работу"
                    );

                    Thread.sleep(2000);

                    System.out.println(
                            "Поток " + number + " закончил работу"
                    );

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();

                } finally {
                    semaphore.release(1);
                }

            }).start();
        }
    }
}
