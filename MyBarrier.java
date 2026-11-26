public class MyBarrier {
    private final int n;
    private int count = 0;

    public MyBarrier(int n) {
        if (n <= 0) {
            throw new IllegalArgumentException("n должно быть больше 0");
        }

        this.n = n;
    }

    public synchronized void await() throws InterruptedException {

        count++;

        if (count == n) {
            notifyAll();
        } else {
            while (count < n) {
                wait();
            }
        }
    }
}


class Main {

    public static void main(String[] args) {

        MyBarrier barrier = new MyBarrier(4);

        for (int i = 1; i <= 4; i++) {

            final int id = i;

            new Thread(() -> {
                try {
                    System.out.println("Поток " + id + " начал работу");

                    Thread.sleep(id * 500);

                    System.out.println("Поток " + id + " подошёл к барьеру");

                    barrier.await();

                    System.out.println("Поток " + id + " прошёл барьер");

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }).start();
        }
    }
}

