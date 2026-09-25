public class Integral2 {
    public static final int SIZE = 10000000;
    public static final int THREADS = 6;
    public static final int ITEMS_PER_THREAD = SIZE / THREADS;

    //интеграл вычисляется методом Симпсона
    public static final double A = 0.0;
    public static final double B = 1.0;
    public static final double H = (B - A)/SIZE;

    public static double f(double x) {
        return x/Math.pow(Math.pow(x, 2) + 1, 3);
    }

    public static double coef(int i) {
        if (i == 0 || i == SIZE) return 1.0;
        return (i % 2 == 1) ? 4.0 : 2.0;
    }

    static class Acc {
        volatile double acc = 0;
        synchronized public void addToAcc(double v) {
            acc += v;
        }
    }

    public static Thread taskThread(int n, int[] schedule, double[] results) {
        return new Thread(() -> {
            var start = schedule[n];
            var finish = schedule[n] + ITEMS_PER_THREAD;
            var acc = 0.0;
            for (int i = start; i < finish; i++) {
                acc += coef(i) * f(A + i * H);   // вклад узла i
            }
            results[n] = acc;
        });
    }

    public static Thread taskMonitorThread(int n, int[] schedule, Acc acc) {
        return new Thread(() -> {
            var start = schedule[n];
            var finish = schedule[n] + ITEMS_PER_THREAD;
            for (int i = start; i < finish; i++) {
                acc.addToAcc(coef(i) * f(A + i * H));
            }
        });
    }

    public static Thread taskAtomicThread(int n, int[] schedule, java.util.concurrent.atomic.DoubleAdder acc) {
        return new Thread(() -> {
            var start = schedule[n];
            var finish = schedule[n] + ITEMS_PER_THREAD;
            for (int i = start; i < finish; i++) {
                acc.add(coef(i) * f(A + i * H));
            }
        });
    }

    static void printIntegral(String label, double nodeSum, long timeNs) {
        double integral = nodeSum * H / 3.0;
        System.out.println(label + " result");
        System.out.printf("%.12f%n", integral);
        System.out.println(label + " time (ms)");
        System.out.println((double) timeNs / 1_000_000);
    }

    public static void measureP() throws InterruptedException {
        var threadsStart = new int[THREADS];
        var threads = new Thread[THREADS];
        for (int i = 0; i < THREADS; i++) {
            threadsStart[i] = i * ITEMS_PER_THREAD;
        }

        double[] results = new double[THREADS];
        var pStart = System.nanoTime();

        for (int i = 0; i < THREADS; i++) {
            threads[i] = taskThread(i, threadsStart, results);
        }
        for (int i = 0; i < THREADS; i++) threads[i].start();
        for (int i = 0; i < THREADS; i++) threads[i].join();

        var pResult = 0.0;
        for (int i = 0; i < THREADS; i++) pResult += results[i];
        pResult += coef(SIZE) * f(A + SIZE * H);

        var pFinish = System.nanoTime();
        printIntegral("Parallel", pResult, pFinish - pStart);
    }

    public static void measureMon() throws InterruptedException {
        var threadsStart = new int[THREADS];
        var threads = new Thread[THREADS];
        for (int i = 0; i < THREADS; i++) {
            threadsStart[i] = i * ITEMS_PER_THREAD;
        }

        var pStart = System.nanoTime();
        Acc acc = new Acc();
        for (int i = 0; i < THREADS; i++) {
            threads[i] = taskMonitorThread(i, threadsStart, acc);
        }
        for (int i = 0; i < THREADS; i++) threads[i].start();
        for (int i = 0; i < THREADS; i++) threads[i].join();

        double pResult = acc.acc + coef(SIZE) * f(A + SIZE * H);
        var pFinish = System.nanoTime();
        printIntegral("Monitor", pResult, pFinish - pStart);
    }

    public static void measureAtomic() throws InterruptedException {
        var threadsStart = new int[THREADS];
        var threads = new Thread[THREADS];
        for (int i = 0; i < THREADS; i++) {
            threadsStart[i] = i * ITEMS_PER_THREAD;
        }

        var pStart = System.nanoTime();
        var acc = new java.util.concurrent.atomic.DoubleAdder();
        for (int i = 0; i < THREADS; i++) {
            threads[i] = taskAtomicThread(i, threadsStart, acc);
        }
        for (int i = 0; i < THREADS; i++) threads[i].start();
        for (int i = 0; i < THREADS; i++) threads[i].join();

        double pResult = acc.sum() + coef(SIZE) * f(A + SIZE * H);
        var pFinish = System.nanoTime();
        printIntegral("Atomic", pResult, pFinish - pStart);
    }

    public static void main(String[] args) throws InterruptedException {

        var start = System.nanoTime();
        var acc = 0.0;
        for (int i = 0; i <= SIZE; i++) {
            acc += coef(i) * f(A + i * H);
        }
        var finish = System.nanoTime();

        printIntegral("Sequential", acc, finish - start);

        measureP();
        measureAtomic();
        measureMon();
    }
}