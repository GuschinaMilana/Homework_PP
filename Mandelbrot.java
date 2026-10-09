import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class Mandelbrot {

    private static final int width = 1920;
    private static final int heigh = 1080;

    private static final double x_min = -2.5, x_max = 1.0;
    private static final double y_min = -1.2, y_max = 1.2;

    private static final int max_iter = 1000;
    private static final double radius2 = 4.0;

    private static final int strips = 64;

    public static void main(String[] args) throws Exception {
        int[] pixels = new int[width * heigh];

        int threads = Runtime.getRuntime().availableProcessors();
        ExecutorService pool = Executors.newFixedThreadPool(threads);

        long start = System.nanoTime();

        List<Future<?>> futures = new ArrayList<>();
        int rowsPerStrip = (heigh + strips - 1) / strips;

        for (int s = 0; s < strips; s++) {
            final int yStart = s * rowsPerStrip;
            final int yEnd   = Math.min(yStart + rowsPerStrip, heigh);
            if (yStart >= yEnd) break;

            futures.add(pool.submit(() -> {
                renderStrip(yStart, yEnd, pixels);
                return null;
            }));
        }

        for (Future<?> f : futures) f.get();
        pool.shutdown();

        long elapsed = System.nanoTime() - start;
        System.out.printf("Время выполнения %.2f мс%n",
                elapsed / 1e6, threads);

        BufferedImage img = new BufferedImage(width, heigh, BufferedImage.TYPE_INT_RGB);
        img.setRGB(0, 0, width, heigh, pixels, 0, width);
        ImageIO.write(img, "png", new File("mandelbrot.png"));
    }

    private static void renderStrip(int yStart, int yEnd, int[] pixels) {
        double dx = (x_max - x_min) / (width - 1);
        double dy = (y_max - y_min) / (heigh - 1);

        for (int y = yStart; y < yEnd; y++) {
            double ci = y_max - y * dy;
            int rowOffset = y * width;

            for (int x = 0; x < width; x++) {
                double cr = x_min + x * dx;

                double zr = 0.0, zi = 0.0;
                int iter = 0;
                while (iter < max_iter && (zr * zr + zi * zi) <= radius2) {
                    double zr2 = zr * zr;
                    double zi2 = zi * zi;
                    zi = 2.0 * zr * zi + ci;
                    zr = zr2 - zi2 + cr;
                    iter++;
                }

                pixels[rowOffset + x] = colorFor(iter);
            }
        }
    }

    private static int colorFor(int iter) {
        if (iter >= max_iter) return 0x000000;

        double t = (double) iter / max_iter;
        int r = (int) (9   * (1 - t) * t * t * t * 255);
        int g = (int) (15  * (1 - t) * (1 - t) * t * t * 255);
        int b = (int) (8.5 * (1 - t) * (1 - t) * (1 - t) * t * 255);
        return (r << 16) | (g << 8) | b;
    }
}