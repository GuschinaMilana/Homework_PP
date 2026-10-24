import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/*
 * ПРАКТИЧЕСКОЕ ЗАДАНИЕ: МНОГОПОТОЧНЫЙ АНАЛИЗ ЛОГОВ ВЕБ-СЕРВЕРА
 *
 * Крупные части задания:
 * 1. Сгенерировать набор настоящих файлов с логами.
 * 2. Реализовать разбор строк и последовательный отчёт.
 * 3. Распараллелить обработку нескольких файлов через ExecutorService.
 * 4. Сделать общую статистику потокобезопасной.
 * 5. Ограничить число одновременно читаемых файлов через Semaphore.
 * 6. Добавить Atomic-счётчики, CAS и корректное ожидание завершения.
 * 7. Реализовать досрочную остановку анализа через volatile.
 * 8. Сформулировать и доказать инварианты корректности и отсутствия дедлоков.
 * 9. Реализовать обработку повреждённых файлов и частичный отчёт.
 *
 * ФАЙЛ МОЖНО ЗАПУСТИТЬ САМОСТОЯТЕЛЬНО:
 *   java LogAnalysisAssignment generate logs 20 1000
 *   java LogAnalysisAssignment check
 *
 * Первая команда создаёт каталог logs и файлы access-000.log, access-001.log и т.д.
 * Аргументы: каталог, количество файлов, количество строк в каждом файле.
 * Вторая команда выполняет встроенную проверку генератора и итератора строк.
 *
 * ФОРМАТ СТРОКИ:
 *   METHOD PATH STATUS RESPONSE_BYTES DURATION_MILLIS
 * Например:
 *   GET /api/users 200 820 4
 *
 * Поля разделены одним или несколькими пробелами.
 * Успешный запрос имеет статус от 200 до 399 включительно.
 * Ошибочный запрос имеет статус от 400 и выше.
 *
 * ГОТОВЫЕ СТРУКТУРЫ:
 *   LogEntry       — одна разобранная запись.
 *   LogFile        — путь к одному файлу.
 *   PathStatistics — неизменяемая статистика одного URL.
 *   Report         — итоговый отчёт.
 *   LogFileLines   — итератор строк файла.
 *   GenerateLogs   — генератор входных файлов.
 *
 * ВАЖНО ПРО LogFileLines:
 * Итератор потоконебезопасен. Один экземпляр Iterator<String> нельзя читать
 * из нескольких потоков одновременно. Каждый поток должен получить собственный
 * итератор для своего файла. Нельзя разделить один итератор между задачами.
 * Сам файл можно читать параллельно разными потоками только при условии, что
 * каждый поток создаёт отдельный итератор.
 *
 * ЧАСТИ ЗАДАНИЯ:
 *
 * 1. РАЗБОР СТРОКИ
 * Реализуйте преобразование одной строки в LogEntry. Проверьте количество
 * полей, преобразуйте числовые значения и выбрасывайте понятную ошибку для
 * повреждённой строки.
 *
 * 2. ПОСЛЕДОВАТЕЛЬНЫЙ ОТЧЁТ
 * Напишите обработку всех файлов одним потоком. Для каждого URL собирайте
 * количество запросов, количество успешных запросов и ошибок, объём ответов,
 * суммарное и максимальное время обработки. Этот вариант станет эталоном для
 * проверки многопоточной версии.
 *
 * 3. ПАРАЛЛЕЛЬНАЯ ОБРАБОТКА ФАЙЛОВ
 * Создайте фиксированный ExecutorService. Каждый файл обрабатывается отдельной
 * задачей. Рабочий поток создаёт собственный LogFileLines.Iterator, читает свой
 * файл и передаёт записи в общий аккумулятор. Главный поток должен дождаться
 * завершения всех файлов и только потом построить Report.
 *
 * 4. ПОТОКОБЕЗОПАСНАЯ СТАТИСТИКА
 * Реализуйте аккумулятор статистики. Сначала допустимо использовать один общий
 * synchronized-метод. Затем реализуйте вариант с отдельным synchronized-аккумулятором
 * для каждого URL и ConcurrentHashMap. Сравните эти варианты на данных, где почти
 * все запросы относятся к одному URL, и на данных с большим числом URL.
 *
 * 5. ОГРАНИЧЕНИЕ ЧТЕНИЯ ФАЙЛОВ
 * Добавьте Semaphore, ограничивающий число файлов, которые могут обрабатываться
 * одновременно. Рабочих потоков может быть больше разрешений семафора. Разрешение
 * нужно получить перед чтением файла и гарантированно вернуть после завершения.
 * Добавьте наблюдение за текущим и максимальным числом активных файлов.
 *
 * 6. ATOMIC-СЧЁТЧИКИ И CAS
 * Добавьте счётчики обработанных файлов, прочитанных строк, корректных записей,
 * повреждённых записей, ошибок чтения и активных файлов. Для максимального числа
 * активных файлов используйте AtomicInteger и compareAndSet. Обычные изменяемые
 * int/long для этих глобальных счётчиков не используйте.
 *
 * 7. ОЖИДАНИЕ ЗАВЕРШЕНИЯ
 * Организуйте ожидание всех файлов через CountDownLatch либо через набор Future.
 * Завершение задачи должно учитываться даже при исключении. ExecutorService нужно
 * корректно закрыть и дождаться его завершения.
 *
 * CountDownLatch — это одноразовый счётчик-сигнал. Его создают с начальным
 * числом, например равным количеству файлов. Главный поток вызывает await()
 * и блокируется, пока счётчик не станет равен нулю. Каждая завершившаяся задача
 * один раз вызывает countDown(), уменьшая счётчик. Когда последний счётчик
 * уменьшен до нуля, все ожидающие потоки продолжают работу. Увеличить latch
 * обратно или использовать его повторно нельзя, поэтому для нового запуска
 * создаётся новый объект. Вызов countDown() нужно помещать в finally, чтобы
 * исключение в задаче не оставило главный поток ждать бесконечно.
 *
 * 8. ДОСРОЧНАЯ ОСТАНОВКА
 * Добавьте volatile-флаг остановки. После остановки новые строки не начинают
 * обрабатываться, а уже начатая операция может завершиться. Опишите, какие данные
 * входят в частичный отчёт. Не оставляйте рабочие потоки и разрешения семафора
 * в подвешенном состоянии.
 *
 * 9. ПОВРЕЖДЁННЫЕ ФАЙЛЫ И ЧАСТИЧНЫЙ ОТЧЁТ
 * Сделайте так, чтобы ошибка одного файла не отменяла обработку остальных файлов.
 * В итоговом результате отдельно храните список или число файлов, которые не удалось
 * обработать. Решите, что происходит при повреждённой строке: прекращается обработка
 * всего файла или пропускается только эта строка. Выберите один вариант и явно
 * зафиксируйте его в документации. Для полного запуска корректные файлы должны
 * давать полный отчёт, а ошибка должна быть видна пользователю.
 *
 * ИНВАРИАНТЫ, КОТОРЫЕ НУЖНО ДОКАЗАТЬ:
 * - каждая строка полного запуска обработана ровно один раз;
 * - для каждого URL requests = successful + errors;
 * - maxDuration <= totalDuration при наличии запросов;
 * - 0 <= activeFiles <= semaphoreLimit;
 * - после завершения анализа activeFiles равно нулю;
 * - один повреждённый файл не мешает обработке остальных;
 * - в программе нет дедлока.
 *
 * В доказательстве отсутствия дедлока укажите порядок захвата мониторов и
 * семафоров. Отдельно объясните, может ли поток ждать acquire(), удерживая
 * synchronized-монитор, и почему разрешение семафора возвращается при исключении.
 * Объясните, почему volatile нужен для флага остановки.
 *
 * НЕБОЛЬШИЕ ЭКСПЕРИМЕНТЫ ПОСЛЕ РЕАЛИЗАЦИИ:
 * - сравнить 1 и 4 рабочих потока;
 * - сравнить лимит семафора 1 и 4;
 * - сравнить глобальную блокировку и блокировки по URL;
 * - добавить одну повреждённую строку или один повреждённый файл и проверить,
 *   что остальные данные всё равно попадают в отчёт.
 * Эксперименты не требуют дополнительной архитектуры или новых классов.
 */

public class LogAnalysisAssignment {

    public record LogEntry(
            String method,
            String path,
            int status,
            long responseBytes,
            long durationMillis
    ) {}

    public record LogFile(Path path) {}

    public record PathStatistics(
            long requests,
            long successful,
            long errors,
            long responseBytes,
            long totalDurationMillis,
            long maxDurationMillis
    ) {}

    public record Report(
            long totalRequests,
            long successfulRequests,
            long errorRequests,
            long responseBytes,
            long maxDurationMillis
    ) {}


    public record SequentialResult(
            Map<String, PathStatistics> byPath,
            long totalLines,
            long parsedLines,
            long corruptedLines,
            long failedFiles
    ) {}

    public record ParallelResult(
            Map<String, PathStatistics> byPath,
            long filesProcessed,
            long linesRead,
            long entriesParsed,
            long corruptedLines,
            long failedFiles,
            int  maxActiveFiles,
            boolean stoppedEarly
    ) {}


    // последовательный отчёт
    public static SequentialResult analyzeSequential(Path directory) throws IOException {
        List<Path> files;
        try (var stream = Files.list(directory)) {
            files = stream
                    .filter(Files::isRegularFile)
                    .sorted()
                    .toList();
        }

        Map<String, PathAccumulator> acc = new HashMap<>();
        long totalLines = 0;
        long parsedLines = 0;
        long corruptedLines = 0;
        long failedFiles = 0;

        for (Path file : files) {
            try {
                Iterator<String> it = LogFileLines.iterator(file);
                while (it.hasNext()) {
                    String line = it.next();
                    totalLines++;
                    try {
                        LogEntry entry = parseLogEntry(line);
                        acc.computeIfAbsent(entry.path(), k -> new PathAccumulator())
                                .add(entry);
                        parsedLines++;
                    } catch (IllegalArgumentException badLine) {
                        corruptedLines++;
                    }
                }
            } catch (RuntimeException fileError) {
                failedFiles++;
            }
        }

        Map<String, PathStatistics> byPath = new LinkedHashMap<>();
        acc.entrySet().stream()
                .sorted(Map.Entry.comparingByKey())
                .forEach(e -> byPath.put(e.getKey(), e.getValue().toStatistics()));

        return new SequentialResult(
                byPath, totalLines, parsedLines, corruptedLines, failedFiles
        );
    }

    // частичный отчёт
    public static ParallelResult analyzeParallel(
            Path directory,
            int threadCount,
            int semaphoreLimit,
            boolean usePerPathLock,
            AtomicBoolean stopFlag
    ) throws IOException, InterruptedException {

        List<Path> files;
        try (var stream = Files.list(directory)) {
            files = stream.filter(Files::isRegularFile).sorted().toList();
        }

        GlobalLockAccumulator globalLock = usePerPathLock ? null : new GlobalLockAccumulator();
        PerPathAccumulator  perPath    = usePerPathLock ? new PerPathAccumulator() : null;
        java.util.function.Consumer<LogEntry> sink = usePerPathLock
                ? perPath::add : globalLock::add;

        Counters counters = new Counters();

        Semaphore semaphore = new Semaphore(semaphoreLimit);

        CountDownLatch latch = new CountDownLatch(files.size());

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        List<Future<?>> futures = new ArrayList<>(files.size());

        try {
            for (Path file : files) {
                futures.add(executor.submit(() -> {
                    boolean permitAcquired = false;
                    try {
                        if (stopFlag.get()) return;

                        semaphore.acquire();
                        permitAcquired = true;

                        int now = counters.activeFiles.incrementAndGet();
                        counters.observeActive(now);

                        Iterator<String> it = LogFileLines.iterator(file);
                        while (it.hasNext()) {
                            if (stopFlag.get()) break;

                            String line = it.next();
                            counters.linesRead.incrementAndGet();
                            try {
                                LogEntry entry = parseLogEntry(line);
                                sink.accept(entry);
                                counters.entriesParsed.incrementAndGet();
                            } catch (IllegalArgumentException badLine) {
                                counters.corruptedLines.incrementAndGet();
                            }
                        }
                        counters.filesProcessed.incrementAndGet();
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    } catch (RuntimeException fileError) {
                        counters.failedFiles.incrementAndGet();
                    } finally {
                        if (permitAcquired) {
                            counters.activeFiles.decrementAndGet();
                            semaphore.release();
                        }
                        latch.countDown();
                    }
                }));
            }

            for (Future<?> f : futures) {
                try {
                    f.get();
                } catch (ExecutionException e) {
                    counters.failedFiles.incrementAndGet();
                }
            }


        } finally {
            executor.shutdown();
            if (!executor.awaitTermination(1, TimeUnit.MINUTES)) {
                executor.shutdownNow();
            }
        }

        Map<String, PathStatistics> byPath = usePerPathLock
                ? perPath.snapshot() : globalLock.snapshot();

        return new ParallelResult(
                byPath,
                counters.filesProcessed.get(),
                counters.linesRead.get(),
                counters.entriesParsed.get(),
                counters.corruptedLines.get(),
                counters.failedFiles.get(),
                counters.maxActiveFiles.get(),
                stopFlag.get()
        );
    }


    public static Report toReport(SequentialResult r) {
        long totalRequests = 0, successful = 0, errors = 0;
        long responseBytes = 0, maxDuration = 0;

        for (PathStatistics s : r.byPath().values()) {
            totalRequests += s.requests();
            successful    += s.successful();
            errors        += s.errors();
            responseBytes += s.responseBytes();
            if (s.maxDurationMillis() > maxDuration) {
                maxDuration = s.maxDurationMillis();
            }
        }
        return new Report(totalRequests, successful, errors, responseBytes, maxDuration);
    }

    public static Report toReport(ParallelResult r) {
        long total = 0, ok = 0, err = 0, bytes = 0, maxDur = 0;
        for (PathStatistics s : r.byPath().values()) {
            total += s.requests();
            ok    += s.successful();
            err   += s.errors();
            bytes += s.responseBytes();
            if (s.maxDurationMillis() > maxDur) maxDur = s.maxDurationMillis();
        }
        return new Report(total, ok, err, bytes, maxDur);
    }


    // аккумулятор для одного потока
    private static final class PathAccumulator {
        long requests;
        long successful;
        long errors;
        long responseBytes;
        long totalDurationMillis;
        long maxDurationMillis;

        void add(LogEntry e) {
            requests++;
            responseBytes += e.responseBytes();
            totalDurationMillis += e.durationMillis();
            if (e.durationMillis() > maxDurationMillis) {
                maxDurationMillis = e.durationMillis();
            }
            if (e.status() >= 200 && e.status() <= 399) {
                successful++;
            } else {
                errors++;
            }
        }

        PathStatistics toStatistics() {
            return new PathStatistics(
                    requests, successful, errors,
                    responseBytes, totalDurationMillis, maxDurationMillis
            );
        }
    }


    // общий synhronized-метод
    static final class GlobalLockAccumulator {
        private final Map<String, PathAccumulator> byPath = new HashMap<>();

        synchronized void add(LogEntry e) {
            byPath.computeIfAbsent(e.path(), k -> new PathAccumulator()).add(e);
        }

        synchronized Map<String, PathStatistics> snapshot() {
            Map<String, PathStatistics> r = new LinkedHashMap<>();
            byPath.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(en -> r.put(en.getKey(), en.getValue().toStatistics()));
            return r;
        }
    }

    // для каждого URL и ConcurrentHashMap
    static final class PerPathAccumulator {
        private final ConcurrentHashMap<String, PathAccumulator> byPath =
                new ConcurrentHashMap<>();

        void add(LogEntry e) {
            PathAccumulator acc = byPath.computeIfAbsent(
                    e.path(), k -> new PathAccumulator());
            synchronized (acc) {
                acc.add(e);
            }
        }

        Map<String, PathStatistics> snapshot() {
            Map<String, PathStatistics> r = new LinkedHashMap<>();
            byPath.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .forEach(en -> {
                        PathStatistics s;
                        synchronized (en.getValue()) {
                            s = en.getValue().toStatistics();
                        }
                        r.put(en.getKey(), s);
                    });
            return r;
        }
    }

    static final class Counters {
        final AtomicLong filesProcessed  = new AtomicLong();
        final AtomicLong linesRead       = new AtomicLong();
        final AtomicLong entriesParsed   = new AtomicLong();
        final AtomicLong corruptedLines  = new AtomicLong();
        final AtomicLong failedFiles     = new AtomicLong();
        final AtomicInteger activeFiles  = new AtomicInteger();
        final AtomicInteger maxActiveFiles = new AtomicInteger();

        // CAS-обновление максимума
        void observeActive(int current) {
            int prev;
            do {
                prev = maxActiveFiles.get();
                if (current <= prev) return;
            } while (!maxActiveFiles.compareAndSet(prev, current));
        }
    }



        /**
         * Возвращает отдельный итератор для чтения строк одного файла.
         *
         * ВАЖНО: Iterator<String> потоконебезопасен. Один экземпляр нельзя
         * одновременно читать из нескольких потоков. Для каждого потока нужен
         * отдельный вызов LogFileLines.iterator(path).
         */
        public static final class LogFileLines {
            private LogFileLines() {}

            public static Iterator<String> iterator(Path path) {
                try {
                    BufferedReader reader = Files.newBufferedReader(
                            path,
                            StandardCharsets.UTF_8
                    );

                    return new Iterator<>() {
                        private String nextLine = readNext();
                        private boolean closed;

                        private String readNext() {
                            try {
                                String line = reader.readLine();
                                if (line == null) {
                                    close();
                                }
                                return line;
                            } catch (IOException exception) {
                                close();
                                throw new RuntimeException(
                                        "Cannot read " + path,
                                        exception
                                );
                            }
                        }

                        private void close() {
                            if (closed) {
                                return;
                            }
                            closed = true;
                            try {
                                reader.close();
                            } catch (IOException exception) {
                                throw new RuntimeException(
                                        "Cannot close " + path,
                                        exception
                                );
                            }
                        }

                        @Override
                        public boolean hasNext() {
                            return nextLine != null;
                        }

                        @Override
                        public String next() {
                            if (nextLine == null) {
                                throw new NoSuchElementException();
                            }
                            String result = nextLine;
                            nextLine = readNext();
                            return result;
                        }
                    };
                } catch (IOException exception) {
                    throw new RuntimeException(
                            "Cannot open " + path,
                            exception
                    );
                }
            }
        }

    // парсер
    public static LogEntry parseLogEntry(String line) {
        String[] logParsed = line.split(" ");
        if (logParsed.length != 5) {
            throw new IllegalArgumentException(
                    "Expected 5 parts but got " + logParsed.length + ": " + line
            );
        }
        String method = logParsed[0];
        String path = logParsed[1];
        try {
            int status = Integer.parseInt(logParsed[2]);
            long responseBytes = Long.parseLong(logParsed[3]);
            long durationMillis = Long.parseLong(logParsed[4]);
            return new LogEntry(method, path, status, responseBytes, durationMillis);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Invalid numeric field in line: " + line, e);
        }
    }

        /** Генерирует реальные текстовые файлы с детерминированными данными. */
        public static final class GenerateLogs {
            private static final String[] METHODS = {
                    "GET", "POST", "PUT", "DELETE"
            };
            private static final String[] PATHS = {
                    "/", "/index.html", "/login", "/api/users",
                    "/api/orders", "/api/products", "/search", "/health"
            };
            private static final int[] STATUSES = {
                    200, 200, 200, 201, 204,
                    301, 400, 401, 404, 500
            };

            private GenerateLogs() {}

            public static void generate(
                    Path outputDirectory,
                    int fileCount,
                    int linesPerFile
            ) throws IOException {
                if (fileCount < 0 || linesPerFile < 0) {
                    throw new IllegalArgumentException(
                            "Counts must be non-negative"
                    );
                }

                Files.createDirectories(outputDirectory);
                Random random = new Random(42);

                for (int fileId = 0; fileId < fileCount; fileId++) {
                    Path file = outputDirectory.resolve(
                            "access-%03d.log".formatted(fileId)
                    );

                    try (BufferedWriter writer = Files.newBufferedWriter(
                            file,
                            StandardCharsets.UTF_8
                    )) {
                        for (int line = 0; line < linesPerFile; line++) {
                            String method =
                                    METHODS[random.nextInt(METHODS.length)];
                            String path =
                                    PATHS[random.nextInt(PATHS.length)];
                            int status =
                                    STATUSES[random.nextInt(STATUSES.length)];
                            long responseBytes =
                                    100L + random.nextInt(100_000);
                            long durationMillis =
                                    1L + random.nextInt(500);

                            writer.write(
                                    "%s %s %d %d %d%n".formatted(
                                            method,
                                            path,
                                            status,
                                            responseBytes,
                                            durationMillis
                                    )
                            );
                        }
                    }
                }
            }
        }

        /** Проверяет генератор и чтение строк без участия студенческого анализатора. */
        private static void selfCheck() throws IOException {
            Path directory = Files.createTempDirectory("log-assignment-");
            GenerateLogs.generate(directory, 3, 7);

            int files = 0;
            int lines = 0;

            try (var paths = Files.list(directory)) {
                for (Path path : paths.toList()) {
                    files++;
                    Iterator<String> iterator = LogFileLines.iterator(path);

                    while (iterator.hasNext()) {
                        String line = iterator.next();
                        String[] fields = line.trim().split("\\s+");
                        if (fields.length != 5) {
                            throw new AssertionError(
                                    "Unexpected line: " + line
                            );
                        }
                        Integer.parseInt(fields[2]);
                        Long.parseLong(fields[3]);
                        Long.parseLong(fields[4]);
                        lines++;
                    }
                }
            }

            if (files != 3 || lines != 21) {
                throw new AssertionError(
                        "Expected 3 files and 21 lines, got "
                                + files + " files and " + lines + " lines"
                );
            }

            System.out.println(
                    "OK: generated " + files
                            + " files and read " + lines + " lines"
            );
        }

        public static void main(String[] args) throws Exception {
            if (args.length > 0 && args[0].equals("generate")) {
                Path directory = Path.of(
                        args.length > 1 ? args[1] : "logs"
                );
                int fileCount = args.length > 2
                        ? Integer.parseInt(args[2])
                        : 20;
                int linesPerFile = args.length > 3
                        ? Integer.parseInt(args[3])
                        : 1_000;

                GenerateLogs.generate(
                        directory,
                        fileCount,
                        linesPerFile
                );

                System.out.println(
                        "Generated " + fileCount
                                + " files in " + directory
                );
                return;
            }

            if (args.length > 0 && args[0].equals("check")) {
                selfCheck();
                return;
            }

            if (args.length > 0 && args[0].equals("sequential")) {
                Path directory = Path.of(args.length > 1 ? args[1] : "logs");
                SequentialResult result = analyzeSequential(directory);
                Report report = toReport(result);

                System.out.println("Files failed: " + result.failedFiles());
                System.out.println("Lines total: " + result.totalLines());
                System.out.println("Lines parsed: " + result.parsedLines());
                System.out.println("Lines corrupted: " + result.corruptedLines());
                System.out.println("Report: " + report);
                result.byPath().forEach((path, s) ->
                        System.out.println("  " + path + " -> " + s));
                return;
            }

            if (args.length > 0 && args[0].equals("parallel")) {
                Path dir = Path.of(args.length > 1 ? args[1] : "logs");
                int threads = args.length > 2 ? Integer.parseInt(args[2]) : 4;
                int semLimit = args.length > 3 ? Integer.parseInt(args[3]) : 4;
                boolean perPath = args.length > 4 && args[4].equals("perPath");

                AtomicBoolean stop = new AtomicBoolean(false);

                ParallelResult r = analyzeParallel(dir, threads, semLimit, perPath, stop);
                System.out.println("stoppedEarly: " + r.stoppedEarly());
                System.out.println("maxActiveFiles: " + r.maxActiveFiles());
                System.out.println("filesProcessed: " + r.filesProcessed());
                System.out.println("linesRead: " + r.linesRead());
                System.out.println("entriesParsed: " + r.entriesParsed());
                System.out.println("corruptedLines: " + r.corruptedLines());
                System.out.println("failedFiles: " + r.failedFiles());
                System.out.println("Report: " + toReport(r));
                return;
            }

            System.out.println(
                    "Usage: java LogAnalysisAssignment "
                            + "generate <directory> <files> <lines>"
            );
            System.out.println(
                    "       java LogAnalysisAssignment check"
            );
        }
}
