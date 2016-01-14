package com.lucidworks.fusion;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListenerAdapter;
import org.jctools.queues.QueueFactory;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Pattern;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import parsers.LogLineParser;

import org.jctools.queues.spec.ConcurrentQueueSpec;

/**
 * Command-line utility for sending log messages to a Fusion pipeline.
 */
public class LogIndexer {

  static {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
  }

  public static Logger log = LoggerFactory.getLogger(LogIndexer.class);

  static final SimpleDateFormat ISO_8601_DATE_FMT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

  static Option[] options() {
    return new Option[]{
            OptionBuilder
                    .withArgName("PATH")
                    .hasArg()
                    .isRequired(true)
                    .withDescription("Path to a directory containing logs; uncompressed or gzip compressed files (*.gz) are supported")
                    .create("dir"),
            OptionBuilder
                    .withArgName("PATTERN")
                    .hasArg()
                    .isRequired(false)
                    .withDescription("Regex to match log files in the watched directory, default is *.log")
                    .create("match"),
            OptionBuilder
                    .withArgName("CHARSET")
                    .hasArg()
                    .isRequired(false)
                    .withDescription("Name of the character set for the log file; defaults to UTF-8")
                    .create("charset"),
            OptionBuilder
                    .withArgName("DELIM")
                    .hasArg()
                    .isRequired(false)
                    .withDescription("Line delimiter to use when scanning for lines; defaults to the line.separator system property")
                    .create("lineDelimiter"),
            OptionBuilder
                    .isRequired(false)
                    .withDescription("Tail matched files for new log entries; note that you cannot tail compressed log files")
                    .create("tail"),
            OptionBuilder
                    .withArgName("MS")
                    .hasArg()
                    .isRequired(false)
                    .withDescription("Tail delay in milliseconds; default is 500")
                    .create("tailerDelayMs"),
            OptionBuilder
                    .withArgName("MS")
                    .hasArg()
                    .isRequired(false)
                    .withDescription("Stop tailing a file if no new events have arrived since this threshold in milliseconds, default is 120000 (2 minutes)")
                    .create("tailerReaperThresholdMs"),
            OptionBuilder
                    .withArgName("FILE")
                    .hasArg()
                    .isRequired(false)
                    .withDescription("Path to a file containing a list of files already processed")
                    .create("alreadyProcessedFiles"),
            OptionBuilder
                    .withArgName("SIZE")
                    .hasArg()
                    .isRequired(false)
                    .withDescription("Size of the thread pool to process files in the directory; default is 10")
                    .create("fileReaderPoolSize"),
            OptionBuilder
                    .withArgName("DATE")
                    .hasArg()
                    .isRequired(false)
                    .withDescription("Ignore any docs that occur before date/time")
                    .create("ignoreBefore"),
            OptionBuilder
                    .withArgName("#")
                    .hasArg()
                    .isRequired(false)
                    .withDescription("Metrics reporting frequency; default is every 1 minute")
                    .create("metricsReportingFrequency"),
            OptionBuilder
                    .isRequired(false)
                    .withDescription("Set this flag if you want to watch the directory for incoming files; this implies this application will run until killed")
                    .create("watch"),
            OptionBuilder
                    .withArgName("true|false")
                    .hasArg()
                    .isRequired(false)
                    .withDescription("Delete files after indexing; default is false, pass true if you want to delete files after they are processed (uncommon).")
                    .create("deleteAfterIndexing"),
            OptionBuilder
                    .withArgName("URL(s)")
                    .hasArg()
                    .isRequired(true)
                    .withDescription("Fusion endpoint(s)")
                    .create("fusion"),
            OptionBuilder
                    .withArgName("USERNAME")
                    .hasArg()
                    .isRequired(false)
                    .withDescription("Fusion username; default is admin")
                    .create("fusionUser"),
            OptionBuilder
                    .withArgName("PASSWORD")
                    .hasArg()
                    .isRequired(false)
                    .withDescription("Fusion password; required if fusionAuthEnbled=true")
                    .create("fusionPass"),
            OptionBuilder
                    .withArgName("PATH")
                    .hasArg()
                    .isRequired(false)
                    .withDescription("File containing the Fusion password, it will be deleted after the password is read; this allows you to avoid passing the password on the command-line.")
                    .create("fusionPassFile"),
            OptionBuilder
                    .withArgName("REALM")
                    .hasArg()
                    .isRequired(false)
                    .withDescription("Fusion security realm; default is native")
                    .create("fusionRealm"),
            OptionBuilder
                    .withArgName("true|false")
                    .hasArg()
                    .isRequired(false)
                    .withDescription("Fusion authentication enabled; default is true")
                    .create("fusionAuthEnabled"),
            OptionBuilder
                    .withArgName("INT")
                    .hasArg()
                    .isRequired(false)
                    .withDescription("Fusion indexing batch size; default is 100")
                    .create("fusionBatchSize"),
            OptionBuilder
                    .withArgName("FIELD")
                    .hasArg()
                    .isRequired(false)
                    .withDescription("Document ID field, default is id")
                    .create("idFieldName"),
            OptionBuilder
                    .withArgName("CLASS")
                    .hasArg()
                    .isRequired(false)
                    .withDescription("Class name of a line parser, defaults to GrokLogLineParser; must implement the LogLineParser interface")
                    .create("lineParserClass"),
            OptionBuilder
                    .withArgName("PATH")
                    .hasArg()
                    .isRequired(false)
                    .withDescription("Path to a properties file for configuring the log line parser.")
                    .create("lineParserConfig"),
            OptionBuilder
                    .withArgName("INT")
                    .hasArg()
                    .isRequired(false)
                    .withDescription("Size of the queue holding pending docs to be sent to Fusion for indexing; default is 1000000")
                    .create("docsToIndexQueueSize"),
            OptionBuilder
                    .withArgName("INT")
                    .hasArg()
                    .isRequired(false)
                    .withDescription("Number of sender threads, i.e. those consuming docs from the queue; defaults to twice the number of Fusion service endpoints")
                    .create("senderThreads"),
            OptionBuilder
                    .withArgName("INT")
                    .hasArg()
                    .isRequired(false)
                    .withDescription("Milliseconds to wait to see a document on the queue; defaults to 200 ms")
                    .create("pollQueueTimeMs")
    };
  }

  public static CommandLine processCommandLineArgs(String[] args) {
    Options opts = getOptions();
    if (args == null || args.length == 0 || args[0] == null || args[0].trim().length() == 0) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(125, LogIndexer.class.getName(), null, opts, null);
      System.exit(1);
    }
    return processCommandLineArgs(opts, args);
  }

  public static void main(String[] args) throws Exception {
    LogIndexer app = new LogIndexer();
    app.run(processCommandLineArgs(args));
  }

  protected FusionPipelineClient fusion;
  protected int fusionBatchSize;

  public MetricRegistry metrics = new MetricRegistry();
  public Meter linesProcessed = metrics.meter("linesProcessed");
  public Counter docCounter = metrics.counter("docsToBeIndexed");
  public Counter linesRead = metrics.counter("linesRead");
  public Counter parsedFiles = metrics.counter("parsedFiles");
  public Counter totalFiles = metrics.counter("totalFiles");
  public Counter totalSkippedLines = metrics.counter("totalSkippedLines");

  protected long _startedAtMs = 0L;
  protected int fileReaderPoolSize = 10;
  protected File logDir;
  protected FileWriter processedFileSetWriter;
  protected Date ignoreBeforeDate;
  protected boolean watch = false;
  protected boolean deleteAfterIndexing = true;
  protected String idFieldName;
  protected boolean tail = false;
  protected long tailerDelayMs = 500;
  protected TailerReaperThread tailerReaperBgThread = null;
  protected LogLineParser logLineParser = null;
  private ConsoleReporter reporter = null;
  protected String charsetName = null;
  protected String lineDelimiter = null;

  protected Queue<Map> docsToIndexQueue;

  public void run(CommandLine cli) throws Exception {

    this.logDir = new File(cli.getOptionValue("dir"));
    if (!logDir.isDirectory())
      throw new FileNotFoundException(logDir.getAbsolutePath() + " not found!");

    String match = cli.getOptionValue("match", "*.log");
    if (match.startsWith("*.")) {
      String origMatch = match;
      match = "^.*?\\."+match.substring(2)+"$";
      log.info("Converted match="+origMatch+" to regex="+match);
    } else if ("*".equals(match)) {
      // match all files
      match = "^.*$";
      log.info("Converted match=* to regex="+match);
    }

    final Pattern matchLogsPattern = Pattern.compile(match);
    File[] matchedLogFiles = logDir.listFiles(new FileFilter() {
      public boolean accept(File f) {
        return f.isDirectory() || matchLogsPattern.matcher(f.getName()).matches();
      }
    });

    watch = cli.hasOption("watch");
    if (!watch) {
      if (matchedLogFiles.length == 0) {
        log.error("No log files matching "+match+" found in " + logDir.getAbsolutePath());
        return;
      }
    }

    tail = cli.hasOption("tail");
    if (tail) {
      tailerDelayMs = Long.parseLong(cli.getOptionValue("tailerDelayMs","500"));
      tailerReaperBgThread = new TailerReaperThread();
      tailerReaperBgThread.thresholdMs = Long.parseLong(cli.getOptionValue("tailerReaperThresholdMs", "120000"));
      tailerReaperBgThread.start();
      log.info("Started the tailer reaper background thread ("+tailerReaperBgThread+") with threshold: "+tailerReaperBgThread.thresholdMs);
    }

    // users can register a custom line parser if grok doesn't meet their needs
    logLineParser = initLogLineParser(cli);

    if (cli.hasOption("ignoreBefore")) {
      ignoreBeforeDate = ISO_8601_DATE_FMT.parse(cli.getOptionValue("ignoreBefore"));
      log.info("Will ignore any log messages that occurred before: " + ignoreBeforeDate);
    }

    charsetName = cli.getOptionValue("charset",StandardCharsets.UTF_8.name());
    lineDelimiter = cli.getOptionValue("lineDelimiter", System.getProperty("line.separator"));

    deleteAfterIndexing = Boolean.parseBoolean(cli.getOptionValue("deleteAfterIndexing", "false"));

    fileReaderPoolSize = Integer.parseInt(cli.getOptionValue("fileReaderPoolSize", "10"));

    if (reporter == null) {
      reporter = ConsoleReporter.forRegistry(metrics)
              .convertRatesTo(TimeUnit.SECONDS)
              .convertDurationsTo(TimeUnit.MILLISECONDS).build();

      int metricsReportingFrequency = Integer.parseInt(cli.getOptionValue("metricsReportingFrequency", "1"));
      reporter.start(metricsReportingFrequency, TimeUnit.MINUTES);

      log.info("Started metrics console reporter to send reports every " + metricsReportingFrequency + " minutes");
    }

    String alreadyProcessedFiles = cli.getOptionValue("alreadyProcessedFiles");
    int restartAtLine = 0;

    final String fusionEndpoints = cli.getOptionValue("fusion");
    final boolean fusionAuthEnabled = "true".equalsIgnoreCase(cli.getOptionValue("fusionAuthEnabled", "true"));
    final String fusionUser = cli.getOptionValue("fusionUser", "admin");
    String fusionPass = cli.getOptionValue("fusionPass");
    if (fusionAuthEnabled && (fusionPass == null || fusionPass.isEmpty())) {
      String fusionPassFileArg = cli.getOptionValue("fusionPassFile");
      if (fusionPassFileArg != null) {
        File fusionPassFile = new File(fusionPassFileArg);
        BufferedReader isr = null;
        try {
          isr = new BufferedReader(new InputStreamReader(new FileInputStream(fusionPassFile), StandardCharsets.UTF_8));
          fusionPass = isr.readLine().trim();
        } finally {
          if (isr != null) {
            try {
              isr.close();
            } catch (Exception ignore){}
          }
          fusionPassFile.delete();
        }
      }

      if (fusionPass == null || fusionPass.isEmpty()) {
        throw new IllegalArgumentException("Fusion password is required when authentication is enabled!");
      }
    }

    final String fusionRealm = cli.getOptionValue("fusionRealm", "native");
    fusionBatchSize = Integer.parseInt(cli.getOptionValue("fusionBatchSize", "100"));

    idFieldName = cli.getOptionValue("idFieldName", "id");

    FusionPipelineClient.metrics = metrics;

    // setup a multiple producer / multiple consumer queue for holding docs to index
    ConcurrentQueueSpec queueSpec =
            ConcurrentQueueSpec.createBoundedMpmc(Integer.parseInt(cli.getOptionValue("docsToIndexQueueSize", "1000000")));
    docsToIndexQueue = QueueFactory.newQueue(queueSpec);

    String senderThreads = cli.getOptionValue("senderThreads");
    int numFusionEndpoints = fusionEndpoints.split(",").length;
    int numSenderThreads = (senderThreads != null) ? Integer.parseInt(senderThreads) : 2*numFusionEndpoints;
    int pollQueueTimeMs = Integer.parseInt(cli.getOptionValue("pollQueueTimeMs","200"));

    try {
      fusion = fusionAuthEnabled ?
              new FusionPipelineClient(fusionEndpoints, fusionUser, fusionPass, fusionRealm) :
              new FusionPipelineClient(fusionEndpoints);
      log.info("Connected to Fusion. Processing log files in " + logDir.getAbsolutePath());

      // setup a pool of sender threads ...
      Sender[] senders = new Sender[numSenderThreads];
      for (int s=0; s < senders.length; s++)
        senders[s] = new Sender(docsToIndexQueue, pollQueueTimeMs, fusion, fusionBatchSize, linesProcessed);
      ExecutorService senderThreadPool = Executors.newFixedThreadPool(numSenderThreads);
      for (int s=0; s < senders.length; s++)
        senderThreadPool.submit(senders[s]);
      log.info("Created "+numSenderThreads+" sender threads (queue consumers) to send docs to "+numFusionEndpoints+" Fusion endpoints.");

      processLogDir(fusion, logDir, matchedLogFiles, matchLogsPattern, alreadyProcessedFiles, restartAtLine);

      if (!watch) {

        for (int s=0; s < senders.length; s++)
          senders[s].stopRunning();

        log.info("No more log events being queued ... waiting until all "+numSenderThreads+" sender threads complete their work.");
        shutdownAndAwaitTermination(senderThreadPool);

        if (!docsToIndexQueue.isEmpty()) {
          log.info("There are still "+docsToIndexQueue.size()+
                  " docs to be sent after all senders have been shutdown ... spinning up a final Sender to handle the remaining docs.");
          // still some docs to be indexed ...
          (new Sender(docsToIndexQueue, pollQueueTimeMs, fusion, fusionBatchSize, linesProcessed)).run();
        }
      }

    } finally {
      if (fusion != null) {
        fusion.shutdown();
      }

      if (processedFileSetWriter != null) {
        try {
          processedFileSetWriter.flush();
          processedFileSetWriter.close();
        } catch (Exception exc) {
          log.error("Failed to close processed tracking file due to: " + exc);
        }
      }
    }
  }
  
  protected LogLineParser initLogLineParser(CommandLine cli) throws Exception {
    Class lineParserClass =
            getClass().getClassLoader().loadClass(cli.getOptionValue("lineParserClass","parsers.GrokLogLineParser"));
    LogLineParser parser = (LogLineParser)lineParserClass.newInstance();
    String parserConfigFile = cli.getOptionValue("lineParserConfig");
    Properties configProps = new Properties();
    InputStream propsIn = null;
    if (parserConfigFile != null) {
      propsIn = new FileInputStream(parserConfigFile);
    } else {
      // load default config from the classpath
      propsIn = getClass().getClassLoader().getResourceAsStream("grok-parser.properties");
      if (propsIn == null)
        throw new FileNotFoundException("grok-parser.properties not found on the classpath!");
    }

    InputStreamReader isr = null;
    try {
      isr = new InputStreamReader(propsIn, StandardCharsets.UTF_8);
      configProps.load(isr);
    } finally {
      if (isr != null) {
        try {
          isr.close();
        } catch (Exception ignore){}
      }
    }
    String[] otherArgs = cli.getArgs();
    if (otherArgs != null) {
      for (String arg : otherArgs) {
        int eqAt = arg.indexOf("=");
        if (eqAt == -1)
          continue;
        configProps.put(arg.substring(0,eqAt).trim(), arg.substring(eqAt + 1).trim());
      }
    }
    parser.init(configProps);
    log.info("Initialized custom log line parser: "+parser);  
    return parser;
  }

  protected List<File> getFilesToParse(File[] matchedLogFiles, final Pattern matchLogFilePattern) throws Exception {
    List<File> files = new ArrayList<File>();
    for (File f : matchedLogFiles) {
      if (f.isDirectory()) {
        File[] inDir = f.listFiles(new FileFilter() {
          public boolean accept(File file) {
            if (file.isDirectory()) {
              return true;
            } else {
              return matchLogFilePattern.matcher(file.getName()).matches();
            }
          }
        });

        for (File next : inDir) {
          if (next.isDirectory()) {
            files.addAll(getFilesToParse(new File[]{next}, matchLogFilePattern));
          } else {
            files.add(next);
          }
        }
      } else {
        files.add(f);
      }
    }
    Collections.sort(files, new Comparator<File>() {
      public int compare(File o1, File o2) {
        return o1.getName().compareTo(o2.getName());
      }
    });
    return files;
  }

  protected void processLogDir(FusionPipelineClient fusion, 
                               File logDir, 
                               File[] matchedLogFiles,
                               Pattern matchLogFilePattern,
                               String alreadyProcessedFiles, 
                               int restartAtLine)
          throws Exception
  {
    List<File> sortedLogFiles = getFilesToParse(matchedLogFiles, matchLogFilePattern);

    log.info("Found " + sortedLogFiles.size() + " log files to parse");

    Set<String> alreadyProcessed = new HashSet<String>();
    if (alreadyProcessedFiles != null) {
      BufferedReader fr = null;
      String line = null;
      try {
        fr = new BufferedReader(new FileReader(alreadyProcessedFiles));
        while ((line = fr.readLine()) != null) {
          line = line.trim();
          if (line.length() > 0)
            alreadyProcessed.add(line);
        }
      } finally {
        if (fr != null) {
          try {
            fr.close();
          } catch (Exception ignore) {
          }
        }
      }
    }
    log.info("Found " + alreadyProcessed.size() + " already processed files.");

    processedFileSetWriter = new FileWriter(logDir.getName() + "_processed_files_v2", true);

    ExecutorService pool = Executors.newFixedThreadPool(fileReaderPoolSize);
    this._startedAtMs = System.currentTimeMillis();
    for (File file : sortedLogFiles) {
      String fileName = file.getAbsolutePath();
      if (alreadyProcessed.contains(fileName)) {
        log.info("Skipping already processed: " + fileName);
        continue;
      }
      pool.submit(new FileParser(this, file, 0));
      totalFiles.inc();
    }

    if (watch) {

      DirectoryWatcherThread watcherThread = new DirectoryWatcherThread(this, pool, logDir);
      watcherThread.start();

      int numSleeps = 0;
      while (true) {
        ++numSleeps;
        try {
          Thread.sleep(60000);
        } catch (InterruptedException ie) {
          Thread.interrupted();
        }

        // report progress every hour
        if (numSleeps % 60 == 0) {
          double _diff = (double) (System.currentTimeMillis() - _startedAtMs);
          long tookSecs = _diff > 1000 ? Math.round(_diff / 1000d) : 1;
          log.info("Processed " + parsedFiles.getCount() + " of " + totalFiles.getCount() + " files; running for: " + tookSecs +
                  " (secs) to send " + (docCounter.getCount()) + " docs, read " + linesRead.getCount() +
                  " lines; skipped: " + (linesRead.getCount() - docCounter.getCount()));
        }
      }
    } else {
      // wait for all queued work to complete
      shutdownAndAwaitTermination(pool);
      double _diff = (double) (System.currentTimeMillis() - _startedAtMs);
      long tookSecs = _diff > 1000 ? Math.round(_diff / 1000d) : 1;
      log.info("Processed " + parsedFiles.getCount() + " of " + totalFiles.getCount() + " files; took: " + tookSecs +
              " (secs) to send " + (docCounter.getCount()) + " docs, read " + linesRead.getCount() +
              " lines; skipped: " + (linesRead.getCount() - docCounter.getCount()));
    }
  }

  void shutdownAndAwaitTermination(ExecutorService pool) {
    pool.shutdown(); // Disable new tasks from being submitted
    try {
      // Wait a while for existing tasks to terminate
      if (!pool.awaitTermination(96, TimeUnit.HOURS)) {
        pool.shutdownNow(); // Cancel currently executing tasks
        // Wait a while for tasks to respond to being cancelled
        if (!pool.awaitTermination(60, TimeUnit.SECONDS))
          System.err.println("Pool did not terminate");
      }
    } catch (InterruptedException ie) {
      // (Re-)Cancel if current thread also interrupted
      pool.shutdownNow();
      // Preserve interrupt status
      Thread.currentThread().interrupt();
    }
  }

  protected void onFinishedParsingFile(String fileName, int lineNum, int skippedLines, long tookMs) {
    log.info("Finished processing log file " + fileName + ", took " + tookMs + " ms; skipped " + skippedLines + " out of " + lineNum + " lines");

    totalSkippedLines.inc(skippedLines);

    parsedFiles.inc();
    long fileCounter = parsedFiles.getCount();
    long mod = fileCounter % 20;
    if (mod == 0) {
      log.info("Processed " + fileCounter + " of " + totalFiles.getCount() + " files so far");
    }

    synchronized (this) {
      if (processedFileSetWriter != null) {
        try {
          processedFileSetWriter.append(fileName);
          processedFileSetWriter.append('\n');
          processedFileSetWriter.flush();
        } catch (IOException ioexc) {
          log.error("Failed to write " + fileName + " into processed files list due to: " + ioexc);
        }
      }
    }

    if (deleteAfterIndexing) {
      File toDelete = new File(fileName);
      if (toDelete.isFile()) {
        toDelete.delete();
      }
    }
  }

  protected Map<String,Object> parseLogLine(String fileName, int lineNum, String line) throws Exception {
    Map<String,Object> parsed = logLineParser.parseLine(fileName, lineNum, line);
    return (parsed != null) ? buildPipelineDocFromMap(parsed, fileName, lineNum) : null;
  }

  protected Map<String,Object> buildPipelineDocFromMap(Map grokMap, String fileName, int lineNum) {
    String docId = null;
    Object idObj = grokMap.get(idFieldName);
    boolean hasIdField = false;
    if (idObj != null) {
      docId = idObj.toString();
      hasIdField = true;
    } else {
      docId = String.format("%s:%d", fileName, lineNum);
    }

    Map<String,Object> doc = new HashMap<String,Object>(4);
    doc.put("id", docId);
    List fields = new ArrayList(grokMap.size());
    for (Object key : grokMap.keySet()) {
      Object val = grokMap.get(key);
      if (val != null)
        fields.add(mapField(key.toString(), val));
    }
    if (hasIdField) {
      fields.add(mapField("_src_", String.format("%s:%d", fileName, lineNum)));
    }
    doc.put("fields", fields);
    return doc;
  }

  protected final Map<String,Object> mapField(final String fieldName, final Object val) {
    Map<String,Object> fieldMap = new HashMap<String, Object>(4);
    fieldMap.put("name", fieldName.toLowerCase());
    fieldMap.put("value", val);
    return fieldMap;
  }

  static Options getOptions() {
    Options options = new Options();
    options.addOption("h", "help", false, "Print this message");
    options.addOption("v", "verbose", false, "Generate verbose log messages");
    Option[] toolOpts = options();
    for (int i = 0; i < toolOpts.length; i++)
      options.addOption(toolOpts[i]);
    return options;
  }

  public static CommandLine processCommandLineArgs(Options options, String[] args) {
    CommandLine cli = null;
    try {
      cli = (new GnuParser()).parse(options, args);
    } catch (ParseException exp) {
      boolean hasHelpArg = false;
      if (args != null && args.length > 0) {
        for (int z = 0; z < args.length; z++) {
          if ("-h".equals(args[z]) || "-help".equals(args[z])) {
            hasHelpArg = true;
            break;
          }
        }
      }
      if (!hasHelpArg) {
        System.err.println("Failed to parse command-line arguments due to: " + exp.getMessage());
      }
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(125, LogIndexer.class.getName(), null, options, null);
      System.exit(1);
    }

    if (cli.hasOption("help")) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp(125, LogIndexer.class.getName(), null, options, null);
      System.exit(0);
    }

    return cli;
  }

  class DirectoryWatcherThread extends Thread {
    LogIndexer logIndexer;
    File dir;
    boolean keepRunning;
    ExecutorService pool;
    Set<File> knownDirectories = new HashSet<File>();
    Map<WatchKey, Path> keys = new HashMap<WatchKey, Path>();

    DirectoryWatcherThread(LogIndexer logIndexer, ExecutorService pool, File dir) {
      this.logIndexer = logIndexer;
      this.pool = pool;
      this.dir = dir;
      this.keepRunning = true;
    }

    public void run() {
      log.info("Directory watcher thread running for: " + logDir.getAbsolutePath());
      try {
        doRun();
      } catch (Exception exc) {
        log.error("Directory watcher on '" + dir.getAbsolutePath() + "' failed due to: " + exc, exc);
      }
    }

    protected void doRun() throws Exception {
      try (WatchService watchService = FileSystems.getDefault().newWatchService()) {
        Path folder = Paths.get(logDir.getAbsolutePath());
        knownDirectories.add(logDir);
        WatchKey folderKey =
                folder.register(watchService, StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_MODIFY);
        keys.put(folderKey, folder);

        File[] subDirs = logDir.listFiles(new FileFilter() {
          public boolean accept(File file) {
            return file.isDirectory();
          }
        });

        for (File subDir : subDirs) {
          knownDirectories.add(subDir);
          Path subDirPath = Paths.get(subDir.getAbsolutePath());
          WatchKey subKey = subDirPath.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);
          keys.put(subKey, subDirPath);
        }

        while (keepRunning) {
          WatchKey key = watchService.take();
          if (key == null)
            continue;

          for (WatchEvent<?> watchEvent : key.pollEvents()) {
            if (StandardWatchEventKinds.ENTRY_CREATE == watchEvent.kind()) {
              Path newPath = ((WatchEvent<Path>) watchEvent).context();
              if (newPath == null)
                continue;

              Path dirPath = keys.get(key);
              if (dirPath == null) {
                log.error("No path for key: " + key);
                continue;
              }

              File newFile = dirPath.resolve(newPath).toFile();
              if (newFile.isDirectory()) {
                if (!knownDirectories.contains(newFile)) {
                  knownDirectories.add(newFile);

                  log.info("New sub-directory detected: " + newFile.getAbsolutePath());

                  // register a watch on this new child directory
                  Path absPath = Paths.get(newFile.getAbsolutePath());
                  WatchKey subKey = absPath.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);
                  keys.put(subKey, absPath);

                  File[] inDir = newFile.listFiles(new FileFilter() {
                    public boolean accept(File pathname) {
                      return pathname.getName().endsWith(".gz");
                    }
                  });
                  for (File next : inDir) {
                    pool.submit(new FileParser(logIndexer, next, 0));
                    log.info("Scheduled new file '" + next + "' for parsing.");
                    totalFiles.inc();
                  }
                }
              } else {
                pool.submit(new FileParser(logIndexer, newFile, 0));
                log.info("Scheduled new file '" + newFile + "' for parsing.");
                totalFiles.inc();
              }
            }
          }

          if (!key.reset()) {
            log.error("WatchKey was reset!");
            break; //loop
          }
        }
      }
    }
  }

  // Runs in a thread pool to parse files in-parallel
  class FileParser extends TailerListenerAdapter implements Runnable {

    File fileToParse;
    String fileName;
    int skipOver;
    LogIndexer logIndexer;
    List batchOfDocs;
    int lineNum = 0;
    int skippedLines = 0;
    long startMs;
    long lastEventAtMs = 0l;
    Tailer tailer = null;

    FileParser(LogIndexer logIndexer, File fileToParse, int skipOver) {
      this.logIndexer = logIndexer;
      this.fileToParse = fileToParse;
      this.fileName = fileToParse.getAbsolutePath();
      this.skipOver = skipOver;
      this.batchOfDocs = new ArrayList(logIndexer.fusionBatchSize);
      this.lineNum = 0;
      this.startMs = 0L;
    }

    public void handle(String line) {
      if (tailer != null) {
        // tailer reaper thread uses last activity time to determine if it should stop the tailer on this file
        lastEventAtMs = System.currentTimeMillis();
      }

      logIndexer.linesRead.inc();

      ++lineNum;

      if (lineNum < skipOver) {
        // support for skipping over lines
        return;
      }

      line = line.trim();
      if (line.length() == 0)
        return;

      Map<String,Object> doc = null;
      try {
        doc = logIndexer.parseLogLine(fileName, lineNum, line);
      } catch (Exception exc) {
        // TODO: guard against flood of errors here
        log.error("Failed to parse line "+lineNum+" in "+fileName+" due to: "+exc);
      }

      if (doc != null) {
        // queue this doc to be consumed by a Fusion sender thread
        docsToIndexQueue.offer(doc);
        docCounter.inc();
      } else {
        ++skippedLines;
      }

      if (lineNum > 10000) {
        if (lineNum % 10000 == 0) {
          long diffMs = System.currentTimeMillis() - startMs;
          log.info("Processed " + lineNum + " lines in " + fileName + "; running for " + diffMs + " ms");
        }
      }
    }

    public void run() {
      if (!fileToParse.isFile()) {
        log.warn("Skipping " + fileToParse.getAbsolutePath() + " because it doesn't exist anymore!");
        return;
      }

      lineNum = 0;
      batchOfDocs.clear();
      startMs = System.currentTimeMillis();

      if (logIndexer.tail) {
        tailer = new Tailer(fileToParse, this, logIndexer.tailerDelayMs);
        log.info("Tailing "+fileToParse+" with delay "+logIndexer.tailerDelayMs+" ms");

        logIndexer.tailerReaperBgThread.trackTailer(this);

        tailer.run(); // we're already in a thread, so just delegate to run
        log.info("Tailer stopped ... LogParser for " + fileName + " is done running.");
        logIndexer.onFinishedParsingFile(fileName, lineNum, skippedLines, System.currentTimeMillis() - startMs);
      } else {
        try {
          doParseFile(fileToParse);
        } catch (Exception exc) {
          log.error("Failed to process file '" + fileName + "' due to: " + exc, exc);
        }
      }
    }

    protected void doParseFile(File fileToParse) throws Exception {
      Scanner scanner = null;
      try {
        InputStream inputStream = new BufferedInputStream(new FileInputStream(fileToParse));
        if (fileName.endsWith(".gz"))
          inputStream = new GzipCompressorInputStream(inputStream);
        scanner = new Scanner(inputStream, logIndexer.charsetName);

        if (logIndexer.lineDelimiter != null) {
          scanner.useDelimiter(logIndexer.lineDelimiter);
        }

        if (log.isDebugEnabled())
          log.debug("Reading lines in file: " + fileName);

        while (scanner.hasNext())
          handle(scanner.next());

      } catch (IOException ioExc) {
        log.error("Failed to process " + fileToParse.getAbsolutePath() + " due to: " + ioExc);
      } finally {
        if (scanner != null) {
          try {
            scanner.close();
          } catch (Exception ignore) {}
        }
      }

      logIndexer.onFinishedParsingFile(fileName, lineNum, skippedLines, System.currentTimeMillis() - startMs);
    }
  }

  class TailerReaperThread extends Thread {

    boolean stopped = false;
    List<FileParser> fileParsers = new ArrayList<FileParser>();
    long thresholdMs = 120*1000L; // if no events for 2-minutes

    TailerReaperThread() {
      super("TailerReaperThread");
      setDaemon(true);
    }

    public void trackTailer(FileParser fp) {
      synchronized (fileParsers) {
        fileParsers.add(fp);
      }
    }

    @Override
    public void run() {
      while (!stopped) {
        try {
          Thread.sleep(30000);
        } catch (InterruptedException e) {
          e.printStackTrace();
          stopped = true;
        }

        if (stopped)
          break;

        long nowMs = System.currentTimeMillis();
        synchronized (fileParsers) {
          Iterator<FileParser> iter = fileParsers.iterator();
          while (iter.hasNext()) {
            FileParser fp = iter.next();
            if ((nowMs - fp.lastEventAtMs) > thresholdMs) {
              log.warn("No new lines added to " + fp.fileName + " in more than " + thresholdMs + " ms ... stopping the tailer");
              fp.tailer.stop();
              iter.remove();
            }
          }
        }
      }
    }
  }

  class Sender implements Runnable {

    Queue<Map> queue;
    int pollQueueTimeMs;
    List<Map> batchOfDocs;
    int batchSize;
    FusionPipelineClient fusion;
    Meter linesProcessed;
    long docsSentByMe = 0;
    volatile boolean keepRunning = true;

    Sender(Queue<Map> queue, int pollQueueTimeMs, FusionPipelineClient fusion, int batchSize, Meter linesProcessed) {
      this.queue = queue;
      this.pollQueueTimeMs = pollQueueTimeMs;
      this.fusion = fusion;
      this.batchSize = batchSize;
      this.batchOfDocs = new ArrayList<Map>(batchSize);
      this.linesProcessed = linesProcessed;
    }

    public void stopRunning() {
      this.keepRunning = false;
    }

    public void run() {

      while (keepRunning || !queue.isEmpty()) {
        Map doc = doc = queue.poll();
        if (doc == null) {
          try {
            Thread.sleep(pollQueueTimeMs);
          } catch (InterruptedException ie){}
          continue;
        }

        // got a doc ... add it to the batch
        batchOfDocs.add(doc);
        if (batchOfDocs.size() >= batchSize) {
          try {
            fusion.postBatchToPipeline(batchOfDocs);
            linesProcessed.mark(batchOfDocs.size());
            docsSentByMe += batchOfDocs.size();
          } catch (Exception exc) {
            if (exc instanceof RuntimeException) {
              throw (RuntimeException)exc;
            } else {
              throw new RuntimeException(exc);
            }
          } finally {
            batchOfDocs.clear();
          }
        }
      } // end while

      // send any remaining docs
      if (!batchOfDocs.isEmpty()) {
        try {
          fusion.postBatchToPipeline(batchOfDocs);
          linesProcessed.mark(batchOfDocs.size());
          docsSentByMe += batchOfDocs.size();
        } catch (Exception exc) {
          if (exc instanceof RuntimeException) {
            throw (RuntimeException)exc;
          } else {
            throw new RuntimeException(exc);
          }
        } finally {
          batchOfDocs.clear();
        }
      }

      log.info("Sender thread "+Thread.currentThread().getName()+" ending after sending "+docsSentByMe+" docs");
    }
  }
}
