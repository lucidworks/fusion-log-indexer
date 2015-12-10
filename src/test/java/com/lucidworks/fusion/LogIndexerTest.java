package com.lucidworks.fusion;

import org.apache.commons.cli.CommandLine;
import org.junit.*;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.github.tomakehurst.wiremock.junit.WireMockRule;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Random;

import org.apache.commons.io.FileUtils;
import parsers.DnsLogLineParser;

public class LogIndexerTest {

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(8976);

  String fusionUser = "admin";
  String fusionPass = "password123";
  String fusionRealm = "native";
  String fusionHost = "localhost";

  int fusionApiPort;
  String fusionHostAndPort;
  String fusionPipelineEndpoint;
  String fusionEndpoints;

  File testDataDir;

  @Before
  public void setupTest() {
    testDataDir = new File("src/test/test-data");
    if (!testDataDir.isDirectory())
      fail("Request test data directory "+testDataDir.getAbsolutePath()+" not found!");

    fusionApiPort = wireMockRule.port();
    fusionHostAndPort = "http://" + fusionHost + ":" + fusionApiPort;
    fusionPipelineEndpoint = "/api/apollo/index-pipelines/logs-default/collections/logs";
    fusionEndpoints = fusionHostAndPort + fusionPipelineEndpoint;

    // mock out the Fusion indexing pipeline endpoint and the session API endpoint
    stubFor(post(urlEqualTo(fusionPipelineEndpoint)).willReturn(aResponse().withStatus(200)));
    stubFor(post(urlEqualTo("/api/session?realmName=" + fusionRealm)).willReturn(aResponse().withStatus(200)));
  }

  @After
  public void tearDownTest() {
    File tmp = new File(testDataDir.getName()+"-data_processed_files_v2");
    if (tmp.isFile())
      tmp.delete();
  }

  //@Ignore
  @Test
  public void testDnsLog() throws Exception {

    /*
    SimpleDateFormat syslogtimestamp = new SimpleDateFormat("MMM dd HH:mm:ss");
    SimpleDateFormat ts_w_dayOfWeek = new SimpleDateFormat("EEE");
    Random random = new Random(5150);
    OutputStreamWriter osw = null;
    try {
      osw = new OutputStreamWriter(new FileOutputStream(new File(testDataDir,"dns_log")), StandardCharsets.UTF_8);
      StringBuilder sb = new StringBuilder();

      for (int i=0; i < 30000; i++) {
        Date logdate = new Date(1449619200000L + (i*1000));
        sb.append(syslogtimestamp.format(logdate));
        sb.append(" dns-server.example.net MSWinEventLog");
        sb.append("|");
        sb.append("1");
        sb.append("|");
        sb.append("Application");
        sb.append("|");
        sb.append(i+10000);
        sb.append("|");
        sb.append(ts_w_dayOfWeek.format(logdate));
        sb.append(" ");
        sb.append(syslogtimestamp.format(logdate));
        sb.append(" 2015");
        sb.append("|");
        sb.append("3");
        sb.append("|");
        sb.append("Lucent DNS Service");
        sb.append("|");
        sb.append("N/A");
        sb.append("|");
        sb.append("N/A");
        sb.append("|");
        sb.append("Information");
        sb.append("|");
        sb.append("a" + random.nextInt(1000) + ".example.com");
        sb.append("|");
        sb.append("None");
        sb.append("|");
        sb.append("|");
        sb.append("client " + random.nextInt(256) + "." + random.nextInt(256) + "." + random.nextInt(256) + "." + random.nextInt(256));
        sb.append("#").append(random.nextInt(100000));
        sb.append(": query: ").append("a"+random.nextInt(100)+".lucidworks.com");
        sb.append(" IN ").append(random.nextInt(256) + "." + random.nextInt(256) + "." + random.nextInt(256) + "." + random.nextInt(256));
        sb.append("\n");
        osw.write(sb.toString());
        sb.setLength(0);
      }
      osw.flush();
    } finally {
      if (osw != null) {
        try {
          osw.close();
        } catch (Exception ignore){}
      }
    }
    */

    String[] args = new String[] {
            "-fusionUser",fusionUser,
            "-fusionPass",fusionPass,
            "-fusionRealm",fusionRealm,
            "-fusion",fusionEndpoints,
            "-dir",testDataDir.getAbsolutePath(),
            "-match", "^dns_log$",
            "-lineParserClass", DnsLogLineParser.class.getName()
    };
    CommandLine cli = LogIndexer.processCommandLineArgs(args);
    LogIndexer logIndexer = new LogIndexer();
    logIndexer.run(cli);
  }

  //@Ignore
  @Test
  public void testHttpdAccessLog() throws Exception {

    String[] args = new String[] {
            "-fusionUser",fusionUser,
            "-fusionPass",fusionPass,
            "-fusionRealm",fusionRealm,
            "-fusion",fusionEndpoints,
            "-dir",testDataDir.getAbsolutePath()
    };
    CommandLine cli = LogIndexer.processCommandLineArgs(args);
    LogIndexer logIndexer = new LogIndexer();
    logIndexer.run(cli);

    // do some asserts based on the httpd_access.log
    assertTrue(logIndexer.parsedFiles.getCount() == 1);
    assertTrue(logIndexer.docCounter.getCount() == 1536);
    assertTrue(logIndexer.totalSkippedLines.getCount() == 0);

    File tmp = new File("test-data_processed_files_v2");
    if (tmp.isFile())
      tmp.delete();

    LogIndexer.log.info("\n\n");

    args = new String[] {
            "-fusionUser",fusionUser,
            "-fusionPass",fusionPass,
            "-fusionRealm",fusionRealm,
            "-fusion",fusionEndpoints,
            "-dir",testDataDir.getAbsolutePath(),
            "-match","*.gz"
    };
    cli = LogIndexer.processCommandLineArgs(args);
    logIndexer = new LogIndexer();
    logIndexer.run(cli);

    // do some asserts based on the httpd_access.log
    assertTrue("Expected 1 files parsed, but found "+logIndexer.parsedFiles.getCount(), logIndexer.parsedFiles.getCount() == 1);
    assertTrue(logIndexer.docCounter.getCount() == 1536);
    assertTrue(logIndexer.totalSkippedLines.getCount() == 0);

    tmp = new File("test-data_processed_files_v2");
    if (tmp.isFile())
      tmp.delete();

    /*
    // TODO: need a way to kill the watching log indexer

    // test watch and tail support
    final File watchDir = new File("target/watchdir");
    if (watchDir.isDirectory())
      watchDir.delete();
    watchDir.mkdirs();
    if (!watchDir.isDirectory())
      fail("Failed to create test directory "+watchDir.getAbsolutePath());

    final File fileToCopy = new File(testDataDir,"httpd_access.log");
    Thread copyTestLogIntoWatchDirectoryThread = new Thread() {
      public void run() {
        try {
          Thread.sleep(10000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        File destFile = new File(watchDir, "httpd_access.log");
        try {
          FileUtils.copyFile(fileToCopy, destFile);
        } catch (IOException e) {
          fail("Failed to copy file due to: "+e);
        }
      }
    };
    copyTestLogIntoWatchDirectoryThread.start();

    args = new String[] {
            "-fusionUser",fusionUser,
            "-fusionPass",fusionPass,
            "-fusionRealm",fusionRealm,
            "-fusion",fusionEndpoints,
            "-dir",watchDir.getAbsolutePath(),
            "-watch", "-tail",
            "-tailerReaperThresholdMs", "10000"
    };
    cli = LogIndexer.processCommandLineArgs(args);
    logIndexer = new LogIndexer();
    logIndexer.run(cli);
    */
  }
}
