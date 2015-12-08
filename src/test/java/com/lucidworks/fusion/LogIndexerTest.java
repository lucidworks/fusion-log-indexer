package com.lucidworks.fusion;

import org.apache.commons.cli.CommandLine;
import org.junit.Rule;
import org.junit.Test;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.github.tomakehurst.wiremock.junit.WireMockRule;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

public class LogIndexerTest {

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(8976);

  @Test
  public void testHttpdAccessLog() throws Exception {

    String fusionUser = "admin";
    String fusionPass = "password123";
    String fusionRealm = "native";
    String fusionHost = "localhost";
    int fusionApiPort = wireMockRule.port();

    String fusionHostAndPort = "http://" + fusionHost + ":" + fusionApiPort;
    String fusionPipelineEndpoint = "/api/apollo/index-pipelines/logs-default/collections/logs";
    String fusionEndpoints = fusionHostAndPort + fusionPipelineEndpoint;

    // mock out the Fusion indexing pipeline endpoint and the session API endpoint
    stubFor(post(urlEqualTo(fusionPipelineEndpoint)).willReturn(aResponse().withStatus(200)));
    stubFor(post(urlEqualTo("/api/session?realmName=" + fusionRealm)).willReturn(aResponse().withStatus(200)));

    File testDataDir = new File("src/test/test-data");
    if (!testDataDir.isDirectory())
      fail("Request test data directory "+testDataDir.getAbsolutePath()+" not found!");

    String[] args = new String[] {
            "-fusionUser",fusionUser,
            "-fusionPass",fusionPass,
            "-fusionRealm",fusionRealm,
            "-fusion",fusionEndpoints,
            "-dir",testDataDir.getAbsolutePath(),
            "-grokPatternFile","patterns/grok-patterns",
            "-grokPattern","COMMONAPACHELOG"
    };
    CommandLine cli = LogIndexer.processCommandLineArgs(args);
    LogIndexer logIndexer = new LogIndexer();
    logIndexer.run(cli);

    // do some asserts based on the httpd_access.log
    assertTrue(logIndexer.parsedFiles.get() == 1);
    assertTrue(logIndexer.docCounter.get() == 1536);
    assertTrue(logIndexer.totalSkippedLines.get() == 0);

    File tmp = new File("test-data_processed_files_v2");
    if (tmp.isFile())
      tmp.delete();

    args = new String[] {
            "-fusionUser",fusionUser,
            "-fusionPass",fusionPass,
            "-fusionRealm",fusionRealm,
            "-fusion",fusionEndpoints,
            "-dir",testDataDir.getAbsolutePath(),
            "-grokPatternFile","patterns/grok-patterns",
            "-grokPattern","COMMONAPACHELOG",
            "-match","*.gz"
    };
    cli = LogIndexer.processCommandLineArgs(args);
    logIndexer = new LogIndexer();
    logIndexer.run(cli);

    // do some asserts based on the httpd_access.log
    assertTrue(logIndexer.parsedFiles.get() == 1);
    assertTrue(logIndexer.docCounter.get() == 1536);
    assertTrue(logIndexer.totalSkippedLines.get() == 0);

    tmp = new File("test-data_processed_files_v2");
    if (tmp.isFile())
      tmp.delete();

    /*

    TODO: need a way to kill the watching log indexer

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
            "-grokPatternFile","patterns/grok-patterns",
            "-grokPattern","COMMONAPACHELOG",
            "-tailerReaperThresholdMs", "10000"
    };
    cli = LogIndexer.processCommandLineArgs(args);
    logIndexer = new LogIndexer();
    logIndexer.run(cli);
    */
  }
}
