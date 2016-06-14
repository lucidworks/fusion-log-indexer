package parsers;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

import oi.thekraken.grok.api.Grok;
import oi.thekraken.grok.api.Match;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrokLogLineParser implements LogLineParser {

  public static Logger log = LoggerFactory.getLogger(GrokLogLineParser.class);

  public static final String ISO_8601_TIMESTAMP_FIELD_PROP = "iso8601TimestampFieldName";
  public static final String LOG_DATE_FIELD_PROP = "dateFieldName";
  public static final String LOG_DATE_FORMAT_PROP = "dateFieldFormat";
  public static final String UNMATCHED_LINE_PROP = "unmatchedLineName";

  protected Grok grok;
  protected String grokPattern;
  protected String dateFieldName = null;
  protected String dateFieldFormat = null;
  protected String timestampFieldName = null;
  protected String unmatchedLineName = null;
  protected ThreadLocal<SimpleDateFormat> df = null;
  protected ThreadLocal<SimpleDateFormat> iso8601 = null;

  public void init(Properties config) throws Exception {
    // setup grok
    grok = new Grok();
    String grokPatternDir = config.getProperty("grokPatternDir", "src/main/resources/patterns");
    //loop over and add all the pattern files in the directory
    if (grokPatternDir != null) {
      File patternDir = new File(grokPatternDir);
      if (patternDir.exists() && patternDir.isDirectory()){
        File[] patterns = patternDir.listFiles();
        if (patterns != null && patterns.length > 0) {
          for (File pattern : patterns) {
            log.info("Loading pattern {} from {}", pattern, patternDir);
            grok.addPatternFromFile(pattern.getAbsolutePath());
          }
        } else {
          log.warn("grokPatternDir specified, but no patterns present");
        }
      } else {
        log.error("Unable to find grokPatternDir: " + grokPatternDir);
      }
    }

    unmatchedLineName = config.getProperty(UNMATCHED_LINE_PROP);

    String grokPatternFile = config.getProperty("grokPatternFile", "patterns/grok-patterns");
    if (grokPatternFile.startsWith("patterns/")) {
      // load built-in from classpath

      InputStreamReader isr = null;
      try {
        InputStream in = getClass().getClassLoader().getResourceAsStream(grokPatternFile);
        if (in == null)
          throw new FileNotFoundException(grokPatternFile+" not found on classpath!");

        isr = new InputStreamReader(in, StandardCharsets.UTF_8);
        grok.addPatternFromReader(isr);
      } finally {
        if (isr != null) {
          try {
            isr.close();
          } catch (Exception ignore){}
        }
      }
    } else {
      // initialize from an external file
      grok.addPatternFromFile(grokPatternFile);
    }

    grokPattern = config.getProperty("grokPattern");
    if (grokPattern == null || grokPattern.isEmpty())
      throw new IllegalArgumentException("Must specify a grokPattern!");

    grok.compile(grokPattern);

    // optionally, we can set the iso 8601 timestamp field on each log message by parsing a custom date in the log
    timestampFieldName = config.getProperty(ISO_8601_TIMESTAMP_FIELD_PROP);
    if (timestampFieldName != null) {
      dateFieldName = config.getProperty(LOG_DATE_FIELD_PROP);
      dateFieldFormat = config.getProperty(LOG_DATE_FORMAT_PROP);
      if (dateFieldFormat != null) {
        df = new ThreadLocal<SimpleDateFormat>() {
          @Override
          protected SimpleDateFormat initialValue() {
            SimpleDateFormat sdf = new SimpleDateFormat(dateFieldFormat);
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
            return sdf;
          }
        };
        iso8601 = new ThreadLocal<SimpleDateFormat>() {
          @Override
          protected SimpleDateFormat initialValue() {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
            sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
            return sdf;
          }
        };
      }
      log.info("Configured "+getClass().getSimpleName()+" to set the "+ timestampFieldName+
              " field to an ISO-8601 timestamp by parsing "+dateFieldName+" using format: "+dateFieldFormat);
    }
  }

  public Map<String, Object> parseLine(String fileName, int lineNum, String line) throws Exception {
    if (line == null || line.isEmpty())
      return null;

    Match gm = grok.match(line);
    gm.captures();

    if (gm.isNull()){
      if (unmatchedLineName != null && unmatchedLineName.isEmpty() == false){
        return Collections.<String, Object>singletonMap(unmatchedLineName, line);
      } else {
        return null;
      }
    }

    Map<String,Object> grokMap = gm.toMap();

    // add the ISO-8601 timestamp field if was requested in the config
    if (timestampFieldName != null) {
      Date timestamp = getLogDate(grokMap);
      if (timestamp != null) {
        grokMap.put(timestampFieldName, iso8601.get().format(timestamp));
      }
    }

    return grokMap;
  }

  protected Date getLogDate(Map<String,Object> grokMap) throws ParseException {
    Date timestamp = null;
    if (dateFieldName != null) {
      Object dateFieldValue = grokMap.get(dateFieldName);
      if (dateFieldValue != null) {
        timestamp = df.get().parse((String)dateFieldValue);
      }
    }
    return timestamp;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName()+": "+grokPattern;
  }
}
