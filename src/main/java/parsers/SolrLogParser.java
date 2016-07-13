package parsers;

import oi.thekraken.grok.api.Grok;
import oi.thekraken.grok.api.Match;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

import static parsers.MultilinePart.MultilineState.*;

/**
 * This class uses Grok to parse log4j log entries in a Solr log with support for detecting multiple line
 * log entries, such as stack traces. Users need to supply a grok pattern that matches the Log4J config used
 * to create the Solr log, such as:
 * <pre>
 *   %d{yyyy-MM-dd HH:mm:ss.SSS} %-5p (%t) [%X{collection} %X{shard} %X{replica} %X{core}] %c{1.} %m%n
 * </pre>
 * would be:
 * <pre>
 *   SOLR_6_LOG4J %{TIMESTAMP_ISO8601:logdate} %{LOGLEVEL:level_s} \(%{DATA:thread_s}\) \[(?:%{DATA:mdc_s}| )\] %{DATA:category_s} %{JAVALOGMESSAGE:logmessage}
 * </pre>
 * You can also setup to parse Solr query requests into more specific fields using an additional grok pattern for
 * Solr request messages, such as:
 * <pre>
 *   SOLR_6_REQUEST \[%{DATA:core_node_name_s}\] webapp=\/%{WORD:webapp_s} path=\/%{DATA:path_s} params=\{%{GREEDYDATA:params_s}\} hits=%{NUMBER:hits_i} status=%{NUMBER:status_i} QTime=%{NUMBER:qtime_i}
 * </pre>
 */
public class SolrLogParser extends GrokLogLineParser implements MultilineParser {

  public static Logger log = LoggerFactory.getLogger(SolrLogParser.class);

  public static final String DEFAULT_LOG_MESSAGE_FIELD = "message_txt_en";

  // used to build up multi-line messages per thread
  protected ThreadLocal<StringBuilder> msgBuilder = new ThreadLocal<StringBuilder>() {
    protected StringBuilder initialValue() {
      return new StringBuilder();
    }
  };

  /*
  You'll need to pass a configuration properties file in to configure this parser, such as:

  logMessageFieldName=message_txt_en

  grokPatternFile=patterns/grok-patterns
  grokPattern=%{SOLR_6_LOG4J}
  iso8601TimestampFieldName=timestamp_tdt

  # set in the grok pattern
  dateFieldName=logdate
  dateFieldFormat=yyyy-MM-dd HH:mm:ss.SSS

  # for parsing out additional fields from query requests
  solrRequestGrokPattern=%{SOLR_6_REQUEST}

  */

  protected String logMessageFieldName;
  protected Grok requestGrok;

  public void init(Properties config) throws Exception {
    super.init(config);

    logMessageFieldName = config.getProperty("logMessageFieldName", DEFAULT_LOG_MESSAGE_FIELD);

    // setup another grok for parsing request log entries into more fine-grained fields
    Properties requestGrokConfig = (Properties)config.clone();
    requestGrokConfig.setProperty("grokPattern", config.getProperty("solrRequestGrokPattern", "%{SOLR_6_REQUEST}"));
    requestGrok = setupGrok(requestGrokConfig);
  }

  @Override
  public MultilinePart parseNextPart(String fileName, int lineNum, String line) throws Exception {

    line = line.replaceAll("\\s+", " "); // collapse all whitespace to 1 space

    Match gm = grok.match(line);
    gm.captures();

    // test if this line matched the grok pattern, if not, we'll treat it as a continuation of a previous log entry
    if (gm.isNull()) {
      // treat a non-match as a continuation of the previous log entry, like a stacktrace
      StringBuilder sb = msgBuilder.get();
      sb.append(" ").append(line);
      return new MultilinePart(CONT, Collections.singletonMap(logMessageFieldName, (Object)sb.toString()));
    }

    // the line matches our grok pattern
    Map<String,Object> grokMap = gm.toMap();

    // set the timestamp field in a format that solr likes
    if (timestampFieldName != null) {
      Date timestamp = getLogDate(grokMap);
      if (timestamp != null) {
        grokMap.put(timestampFieldName, iso8601.get().format(timestamp));
        grokMap.remove(dateFieldName); // not needed after conversion
      }
    }

    // mdc is sometimes just a space, so drop it in that case
    String mdc = (String)grokMap.get("mdc");
    if (mdc != null && mdc.trim().isEmpty()) {
      grokMap.remove("mdc");
    }

    // setup to handle multi-lie log messages (we use a ThreadLocal to append additional lines as we see them)
    StringBuilder sb = msgBuilder.get();
    sb.setLength(0); // clear the buffer that captures multiple lines when we start a new entry

    String logmessage = (String)grokMap.get("logmessage");
    if (logmessage != null) {
      logmessage = logmessage.trim();
      if (!logmessage.isEmpty())
        sb.append(logmessage);
      grokMap.put(logMessageFieldName, sb.toString());
      grokMap.remove("logmessage");
    }

    return new MultilinePart(START, pruneEmpty(grokMap));
  }

  @Override
  public void afterAllLinesRead(Map<String, Object> mutable) {
    mutable.remove("SECOND");
    mutable.remove("MONTHNUM");
    mutable.remove("YEAR");
    mutable.remove("MONTHDAY");
    String pat = grok.getOriginalGrokPattern();
    mutable.remove(pat.substring(2, pat.length() - 1));

    // try to break out the mdc context if available
    String mdc = (String)mutable.get("mdc_s");
    if (mdc != null)
      unpackMdcInfo(mdc, mutable);

    // if this message looks like a request message (such as a query), try to parse out the additional fields
    String category = (String)mutable.get("category_s");
    String message = (String)mutable.get(logMessageFieldName);
    if ("o.a.s.c.S.Request".equals(category) && message != null) {
      // see if this request line matches the Solr request format to parse out additional fields like query params
      Match requestMatch = requestGrok.match(message.replaceAll("\\s+"," ").trim());
      requestMatch.captures();
      if (!requestMatch.isNull()) {
        Map<String,Object> requestMap = requestMatch.toMap();
        requestMap.remove("logmessage");
        requestMap.remove("BASE10NUM");
        String rpat = requestGrok.getOriginalGrokPattern();
        requestMap.remove(rpat.substring(2,rpat.length()-1));
        mutable.putAll(requestMap);
      }
    }
  }

  protected void unpackMdcInfo(String mdc, Map<String,Object> mutable) {
    for (String f : mdc.split("\\s+")) {
      String[] pair = f.split(":");
      if (pair.length != 2)
        continue;

      String k = pair[0];
      String v = pair[1];
      if ("c".equals(k)) {
        mutable.put("collection_s", v);
      } else if ("s".equals(k)) {
        mutable.put("shard_s", v);
      } else if ("r".equals(k)) {
        mutable.put("replica_s", v);
      } else if ("x".equals(k)) {
        mutable.put("core_s", v);
      }
    }
  }
}