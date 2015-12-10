package parsers;

import java.text.ParseException;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

/**
 * Example of a custom dns log line parser which still leverages grok for parsing.
 */
public class DnsLogLineParser extends GrokLogLineParser {

  @Override
  public void init(Properties config) throws Exception {
    // override with a specific grok pattern
    config.setProperty("grokPattern",
            "%{SYSLOGTIMESTAMP} %{HOSTNAME} MSWinEventLog 1 Application %{NUMBER} %{DAY} %{SYSLOGTIMESTAMP:logdate} %{YEAR} 3 Lucent DNS Service N/A N/A Information %{HOSTNAME:host} None  client %{IP:client}#%{NUMBER}: query: (?<query>\\b((xn--)?[a-z0-9\\w]+(-[a-z0-9]+)*\\.)+[a-z]{2,}\\b) IN %{WORD:type}");

    config.setProperty(ISO_8601_TIMESTAMP_FIELD_PROP, "timestamp");
    config.setProperty(LOG_DATE_FIELD_PROP, "logdate");
    config.setProperty(LOG_DATE_FORMAT_PROP, "MMM dd HH:mm:ss yyyy");

    super.init(config);
  }

  @Override
  public Map<String, Object> parseLine(String fileName, int lineNum, String line) throws Exception {
    line = line.replace('|',' ');
    return super.parseLine(fileName, lineNum, line);
  }

  @Override
  protected Date getLogDate(Map<String,Object> grokMap) throws ParseException {
    // parse the logdate + YEAR into a java Date and then the framework will set an ISO-8601 timestamp on the doc
    Object dateFieldValue = grokMap.get("logdate");
    return (dateFieldValue != null) ? df.get().parse(dateFieldValue+" "+grokMap.get("YEAR")) : null;
  }
}

