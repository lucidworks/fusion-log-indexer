package parsers;

import oi.thekraken.grok.api.Grok;
import oi.thekraken.grok.api.Match;

import java.util.Map;

public class DnsLogLineParser implements CustomLogLineParser {
  public Map<String, Object> parseLine(String fileName, int lineNum, String line, Grok grok) throws Exception {
    line = line.replace('|',' ');
    Match gm = grok.match(line);
    gm.captures();
    return gm.isNull() ? null : gm.toMap();
  }
}

