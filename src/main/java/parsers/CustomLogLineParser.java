package parsers;

import java.util.Map;

import oi.thekraken.grok.api.Grok;

public interface CustomLogLineParser {
  Map<String,Object> parseLine(String fileName, int lineNum, String line, Grok grok) throws Exception;
}
