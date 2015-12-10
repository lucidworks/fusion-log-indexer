package parsers;

import java.util.Map;
import java.util.Properties;

/**
 * Defines a basic interface to a class that knows how to parse log lines.
 */
public interface LogLineParser {
  void init(Properties config) throws Exception;

  /**
   * This method must be thread safe as it will be invoked by many threads concurrently.
   */
  Map<String,Object> parseLine(String fileName, int lineNum, String line) throws Exception;
}
