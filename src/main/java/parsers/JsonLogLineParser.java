package parsers;

import java.util.Map;
import java.util.Properties;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonLogLineParser implements LogLineParser {

  protected ObjectMapper jsonMapper = new ObjectMapper();

  public void init(Properties config) throws Exception {
    jsonMapper = new ObjectMapper();
  }

  public Map<String, Object> parseLine(String fileName, int lineNum, String line) throws Exception {
    return jsonMapper.readValue(line, Map.class);
  }
}
