package parsers;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class XmlMultilineParser implements LogLineParser, MultilineParser {

  private static final String fieldPfx = "<field name=\"";
  private static final String fieldSfx = "</field>";

  @Override
  public void init(Properties config) throws Exception {
    // no-op
  }

  @Override
  public Map<String, Object> parseLine(String fileName, int lineNum, String line) throws Exception {
    return Collections.EMPTY_MAP;
  }

  @Override
  public MultilinePart parseNextPart(String fileName, int lineNum, String line) throws Exception {
    MultilinePart part;
    if (line.startsWith("<doc>")) {
      part = new MultilinePart(MultilinePart.MultilineState.START, Collections.EMPTY_MAP, lineNum);
    } else if (line.startsWith("</doc>")) {
      part = new MultilinePart(MultilinePart.MultilineState.END, Collections.EMPTY_MAP, lineNum);
    } else {
      line = line.replace("name = \"", "name=\"");
      if (line.startsWith(fieldPfx) && line.endsWith(fieldSfx)) {
        String sub = line.substring(fieldPfx.length(), line.length()-fieldSfx.length());
        int at = sub.indexOf("\"");
        String key = sub.substring(0, at);
        Object val = sub.substring(at+2); // skip over >
        part = new MultilinePart(MultilinePart.MultilineState.CONT, Collections.singletonMap(key,val), lineNum);
      } else {
        part = new MultilinePart(MultilinePart.MultilineState.SKIP, Collections.EMPTY_MAP, lineNum);
      }
    }
    return part;
  }

  @Override
  public void afterAllLinesRead(Map<String, Object> mutable) {
    // TODO: do some post-processing after all lines have been read into the map
    // such as maybe combining separate date and time fields into a timestamp
  }
}
