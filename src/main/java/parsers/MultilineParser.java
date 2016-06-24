package parsers;

import java.util.Map;

public interface MultilineParser {
  MultilinePart parseNextPart(String fileName, int lineNum, String line) throws Exception;

  /**
   * Called after all lines have been read with the final map holding all parsed fields. Allows the custom parser
   * to do any post-processing before the map is converted into a PipelineDocument.
   *
   * @param mutable - You can make changes to this map as needed.
   */
  void afterAllLinesRead(Map<String,Object> mutable);
}
