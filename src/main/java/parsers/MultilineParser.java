package parsers;

public interface MultilineParser {
  MultilinePart parseNextPart(String fileName, int lineNum, String line) throws Exception;
}
