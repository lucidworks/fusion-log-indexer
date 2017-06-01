package parsers;

import java.util.Map;

public class MultilinePart {
  public enum MultilineState {
    START, CONT, END, SKIP;
  }

  public final MultilineState state;
  public final Map<String,Object> part;
  public final int lineNum;

  public MultilinePart(MultilineState state, Map<String,Object> part, int lineNum) {
    this.state = state;
    this.part = part;
    this.lineNum = lineNum;
  }

  public String toString() {
    return state+" "+part;
  }
}
