package parsers;

import java.util.Map;

public class MultilinePart {
  public enum MultilineState {
    START, CONT, END, SKIP;
  }

  public final MultilineState state;
  public final Map<String,Object> part;

  public MultilinePart(MultilineState state, Map<String,Object> part) {
    this.state = state;
    this.part = part;
  }
}
