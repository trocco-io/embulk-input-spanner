package org.embulk.input.spanner.extension.embulk;

import java.util.ArrayList;
import java.util.List;
import org.embulk.spi.Page;
import org.embulk.spi.PageOutput;

// https://github.com/embulk/embulk/blob/ae81178/embulk-junit4/src/main/java/org/embulk/spi/TestPageBuilderReader.java
public class MockPageOutput implements PageOutput {
  public List<Page> pages;

  public MockPageOutput() {
    this.pages = new ArrayList<>();
  }

  @Override
  public void add(Page page) {
    pages.add(page);
  }

  @Override
  public void finish() {}

  @Override
  public void close() {}
}
