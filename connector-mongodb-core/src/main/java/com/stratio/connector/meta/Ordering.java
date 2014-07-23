package com.stratio.connector.meta;

import com.stratio.meta.common.statements.structures.selectors.SelectorIdentifier;

import java.io.Serializable;

public class Ordering implements Serializable {

  private static final long serialVersionUID = -5118851402738503002L;

  private SelectorIdentifier selectorIdentifier;
  private boolean dirInc;
  private OrderDirection orderDir;

  public Ordering(String identifier, boolean dirInc, OrderDirection orderDir) {
    this.selectorIdentifier = new SelectorIdentifier(identifier);
    this.dirInc = dirInc;
    this.orderDir = orderDir;
  }

  public Ordering(String identifier) {
    this(identifier, false, null);
  }

  public SelectorIdentifier getSelectorIdentifier() {
    return selectorIdentifier;
  }

  public void setSelectorIdentifier(SelectorIdentifier selectorIdentifier) {
    this.selectorIdentifier = selectorIdentifier;
  }

  public boolean isDirInc() {
    return dirInc;
  }

  public void setDirInc(boolean dirInc) {
    this.dirInc = dirInc;
  }

  public OrderDirection getOrderDir() {
    return orderDir;
  }

  public void setOrderDir(OrderDirection orderDir) {
    this.dirInc = true;
    this.orderDir = orderDir;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(selectorIdentifier.toString());
    if (dirInc) {
      sb.append(" ").append(orderDir);
    }
    return sb.toString();
  }

}