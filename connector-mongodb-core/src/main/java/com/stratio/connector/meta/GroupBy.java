package com.stratio.connector.meta;

import com.stratio.meta.common.logicalplan.LogicalStep;
import com.stratio.meta.common.statements.structures.selectors.SelectorIdentifier;

import java.io.Serializable;


public class GroupBy  extends LogicalStep implements Serializable{

  private static final long serialVersionUID = 1946514142415876581L;

  private SelectorIdentifier selectorIdentifier;

  public GroupBy(String identifier) {
    this.selectorIdentifier = new SelectorIdentifier(identifier);
  }

  public SelectorIdentifier getSelectorIdentifier() {
    return selectorIdentifier;
  }

  public void setSelectorIdentifier(SelectorIdentifier selectorIdentifier) {
    this.selectorIdentifier = selectorIdentifier;
  }

  @Override
  public String toString() {

    return selectorIdentifier.toString();
  }
}
