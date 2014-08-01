/**
* Copyright (C) 2014 Stratio (http://stratio.com)
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.stratio.connector.meta;

import java.io.Serializable;

import com.stratio.meta.common.statements.structures.selectors.SelectorIdentifier;

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