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

import com.stratio.meta.common.logicalplan.LogicalStep;
import com.stratio.meta.common.statements.structures.selectors.SelectorIdentifier;


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
