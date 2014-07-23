package com.stratio.connector.meta;



import com.stratio.meta.common.statements.structures.terms.Term;

public class StringTerm extends Term<String> {

  private static final long serialVersionUID = 4470491967411363431L;

  private boolean quotedLiteral = false;

  public StringTerm(String term, boolean quotedLiteral) {
    super(String.class, term);
    this.type = TYPE_TERM;
    this.quotedLiteral = quotedLiteral;
  }

  public StringTerm(String term) {
    this(term, false);
  }

  public boolean isQuotedLiteral() {
    return quotedLiteral;
  }

  /** {@inheritDoc} */
  @Override
  public String toString() {
    if (this.isQuotedLiteral()) {
      return "'" + value + "'";
    } else {
      return value;
    }
  }
}

