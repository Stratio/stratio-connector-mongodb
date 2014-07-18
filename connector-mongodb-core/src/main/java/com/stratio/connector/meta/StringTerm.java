package com.stratio.connector.meta;


import com.stratio.meta.common.statements.structures.terms.Term;

public class StringTerm extends Term<String> {

	private static final long serialVersionUID = 7097178218828822795L;

	public StringTerm(String term) {
		super(String.class, term);
	}

	public StringTerm(Term<String> term) {
		super(String.class, term.getTermValue());
	}
}

