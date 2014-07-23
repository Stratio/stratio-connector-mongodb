package com.stratio.connector.meta;

/*
* Stratio Meta
*
* Copyright (c) 2014, Stratio, All rights reserved.
*
* This library is free software; you can redistribute it and/or
* modify it under the terms of the GNU Lesser General Public
* License as published by the Free Software Foundation; either
* version 3.0 of the License, or (at your option) any later version.
*
* This library is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
* Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public
* License along with this library.
*/



import com.stratio.meta.common.statements.structures.terms.Term;

public class BooleanTerm extends Term<Boolean> {

private static final long serialVersionUID = 2872212148572680680L;

/**
* Class constructor.
*
* @param term
* The string representation of a Boolean value.
*/
public BooleanTerm(String term) {
super(Boolean.class, Boolean.valueOf(term));
}
}
