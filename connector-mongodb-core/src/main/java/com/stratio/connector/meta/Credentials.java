package com.stratio.connector.meta;

public class Credentials {
String user;
String pass;
public Credentials(String u, String p){
	user=u;
	pass=p;
}
public String getUser() {
	return user;
}
public String getPass() {
	return pass;
}

}
