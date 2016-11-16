package fr.finaxys.model;

public abstract class User {

	public String user;
	public String name;
	public String email;
	public String password;

	
	public String toString() {
		return String.format("user %s, %s, %s ",user,name,email);
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getEmail() {
		return email;
	}

	public void setEmail(String email) {
		this.email = email;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

}
