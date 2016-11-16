package fr.finaxys.client;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTablePool;

import fr.finaxys.dao.UsersDAO;
import fr.finaxys.model.User;

public class UsersTool {
	
	public static void main(String[] args) throws IOException {

		
		HTablePool pool = new HTablePool();
		UsersDAO dao = new UsersDAO(pool);
		
		
		//get 
		if ("get".equals(args[0])) {
			System.out.println("Getting user " + args[1]);
			User u = dao.getUser(args[1]);
			System.out.println(u);
		}
		
		//add 
		if ("add".equals(args[0])) {
			System.out.println("Adding user...");
			dao.addUser(args[1], args[2], args[3], args[4]);
			User u = dao.getUser(args[1]);
			System.out.println("Successfully added user " + u);
		}
		
		//list : scanning the table users 
		if ("list".equals(args[0])) {
			for (User u : dao.scanUsers()) {
				System.out.println(u);
			}
			
        // delete : removing specific row key for users table
			if ("delete".equals(args[0])) {
				System.out.println("deleting a specif row user...");
				dao.deleteUser(args[1]);
				System.out.println("Successfully removing the user"+args[1]);
			}
			
			
    	//TODO Update 
			
			
			
		}
		
		pool.closeTablePool(UsersDAO.TABLE_NAME);
	}
}
