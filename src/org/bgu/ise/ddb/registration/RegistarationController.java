/**
 * 
 */
package org.bgu.ise.ddb.registration;



import java.io.IOException;
import java.sql.Date;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.TimeZone;


import javax.servlet.http.HttpServletResponse;

import org.bgu.ise.ddb.ParentController;
import org.bgu.ise.ddb.User;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import javafx.util.Duration;

import org.bson.Document; 

/**
 * 
 *
 */
@RestController
@RequestMapping(value = "/registration")
public class RegistarationController extends ParentController{
	
	
	/**
	 * The function checks if the username exist,
	 * in case of positive answer HttpStatus in HttpServletResponse should be set to HttpStatus.CONFLICT,
	 * else insert the user to the system  and set to HttpStatus in HttpServletResponse HttpStatus.OK
	 * @param username
	 * @param password
	 * @param firstName
	 * @param lastName
	 * @param response
	 */
	@RequestMapping(value = "register_new_customer", method={RequestMethod.POST})
	public void registerNewUser(@RequestParam("username") String username,
			@RequestParam("password")    String password,
			@RequestParam("firstName")   String firstName,
			@RequestParam("lastName")  String lastName,
			HttpServletResponse response){
		System.out.println(username+" "+password+" "+lastName+" "+firstName);
		
		//:TODO your implementation
		HttpStatus status;
		try {
			
			//check if the users exists
			if (!isExistUser(username)) 
			{
				//connect to datebase
				MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
				MongoDatabase db = mongoClient.getDatabase("projectDB");	
				MongoCollection<Document> collection = db.getCollection("Users");
				
				// get the current date			
				java.util.Date currentDate=Calendar.getInstance().getTime();
				
				//create new documnet	
				Document document = new Document() 
					      .append("username", username)
					      .append("firstName", firstName) 
					      .append("lastName", lastName) 
					      .append("password", password)
					      .append("currentDate", currentDate);
				collection.insertOne(document); 
				mongoClient.close();				
				status = HttpStatus.OK;
			}
			
			else
			{ 
				status = HttpStatus.CONFLICT;					
			}
			
			response.setStatus(status.value());		
		} 
	
		catch (MongoException e) 
		{
			System.out.println(e);
		}				
		catch (IOException e) 
		{
			e.printStackTrace();
		}
		
	}
	
	/**
	 * The function returns true if the received username exist in the system otherwise false
	 * @param username
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "is_exist_user", method={RequestMethod.GET})
	public boolean isExistUser(@RequestParam("username") String username) throws IOException{
		System.out.println(username);
		boolean result = false;
		//:TODO your implementation
		
		try {
			MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
			MongoDatabase db = mongoClient.getDatabase("projectDB");	
			MongoCollection<Document> collection = db.getCollection("Users");
			
			//update the userNAme
			BasicDBObject searchUserQuery = new BasicDBObject();
			searchUserQuery.put("username", username);
			FindIterable<Document> iterDoc = collection.find(searchUserQuery);
		    int i = 1;      
		    Iterator<Document> it = iterDoc.iterator();  // Getting the iterator
		    //move all over the collection
		    while (it.hasNext()) {  
		   	  result = true;   
		   	  i++; 
		   	  break;
		    }				
			mongoClient.close();
		} 
		
		catch (MongoException e) 
		{	
			System.out.println(e);
		} 
		catch (NoSuchElementException e) {
			System.out.println(e);
		}
	 
		return result;
		
	}
	
	/**
	 * The function returns true if the received username and password match a system storage entry, otherwise false
	 * @param username
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "validate_user", method={RequestMethod.POST})
	public boolean validateUser(@RequestParam("username") String username,
			@RequestParam("password")    String password) throws IOException{
		System.out.println(username+" "+password);
		boolean result = false;
		//:TODO your implementation
		try {
			//connect to DataBase
			MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
			MongoDatabase db = mongoClient.getDatabase("projectDB");	
			MongoCollection<Document> collection = db.getCollection("Users");
			
			//update the userName and password
			BasicDBObject searchUserQuery = new BasicDBObject();
			searchUserQuery.put("username", username);
			searchUserQuery.put("password", password);
			//iterator
			FindIterable<Document> iterDoc = collection.find(searchUserQuery);
		    int i = 1;      
		    Iterator<Document> it = iterDoc.iterator();  // Getting the iterator 
		    while (it.hasNext()) {  
		   	  result = true;  
		   	  i++; 
		   	  break;
		    }				
			mongoClient.close();
		} 
		
		catch (MongoException e) 
		{	
			System.out.println(e);
		} 
		catch (NoSuchElementException e) {
			System.out.println(e);
		}
	
		return result;
		
	}
	
	/**
	 * The function retrieves number of the registered users in the past n days
	 * @param days
	 * @return
	 * @throws IOException
	 */
	@RequestMapping(value = "get_number_of_registred_users", method={RequestMethod.GET})
	public int getNumberOfRegistredUsers(@RequestParam("days") int days) throws IOException{
		System.out.println(days+"");
		int result = 0;
		//:TODO your implementation
				
		try {
			//connect to dateBase
			MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
			MongoDatabase db = mongoClient.getDatabase("projectDB");	
			MongoCollection<Document> collection = db.getCollection("Users");
			
			// get the current date
			Calendar cal = Calendar.getInstance();
			java.util.Date DateToday=Calendar.getInstance().getTime();
			System.out.println("DateToday: "+DateToday);
			
			cal.setTime(DateToday);
			cal.add(Calendar.DATE, -days); // remove the days and go to the corret date
			
			java.util.Date dateBeforeDays=cal.getTime();
			//java.util.Date dateBeforeDays=cal.getInstance().getTime();
			System.out.println("dateBeforeDays: "+dateBeforeDays);
		
			FindIterable<Document> iter = collection.find(); // find the collection
			Iterator<Document> it = iter.iterator();
		    //move all the dates
		    while (it.hasNext()) { 	
		    	//System.out.println("DateOfUser");
		    	java.util.Date d = (java.util.Date) it.next().get("currentDate");
		    	//System.out.println("DateOfUser"+d);
		    	//check if the dates are between the days
		    	if (d.after(dateBeforeDays) && d.before(DateToday))
		    	{
		    		//add to cound
		    		result++;
		    	}
		    	//break;
		    
		    }		

			mongoClient.close();
		} 
		catch (MongoException e) 
		{
				System.out.println(e);
		} catch(java.util.NoSuchElementException e) {
			System.out.println(e);
		}
	
		return result;
		
	}
	
	/**
	 * The function retrieves all the users
	 * @return
	 */
	@RequestMapping(value = "get_all_users",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(User.class)
	public  User[] getAllUsers(){
		//:TODO your implementation
		List<User> usersList = new ArrayList<User>();
		try {
			//connect to DateBase
			MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
			MongoDatabase db = mongoClient.getDatabase("projectDB");	
			MongoCollection<Document> collection = db.getCollection("Users");
			//get the collection
			FindIterable<Document> iterDoc = collection.find(); 	
			 int i = 1;      
			 Iterator<Document> it = iterDoc.iterator();  // Getting the iterator 
			 // add the user to lists
			 while (it.hasNext()) 
			 {  
				 Document d = (Document) it.next();
			     usersList.add(new User((String) d.get("username"), (String) (d).get("firstName"), (String)(d).get("lastName")));
			     i++; 
			 }		
			    
			mongoClient.close();
		} 
		catch (MongoException e) 
		{
			System.out.println(e);
		} 
		catch (NoSuchElementException e) {
			System.out.println(e);
		}
		
		int size= usersList.size();
		return usersList.toArray(new User[size]); 
	
	}

}
