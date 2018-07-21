/**
 * 
 */
package org.bgu.ise.ddb.history;

import java.net.UnknownHostException;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import javax.servlet.http.HttpServletResponse;

import org.bgu.ise.ddb.MediaItems;
import org.bgu.ise.ddb.ParentController;
import org.bgu.ise.ddb.User;
import org.bgu.ise.ddb.items.ItemsController;
import org.bgu.ise.ddb.registration.RegistarationController;
import org.bson.Document;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;


/**
 * 
 *
 */
@RestController
@RequestMapping(value = "/history")
public class HistoryController extends ParentController{
	
	
	
	/**
	 * The function inserts to the system storage triple(s)(username, title, timestamp). 
	 * The timestamp - in ms since 1970
	 * Advice: better to insert the history into two structures( tables) in order to extract it fast one with the key - username, another with the key - title
	 * @param username
	 * @param title
	 * @param response
	 */
	@RequestMapping(value = "insert_to_history", method={RequestMethod.GET})
	public void insertToHistory (@RequestParam("username")    String username,
			@RequestParam("title")   String title,
			HttpServletResponse response){
//		System.out.println(username+" "+title);
//		//:TODO your implementation
//		//
		try
		{
			RegistarationController registration = new RegistarationController();
			ItemsController items = new ItemsController();
			if (!registration.isExistUser(username) || !items.isItemExist(title))
			{
				HttpStatus status = HttpStatus.CONFLICT;
				response.setStatus(status.value());
				System.out.println("User or Item doesn't exist");
				return;
			}
				
			//connection
			MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
			MongoDatabase db = mongoClient.getDatabase("projectDB");	
			
			//create timeStamp
			long TimeStamp = Instant.now().toEpochMilli();
				
			//get collection of title history.
			MongoCollection<Document> collection_titles = db.getCollection("ItemHistory");
			//if the movie exists in the history
			if (getUsersByItem(title).length!=0)
			{			
				BasicDBList UserList_TimeStamp = new BasicDBList(); 	
				//create New user
				BasicDBObject New_User = new BasicDBObject(); 
				New_User.put("username", username);
				New_User.put("timestamp", TimeStamp);		
				UserList_TimeStamp.add(New_User); // add the new user to list
				
				//Sort the TimeStamp in Descending order.
				BasicDBObject TimesTstamp_ToInsert = new BasicDBObject();
				TimesTstamp_ToInsert.put("$each", UserList_TimeStamp);
				TimesTstamp_ToInsert.put("$sort", new BasicDBObject("timestamp", -1));
				
				// the new TimeStamp of movie
				BasicDBObject History_Title = new BasicDBObject();
				History_Title.put("title", title);
				BasicDBObject Users_TimeStamps = new BasicDBObject("users", TimesTstamp_ToInsert);
				//update on the collection of titles.
				collection_titles.updateOne(History_Title, new BasicDBObject("$push", Users_TimeStamps));								
			}
			
			//the Movie doesn't exists in history.
			else 
			{
				List <BasicDBObject> TimeStamp_user = new ArrayList<>();
				BasicDBObject user = new BasicDBObject();
				user.put("username", username);
				user.put("timestamp", TimeStamp);
				TimeStamp_user.add((BasicDBObject) user);
				
				//create new document
				Document document = new Document() 
					      .append("title", title)
					      .append("users", TimeStamp_user);
				collection_titles.insertOne(document); 	
			}
					
			
			//get collection of user history.
			MongoCollection<Document> collection_Users = db.getCollection("UserHistory");
			
			// the user name exists in historyUsername collection
			if (getHistoryByUser(username).length!=0)
			{	
				// create list of timestamps title
				BasicDBList TitleList_TimeStamp = new BasicDBList(); 
				//create new title
				BasicDBObject New_Title = new BasicDBObject(); 
				New_Title.put("title", title);
				New_Title.put("timestamp", TimeStamp);									
				TitleList_TimeStamp.add(New_Title);
				
				//Sort the TimeStamp in Descending order.
				BasicDBObject Timestamp_ToInsert = new BasicDBObject();
				Timestamp_ToInsert.put("$each", TitleList_TimeStamp);
				Timestamp_ToInsert.put("$sort", new BasicDBObject("timestamp", -1)); 
				
				// the new TimeStamp of user
				BasicDBObject History_User = new BasicDBObject();
				History_User.put("username", username);
				BasicDBObject Title_TimeStamps = new BasicDBObject("titles", Timestamp_ToInsert);
				//update on the collection of titles.
				collection_Users.updateOne(History_User, new BasicDBObject("$push", Title_TimeStamps));
			}
			
			//the user name doesnt exist in historyUsername collection
			else 
			{
				List <BasicDBObject> TimeStamp_of_titles = new ArrayList<>();
				BasicDBObject movie = new BasicDBObject();
				movie.put("title", title);
				movie.put("timestamp", TimeStamp);
				TimeStamp_of_titles.add((BasicDBObject) movie);
				
				//create new document
				Document document = new Document() 
					      .append("username", username)
					      .append("titles", TimeStamp_of_titles);
				collection_Users.insertOne(document); 	
			}		
			
			mongoClient.close();
			System.out.println("Success");
			HttpStatus status = HttpStatus.OK;
			response.setStatus(status.value());
		
		} 
		catch (NoSuchElementException e)
		{
			System.out.println(e);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}		
		
	}
	
	
	/**
	 * The function retrieves  users' history
	 * The function return array of pairs <title,viewtime> sorted by VIEWTIME in descending order
	 * @param username
	 * @return
	 */
	@RequestMapping(value = "get_history_by_users",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public  HistoryPair[] getHistoryByUser(@RequestParam("entity")    String username){
		//:TODO your implementation
		
		List<HistoryPair> historyList = new ArrayList<HistoryPair>();

		try {
			//connection
			MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
			MongoDatabase db = mongoClient.getDatabase("projectDB");	
			MongoCollection<Document> collection = db.getCollection("UserHistory");
	
			
			BasicDBObject userH = new BasicDBObject();
			userH.put( "username", username );
			FindIterable<Document> iter = collection.find(userH) ;   // making iterator 
			Iterator it = iter.iterator();  // Getting the iterator 
			
			if (it.hasNext())
			{
				Document History_User = (Document) it.next(); //getting the object	
				List<Document> UserTitels = (List<Document>) History_User.get("titles");
				for (Document title : UserTitels) 
				{
					Date time = new Date(title.getLong("timestamp"));
					historyList.add(new HistoryPair(title.getString("title"), time));
				}									
			}
			
			
									
			mongoClient.close();
			
		}
		catch (MongoException e) 
		{
			System.out.println(e);
		} 
		
		int HistorySize = historyList.size();
		return historyList.toArray(new HistoryPair[HistorySize]);
	
		//HistoryPair hp = new HistoryPair("aa", new Date());
		//System.out.println("ByUser "+hp);
		//return new HistoryPair[]{hp};
	}
	
	
	/**
	 * The function retrieves  items' history
	 * The function return array of pairs <username,viewtime> sorted by VIEWTIME in descending order
	 * @param title
	 * @return
	 */
	@RequestMapping(value = "get_history_by_items",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public  HistoryPair[] getHistoryByItems(@RequestParam("entity")    String title){
		//:TODO your implementation
		
		List<HistoryPair> historyList = new ArrayList<HistoryPair>();

		try {
			//connection
			MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
			MongoDatabase db = mongoClient.getDatabase("projectDB");	
			MongoCollection<Document> collection = db.getCollection("ItemHistory");
				
			BasicDBObject userH = new BasicDBObject();
			userH.put( "title", title );

			FindIterable<Document> iter = collection.find(userH) ;   // making iterator 
			Iterator it = iter.iterator();  // Getting the iterator 	
			
			if (it.hasNext())
			{
				Document userRecord = (Document) it.next(); //getting the object		
				
				List<Document> UserTitels = (List<Document>) userRecord.get("users");
				for (Document user : UserTitels) 
				{
					Date time = new Date(user.getLong("timestamp"));
					historyList.add(new HistoryPair(user.getString("username"), time));
				}
			}
											
			mongoClient.close();
			
		}
		catch (MongoException e) 
		{
			System.out.println(e);
		} 
		int HistorySize = historyList.size();
		return historyList.toArray(new HistoryPair[HistorySize]);
		
		
		//HistoryPair hp = new HistoryPair("aa", new Date());
		//System.out.println("ByItem "+hp);
		//return new HistoryPair[]{hp};
	}
	
	/**
	 * The function retrieves all the  users that have viewed the given item
	 * @param title
	 * @return
	 */
	@RequestMapping(value = "get_users_by_item",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(HistoryPair.class)
	public  User[] getUsersByItem(@RequestParam("title") String title){
		//:TODO your implementation
		
		List<User> ListOfUsers = new ArrayList<User>();
		try {
			//connection
			MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
			MongoDatabase db = mongoClient.getDatabase("projectDB");	
			MongoCollection<Document> collection = db.getCollection("ItemHistory");
			
			BasicDBObject userH = new BasicDBObject();
			userH.put( "title", title );

			FindIterable<Document> iter = collection.find(userH) ;   // making iterator 
			Iterator it = iter.iterator();  // Getting the iterator 	
			
			if (it.hasNext())
			{
				Document userRecord = (Document) it.next(); //getting the object		
				
				List<Document> Users = (List<Document>) userRecord.get("users");
				for (Document user : Users) 
				{
					String username = user.getString("username");
					ListOfUsers.add(new User(username,"aa","aa"));
				}
			}
											
			mongoClient.close();			
		} 
		
		catch (MongoException e) 
		{
			System.out.println(e);
		}
		
		int sizeList = ListOfUsers.size();
		return ListOfUsers.toArray(new User[sizeList]);
	
		//User hp = new User("aa","aa","aa");
	//	System.out.println(hp);
		//return new User[]{hp};		
		
	}
	
	/**
	 * The function calculates the similarity score using Jaccard similarity function:
	 *  sim(i,j) = |U(i) intersection U(j)|/|U(i) union U(j)|,
	 *  where U(i) is the set of usernames which exist in the history of the item i.
	 * @param title1
	 * @param title2
	 * @return
	 */
	@RequestMapping(value = "get_items_similarity",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	public double  getItemsSimilarity(@RequestParam("title1") String title1,
			@RequestParam("title2") String title2){
		//:TODO your implementation
		double ret = 0.0;
		User[] title_1 = getUsersByItem(title1);		
		User[] title_2 = getUsersByItem(title2);
		
		//no such this title
		if (title_1.length==0 || title_2.length==0)
			return ret;
		
		Set<String> title_1Set=new HashSet<String>() ; 
		for(int i=0;i<title_1.length;i++) {
			title_1Set.add(title_1[i].getUsername());
		}
		
		Set<String> title_2Set=new HashSet<String>() ; 
		for(int i=0;i<title_2.length;i++) {
			title_2Set.add(title_2[i].getUsername());
		}
		
		//merge between the two set
		Set<String> mergeSets = new HashSet<String>();
		mergeSets.addAll(title_1Set);
		mergeSets.addAll(title_2Set);
		
		Set<String> overlapingRecords = new HashSet<String>(title_1Set);
		overlapingRecords.retainAll(title_2Set);
		
		ret=(double)((double)overlapingRecords.size())/(double)(mergeSets.size());		
		return ret;		
	}
	
}
