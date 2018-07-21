/**
 * 
 */
package org.bgu.ise.ddb.items;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import javax.servlet.http.HttpServletResponse;

import org.bgu.ise.ddb.MediaItems;
import org.bgu.ise.ddb.ParentController;
import org.bson.Document;
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



/**
 * 
 *
 */
@RestController
@RequestMapping(value = "/items")
public class ItemsController extends ParentController {
	
	
	
	/**
	 * The function copy all the items(title and production year) from the Oracle table MediaItems to the System storage.
	 * The Oracle table and data should be used from the previous assignment
	 */
	@RequestMapping(value = "fill_media_items", method={RequestMethod.GET})
	public void fillMediaItems(HttpServletResponse response){
		System.out.println("was here");
		//:TODO your implementation
		try {
			//connect to ORACLE
			Class.forName("oracle.jdbc.driver.OracleDriver");
			String connectionURL = "jdbc:oracle:thin:@ora1.ise.bgu.ac.il:1521/ORACLE";
			Connection conn = DriverManager.getConnection(connectionURL, "danafeld","abcd");
			String selectQuery = "SELECT * FROM MediaItems"; // query of MediaItems
			PreparedStatement pStatement = conn.prepareStatement(selectQuery);
			ResultSet it = pStatement.executeQuery();
			//connect to database
			MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
			MongoDatabase db = mongoClient.getDatabase("projectDB");	
			MongoCollection<Document> collection = db.getCollection("MediaItems");
			
			//enter the title and the year
			while(it.next()){
				Document document = new Document() 
					      .append("title", it.getString("title"))
					      .append("year", it.getInt("prod_year"));
				collection.insertOne(document); 
			}
			
			mongoClient.close();	
			it.close();
			pStatement.close();
			conn.close();
			System.out.println("Success updating");
		} 
		catch (ClassNotFoundException e) 
		{
			e.printStackTrace();
		}
		catch (SQLException e) 
		{
			e.printStackTrace();
		} 
		
		HttpStatus status = HttpStatus.OK;
		response.setStatus(status.value());
	}
	
	

	/**
	 * The function copy all the items from the remote file,
	 * the remote file have the same structure as the films file from the previous assignment.
	 * You can assume that the address protocol is http
	 * @throws IOException 
	 */
	@RequestMapping(value = "fill_media_items_from_url", method={RequestMethod.GET})
	public void fillMediaItemsFromUrl(@RequestParam("url")    String urladdress,
			HttpServletResponse response) throws IOException{
		System.out.println(urladdress);
		
		//:TODO your implementation
		String split = ",";
		try {
			
			// read from the buffer URL
			BufferedReader reader = null;	
			//create URL
			URL siteURL = new URL(urladdress);
			// open the stream
			reader = new BufferedReader(new InputStreamReader(siteURL.openStream())); 

			//connect to database
			MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
			MongoDatabase db = mongoClient.getDatabase("projectDB");	
			MongoCollection<Document> collection = db.getCollection("MediaItems");
		
			//add the details from the URL
			String line =  reader.readLine();
			while (line != null) 
            {
				//create new movie
                String[] movie = line.split(split);               
                Document document = new Document("MediaItems", "projectDB") 
			      .append("title", movie[0])
			      .append("year", Integer.parseInt(movie[1].toString()));
				collection.insertOne(document); 
				//get the next line
				line= reader.readLine();
            }
        	mongoClient.close();
        	reader.close();
        } 
		catch (FileNotFoundException e) 
		{
                e.printStackTrace();
        }
		catch (IOException e) 
		{
            e.printStackTrace();
        } 	
		
		HttpStatus status = HttpStatus.OK;
		response.setStatus(status.value());
	}
	
	
	/**
	 * The function retrieves from the system storage N items,
	 * order is not important( any N items) 
	 * @param topN - how many items to retrieve
	 * @return
	 */
	@RequestMapping(value = "get_topn_items",headers="Accept=*/*", method={RequestMethod.GET},produces="application/json")
	@ResponseBody
	@org.codehaus.jackson.map.annotate.JsonView(MediaItems.class)
	public  MediaItems[] getTopNItems(@RequestParam("topn")    int topN){
		
		//:TODO your implementation
		MediaItems[] MediaItems = new MediaItems[topN];
		int i = 0;
		try {
			//connection
			MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
			MongoDatabase db = mongoClient.getDatabase("projectDB");	
			MongoCollection<Document> collection = db.getCollection("MediaItems");
			
			FindIterable<Document> iter = collection.find().limit(topN);// making iterator no TOPN
			Iterator<Document> it = iter.iterator();  // Getting the iterator 			 
			//moving on the mediaItems
			while(it.hasNext()) {	
				Document d=(Document) it.next();
		    	MediaItems[i] = new MediaItems((String) (d).get("title"),(int) (d).get("year"));
		    	i++;
			}
			mongoClient.close();
			System.out.println("Success updating");
		} 
		catch (MongoException e) 
		{
			System.out.println(e);
		} 
		catch(java.util.NoSuchElementException e) {
			System.out.println(e);
		}
		
		//MediaItems m = new MediaItems("Game of Thrones", 2011);
		//System.out.println(m);
		//return new MediaItems[]{m};
		return MediaItems;

	}
		
	// function that check if item exist in the table
	public boolean isItemExist(String title) {
		boolean result=false;
		try {
			//connection
			MongoClient mongoClient = new MongoClient( "localhost" , 27017 );
			MongoDatabase db = mongoClient.getDatabase("projectDB");	
			MongoCollection<Document> collection = db.getCollection("MediaItems");
			
			//update the title
			BasicDBObject searchUserQuery = new BasicDBObject();
			searchUserQuery.put("title", title);
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

}
