import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;


import org.json.*;

public class RedditDB
{
	public static void main(String[] args) throws ParseException
	{
		Connection connection = null;
		try
		{
			// create a database connection
			connection = DriverManager.getConnection("jdbc:sqlite:reddit2011-07-1.1.db");
			
			Statement statement = connection.createStatement();
			connection.setAutoCommit(false);
			statement.setQueryTimeout(30);  // set timeout to 30 sec.
			
			statement.executeUpdate("drop table if exists comment");
			statement.executeUpdate("create table comment (id string, parent_id string, link_id string, name string, author string, body string, subreddit_id string, subreddit string, score integer, created_utc integer)");
			
			// Alternative scheme with constraints
			/*statement.executeUpdate("create table comment ("
					+ "id string NOT NULL UNIQUE PRIMARY KEY"
					+ ", parent_id string NOT NULL"
					+ ", link_id string NOT NULL"
					+ ", name string NOT NULL"
					+ ", author string NOT NULL"
					+ ", body string NOT NULL"
					+ ", subreddit_id string NOT NULL"
					+ ", subreddit string NOT NULL"
					+ ", score integer NOT NULL"
					+ ", created_utc integer NOT NULL)");*/
			
			PreparedStatement ps = connection.prepareStatement("INSERT INTO comment VALUES (?,?,?,?,?,?,?,?,?,?)");
			loadFileBufferedReaderStringBuilder("C:\\Users\\emile\\Desktop\\databas\\RC_2011-07.json", ps);
			
			// UTC epoch second converter
			/*String str = "Jun 13 2003 23:11:52 UTC";
		    SimpleDateFormat df = new SimpleDateFormat("MMM dd yyyy HH:mm:ss.SSS zzz");
		    Date date = df.parse(str);
		    long epoch = date.getTime();
		    System.out.println(epoch); // 1055545912454
			*/
			
			// 5.1
			ResultSet rs = statement.executeQuery("select count(*) from comment WHERE author = 'matts2'"); 
			System.out.println(rs.getInt(1)); 
			
			// 5.2
			rs = statement.executeQuery("select count(*) from comment where subreddit_id = 't5_6' and created_utc > 1193616000 and created_utc < 1193702399"); 
			System.out.println(rs.getInt(1)); // --> 5971 
			
			// 5.3
			rs = statement.executeQuery("SELECT COUNT(*) FROM comment WHERE body LIKE '% lol %'");
			System.out.println(rs.getInt(1));
			
			// 5.4
			rs = statement.executeQuery("SELECT distinct subreddit, author FROM comment WHERE author IN (SELECT author FROM comment WHERE link_id = 't3_2zxms') order by author asc ");  
			while (rs.next()) {  
			 System.out.print(rs.getString("subreddit"));  
			 System.out.println(" author:"+rs.getString("author"));  
			} 
			
			// 5.5
			rs = statement.executeQuery("select author, score from (select sum(score) as 'Score', author from comment group by author order by score desc limit 1)"
					+ " union all"
					+ " select author, score from (select sum(score) as 'Score', author from comment group by author order by score asc limit 1)");
			while (rs.next()) {
				System.out.println(rs.getString("author") + " " + rs.getString("score"));
			}
			
			// 5.6
			rs = statement.executeQuery("select subreddit, score from (select sum(score) as 'Score', subreddit from comment group by subreddit order by score desc limit 1)"
					+ " union all"
					+ " select subreddit, score from (select sum(score) as 'Score', subreddit from comment group by subreddit order by score asc limit 1)");
			while (rs.next()) {
				System.out.println(rs.getString("subreddit") + " " + rs.getString("score"));
			}
			
			// 5.7
			rs = statement.executeQuery("SELECT  distinct author FROM comment WHERE link_id in( SELECT distinct link_id FROM comment WHERE author = 'igiveyoumylife') "); 
			while (rs.next()) { 
				System.out.println("Author: "+rs.getString("author")); 
			}
			
			// 5.8
			rs = statement.executeQuery("select author, subreddit from comment group by author having count(distinct  subreddit) = 1"); 
			while (rs.next()) { 
				System.out.print("Author: "+rs.getString("author")); 
				System.out.println(" only subreddit commited to: "+rs.getString("subreddit")); 
			}
			
			System.out.println("done!");
		}
		catch(SQLException e)
		{
			// if the error message is "out of memory",
			// it probably means no database file is found
			System.err.println(e.getMessage());
		}
		finally
		{
			try
			{
				if(connection != null)
					connection.close();
			}
			catch(SQLException e)
			{
				// connection close failed.
				System.err.println(e);
			}
		}
	}
	private static void loadFileBufferedReaderStringBuilder(String path, PreparedStatement ps){
		try{
			BufferedReader br = new BufferedReader(new FileReader(path));
			String line;
			long start = System.currentTimeMillis();
			int counter = 0;
			while((line = br.readLine())!= null){
				
				JSONObject jsObj = new JSONObject(line);
				++counter;
				ps.setString(1, jsObj.getString("id"));
				ps.setString(2, jsObj.getString("parent_id"));
				ps.setString(3, jsObj.getString("link_id"));
				ps.setString(4, jsObj.getString("name"));
				ps.setString(5, jsObj.getString("author"));
				ps.setString(6, jsObj.getString("body"));
				ps.setString(7, jsObj.getString("subreddit_id"));
				ps.setString(8, jsObj.getString("subreddit"));
				ps.setInt(9, jsObj.getInt("score"));
				ps.setInt(10, jsObj.getInt("created_utc"));
				ps.addBatch();
				
				if ((counter % 10000) == 0) {
					System.out.println(counter);
					ps.executeBatch();
					ps.getConnection().commit();
		            ps.clearBatch();
				}
			}
			ps.executeBatch();
			ps.getConnection().commit();
            ps.clearBatch();
            
			br.close();
			long end = System.currentTimeMillis();

			System.out.println(end-start + " ms " + (end-start)/1000 + " s " +(end-start)/60000 + " min ");
		}catch(Exception e){
			System.out.println(e);
		}

	}
}