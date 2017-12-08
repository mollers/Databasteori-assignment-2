import java.io.File;
import java.io.FileNotFoundException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;

import org.json.*;

public class RedditDB
{
	public static void main(String[] args)
	{
		Connection connection = null;
		try
		{
			// create a database connection
			connection = DriverManager.getConnection("jdbc:sqlite:reddit.db");
			Statement statement = connection.createStatement();
			statement.setQueryTimeout(30);  // set timeout to 30 sec.

			statement.executeUpdate("drop table if exists comment");
			statement.executeUpdate("create table comment (id string, parent_id string, link_id string, name string, author string, body string, subreddit_id string, subreddit string, score integer, created_utc integer)");
			loadFile("C:\\Users\\Andreas\\OneDrive\\Documents\\Datateknik högskoleingenjör\\2DV513 Databasteori\\Assignment 2\\Reddit Database\\src\\RC_2007-10.json", statement);
			//ResultSet rs = statement.executeQuery("select * from comment");
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
	private static void loadFile(String path, Statement statement){
		File file = new File(path);
		Scanner scan;
		try {
			scan = new Scanner(file);
			while(scan.hasNextLine()){
				JSONObject jsObj = new JSONObject(scan.nextLine());
																	//(id string, parent_id string, link_id string, name string, author string, body string, subreddit_id string, subreddit string, score integer, created_utc integer)
				String str = "insert into comment values(" + "'" + jsObj.getString("id") + "'"  + ","
						+ "'"+ jsObj.getString("parent_id")+ "'"+ "," + "'"+ jsObj.getString("link_id")+ "'"+ ","
						+ "'"+ jsObj.getString("name")+ "'"+ ","+ "'"+ jsObj.getString("author")+ "'"+ ","
						+ "'"+ jsObj.getString("body")+ "'"+ ","+ "'"+ jsObj.getString("subreddit_id")+ "'"+ ","
						+ "'"+ jsObj.getString("subreddit")+ "'"+ ","+ jsObj.getInt("score")+ ","
					+ jsObj.getInt("created_utc") + ")";
				System.out.println(str);
				statement.executeUpdate(str);
			}
			scan.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}