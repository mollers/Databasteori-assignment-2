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
			PreparedStatement ps = connection.prepareStatement("INSERT INTO comment VALUES (?,?,?,?,?,?,?,?,?,?)");
			loadFileBufferedReaderStringBuilder("RC_2007-10.json", ps);
			ResultSet rs = statement.executeQuery("select * from comment limit 10");
			while (rs.next()) {
				System.out.println("id = " + rs.getString("id"));
				System.out.println("name = " + rs.getString("parent_id"));
				System.out.println("name = " + rs.getString("link_id"));
				System.out.println("name = " + rs.getString("name"));
				System.out.println("name = " + rs.getString("author"));
				System.out.println("name = " + rs.getString("body"));
				System.out.println("name = " + rs.getString("subreddit_id"));
				System.out.println("name = " + rs.getString("subreddit"));
				System.out.println("name = " + rs.getInt("score"));
				System.out.println("name = " + rs.getInt("name"));
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
	private static void loadFileScannerConcatenation(String path, Statement statement){
		File file = new File(path);
		Scanner scan;
		try {
			scan = new Scanner(file);
			long start = System.currentTimeMillis();
			int counter = 0;
			while(scan.hasNextLine()){
				System.out.println(++counter);
				JSONObject jsObj = new JSONObject(scan.nextLine());
				//(id string, parent_id string, link_id string, name string, author string, body string, subreddit_id string, subreddit string, score integer, created_utc integer)
				statement.executeUpdate("INSERT INTO comment VALUES ('id'");
				
				String str = "insert into comment values(" + "'" + jsObj.getString("id") + "'"  + ","
						+ "'"+ jsObj.getString("parent_id")+ "'"+ "," + "'"+ jsObj.getString("link_id")+ "'"+ ","
						+ "'"+ jsObj.getString("name")+ "'"+ ","+ "'"+ jsObj.getString("author")+ "'"+ ","
						+ "'"+ "body"+ "'"+ ","+ "'"+ jsObj.getString("subreddit_id")+ "'"+ ","
						+ "'"+ jsObj.getString("subreddit")+ "'"+ ","+ jsObj.getInt("score")+ ","
						+ jsObj.getInt("created_utc") + ")";
				statement.executeUpdate(str);
			}
			scan.close();
			long end = System.currentTimeMillis();

			System.out.println(end-start + " ms " + (end-start)/1000 + " s " +(end-start)/60000 + " min ");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	private static void loadFileBufferedReaderStringBuilder(String path, PreparedStatement ps){
		try{
			BufferedReader br = new BufferedReader(new FileReader(path));
			String line;
			long start = System.currentTimeMillis();
			int counter = 0;
			while((line = br.readLine())!= null){
				System.out.println(++counter);
				JSONObject jsObj = new JSONObject(line);

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
				/*if((counter % 10000) == 0)
		        {
		            System.out.println(counter);
		            ps.executeBatch();
		            ps.clearBatch();
		        }*/
			}
			ps.executeBatch();
            ps.clearBatch();
			br.close();
			long end = System.currentTimeMillis();

			System.out.println(end-start + " ms " + (end-start)/1000 + " s " +(end-start)/60000 + " min ");
		}catch(Exception e){
			System.out.println(e);
		}

	}
}