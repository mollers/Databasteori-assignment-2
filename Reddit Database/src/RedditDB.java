import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
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
			loadFileBufferedReaderStringBuilder("C:\\Users\\Andreas\\Desktop\\databas\\RC_2011-07.json", statement);
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
	private static void loadFileBufferedReaderStringBuilder(String path, Statement statement){
		try{
			BufferedReader br = new BufferedReader(new FileReader(path));
			String line;
			long start = System.currentTimeMillis();
			int counter = 0;
			while((line = br.readLine())!= null){
				System.out.println(++counter);
				JSONObject jsObj = new JSONObject(line);
				StringBuilder sb = new StringBuilder();
				sb.append("insert into comment values('").append(jsObj.getString("id")).append("','").append(jsObj.getString("parent_id"))
				.append("','").append(jsObj.getString("link_id")).append("','").append(jsObj.getString("name")).append("','")
				.append(jsObj.getString("author")).append("','").append("body").append("','").append(jsObj.getString("subreddit_id"))
				.append("','").append(jsObj.getString("subreddit")).append("',").append(jsObj.getInt("score")).append(",").append(jsObj.getInt("created_utc")).append(")");
				
				statement.executeUpdate(sb.toString());
			}
			br.close();
			long end = System.currentTimeMillis();

			System.out.println(end-start + " ms " + (end-start)/1000 + " s " +(end-start)/60000 + " min ");
		}catch(Exception e){
			System.out.println(e);
		}

	}
}