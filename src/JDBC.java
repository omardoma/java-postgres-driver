import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

public class JDBC
{
	private Connection c;
	private Statement stmt;
	private ResultSet rs;
	
	public static final String DB_URL = "jdbc:postgresql://localhost:5432/Ass2";
	public static final String USER = "postgres";
	public static final String PASSWORD = "1234";

	public JDBC()
	{
		//IMPORTANT: create a database named "Ass2" first in pgAdmin before running the program.
		try
		{
			//Import the postgresql Java driver
			Class.forName("org.postgresql.Driver");
			c = DriverManager.getConnection(DB_URL, USER, PASSWORD);
			//Disable auto commit until all changes have been made
			c.setAutoCommit(false);
			stmt = c.createStatement();
		}
		catch (Exception e)
		{
			e.printStackTrace();
			System.exit(0);
		}
		System.out.println("Opened database connection successfully.");
	}

	//Create the tables
	public void step1()
	{
		String query = "CREATE TABLE cup_matches (" + "mid integer PRIMARY KEY," + "round varchar(20),"
				+ "year integer ," + "num_ratings integer ," + "rating real" + ");" +

				"CREATE TABLE played_in(" + "mid integer references cup_matches(mid)," + "name varchar(80) ,"
				+ "year integer ," + "position integer ," + "PRIMARY KEY(mid,name)" + ");";
		try
		{
			stmt.executeUpdate(query);
			System.out.println("Tables were created successfully.");
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			System.exit(0);
		}
	}

	//Populate the table with data
	public void step2()
	{
		// Populate cup_matches table
		String[] rounds =
		{ "'32nd'", "'16th'", "'Quarter Final'", "'Semi Final'", "'Final'" };

		Random random = new Random();
		for (int i = 0; i < 2680; i++)
		{
			String query = "insert into cup_matches values(" + i + ", " + rounds[random.nextInt(rounds.length)] + " , "
					+ (random.nextInt(2017 + 1 - 1950) + 1950) + " , " + random.nextInt(80000) + ", "
					+ (double) random.nextInt(10 + 1 - 1) + 1 + ")";
			try
			{
				stmt.executeUpdate(query);
			}
			catch (SQLException e)
			{
				e.printStackTrace();
				System.exit(0);
			}

		}

		// Populate played_in table
		for (int i = 0; i < 58960; i++)
		{
			String query = "";
			if (i < 118)
				query = "insert into played_in values(" + i % 2680 + ", 'pele' , 1950 , 10)";
			else
				if (i < 20000)
					query = "insert into played_in values(" + i % 2680 + ", 'Messi" + i + "' , 2003 , 10)";
				else
					if (i < 40000)
						query = "insert into played_in values(" + i % 2680 + ", 'Neymar" + i + "' , 2009 , 11)";
					else
						query = "insert into played_in values(" + i % 2680 + ", 'Suarez" + i + "' , 2005 , 9)";

			try
			{
				stmt.executeUpdate(query);
			}
			catch (SQLException e)
			{
				e.printStackTrace();
				System.exit(0);
			}
		}
		System.out.println("Tables records were inserted successfully.");
	}

	//Check on the tables' size
	public void step3()
	{
		try
		{
			String query = "select count (*) from cup_matches";
			rs = stmt.executeQuery(query);
			if (rs.next())
				System.out.println("Table \"cup_matches\" count: " + rs.getInt("count"));
			query = "select count (*) from played_in";
			rs = stmt.executeQuery(query);
			if (rs.next())
				System.out.println("Table \"played_in\" count: " + rs.getInt("count"));

		}
		catch (SQLException e)
		{
			e.printStackTrace();
			System.exit(0);
		}
	}
	

	public void commitAndCloseResources()
	{
		try
		{
			rs.close();
			stmt.close();
			c.commit();
			c.close();
			System.out.println("Closed connection to database successfully.");
		}
		catch (SQLException e)
		{
			e.printStackTrace();
			System.exit(0);
		}
	}

	public static void main(String[] args)
	{
		JDBC sql = new JDBC();
		sql.step1();
		sql.step2();
		sql.step3();
		sql.commitAndCloseResources();
	}
}
