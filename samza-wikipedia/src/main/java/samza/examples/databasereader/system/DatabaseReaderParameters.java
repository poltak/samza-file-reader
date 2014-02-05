package samza.examples.databasereader.system;

/**
 * Aggregate class containing details for the system to connect to specified database.
 */
public class DatabaseReaderParameters
{
  private final String host;
  private final String port;
  private final String username;
  private final String password;
  private final String dbmsType;
  private final String databaseName;

  public DatabaseReaderParameters(final String host, final String port, final String username, final String password,
                                  final String dbmsType, final String databaseName)
  {
    this.host = host;
    this.port = port;
    this.username = username;
    this.password = password;
    this.dbmsType = dbmsType;
    this.databaseName = databaseName;
  }

  public String getHost()
  {
    return host;
  }

  public String getPort()
  {
    return port;
  }

  public String getUsername()
  {
    return username;
  }

  public String getPassword()
  {
    return password;
  }

  public String getDbmsType()
  {
    return dbmsType;
  }

  public String getDatabaseName()
  {
    return databaseName;
  }
}
