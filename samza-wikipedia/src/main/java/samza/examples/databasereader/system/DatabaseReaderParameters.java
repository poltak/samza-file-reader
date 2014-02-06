package samza.examples.databasereader.system;

import samza.examples.databasereader.util.InvalidDbmsTypeException;
import samza.examples.databasereader.util.SupportedDbmsTypes;

/**
 * Aggregate class containing details for the system to connect to specified database.
 */
public class DatabaseReaderParameters
{
  private final String host;
  private final int port;
  private final String username;
  private final String password;
  private final SupportedDbmsTypes dbmsType;
  private final String databaseName;

  public DatabaseReaderParameters(final String host, final int port, final String username, final String password,
                                  final String dbmsType, final String databaseName)
      throws InvalidDbmsTypeException
  {
    this.host = host;
    this.port = port;
    this.username = username;
    this.password = password;
    this.dbmsType = SupportedDbmsTypes.getTypeFromString(dbmsType);
    this.databaseName = databaseName;
  }

  public String getHost()
  {
    return host;
  }

  public int getPort()
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

  public SupportedDbmsTypes getDbmsType()
  {
    return dbmsType;
  }

  public String getDatabaseName()
  {
    return databaseName;
  }
}
