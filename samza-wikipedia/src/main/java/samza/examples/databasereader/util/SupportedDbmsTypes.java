package samza.examples.databasereader.util;

public enum SupportedDbmsTypes
{
  MYSQL   ("mysql", "com.mysql.jdbc.Driver");


  private final String value;
  private final String driver;

  SupportedDbmsTypes(final String value, final String driver)
  {
    this.value = value;
    this.driver = driver;
  }

  public String asString()
  {
    return value;
  }

  public String getDriver()
  {
    return driver;
  }

  public static SupportedDbmsTypes getTypeFromString(String dbmsType) throws InvalidDbmsTypeException
  {
    if (dbmsType.equals(MYSQL.asString()))
    {
      return MYSQL;
    } else
    {
      throw new InvalidDbmsTypeException();
    }
  }
}
