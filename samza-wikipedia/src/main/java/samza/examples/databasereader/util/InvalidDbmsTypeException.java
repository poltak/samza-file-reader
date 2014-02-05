package samza.examples.databasereader.util;

import org.apache.samza.SamzaException;

public class InvalidDbmsTypeException extends SamzaException
{
  public InvalidDbmsTypeException(String message)
  {
    super(message);
  }

  public InvalidDbmsTypeException()
  {
    super();
  }
}
