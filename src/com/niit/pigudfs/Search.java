package com.niit.pigudfs;

import java.io.IOException;

import org.apache.pig.FilterFunc;
import org.apache.pig.data.Tuple;

public class Search extends FilterFunc
{

  public Boolean exec(Tuple input) throws IOException {

      if (input == null || input.size() == 0)

          return false;

      try{

          String str1 = (String)input.get(0);
          String str2 = (String)input.get(1);
          
          if (str1 != null){
          	if (str1.contains(str2))
          	{
          		return true;
          	}
          	else
          	{
          		return false;
          	}
          }	
          else
          	return false;
          
      }catch(Exception e){

        throw new IOException("Caught exception processing input row ", e);
      }

  }

}