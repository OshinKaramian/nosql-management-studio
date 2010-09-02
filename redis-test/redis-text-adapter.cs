using System.Collections;
using System.Text.RegularExpressions;
using System.Text;
using System;

namespace redistest
{
	public class redis_text_adapter
	{
		Hashtable textHash = new Hashtable();
		Redis _r = new Redis("",0);

		public redis_text_adapter (Redis r)
		{
			textHash.Add("get", 1);
			textHash.Add("set", 2);
			_r = r;
		}
		
		//GET [keyname]
		//SET [keyname]
		public string ParseText(string text)
		{	
			cleanString(ref text);
			
			string[] evaluate = text.Split(' ');
			string returnValue = string.Empty;
			for(int i = 0; i < evaluate.Length; i++)
			{
				if(textHash.ContainsKey(evaluate[i]))
				{
					returnValue += functionEvaluator(ref i, evaluate);
				}
			}
			
			return returnValue;	
		}
		
		private string functionEvaluator(ref int index, string[] userInput)
		{
			string redisCommand = userInput[index];
			string key = string.Empty;
			string val = string.Empty;
			string returnValue = string.Empty;
			
			switch(redisCommand)
			{
				case "get":
					key = userInput[index+1];
					index = index + 1;
					returnValue = _r.GetString(key);
					break;		
				
				case "set":
					key = userInput[index+1];
					val = userInput[index+2];
					index = index + 2;
					bool isSuccess = _r.Set(key, val);
					returnValue = Convert.ToString(isSuccess);
					break;	
				
			}
			
			return returnValue;
		}
		
		
		private void cleanString(ref string input)
		{
		//	input = Regex.Replace(input, @”\s+”, ” “);
		}
		
		
	}
}
