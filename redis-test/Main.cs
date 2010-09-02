using System;

namespace redistest
{
	class MainClass
	{
		public static void Main (string[] args)
		{
			Console.WriteLine ("Hello World!");
			
			Redis r = new Redis("localhost", 6379);
			//string s = r.GetString("mykey");
			redis_text_adapter userInput = new redis_text_adapter(r);
						
			string t = userInput.ParseText("GET mykey");
			t += userInput.ParseText("SET mykey dude");
			// r.SendCommand("GET mykey");
			Console.Write(t);
			
		}
	}
}
