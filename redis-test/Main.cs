using System;

namespace redistest
{
	class MainClass
	{
		public static void Main (string[] args)
		{
			Console.WriteLine ("Hello World!");
			
			Redis r = new Redis("localhost", 6379);
			string s = r.GetString("mykey");
			string t = r.SendCommand("GET mykey");
			Console.Write(t);
			
		}
	}
}
