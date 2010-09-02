using System;
using Gtk;

namespace redisclient
{
	class MainClass
	{		
		static Label myLabel;
		static TextView userInputField;
		
		public static void Main (string[] args)
		{
			Application.Init();
 			
			//Create the Window
			Window myWin = new Window("My first GTK# Application! ");
		    myWin.Resize(200,200);
			
			myLabel = new Label();
			userInputField = new TextView();
		    
			Button runCommand = new Button("run_command");
		    //Create a label and put some text in it.     
		    runCommand.Clicked += HandleRunCommandClicked;
			
			VBox testBox = new VBox(false, 3);
			
			testBox.PackStart(userInputField);
			testBox.PackStart(myLabel);
			testBox.PackStart(runCommand);
			 
			myWin.Add(testBox);
			 
		    //Add the label to the form     
		    //myWin.Add(myLabel);			

		     
		    //Show Everything     
		    myWin.ShowAll();
		     
		    Application.Run();   
		}

		static void HandleRunCommandClicked (object sender, EventArgs e)
		{
			TextBuffer b = userInputField.Buffer;
			Redis r = new Redis("localhost", 6379);
			//string s = r.GetString("mykey");
			redis_text_adapter userInput = new redis_text_adapter(r);
						
			string t = userInput.ParseText(b.Text);
			myLabel.Text = t;
			
		}
	}
}
