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
		    myWin.Resize(500,500);
			
			myLabel = new Label();
			userInputField = new TextView();
		    
			Button runCommand = new Button("run_command");
		    runCommand.Clicked += HandleRunCommandClicked;
		
		
			MenuBar mainBar = new MenuBar();
			MenuItem exitItem = new MenuItem("File");
			
			MenuBar subMenu = new MenuBar();
			MenuItem subItem = new MenuItem("File");
			
			subMenu.Add(subItem);
			mainBar.Add(exitItem);
				
			VBox testBox = new VBox(false, 3);
			
			testBox.PackStart(mainBar);
			testBox.PackStart(userInputField);
			testBox.PackStart(myLabel);
			testBox.PackStart(runCommand);
			 
			myWin.Add(testBox);
				     
		    //Show Everything     
		    myWin.ShowAll();
		     
		    Application.Run();   
		}

		static void HandleRunCommandClicked (object sender, EventArgs e)
		{
			TextBuffer b = userInputField.Buffer;
			Redis r = new Redis("localhost", 6379);
			redis_text_adapter userInput = new redis_text_adapter(r);
						
			string t = userInput.ParseText(b.Text);
			myLabel.Text = t;
			
		}
	}
}
