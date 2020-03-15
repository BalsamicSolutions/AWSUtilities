using BalsamicSolutions.AWSUtilities.Extensions;
using Demo.Data;
using Demo.Data.Testing;
using System;
using System.Linq;

namespace ConsoleCore.Demos
{
    class Program
    {
        static void Main(string[] args)
        {


            using (DemoDataContext dataCtx = new DemoDataContext())
            {
                dataCtx.RunMigrations();
                dataCtx.EnsureFullTextIndices();
                LoadSampleData(dataCtx);
            }



            Console.WriteLine("Press X to exit...");
            char exitChar = Console.ReadKey().KeyChar;
            while (exitChar != 'X' && exitChar != 'x')
            {
                exitChar = Console.ReadKey().KeyChar;
            }
            Environment.Exit(0);
        }

        /// <summary>
        /// loads sample data into the database.
        /// </summary>
        public static void LoadSampleData(DemoDataContext dataCtx, int numItems = 100)
        {
            dataCtx.RemoveRange(dataCtx.Contacts.ToList());
            dataCtx.SaveChanges();
            dataCtx.RemoveRange(dataCtx.NotesWithoutFullText.ToList());
            dataCtx.SaveChanges();
            dataCtx.RemoveRange(dataCtx.NotesWithFulltext.ToList());
            dataCtx.SaveChanges();
            string[] emailDomains = new string[] { "gmail.com", "outlook.com", "yahoo.com", "balsamicsoftware.org" };
            foreach (NameInfo nameInfo in RandomStuff.RandomNames(numItems, emailDomains, null, 5))
            {
                Contact addMe = new Contact(nameInfo);
                dataCtx.Contacts.Add(addMe);
            }
            dataCtx.SaveChanges();
            for (int idx = 0; idx < numItems; idx++)
            {
                string noteText = RandomStuff.RandomSentance(100, 2048);
                string noteTopic = RandomStuff.RandomSentance(50, 512);
                NoteWithFulltext noteWith = new NoteWithFulltext
                {
                    Topic = noteTopic,
                    Note = noteText
                };
                NoteWithoutFulltext noteWithOut = new NoteWithoutFulltext
                {
                    Topic = noteTopic,
                    Note = noteText
                };
                dataCtx.NotesWithFulltext.Add(noteWith);
                dataCtx.NotesWithoutFullText.Add(noteWithOut);
                dataCtx.SaveChanges();
            }
        }
    }
}
