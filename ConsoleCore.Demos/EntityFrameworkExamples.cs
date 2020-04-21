//  -----------------------------------------------------------------------------
//   Copyright  (c) Balsamic Solutions, LLC. All rights reserved.
//   THIS CODE IS PROVIDED *AS IS* WITHOUT WARRANTY OF  ANY KIND, EITHER
//   EXPRESS OR IMPLIED, INCLUDING ANY IMPLIED WARRANTIES OF FITNESS FOR
//  -----------------------------------------------------------------------------
using BalsamicSolutions.AWSUtilities.Extensions;
using Demo.Data;
using Demo.Data.Testing;
using System;
using System.Linq;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using BalsamicSolutions.AWSUtilities.EntityFramework;

namespace ConsoleCore.Demos
{
    public class EntityFrameworkExamples
    {
        /// <summary>
        /// some examples
        /// </summary>
        public static void ExampleFullTextQueries()
        {
            using (DemoDataContext dataCtx = new DemoDataContext())
            {
                //Boolean references https://dev.mysql.com/doc/refman/8.0/en/fulltext-boolean.html
                //simple find any text by Boyett
                long totalNotes = dataCtx.NotesWithFulltext.Count();

                Contact oneCt = dataCtx.Contacts.BooleanFullTextContains("Boyett").FirstOrDefault();
                Stopwatch watchOne = new Stopwatch();
                //Find all notes with ignorant or marriage in it
                watchOne.Start();
                List<NoteWithFulltext> matchOne = dataCtx.NotesWithFulltext.BooleanFullTextContains("ignorant marriage").ToList();
                watchOne.Stop();
                long ftElapsedMilliseconds = watchOne.ElapsedMilliseconds;
                Console.WriteLine($"using fulltext found {matchOne.Count} in {watchOne.ElapsedMilliseconds} milliseconds");

                //Now do it with a simple contains
                watchOne.Reset();
                watchOne.Start();
                List<NoteWithoutFulltext> matchOldFashioned = dataCtx.NotesWithoutFullText.Where(nt => nt.Note.Contains("ignorant")
                                                                                    || nt.Note.Contains("marriage")
                                                                                    || nt.Topic.Contains("marriage")
                                                                                    || nt.Topic.Contains("ignorant")).ToList();
                watchOne.Stop();
                long containsElapsedMilliseconds = watchOne.ElapsedMilliseconds;
                Console.WriteLine($"using contains found {matchOldFashioned.Count} in {watchOne.ElapsedMilliseconds} milliseconds");

                decimal percentImproved = ((decimal)containsElapsedMilliseconds / (decimal)ftElapsedMilliseconds);
                string textPercent = percentImproved.ToString("P2");
                Console.WriteLine($"FullText is {textPercent} %  better than Contains for searching accross {totalNotes} items");

                //Find all notes with ignorant and marriage in it
                List<NoteWithFulltext> matchTwo = dataCtx.NotesWithFulltext.BooleanFullTextContains("+ignorant +marriage").ToList();
                Console.WriteLine($"using fulltext found {matchTwo.Count} with complex rules '+ignorant +marriage'");

                //fully composable query example
                List<NoteWithFulltext> matchThree = dataCtx.NotesWithFulltext.BooleanFullTextContains("ignorant +marriage -wife")
                                                                             .Where(nt => nt.Id < 50).ToList();
                Console.WriteLine($"combined fulltext and Where found {matchThree.Count} with complex rules '+ignorant +marriage -wife' ");

                //Search an pull back a sorter to sort the results by rank
                List<NoteWithFulltext> matchFourPointOne = dataCtx.NotesWithFulltext.NaturalLanguageFullTextSearch("what the hell?", out OrderedResultSetComparer<NoteWithFulltext> rankSorter).Where(f => f.Id > 0).ToList();
                Console.WriteLine($"Natural language fulltext  found {matchFourPointOne.Count} with complex rules 'what the hell?' ");
                matchFourPointOne.Sort(rankSorter);
                Console.WriteLine($"Results now sorted in ranking order ");

                List<NoteWithFulltext> matchFour = dataCtx.NotesWithFulltext.Where(f => f.Id > 0).NaturalLanguageFullTextSearch("what the hell?").ToList();
                Console.WriteLine($"Natural language fulltext  found {matchFour.Count} with complex rules 'what the hell?' ");

                //search again with natural language query expansion, ordered by score 
                List<NoteWithFulltext> matchFive = dataCtx.NotesWithFulltext.NaturalLanguageFullTextSearchWithQueryExpansion("what the hell?").ToList();

                Console.WriteLine($"Natural language expanded fulltext  found {matchFive.Count} with complex rules 'what the hell?' ");


            }
        }

        /// <summary>
        /// setup the database with some sample data
        /// </summary>
        public static void InitializeSampleDatabase()
        {
            using (DemoDataContext dataCtx = new DemoDataContext())
            {
                dataCtx.RunMigrations();
                dataCtx.EnsureFullTextIndices();
                LoadSampleData(dataCtx);
            }
        }

        /// <summary>
        /// loads sample data into the database.
        /// </summary>
        public static void LoadSampleData(DemoDataContext dataCtx, int numItems = 1000)
        {
            if (dataCtx.Contacts.Count() != numItems)
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
}