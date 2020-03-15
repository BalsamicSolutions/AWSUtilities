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

namespace ConsoleCore.Demos
{
    internal class Program
    {
        //TODO : show a dialog with info
        //       about how to configure the environment
        //       option 1 is provision db
        //       option 2 is full text examples
        //       option 3 is iam access to full text examples
        //       option 4 is sqs wait
        //       option 5 is sqs wait in asg with self terminate

        private static void Main(string[] args)
        {
            //EntityFrameworkExamples.ExampleFullTextQueries();

            Console.WriteLine("Press X to exit...");
            char exitChar = Console.ReadKey().KeyChar;
            while (exitChar != 'X' && exitChar != 'x')
            {
                exitChar = Console.ReadKey().KeyChar;
            }
            Environment.Exit(0);
        }

    }
}