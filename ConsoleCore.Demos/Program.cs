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
        private static void Main(string[] args)
        {
            EntityFrameworkExamples.ExampleFullTextQueries();
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