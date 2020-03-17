//  -----------------------------------------------------------------------------
//   Copyright  (c) Balsamic Solutions, LLC. All rights reserved.
//   THIS CODE IS PROVIDED *AS IS* WITHOUT WARRANTY OF  ANY KIND, EITHER
//   EXPRESS OR IMPLIED, INCLUDING ANY IMPLIED WARRANTIES OF FITNESS FOR
//  -----------------------------------------------------------------------------
using BalsamicSolutions.AWSUtilities.EntityFramework.DataAnnotations;
using BalsamicSolutions.AWSUtilities.Extensions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using MySql.Data.EntityFrameworkCore.Infraestructure;
using System;
using System.Linq;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using BalsamicSolutions.AWSUtilities.EntityFramework;
using BalsamicSolutions.AWSUtilities.RDS;
using Microsoft.Extensions.Configuration;

namespace Demo.Data
{
    /// <summary>
    /// data context for our demos.
    /// </summary>
    public class DemoDataContext : DbContextBase
    {
 
        public DbSet<Contact> Contacts { get; set; }
        public DbSet<NoteWithFulltext> NotesWithFulltext { get; set; }
        public DbSet<NoteWithoutFulltext> NotesWithoutFullText { get; set; }

        /// <summary>
        /// example OnConfiguring
        /// </summary>
        /// <param name="optionsBuilder"></param>
        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            
            if (!optionsBuilder.IsConfigured)
            {
                // obviously get this from your configuration tools
                string connectionString = Configuration.GetConnectionString("DemoData");

                //UseAuroraExecutionStrategy is the same as
                //optionsBuilder.UseMySQL(connectionString, optAct => optAct.ExecutionStrategy(exStg => new AuroraExecutionStrategy(this)));

                optionsBuilder.UseAuroraExecutionStrategy(this, connectionString);
                base.OnConfiguring(optionsBuilder);
            }
        }



    }
}