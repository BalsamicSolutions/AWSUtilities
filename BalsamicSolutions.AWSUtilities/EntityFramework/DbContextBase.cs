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
using Microsoft.Extensions.Configuration;

namespace BalsamicSolutions.AWSUtilities.EntityFramework
{
    /// <summary>
    /// base class with all of our attributes and
    /// extensions wired up, use it or clone it
    /// for activating index, fulltext and uppercase
    /// </summary>
    public class DbContextBase : DbContext
    {
        /// <summary>
        /// convient wrapper for appsettings.json
        /// </summary>
        IConfigurationRoot _Configuration = null;
        protected IConfigurationRoot Configuration
        {
            get
            {
                if (null == _Configuration)
                {
                    IConfigurationBuilder builder = new ConfigurationBuilder()
                               .SetBasePath(System.IO.Directory.GetCurrentDirectory())
                               .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);

                    _Configuration = builder.Build();
                }
                return _Configuration;
            }
        }


        /// <summary>
        /// wrapper for migrations, that also activates
        /// any full text attributes
        /// </summary>
        public void RunMigrations()
        {
            IEnumerable<string> migrationList = this.Database.GetPendingMigrations();
            if (migrationList.Count() > 0)
            {
                this.Database.Migrate();
            }
            this.EnsureFullTextIndices();
        }

        /// <summary>
        /// OnModelCreating
        /// </summary>
        /// <param name="modelBuilder"></param>
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);
            modelBuilder.BuildIndexesFromAnnotations();
        }

        /// <summary>
        /// sets all strings of an entity object to uppercase
        /// </summary>
        private void ToUpperCase(List<EntityEntry> changedAndNew)
        {
            foreach (EntityEntry efEntity in changedAndNew)
            {
                object efObject = efEntity.Entity;
                Type efType = efObject.GetType();
                Attribute[] ucaseAttributes = System.Attribute.GetCustomAttributes(efType, typeof(UpperCaseAttribute));
                if (null != ucaseAttributes && ucaseAttributes.Length > 0)
                {
                    UpperCaseAttribute.ToUpperCase(efObject);
                }
            }
        }

        /// <summary>
        /// collect all entries that are new or 
        /// have been changed
        /// </summary>
        /// <returns></returns>
        private List<EntityEntry> CollectChangedAndNewObjectReferences()
        {
            List<EntityEntry> returnValue = new List<EntityEntry>();
            foreach (EntityEntry dbItem in ChangeTracker.Entries())
            {
                //could also just check for modified or added
                if (dbItem.State == EntityState.Added || dbItem.State == EntityState.Modified)
                {
                    returnValue.Add(dbItem);
                }
            }
            return returnValue;
        }

        /// <summary>
        /// saves all changes made in this context to the database.
        /// </summary>
        /// <returns></returns>
        public override int SaveChanges()
        {
            List<EntityEntry> entityReferences = CollectChangedAndNewObjectReferences();
            ToUpperCase(entityReferences);
            return base.SaveChanges();
        }

        /// <summary>
        ///  Asynchronously saves all changes made in this context to the database.
        /// </summary>
        /// <param name="acceptAllChangesOnSuccess"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async override Task<int> SaveChangesAsync(bool acceptAllChangesOnSuccess, CancellationToken cancellationToken = default)
        {
            List<EntityEntry> entityReferences = CollectChangedAndNewObjectReferences();
            ToUpperCase(entityReferences);
            return await base.SaveChangesAsync(acceptAllChangesOnSuccess, cancellationToken);
        }
    }
}