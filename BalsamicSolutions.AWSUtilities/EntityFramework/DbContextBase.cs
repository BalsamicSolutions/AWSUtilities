using BalsamicSolutions.AWSUtilities.EntityFramework.DataAnnotations;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.ChangeTracking;
using MySql.Data.EntityFrameworkCore.Infraestructure;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace BalsamicSolutions.AWSUtilities.EntityFramework
{
    /// <summary>
    /// base class with all of our attributes and 
    /// extensions wired up, use it or clone it
    /// for activating index, fulltext and uppercase
    /// </summary>
    public class DbContextBase : DbContext
    {

        ///// <summary>
        ///// ctor
        ///// </summary>
        ///// <param name="connectionString">connection string</param>
        //public DbContextBase(string connectionString)
        //{

        //}

        /// <summary>
        /// CTOR
        /// </summary>
        /// <param name="options"></param>
        //public DbContextBase(DbContextOptions<DbContextBase> options)
        //{
        //    throw new DataException("Cannot access the DbContext this way, use DbContext:GetContext");
        //}

        /// <summary>
        ///// OnConfiguring
        ///// </summary>
        ///// <param name="optionsBuilder"></param>
        //protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        //{
        //    if (!optionsBuilder.IsConfigured)
        //    {
        //        MySQLDbContextOptionsBuilder mysqlBldr = new MySQLDbContextOptionsBuilder(optionsBuilder);
        //        //string connectionString = this.Database.con
        //        //optionsBuilder.UseMySQL(_ConnectionString, optAct => optAct.ExecutionStrategy(exStg => new AuroraExecutionStrategy(this)));
        //        base.OnConfiguring(optionsBuilder);
        //    }
        //}

        ///// <summary>
        ///// OnModelCreating
        ///// </summary>
        ///// <param name="modelBuilder"></param>
        //protected override void OnModelCreating(ModelBuilder modelBuilder)
        //{
        //    base.OnModelCreating(modelBuilder);
        //    //modelBuilder.BuildIndexesFromAnnotations();
        //    //modelBuilder.UseBoolToZeroOneConverter();
        //}

        /// <summary>
        /// sets all strings of an entity object to uppercase
        /// </summary>
        private void ToUpperCase(List<EntityEntry> changedAndNew)
        {
            foreach (EntityEntry efEntity in changedAndNew)
            {
                object efObject = efEntity.Entity;
                Type efType = efObject.GetType();
                Attribute[] attrs = System.Attribute.GetCustomAttributes(efType, typeof(UpperCaseAttribute));
                if (null != attrs && attrs.Length > 0)
                {
                    UpperCaseAttribute.ToUpperCase(efObject);
                }
            }
        }

        /// <summary>
        /// collect all entries
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
        public override Task<int> SaveChangesAsync(bool acceptAllChangesOnSuccess, CancellationToken cancellationToken = default)
        {
            List<EntityEntry> entityReferences = CollectChangedAndNewObjectReferences();
            ToUpperCase(entityReferences);
            return base.SaveChangesAsync(acceptAllChangesOnSuccess, cancellationToken);
        }
    }
}
