//  -----------------------------------------------------------------------------
//   Copyright  (c) Balsamic Solutions, LLC. All rights reserved.
//   THIS CODE IS PROVIDED *AS IS* WITHOUT WARRANTY OF  ANY KIND, EITHER
//   EXPRESS OR IMPLIED, INCLUDING ANY IMPLIED WARRANTIES OF FITNESS FOR
//  -----------------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Text;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using BalsamicSolutions.AWSUtilities.EntityFramework.DataAnnotations;
using Demo.Data.Testing;

namespace Demo.Data
{
    [Table(TABLE_NAME)]
    public class Contact
    {
        public const string TABLE_NAME = "Contacts";

        /// <summary>
        /// Gets or sets  the primary Key, DB Auto incrimented.
        /// </summary>
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public long Id { get; set; }

        /// <summary>
        /// The patient's first name
        /// </summary>
        [MaxLength(128), FullText]
        public string FirstName { get; set; }

        /// <summary>
        /// The patient's last name
        /// </summary>
        [MaxLength(128), FullText]
        public string LastName { get; set; }

        /// <summary>
        /// The patient's primary email address
        /// </summary>
        [MaxLength(128), FullText]
        public string Email { get; set; }

        /// <summary>
        /// The patient's cell phone number
        /// </summary>
        [MaxLength(128), FullText]
        public string CellPhone { get; set; }

        public Contact()
        {

        }

        public Contact(NameInfo nameInfo)
            : this()
        {
            this.FirstName = nameInfo.GivenName;
            this.LastName = nameInfo.SurName;
            this.Email = nameInfo.Email;
            this.CellPhone = RandomStuff.RandomPhoneNumber();

        }
    }
}