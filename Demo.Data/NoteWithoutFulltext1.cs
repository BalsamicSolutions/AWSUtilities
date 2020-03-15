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

namespace Demo.Data
{
    /// <summary>
    /// notice that its an UpperCase Note
    /// </summary>
    [Table(TABLE_NAME), UpperCase]
    public class NoteWithoutFulltext
    {
        public const string TABLE_NAME = "NotesWithoutFulltext";

        /// <summary>
        /// Gets or sets  the primary Key, DB Auto incrimented.
        /// </summary>
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public long Id { get; set; }

        /// <summary>
        /// Gets or sets  notes about the entity.
        /// </summary>
        [StringLength(2048)]
        public string Note { get; set; }

        /// <summary>
        /// Gets or sets the topic of the note.
        /// </summary>
        [Index, StringLength(512)]
        public string Topic { get; set; }
    }
}