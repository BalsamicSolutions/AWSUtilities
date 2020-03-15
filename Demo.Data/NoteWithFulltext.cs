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
    [Table(TABLE_NAME)]
    public class NoteWithFulltext
    {
        public const string TABLE_NAME = "NotesWithFullText";

        /// <summary>
        /// Gets or sets  the primary Key, DB Auto incrimented.
        /// </summary>
        [Key, DatabaseGenerated(DatabaseGeneratedOption.Identity)]
        public long Id { get; set; }

        /// <summary>
        /// Gets or sets  notes about the entity.
        /// </summary>
        [FullText(0), StringLength(2048)]
        public string Note { get; set; }

        /// <summary>
        /// Gets or sets the topic of the note.
        /// </summary>
        [FullText(1), StringLength(512)]
        public string Topic { get; set; }
    }
}