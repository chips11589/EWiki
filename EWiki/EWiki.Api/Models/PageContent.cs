﻿using System.Collections.Generic;

namespace EWiki.Api.Models
{
    public class PageContent : EntityBase
    {
        public string ContentText { get; set; }
        public string ContentFlags { get; set; }

        public virtual ICollection<Archive> Archives { get; set; }
        public virtual ICollection<Revision> Revisions { get; set; }
    }
}