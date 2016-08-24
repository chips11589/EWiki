﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace WikiApp.Entities.Models
{
    public class CharacterSectionGroup : EntityBase
    {
        public string Name { get; set; }
        public int Priority { get; set; }
        public virtual ICollection<CharacterSection> CharacterSections { get; set; }
    }
}
