//------------------------------------------------------------------------------
// <auto-generated>
//     This code was generated from a template.
//
//     Manual changes to this file may cause unexpected behavior in your application.
//     Manual changes to this file will be overwritten if the code is regenerated.
// </auto-generated>
//------------------------------------------------------------------------------

namespace EWiki.UnitTest
{
    using System;
    using System.Collections.Generic;
    
    public partial class WikiImage
    {
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA2214:DoNotCallOverridableMethodsInConstructors")]
        public WikiImage()
        {
            this.Categories = new HashSet<Category>();
            this.Characters = new HashSet<Character>();
            this.Pages = new HashSet<Page>();
        }
    
        public int Id { get; set; }
        public int BitDepth { get; set; }
        public System.DateTime CreatedDate { get; set; }
        public string CreatedUserId { get; set; }
        public string ImageDescription { get; set; }
        public int ImageHeight { get; set; }
        public string ImageMediaType { get; set; }
        public string ImageMime { get; set; }
        public string ImageName { get; set; }
        public int ImageSize { get; set; }
        public int ImageWidth { get; set; }
        public Nullable<System.DateTime> UpdatedDate { get; set; }
        public string UpdatedUserId { get; set; }
        public string ImageUrl { get; set; }
    
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public virtual ICollection<Category> Categories { get; set; }
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public virtual ICollection<Character> Characters { get; set; }
        [System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Usage", "CA2227:CollectionPropertiesShouldBeReadOnly")]
        public virtual ICollection<Page> Pages { get; set; }
    }
}
