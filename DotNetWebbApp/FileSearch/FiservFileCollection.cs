using SolrNet.Attributes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

namespace FileSearch
{
    public class FiservFileCollection
    {
        [SolrUniqueKey("id")]
        public string Id { get; set; }

        [SolrField("author")]
        public string author { get; set; }

        [SolrField("resourcename")]
        public string resourcename { get; set; }

        [SolrField("text")]
        public ICollection<string> text { get; set; }

        [SolrField("content")]
        public ICollection<string> content { get; set; }
    }
}