using SolrNet;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Security;
using System.Web.SessionState;

namespace FileSearch
{
    public class Global : System.Web.HttpApplication
    {
        internal const string solrServer = "http://172.24.2.167:8983/solr/fiserv";

        protected void Application_Start(object sender, EventArgs e)
        {
            Startup.Init<FiservFileCollection>(solrServer);
        }
    }
}