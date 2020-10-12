using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.EntityFrameworkCore;
namespace BalsamicSolutions.AWSUtilities.EntityFramework
{
    /// <summary>
    /// place holder for regular expressions function which is installed in 
    /// the dbcontext on model creating
    /// </summary>
    public static class DbFunctions
    {
        [DbFunction("RegexMatch", "")]
        public static bool RegexMatch(string match, string pattern) => throw new NotSupportedException();
    }
}
