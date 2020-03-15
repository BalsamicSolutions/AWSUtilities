using System;
using System.Collections.Generic;
using System.Text;

namespace BalsamicSolutions.AWSUtilities.Extensions
{

    [Serializable]
    public class FullTextQueryException : Exception
    {
        public FullTextQueryException()
            : base()
        {
        }

        public FullTextQueryException(string errorText)
            : base(errorText)
        {
        }

        public FullTextQueryException(string errorText, Exception innerException)
            : base(errorText, innerException)
        {
        }

 
    }
}
