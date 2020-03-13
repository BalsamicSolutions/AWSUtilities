using System;
using System.Collections.Generic;
using System.Text;

namespace BalsamicSolutions.AWSUtilities
{

    /// <summary>
    /// simple utility extensions
    /// </summary>
    internal static class UtilityExtensions
    {

        /// <summary>
        /// null or ""
        /// </summary>
        /// <param name="thisStr"></param>
        /// <returns></returns>
        public static bool IsNullOrEmpty(this String thisStr)
        {
            return string.IsNullOrEmpty(thisStr);
        }

        /// <summary>
        /// null or only white space
        /// </summary>
        /// <param name="thisStr"></param>
        /// <returns></returns>
        public static bool IsNullOrWhiteSpace(this String thisStr)
        {
            return string.IsNullOrWhiteSpace(thisStr);
        }

        /// <summary>
        /// case insensitive contains check
        /// </summary>
        /// <param name="thisStr"></param>
        /// <param name="findThis"></param>
        /// <returns></returns>
        public static bool CaseInsensitiveContains(this String thisStr, string findThis)
        {
            if (null == thisStr && null == findThis)
            {
                return true;
            }
            if (null == thisStr || null == findThis)
            {
                return false;
            }
            return (thisStr.IndexOf(findThis, StringComparison.InvariantCultureIgnoreCase) > -1);
        }

        /// <summary>
        /// just what it says, case insensitive equality check
        /// </summary>
        /// <param name="thisStr"></param>
        /// <param name="compareTo"></param>
        /// <returns></returns>
        public static bool CaseInsensitiveEquals(this String thisStr, string compareTo)
        {
            if (null == thisStr && null == compareTo)
            {
                return true;
            }
            if (null == thisStr || null == compareTo)
            {
                return false;
            }
            return thisStr.Equals(compareTo, StringComparison.InvariantCultureIgnoreCase);
        }

        /// <summary>
        /// Deserializes json text to a typed object
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="jsonEncodedObject"></param>
        /// <returns></returns>
        public static T FromJson<T>(this string thisStr)
        {
            if (thisStr.IsNullOrWhiteSpace())
            {
                return default(T);
            }
            return Newtonsoft.Json.JsonConvert.DeserializeObject<T>(thisStr);
        }

        /// <summary>
        /// Deserializes json text to a typed object
        /// returns a null if there is a json error
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="jsonEncodedObject"></param>
        /// <returns></returns>
        public static T TryFromJson<T>(this string thisStr)
        {
            if (thisStr.IsNullOrWhiteSpace())
            {
                return default(T);
            }
            try
            {
                return Newtonsoft.Json.JsonConvert.DeserializeObject<T>(thisStr);
            }
            catch (Newtonsoft.Json.JsonException)
            {
                return default(T);
            }
        }

        public static T FromJson<T>(this string thisStr, bool includeTypes)
        {
            if (thisStr.IsNullOrWhiteSpace()) return default(T);
            Newtonsoft.Json.JsonSerializerSettings jsonSerializerSettings = new Newtonsoft.Json.JsonSerializerSettings();
            jsonSerializerSettings.ReferenceLoopHandling = Newtonsoft.Json.ReferenceLoopHandling.Ignore;
            if (includeTypes) jsonSerializerSettings.TypeNameHandling = Newtonsoft.Json.TypeNameHandling.Objects;
            return Newtonsoft.Json.JsonConvert.DeserializeObject<T>(thisStr, jsonSerializerSettings);
        }

        /// <summary>
        ///  Expand an exception into a readable string
        /// </summary>
        /// <param name="thisException"></param>
        /// <returns></returns>
        public static string ExceptionText(this Exception thisException)
        {
            return GetExceptionText(thisException, true, true, false);
        }

        /// <summary>
        ///  Expand an exception into a readable string
        /// </summary>
        /// <param name="thisException"></param>
        /// <param name="withStackTrace"></param>
        /// <param name="includeInnerException"></param>
        /// <returns></returns>
        public static string ExceptionText(this Exception thisException, bool withStackTrace, bool includeInnerException)
        {
            return GetExceptionText(thisException, withStackTrace, includeInnerException, false);
        }

        /// <summary>
        ///  Expand an exception into a readable string
        /// </summary>
        /// <param name="thisException"></param>
        /// <param name="withStackTrace"></param>
        /// <param name="includeInnerException"></param>
        /// <returns></returns>
        public static string ExceptionText(this Exception thisException, bool withStackTrace, bool includeInnerException, bool webFormated)
        {
            return GetExceptionText(thisException, withStackTrace, includeInnerException, webFormated);
        }

        /// <summary>
        /// Expand an exception into a readable string
        /// </summary>
        /// <param name="ex"></param>
        /// <param name="withStackTrace"></param>
        /// <param name="includeInnerException"></param>
        /// <returns></returns>
        public static string GetExceptionText(Exception ex, bool withStackTrace, bool includeInnerException, bool webFormated)
        {
            StringBuilder returnValue = new StringBuilder();
            Exception innerException = ex.InnerException;

            returnValue.AppendLine(ex.Message);
            if (withStackTrace)
            {
                returnValue.AppendLine(ex.StackTrace);
                if (webFormated)
                {
                    returnValue.AppendLine("<b/>");
                }
            }

            while (innerException != null && includeInnerException)
            {
                returnValue.AppendLine(innerException.Message);
                {
                    returnValue.AppendLine("<b/>");
                }
                if (withStackTrace)
                {
                    returnValue.AppendLine(innerException.StackTrace);
                    {
                        returnValue.AppendLine("<b/>");
                    }
                }

                innerException = innerException.InnerException;
            }

            return returnValue.ToString();
        }
    }
}
