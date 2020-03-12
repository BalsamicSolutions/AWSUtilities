using System;
using System.Collections.Generic;
using System.Globalization;
using System.Reflection;
using System.Text;

namespace BalsamicSolutions.AWSUtilities.EntityFramework.DataAnnotations
{
    /// <summary>
    /// class attribute indicating that all string values 
    /// should be changed to uppercase before saving
    /// </summary>
    /// <summary>
    /// place holder for upper case conversion flag
    /// </summary>
    [AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
    public class UpperCaseAttribute : Attribute
    {

        /// <summary>
        /// Gets or sets the CultureName  .
        /// </summary>
        public string CultureName { get; }

        /// <summary>
        /// Initializes a new UpperCaseAttribute with a culture flag for uppercase operations
        /// </summary>
        public UpperCaseAttribute()
        {
            CultureName = string.Empty;
        }

        /// <summary>
        /// Initializes a new UpperCaseAttribute with a culture flag for uppercase operations
        /// </summary>
        /// <param name="cultureName"></param>
        public UpperCaseAttribute(string cultureName)
        {
            CultureName = cultureName;
        }

        /// <summary>
        /// updates all string objects to upper case
        /// </summary>
        /// <param name="upperCaseThis"></param>
        public static void ToUpperCase(object upperCaseThis)
        {
            if (null != upperCaseThis)
            {
                Type objectType = upperCaseThis.GetType();
                UpperCaseAttribute uCaseAttribute = objectType.GetCustomAttribute(typeof(UpperCaseAttribute)) as UpperCaseAttribute;
                if (null != uCaseAttribute)
                {
                    CultureInfo cInfo = CultureInfo.CurrentCulture;
                    if (null == uCaseAttribute.CultureName
                            || uCaseAttribute.CultureName.Equals("Invariant", StringComparison.OrdinalIgnoreCase))
                    {
                        cInfo = CultureInfo.InvariantCulture;
                    }
                    else
                    {
                        cInfo = CultureInfo.GetCultureInfo(uCaseAttribute.CultureName);
                    }
                    foreach (PropertyInfo propInfo in objectType.GetProperties())
                    {
                        if (propInfo.PropertyType == typeof(string))
                        {
                            string propValue = propInfo.GetValue(upperCaseThis).ToString();
                            if (!string.IsNullOrEmpty(propValue))
                            {
                                string upperValue = propValue.ToUpper(cInfo);
                                if (!upperValue.Equals(propValue, StringComparison.Ordinal))
                                {
                                    propInfo.SetValue(upperCaseThis, upperValue);
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
