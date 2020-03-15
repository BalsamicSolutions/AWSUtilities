//  -----------------------------------------------------------------------------
//   Copyright  (c) Balsamic Solutions, LLC. All rights reserved.
//   THIS CODE IS PROVIDED *AS IS* WITHOUT WARRANTY OF  ANY KIND, EITHER
//   EXPRESS OR IMPLIED, INCLUDING ANY IMPLIED WARRANTIES OF FITNESS FOR
//  -----------------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Reflection;
using System.Security.Cryptography;
using BalsamicSolutions.AWSUtilities.Extensions;

namespace Demo.Data.Testing
{
    /// <summary>
    /// this class is used to generate random
    /// data for testing, it can genrate names
    /// phone numbers, and sentances of various lengths.
    /// All data is in en-US
    /// </summary>
    public static class RandomStuff
    {
        private static readonly Random _RandomNumberGenerator = NewRandomGenerator();
        private const string LOREM_IPSUM = "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum";
        private static IList<string> _GivenNames = null;
        private static IList<string> _FemaleSurNames = null;
        private static IList<string> _MaleSurNames = null;
        private static IList<string> _Sentances = null;

        /// <summary>
        /// generates a random datetime value
        /// as defined by the "nearness" paramaters
        /// </summary>
        /// <param name="nearNessInSeconds"></param>
        /// <param name="nearNessInMinutes"></param>
        /// <param name="nearNessInHours"></param>
        /// <param name="nearNessInDays"></param>
        /// <param name="nearNessInYears"></param>
        /// <returns>DateTime</returns>
        public static DateTime RandomDatetime(int nearNessInSeconds = 10, int nearNessInMinutes = 30, int nearNessInHours = 12, int nearNessInDays = 15, int nearNessInYears = 0)
        {
            long nextNow = DateTime.Now.Ticks;
            int changeIndex = _RandomNumberGenerator.Next(0, nearNessInSeconds);
            long changeValue = changeIndex * TimeSpan.TicksPerSecond;
            changeIndex = _RandomNumberGenerator.Next(0, nearNessInMinutes);
            changeValue += changeIndex * TimeSpan.TicksPerMinute;
            changeIndex = _RandomNumberGenerator.Next(0, nearNessInHours);
            changeValue += changeIndex * TimeSpan.TicksPerHour;
            changeIndex = _RandomNumberGenerator.Next(0, nearNessInDays);
            changeValue += changeIndex * TimeSpan.TicksPerDay;
            changeIndex = _RandomNumberGenerator.Next(0, nearNessInYears);
            changeValue += changeIndex * (TimeSpan.TicksPerDay * 365);
            if (_RandomNumberGenerator.Next(0, 9) > 5)
            {
                nextNow += changeValue;
            }
            else
            {
                nextNow -= changeValue;
            }
            return new DateTime(nextNow);
        }

        /// <summary>
        /// randomize the order of a list object
        /// </summary>
        /// <typeparam name="TObjectType">type of object in list</typeparam>
        /// <param name="shuffleThis">list to randomize</param>
        public static void RandomizeList<TObjectType>(IList<TObjectType> shuffleThis)
        {
            int listIdx = shuffleThis.Count;
            while (listIdx > 1)
            {
                listIdx--;
                int newPos = _RandomNumberGenerator.Next(listIdx + 1);
                TObjectType value = shuffleThis[newPos];
                shuffleThis[newPos] = shuffleThis[listIdx];
                shuffleThis[listIdx] = value;
            }
        }

        /// <summary>
        /// randomize the order of an array
        /// </summary>
        /// <param name="shuffleThis">array to randomize</param>
        public static void RandomizeArray(object[] shuffleThis)
        {
            int listIdx = shuffleThis.Length;
            while (listIdx > 1)
            {
                listIdx--;
                int newPos = _RandomNumberGenerator.Next(listIdx + 1);
                object value = shuffleThis[newPos];
                shuffleThis[newPos] = shuffleThis[listIdx];
                shuffleThis[listIdx] = value;
            }
        }

        /// <summary>
        /// Generate a random phone number in teh same area code
        /// and exchange as the provided phone number
        /// </summary>
        /// <param name="adjacentNumber"></param>
        /// <returns>phone number </returns>
        public static string RandomPhoneNumber(string adjacentNumber)
        {
            adjacentNumber = adjacentNumber.Replace("(", string.Empty).Replace(")", string.Empty).Replace("-", string.Empty);
            string returnValue = "(" + adjacentNumber.Substring(0, 3) + ")";
            if (adjacentNumber.Length > 5)
            {
                returnValue += adjacentNumber.Substring(3, 3);
            }
            else
            {
                returnValue += _RandomNumberGenerator.Next(200, 999).ToString();
            }
            returnValue += "-" + _RandomNumberGenerator.Next(1000, 9999).ToString();

            return returnValue;
        }

        /// <summary>
        /// generate a random US formated phone number
        /// </summary>
        /// <returns>phone number</returns>
        public static string RandomPhoneNumber()
        {
            string returnValue = "(";
            returnValue += _RandomNumberGenerator.Next(200, 799).ToString();
            returnValue += ")";
            returnValue += _RandomNumberGenerator.Next(200, 999).ToString();
            returnValue += "-" + _RandomNumberGenerator.Next(1000, 9999).ToString();

            return returnValue;
        }

        /// <summary>
        /// select an item from the array
        /// </summary>
        /// <param name="arrayOfValues">data </param>
        /// <returns>string </returns>
        private static string RandomSelect(string[] arrayOfValues)
        {
            string returnValue = arrayOfValues[0];
            if (arrayOfValues.Length > 1)
            {
                returnValue = arrayOfValues[_RandomNumberGenerator.Next(0, arrayOfValues.Length)];
            }
            return returnValue;
        }

        /// <summary>
        /// generate a random sentance
        /// </summary>
        /// <param name="minLength">minimum length of the sentance</param>
        /// <param name="maxLength">maximum length </param>
        /// <returns>sentance</returns>
        public static string RandomSentance(int minLength = 1, int maxLength = 4096)
        {
            return RandomSentance(Sentences, minLength, maxLength);
        }

        /// <summary>
        /// get a sentance from an array and trim it to the length specified
        /// </summary>
        /// <param name="candidateText">text </param>
        /// <param name="minLength">minimum length of the sentance</param>
        /// <param name="maxLength">maximum length </param>
        /// <returns>sentance</returns>
        public static string RandomSentance(IList<string> candidateText, int minLength = 1, int maxLength = 4096)
        {
            string returnValue = string.Empty;
            while (returnValue.Length < minLength)
            {
                returnValue += candidateText[_RandomNumberGenerator.Next(0, candidateText.Count)];
                for (int idx = 0; idx < _RandomNumberGenerator.Next(0, 3); idx++)
                {
                    returnValue += candidateText[_RandomNumberGenerator.Next(0, candidateText.Count)];
                }
            }
            return returnValue.TrimTo(maxLength);
        }

        /// <summary>
        /// get a random last name
        /// </summary>
        /// <returns>last name</returns>
        public static string RandomGivenName()
        {
            return GivenNames[_RandomNumberGenerator.Next(0, _GivenNames.Count)];
        }

        /// <summary>
        /// get a random first name
        /// </summary>
        /// <returns>first name </returns>
        public static string RandomSurName()
        {
            if (_RandomNumberGenerator.Next(0, 100) > 60)
            {
                return FemaleSurNames[_RandomNumberGenerator.Next(0, _FemaleSurNames.Count)];
            }
            else
            {
                return MaleSurNames[_RandomNumberGenerator.Next(0, _MaleSurNames.Count)];
            }
        }

        /// <summary>
        /// return an array of all candidate sentences
        /// </summary>
        public static IList<string> Sentences
        {
            get
            {
                if (null == _Sentances)
                {
                    _Sentances = LoadText("Demo.Data.Testing.Sentences.txt").AsReadOnly();
                }
                return _Sentances;
            }
        }

        /// <summary>
        /// return a list of all last names
        /// </summary>
        public static IList<string> GivenNames
        {
            get
            {
                if (null == _GivenNames)
                {
                    _GivenNames = LoadNames("Demo.Data.Testing.dist.all.last.txt").AsReadOnly();
                }
                return _GivenNames;
            }
        }

        /// <summary>
        /// return a list of all first names
        /// </summary>
        /// <returns></returns>
        public static IList<string> SurNames()
        {
            List<string> returnValue = new List<string>();
            returnValue.AddRange(FemaleSurNames);
            returnValue.AddRange(MaleSurNames);
            return returnValue.AsReadOnly();
        }

        /// <summary>
        /// return a list of female first names
        /// </summary>
        public static IList<string> FemaleSurNames
        {
            get
            {
                if (null == _FemaleSurNames)
                {
                    _FemaleSurNames = LoadNames("Demo.Data.Testing.dist.female.first.txt").AsReadOnly();
                }
                return _FemaleSurNames;
            }
        }

        /// <summary>
        /// return a list of male first names
        /// </summary>
        public static IList<string> MaleSurNames
        {
            get
            {
                if (null == _MaleSurNames)
                {
                    _MaleSurNames = LoadNames("Demo.Data.Testing.dist.male.first.txt").AsReadOnly();
                }
                return _MaleSurNames;
            }
        }

        /// <summary>
        /// initialize the name arrays
        /// </summary>
        /// <param name="resName"></param>
        /// <returns>all of the names </returns>
        private static List<string> LoadNames(string resName)
        {
            List<string> returnValue = new List<string>();
            var thisAssm = Assembly.GetExecutingAssembly();
            Stream ioStream = thisAssm.GetManifestResourceStream(resName);
            using (StreamReader srNames = new StreamReader(ioStream))
            {
                string lineText = srNames.ReadLine();
                while (null != lineText && lineText.Length > 0)
                {
                    string nameText = lineText.Trim();
                    string prettyName = nameText.Substring(0, 1).ToUpper() + nameText.Substring(1).ToLower();
                    returnValue.Add(prettyName);
                    lineText = srNames.ReadLine();
                }
            }
            return returnValue;
        }

        /// <summary>
        /// load embeded resource strings as text
        /// </summary>
        /// <param name="resName"></param>
        /// <returns></returns>
        private static List<string> LoadText(string resName)
        {
            List<string> returnValue = new List<string>();
            var thisAssm = Assembly.GetExecutingAssembly();
            Stream ioStream = thisAssm.GetManifestResourceStream(resName);
            using (StreamReader srNames = new StreamReader(ioStream))
            {
                string lineText = srNames.ReadLine();
                while (null != lineText && lineText.Length > 0)
                {
                    returnValue.Add(lineText);
                    lineText = srNames.ReadLine();
                }
            }
            return returnValue;
        }

             /// <summary>
        /// generate a random list of users from the provided email domains 
        /// and active directory domains
        /// </summary>
        /// <param name="numOfNames">number of names to generate</param>
        /// <param name="emailDomains">array valid email domain suffixes</param>
        /// <param name="adDomains">valid active directory domain names</param>
        /// <returns></returns>
        public static List<NameInfo> RandomNames(int numOfNames, string[] emailDomains=null, string[] adDomains=null, int minNameLength = 4)
        {
            if (null == emailDomains || emailDomains.Length == 0)
            {
                emailDomains = new string[] { "@domain.email"};
            }
            if (null == adDomains || adDomains.Length == 0)
            {
                adDomains = new string[] { "@domain.local" };
            }
            for (int mailIdx = 0; mailIdx < emailDomains.Length; mailIdx++)
            {
                if (!emailDomains[mailIdx].StartsWith("@"))
                {
                    emailDomains[mailIdx] = "@" + emailDomains[mailIdx];
                }
            }
            for (int mailIdx = 0; mailIdx < adDomains.Length; mailIdx++)
            {
                if (!adDomains[mailIdx].StartsWith("@"))
                {
                    adDomains[mailIdx] = "@" + adDomains[mailIdx];
                }
            }
            Dictionary<string, NameInfo> uniqueValues = new Dictionary<string, NameInfo>();
            List<NameInfo> returnValue = new List<NameInfo>();
            while (returnValue.Count < numOfNames)
            {
                string firstName = RandomSurName();
                string lastName = RandomGivenName();
                string emailDomain = RandomSelect(emailDomains);
                string adDomain = RandomSelect(adDomains);
                NameInfo addMe = new NameInfo(firstName, lastName, emailDomain, adDomain);
                if (!uniqueValues.ContainsKey(addMe.UserPrincipalName)
                    && !uniqueValues.ContainsKey(addMe.sAMAccountName)
                    && !uniqueValues.ContainsKey(addMe.Email)
                    && addMe.sAMAccountName.Length>=minNameLength)
                {
                    uniqueValues.Add(addMe.UserPrincipalName,addMe);
                    uniqueValues.Add(addMe.Email, addMe);
                    uniqueValues.Add(addMe.sAMAccountName, addMe);
                    returnValue.Add(addMe);
                }
            }
            return returnValue;
        
        }

        /// <summary>
        /// create a random number generator
        /// </summary>
        /// <returns></returns>
        public static Random NewRandomGenerator()
        {
            byte[] randomBytes = new byte[4];

            // Generate 4 random bytes.
            RNGCryptoServiceProvider rngSeed = new RNGCryptoServiceProvider();
            rngSeed.GetBytes(randomBytes);

            // Convert 4 bytes into a 32-bit integer value.
            int randomSeed = (randomBytes[0] & 0x7f) << 24 |
                             randomBytes[1] << 16 |
                             randomBytes[2] << 8 |
                             randomBytes[3];
            return new Random(randomSeed);
        }
    }
}