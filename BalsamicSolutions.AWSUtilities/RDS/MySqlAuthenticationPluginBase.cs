//  -----------------------------------------------------------------------------
//   Copyright  (c) Balsamic Solutions, LLC. All rights reserved.
//   THIS CODE IS PROVIDED *AS IS* WITHOUT WARRANTY OF  ANY KIND, EITHER
//   EXPRESS OR IMPLIED, INCLUDING ANY IMPLIED WARRANTIES OF FITNESS FOR
//  -----------------------------------------------------------------------------
using MySql.Data.MySqlClient.Authentication;
using System;
using System.Collections.Generic;
using System.Text;
using Ubiety.Dns.Core;

namespace BalsamicSolutions.AWSUtilities.RDS
{
    /// <summary>
    /// base class for both authentication plugins
    /// </summary>
    public class MySqlAuthenticationPluginBase : MySqlAuthenticationPlugin
    {
        /// <summary>
        /// for caching the ticket
        /// </summary>
        public class ExpiringRDSTicket
        {
            internal string AuthorizationTicket { get; set; }
            internal DateTime ExpiresUtc { get; set; }
        }

        private static Dictionary<string, ExpiringRDSTicket> _TicketCache = new Dictionary<string, ExpiringRDSTicket>(StringComparer.OrdinalIgnoreCase);
        private static object _LockProxy = new object();

        /// <summary>
        /// PluginName is always mysql_clear_password
        /// </summary>
        public override string PluginName
        {
            get { return "mysql_clear_password"; }
        }

        /// <summary>
        /// GetUsername
        /// </summary>
        /// <returns></returns>
        public override string GetUsername()
        {
            return base.GetUsername();
        }

        /// <summary>
        /// checks to see if the name has a cname
        /// we use Ubiety because MySQL uses it and
        /// we dont want to introduce a new DNS class
        /// </summary>
        /// <param name="hostName"></param>
        /// <returns></returns>
        public static string GetCNameOfHostOrNull(string hostName)
        {
 
            Resolver dnsLookup = ResolverBuilder.Begin()
                .SetTimeout(1000)
                .SetRetries(3)
                .UseRecursion()
                .Build();
            string returnValue = null;
            Response dnsResponse = dnsLookup.Query(hostName, Ubiety.Dns.Core.Common.QuestionType.CNAME, Ubiety.Dns.Core.Common.QuestionClass.IN);
            List<Ubiety.Dns.Core.Records.General.RecordCname> cnameRecords = dnsResponse.GetRecords<Ubiety.Dns.Core.Records.General.RecordCname>();
            if (cnameRecords.Count > 0)
            {
                returnValue = cnameRecords[0].Cname;
            }

            return returnValue;
        }

        /// <summary>
        /// if a server name is actually a CNAME that points to an
        /// RDS instance or cluster, we resolve it here
        /// </summary>
        /// <param name="hostAddress"></param>
        /// <returns></returns>
        protected string VerifyRdsAddress(string hostAddress)
        {
            const string RDSSuffix = ".rds.amazonaws.com";
            string returnValue = hostAddress.ToLowerInvariant();
            if (!returnValue.EndsWith(RDSSuffix) && hostAddress.IndexOf('.') > -1)
            {
                string cName = GetCNameOfHostOrNull(hostAddress);
                if (!string.IsNullOrEmpty(cName))
                {
                    returnValue = cName.Trim(new char[] { ' ', '.' });
                }
            }
            return returnValue;
        }

        /// <summary>
        /// Called to continue the authentication after the challenge for mysql_clear_password
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        protected override byte[] MoreData(byte[] data)
        {
            string rdsPassword = GetRDSAuthenticationPassword(Settings.Server, (int)Settings.Port, Settings.UserID);
            byte[] passBytes = Encoding.GetBytes(rdsPassword);
            byte[] returnValue = new byte[passBytes.Length + 1];
            Array.Copy(passBytes, 0, returnValue, 0, passBytes.Length);
            //zero terminate the array to indicate the end of the string
            returnValue[returnValue.Length - 1] = 0 & 0xFF;
            return returnValue;
        }

        /// <summary>
        /// never called in our scenairo
        /// </summary>
        /// <returns></returns>
        public override object GetPassword()
        {
            byte[] passBytes = Encoding.GetBytes(Settings.Password);
            byte[] returnValue = new byte[passBytes.Length + 1];
            Array.Copy(passBytes, 0, returnValue, 0, passBytes.Length);
            //zero terminate the array to indicate the end of the string
            returnValue[returnValue.Length - 1] = 0 & 0xFF;
            return returnValue;
        }

        /// <summary>
        /// get an IAM RDS encoded password
        /// </summary>
        /// <param name="serverName"></param>
        /// <param name="portNumber"></param>
        /// <param name="userId"></param>
        /// <returns></returns>
        protected string GetRDSAuthenticationPassword(string serverName, int portNumber, string userId)
        {
            string returnValue = null;
            string keyName = string.Format("{0}:{1}:{2}", serverName, portNumber, userId);
            DateTime utcNow = DateTime.UtcNow;
            lock (_LockProxy)
            {
                //check cache first
                ExpiringRDSTicket rdsTicket = null;
                if (_TicketCache.TryGetValue(keyName, out rdsTicket))
                {
                    if (rdsTicket.ExpiresUtc <= utcNow) rdsTicket = null;
                }

                if (null == rdsTicket)
                {
                    //generate a new ticket
                    rdsTicket = GetRDSAuthenticationTicket(serverName, portNumber, userId);
                    _TicketCache[keyName] = rdsTicket;
                }
                returnValue = rdsTicket.AuthorizationTicket;
            }
            return returnValue;
        }

        /// <summary>
        /// handles the "cheater" way of installing without a config file
        /// </summary>
        /// <param name="pluginTypeAssemblyQualifiedName"></param>
        private static void RegisterPlugin(string pluginTypeAssemblyQualifiedName)
        {
            Type t = typeof(MySqlAuthenticationPlugin);
            Type pluginManagerType = t.Assembly.GetType("MySql.Data.MySqlClient.Authentication.AuthenticationPluginManager");
            System.Reflection.FieldInfo plugins = pluginManagerType.GetField("Plugins", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Static);
            object obj = plugins.GetValue(null);
            System.Reflection.PropertyInfo piItem = obj.GetType().GetProperty("Item");
            Type pluginType = t.Assembly.GetType("MySql.Data.MySqlClient.Authentication.PluginInfo");
            object pluginInfo = Activator.CreateInstance(pluginType, pluginTypeAssemblyQualifiedName);
            piItem.SetValue(obj, pluginInfo, new object[] { "mysql_clear_password" });
        }

        /// <summary>
        /// registers the user plug in directly
        /// </summary>
        public static void RegisterUserPlugin()
        {
            RegisterPlugin(typeof(MySQLUserAuthenticationPlugin).AssemblyQualifiedName);
        }

        /// <summary>
        /// registers the role plug in directly
        /// </summary>
        public static void RegisterRolePlugin()
        {
            RegisterPlugin(typeof(MySQLRoleAuthenticationPlugin).AssemblyQualifiedName);
        }

        /// <summary>
        /// role and user implementations override this
        /// </summary>
        /// <param name="serverName"></param>
        /// <param name="portNumber"></param>
        /// <param name="userId"></param>
        /// <returns></returns>
        protected virtual ExpiringRDSTicket GetRDSAuthenticationTicket(string serverName, int portNumber, string userId)
        {
            throw new NotImplementedException();
        }
    }
}