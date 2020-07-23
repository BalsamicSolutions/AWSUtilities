//  -----------------------------------------------------------------------------
//   Copyright  (c) Balsamic Solutions, LLC. All rights reserved.
//   THIS CODE IS PROVIDED *AS IS* WITHOUT WARRANTY OF  ANY KIND, EITHER
//   EXPRESS OR IMPLIED, INCLUDING ANY IMPLIED WARRANTIES OF FITNESS FOR
//  -----------------------------------------------------------------------------
using Amazon.SecretsManager;
using Amazon.SecretsManager.Model;
using ReportingXpress.Common.Extensions;
using MySql.Data.MySqlClient;
using MySql.Data.MySqlClient.Authentication;
using System;
using System.Collections.Generic;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Memory;
using System.Collections.Concurrent;

namespace BalsamicSolutions.AWSUtilities.RDS
{
    /// <summary>
    /// user authenticaiton plug in for IAM/RDS authentication
    /// this uses the AWS Secrets manager to extract the password for use by the
    /// connection string. This one does not use plain text password, instead it
    /// uses SHA256, mostly  this code was lifted from the MySql.Data.MySqlClient.Authentication.Sha256AuthenticationPlugin
    /// a user for access might be created with this CREATE USER 'sha256user'@'%'  IDENTIFIED WITH sha256_password BY 'P@$$w0rd!';
    /// The secret (by default) would be named by the name of the server then the user (for example)
    /// rptprod.reportingxpress.org/sha256user and the key pair would be password P@$$w0rd!
    /// </summary>
    public class MySQLSecretAuthenticationPlugin : MySqlAuthenticationPlugin
    {
        /*
        Activation of this module requires an app.config file with a MySQL section, this applies to both  .NET Framework and .NET Core applications
        first add a new section to config sections for MySQL
        <section name="MySQL" type="MySql.Data.MySqlClient.MySqlConfiguration,MySql.Data"/>
        then add a MySQL section that looks like this
            <MySQL>
              <Replication>
                <ServerGroups>
                </ServerGroups>
              </Replication>
              <CommandInterceptors/>
              <ExceptionInterceptors/>
              <AuthenticationPlugins>
                <add name="sha256_password" type="ReportingXpress.Common.AWS.RDS.MySQLSecretAuthenticationPlugin, ReportingXpress.Common"></add>
              </AuthenticationPlugins>
            </MySQL>
       */

        /// <summary>
        /// The byte array representation of the public key provided by the server.
        /// </summary>
        protected byte[] rawPubkey;

        private static readonly Dictionary<string, CachedPassword> _Cache = new Dictionary<string, CachedPassword>();

        private static readonly object _LockProxy = new object();

        public override string PluginName
        {
            get { return "sha256_password"; }
        }

        static internal string SecretName { get; set; }

        /// <summary>
        /// gets the password from the secrets manager
        /// </summary>
        private string Password
        {
            get
            {
                if (!Settings.Password.IsNullOrWhiteSpace())
                {
                    return Settings.Password;
                }
                else
                {
                    string secretName = SecretName;
                    if (secretName.IndexOf('{') > -1)
                    {
                        //the secret name is a template
                        secretName = secretName.Replace("{server}", Settings.Server);
                        secretName = secretName.Replace("{userid}", Settings.UserID);
                        secretName = secretName.Replace("{database}", Settings.Database);
                    }
                    return GetSecretPassword(secretName);
                }
            }
        }

        public override object GetPassword()
        {
            if (Settings.SslMode != MySqlSslMode.None)
            {
                // send as clear text, since the channel is already encrypted
                byte[] passBytes = Encoding.GetBytes(Password);
                byte[] buffer = new byte[passBytes.Length + 2];
                Array.Copy(passBytes, 0, buffer, 1, passBytes.Length);
                buffer[0] = (byte)(passBytes.Length + 1);
                buffer[buffer.Length - 1] = 0x00;
                return buffer;
            }
            else
            {
                if (Password.Length == 0) return new byte[1];
                // send RSA encrypted, since the channel is not protected
                else if (rawPubkey == null) return new byte[] { 0x01 };
                else if (!Settings.AllowPublicKeyRetrieval)
                {
                    throw new MySQLSecretException("RSAPublicKeyRetrievalNotEnabled");
                }
                else
                {
                    byte[] bytes = GetRsaPassword(Password, AuthenticationData, rawPubkey);
                    if (bytes != null && bytes.Length == 1 && bytes[0] == 0) return null;
                    return bytes;
                }
            }
        }

        /// <summary>
        /// Applies XOR to the byte arrays provided as input.
        /// </summary>
        /// <returns>A byte array that contains the results of the XOR operation.</returns>
        protected byte[] GetXor(byte[] src, byte[] pattern)
        {
            byte[] src2 = new byte[src.Length + 1];
            Array.Copy(src, 0, src2, 0, src.Length);
            src2[src.Length] = 0;
            byte[] result = new byte[src2.Length];
            for (int i = 0; i < src2.Length; i++)
            {
                result[i] = (byte)(src2[i] ^ (pattern[i % pattern.Length]));
            }
            return result;
        }

        protected override byte[] MoreData(byte[] data)
        {
            rawPubkey = data;
            byte[] buffer = GetNonLengthEncodedPassword();
            return buffer;
        }

        private byte[] GetNonLengthEncodedPassword()
        {
            // Required for AuthChange requests.
            if (Settings.SslMode != MySqlSslMode.None)
            {
                // Send as clear text, since the channel is already encrypted.
                byte[] passBytes = Encoding.GetBytes(Password);
                byte[] buffer = new byte[passBytes.Length + 1];
                Array.Copy(passBytes, 0, buffer, 0, passBytes.Length);
                buffer[passBytes.Length] = 0;
                return buffer;
            }
            else return GetPassword() as byte[];
        }

        private byte[] GetRsaPassword(string password, byte[] seedBytes, byte[] rawPublicKey)
        {
            if (password.Length == 0) return new byte[1];
            // Obfuscate the plain text password with the session scramble
            byte[] obfuscated = GetXor(Encoding.Default.GetBytes(password), seedBytes);
            // Encrypt the password and send it to the server
            RSACryptoServiceProvider rsa = MySqlPemReader.ConvertPemToRSAProvider(rawPublicKey);
            if (rsa == null) throw new MySQLSecretException("Unable To ReadRSA Key");
            return rsa.Encrypt(obfuscated, true);
        }

        /// <summary>
        /// get the secret from AWS Secrets manager
        /// </summary>
        /// <param name="secretName"></param>
        /// <returns></returns>
        private string GetSecretPassword(string secretName)
        {
            string[] nameParts = secretName.Split('/');
            string jsonKey = nameParts[nameParts.Length - 1];
            secretName = string.Join("/", nameParts, 0, nameParts.Length - 1);
            lock (_LockProxy)
            {
                if (_Cache.TryGetValue(secretName, out CachedPassword cachedPassword))
                {
                    if (cachedPassword.Expires < DateTime.UtcNow)
                    {
                        return cachedPassword.Password;
                    }
                    else
                    {
                        _Cache.Remove(secretName);
                    }
                }
            }
            string returnValue = null;
            using (AmazonSecretsManagerClient secretsClient = Utilities.SystemSecretsManagerClient())
            {
                var awsRequest = new GetSecretValueRequest
                {
                    SecretId = secretName,
                    VersionStage = "AWSCURRENT",
                };
                GetSecretValueResponse awsResponse = null;
                try
                {
                    awsResponse = Task.Run(async () => await secretsClient.GetSecretValueAsync(awsRequest)).Result;
                    string jsonText = awsResponse.SecretString;
                    dynamic jsonObj = jsonText.FromJson();
                    returnValue = jsonObj[jsonKey];
                    CachedPassword cachedPassword = new CachedPassword
                    {
                        Password = returnValue,
                        Expires = DateTime.UtcNow.AddMinutes(5)
                    };
                    lock (_LockProxy)
                    {
                        _Cache[secretName] = cachedPassword;
                    }
                }
                catch (ResourceNotFoundException)
                {
                    throw new MySQLSecretException("The requested secret " + secretName + " was not found");
                }
                catch (InvalidRequestException e)
                {
                    throw new MySQLSecretException("The request was invalid due to: " + e.Message);
                }
                catch (InvalidParameterException e)
                {
                    throw new MySQLSecretException("The request had invalid params: " + e.Message);
                }
            }
            return returnValue;
        }

        [Serializable]
        public sealed class MySQLSecretException : System.Data.Common.DbException
        {
            internal MySQLSecretException(string msg)
            : base(msg)
            {
            }
        }

        private class CachedPassword
        {
            public DateTime Expires { get; set; }
            public string Password { get; set; }
        }
    }

}