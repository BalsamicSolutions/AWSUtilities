//  -----------------------------------------------------------------------------
//   Copyright  (c) Balsamic Solutions, LLC. All rights reserved.
//   THIS CODE IS PROVIDED *AS IS* WITHOUT WARRANTY OF  ANY KIND, EITHER
//   EXPRESS OR IMPLIED, INCLUDING ANY IMPLIED WARRANTIES OF FITNESS FOR
//  -----------------------------------------------------------------------------
using Amazon.SecretsManager;
using Amazon.SecretsManager.Model;
 
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
using BalsamicSolutions.AWSUtilities.Extensions;

namespace BalsamicSolutions.AWSUtilities.RDS
{
    /// <summary>
    ///https://dev.mysql.com/doc/refman/8.0/en/sha256-pluggable-authentication.html
    /// user authenticaiton plug in for IAM/RDS authentication
    /// this uses the AWS Secrets manager to extract the password for use by the
    /// connection string. This one does not use plain text password, instead it
    /// uses SHA256, mostly  this code was lifted from the MySql.Data.MySqlClient.Authentication.Sha256AuthenticationPlugin
    ///
    /// A user for access might be created with this CREATE USER 'sha256user'@'%'  IDENTIFIED WITH sha256_password BY 'P@$$w0rd!';
    /// The secret (by default) would be named by the name of the server then the user delimited by a / (for example)
    /// rptprod.reportingxpress.org/sha256user and the key pair would be password=P@$$w0rd!
    /// If you need to run over non-ssl connections you will also need to store the public key from
    /// your server. You can get the public key in text form with this command SHOW STATUS LIKE 'Rsa_public_key'
    /// The public key should be stored in the same secret with the key name publickey
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
        /// typed storage of secret data
        /// </summary>
        private class CachedSecret
        {
            public DateTime Expires { get; set; }
            public string JsonText { get; set; }
        }

        /// <summary>
        /// lock
        /// </summary>
        private static readonly object _LockProxy = new object();

        /// <summary>
        /// cache object
        /// </summary>
        private static readonly Dictionary<string, CachedSecret> _SecretCache = new Dictionary<string, CachedSecret>();

        /// <summary>
        /// stores the calculated secret name
        /// </summary>
        private string _AWSSecretName = null;

        /// <summary>
        /// The byte array representation of the public key provided by the server.
        /// </summary>
        private byte[] _PublicKey = null;

        /// <summary>
        /// matching the MySQL name
        /// </summary>
        public override string PluginName
        {
            get { return "sha256_password"; }
        }

        /// <summary>
        /// Connector.NET does not support ServerRSAPublicKeyFile so we expect
        /// it to be set externally
        /// path to public key file if its not encoded in the secret
        /// </summary>
        static internal string PublicKeyFilePath { get; set; }

        /// <summary>
        /// stores the secret name template
        /// </summary>
        static internal string SecretNameTemplate { get; set; }

        /// <summary>
        /// calculates the secret name
        /// </summary>
        private string AWSSecretName
        {
            get
            {
                if (null == _AWSSecretName)
                {
                    _AWSSecretName = SecretNameTemplate;
                    if (_AWSSecretName.IndexOf('{') > -1)
                    {
                        //the secret name is a template
                        _AWSSecretName = _AWSSecretName.Replace("{server}", Settings.Server);
                        _AWSSecretName = _AWSSecretName.Replace("{userid}", Settings.UserID);
                        _AWSSecretName = _AWSSecretName.Replace("{database}", Settings.Database);
                    }
                }
                return _AWSSecretName;
            }
        }

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
                    return GetSecret(AWSSecretName, "password");
                }
            }
        }

        /// <summary>
        /// gets the public key from the secret or the local file system
        /// </summary>
        private byte[] PublicKey
        {
            get
            {
                if (null == _PublicKey)
                {
                    string pemText = null;
                    if (!PublicKeyFilePath.IsNullOrWhiteSpace() && System.IO.File.Exists(PublicKeyFilePath))
                    {
                        pemText = System.IO.File.ReadAllText(PublicKeyFilePath);
                    }
                    else
                    {
                        pemText = GetSecret(AWSSecretName, "publickey");
                    }
                    if (!pemText.IsNullOrWhiteSpace())
                    {
                        _PublicKey = System.Text.Encoding.ASCII.GetBytes(pemText);
                    }
                }
                return _PublicKey;
            }
            set
            {
                _PublicKey = value;
            }
        }

        /// <summary>
        /// implementation of GetPassword
        /// </summary>
        /// <returns></returns>
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
                if (Password.Length == 0)
                {
                    return new byte[1];
                }
                // send RSA encrypted, since the channel is not protected
                else if (PublicKey == null)
                {
                    return new byte[] { 0x01 };
                }
                // We do not provide the public key on the callback
                // we expect it in the AWS secret so this is now ignored
                //else if (!Settings.AllowPublicKeyRetrieval)
                //{
                //    throw new MySQLSecretException("RSAPublicKeyRetrievalNotEnabled");
                //}
                else
                {
                    byte[] bytes = GetRsaPassword(Password, AuthenticationData, PublicKey);
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

        /// <summary>
        /// implementation of more data
        /// </summary>
        /// <param name="data"></param>
        /// <returns></returns>
        protected override byte[] MoreData(byte[] data)
        {
            //if data is provided its supposed to be the server public key
            //technically we dont care about this any more as we never
            //actually ask the server for it
            //if (null != data) PublicKey = data;
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
            else
            {
                return GetPassword() as byte[];
            }
        }

        private byte[] GetRsaPassword(string password, byte[] seedBytes, byte[] rawPublicKey)
        {
            byte[] returnValue = null;
            if (password.Length == 0)
            {
                returnValue = new byte[1];
            }
            else
            {

                // Obfuscate the plain text password with the session scramble
                byte[] obfuscated = GetXor(Encoding.Default.GetBytes(password), seedBytes);
                // Encrypt the password and send it to the server

                RSACryptoServiceProvider rsa = MySqlPemReader.ConvertPemToRSAProvider(rawPublicKey);
                if (rsa == null) throw new MySQLSecretException("Unable To ReadRSA Key");

                returnValue = rsa.Encrypt(obfuscated, true);
            }
            return returnValue;
        }

        /// <summary>
        /// get the secret from AWS Secrets manager
        /// </summary>
        /// <param name="secretName"></param>
        /// <returns></returns>
        private string GetSecret(string secretName, string keyName)
        {
            string returnValue = null;
            lock (_LockProxy)
            {
                if (_SecretCache.TryGetValue(secretName, out CachedSecret cachedSecret))
                {
                    dynamic jsonObj = cachedSecret.JsonText.FromJson();
                    if (cachedSecret.Expires >= DateTime.UtcNow)
                    {
                        returnValue = jsonObj[keyName];
                    }
                    else
                    {
                        string passwordCacheKey = jsonObj["password"];
                        _SecretCache.Remove(secretName);
                    }
                }
            }
            if (returnValue.IsNullOrWhiteSpace())
            {
                using (AmazonSecretsManagerClient secretsClient = new AmazonSecretsManagerClient())
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
                        returnValue = jsonObj[keyName];
                        CachedSecret cachedSecret = new CachedSecret
                        {
                            JsonText = jsonText,
                            Expires = DateTime.UtcNow.AddMinutes(5)
                        };
                        lock (_LockProxy)
                        {
                            _SecretCache[secretName] = cachedSecret;
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
            }
            return returnValue;
        }

        /// <summary>
        /// typed exception
        /// </summary>
        [Serializable]
        public sealed class MySQLSecretException : System.Data.Common.DbException
        {
            internal MySQLSecretException(string msg)
            : base(msg)
            {
            }
        }

    }
}