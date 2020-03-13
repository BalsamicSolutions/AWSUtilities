using Amazon;
using Amazon.RDS.Util;
using Amazon.Runtime;
using System;


namespace BalsamicSolutions.AWSUtilities.RDS
{
    /// <summary>
    /// IAM user authenticaiton plug in for IAM/RDS authentication
    /// provides AWS user/API authentication to MySQL or MySQL aurora
    /// RDS clusters, uses credentials from the standard AWS Credentials configuration flow
    /// https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.html
    /// </summary>
    public class MySQLUserAuthenticationPlugin : MySqlAuthenticationPluginBase
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
                 <add name="mysql_clear_password" type="ReportingXpress.Common.AWS.RDS.MySQLUserAuthenticationPlugin, ReportingXpress.Common"></add>
               </AuthenticationPlugins>
             </MySQL>
        */

        ///// <summary>
        ///// generate a new RDS authentication ticket
        ///// </summary>
        ///// <param name="serverName"></param>
        ///// <param name="portNumber"></param>
        ///// <param name="userId"></param>
        ///// <returns></returns>
        protected override ExpiringRDSTicket GetRDSAuthenticationTicket(string serverName, int portNumber, string userId)
        {
            serverName = VerifyRdsAddress(serverName);
            ExpiringRDSTicket returnValue = new ExpiringRDSTicket();
            RegionEndpoint regionEndPoint = FallbackRegionFactory.GetRegionEndpoint();
            AWSCredentials awsCredentials = FallbackCredentialsFactory.GetCredentials();
            returnValue.AuthorizationTicket = RDSAuthTokenGenerator.GenerateAuthToken(awsCredentials, regionEndPoint, serverName, portNumber, userId);
            //tickets expire in 15 minutes, but Windows time drift is up to a minute in this case, so give it a buffer of 3 minutes
            returnValue.ExpiresUtc = DateTime.UtcNow.AddMinutes(14);
            return returnValue;
        }
    }
}
