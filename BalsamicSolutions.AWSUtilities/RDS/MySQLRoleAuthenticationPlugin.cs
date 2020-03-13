using System;
using System.Collections.Generic;
using System.Text;
using MySql.Data.MySqlClient;
using Amazon;
using Amazon.RDS.Util;
using Amazon.Runtime;
using MySql.Data.MySqlClient.Authentication;

namespace BalsamicSolutions.AWSUtilities.RDS
{
    /// <summary>
    /// IAM role authenticaiton plug in for IAM/RDS authentication
    /// provides EC2 role authentication to MySQL or MySQL aurora
    /// RDS clusters
    /// https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/UsingWithRDS.IAMDBAuth.html
    /// </summary>
    public class MySQLRoleAuthenticationPlugin : MySqlAuthenticationPluginBase
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
                  <add name="mysql_clear_password" type="ReportingXpress.Common.AWS.RDS.MySQLRoleAuthenticationPlugin, ReportingXpress.Common"></add>
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
            AWSCredentials roleCredentials = new InstanceProfileAWSCredentials();
            returnValue.AuthorizationTicket = RDSAuthTokenGenerator.GenerateAuthToken(roleCredentials, regionEndPoint, serverName, portNumber, userId);
            //tickets expire in 15 minutes, but Windows time drift is up to a minute in this case, so give it a buffer of 3 minutes
            returnValue.ExpiresUtc = DateTime.UtcNow.AddMinutes(14);
            return returnValue;
        }

    }
}
