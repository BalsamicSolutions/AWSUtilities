using System;
using System.Collections.Generic;
using System.Text;

namespace Demo.Data
{
    public class SQSDemoQueueData
    {
        public long RecordId {get;set;}
        public string RandomData{get;set;}
        public string MoreRandomData{get;set;}


        public static SQSDemoQueueData RandomQueueData()
        {
            SQSDemoQueueData returnValue = new SQSDemoQueueData();
            returnValue.RecordId = DateTime.UtcNow.Ticks;
            returnValue.RandomData = Testing.RandomStuff.RandomGivenName();
            returnValue.MoreRandomData = Testing.RandomStuff.RandomSentance(100,1024);
            return returnValue;
        }
    }
}
