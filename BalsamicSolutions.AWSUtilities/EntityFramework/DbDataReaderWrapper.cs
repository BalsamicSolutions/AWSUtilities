using System;
using System.Data.Common;
using System.Collections.Generic;
using System.Text;
using System.Data;

namespace BalsamicSolutions.AWSUtilities.EntityFramework
{
    /// <summary>
    /// wrapper around the data reader for dispose management
    /// that looks kind of like a RelationalDataReader
    /// </summary>
    public class DbDataReaderWrapper : IDisposable
    {
        private DbCommand _dbCommand = null;
        private DbDataReader _dbReader = null;
        private ConnectionState _InitialState = ConnectionState.Closed;

        public DbDataReaderWrapper(DbCommand cmd)
        {
            _dbCommand = cmd;
            _InitialState = _dbCommand.Connection.State;
            if (_InitialState == ConnectionState.Closed)
            {
                _dbCommand.Connection.Open();
            }
            _dbReader = _dbCommand.ExecuteReader();
        }

        public DbDataReader DbDataReader
        {
            get { return _dbReader; }
        }

        public void Dispose()
        {
            if (null != _dbReader)
            {
                _dbReader.Dispose();
                _dbReader = null;
            }
            
            if (null != _dbCommand)
            {
                if (_InitialState == ConnectionState.Closed)
                {
                    _dbCommand.Connection.Close();
                }
                _dbCommand.Dispose();
                _dbCommand = null;
            }
        }
    }
}
