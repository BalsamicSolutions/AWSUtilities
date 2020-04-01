//  -----------------------------------------------------------------------------
//   Copyright  (c) Balsamic Solutions, LLC. All rights reserved.
//   THIS CODE IS PROVIDED *AS IS* WITHOUT WARRANTY OF  ANY KIND, EITHER
//   EXPRESS OR IMPLIED, INCLUDING ANY IMPLIED WARRANTIES OF FITNESS FOR
//  -----------------------------------------------------------------------------
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using BalsamicSolutions.AWSUtilities.Extensions;
using System.Linq;

namespace BalsamicSolutions.AWSUtilities.ElastiCache
{
    //TODO implement force reconnect on connection
    //      implement connection cleanup
 

    /// <summary>
    /// implementation of the Redis IDatabase
    /// that wraps all calls with a retry handler
    /// allows individual calls to recover from
    /// ElasticCache Redis cluster changes and
    /// network connectivity blips. The
    /// conneciton multiplexer alrady has reconnect
    /// functionality, but the individual calls do not
    /// </summary>
    public class RedisElastiCache : IDatabase
    {
        private string _ConnectionString = null;
        private int _DatabaseId = -1;
        private Lazy<ConnectionMultiplexer> _Multiplexer = null;
        private RedisRetryPolicy _RetryPolicy = null;
        private object _AsyncState = null;

        /// <summary>
        ///  CTOR
        /// </summary>
        /// <param name="connectionString">StackExchange.Redis formated connection string</param>
        public RedisElastiCache(string connectionString)
            : this(connectionString, new DefaultRedisRetryPolicy())
        {
        }

        /// <summary>
        ///  CTOR
        /// </summary>
        /// <param name="connectionString">StackExchange.Redis formated connection string</param>
        /// <param name="retryPolicy">Retry policy</param>
        public RedisElastiCache(string connectionString, RedisRetryPolicy retryPolicy)
             : this(connectionString, new DefaultRedisRetryPolicy(), -1)
        {
        }

        /// <summary>
        ///  CTOR
        /// </summary>
        /// <param name="connectionString">StackExchange.Redis formated connection string</param>
        /// <param name="retryPolicy">Retry policy</param>
        /// <param name="databaseId">Database ID (-1 for clusters or no preference)</param>
        public RedisElastiCache(string connectionString, RedisRetryPolicy retryPolicy, int databaseId)
            : this(connectionString, new DefaultRedisRetryPolicy(), -1, null)
        {
        }

        /// <summary>
        ///  CTOR
        /// </summary>
        /// <param name="connectionString">StackExchange.Redis formated connection string</param>
        /// <param name="retryPolicy">Retry policy</param>
        /// <param name="databaseId">Database ID (-1 for clusters or no preference)</param>
        /// <param name="asyncState">The async state to pass into the underlying databases</param>
        public RedisElastiCache(string connectionString, RedisRetryPolicy retryPolicy, int databaseId, object asyncState)
        {
            if (connectionString.IsNullOrWhiteSpace()) throw new ArgumentNullException(nameof(connectionString));
            if (null == retryPolicy) throw new ArgumentNullException(nameof(retryPolicy));
            _ConnectionString = connectionString;
            _RetryPolicy = retryPolicy;
            _Multiplexer = CreateMultiplexer();
            _DatabaseId = databaseId;
            _AsyncState = asyncState;
        }

        /// <summary>
        /// create the Redis ConnectionMultiplexer
        /// </summary>
        /// <returns></returns>
        private Lazy<ConnectionMultiplexer> CreateMultiplexer()
        {
            return new Lazy<ConnectionMultiplexer>(() => ConnectionMultiplexer.Connect(_ConnectionString));
        }

        /// <summary>
        /// get an actual IDatabase
        /// </summary>
        /// <returns></returns>
        private IDatabase GetDatabaseInternal()
        {
            return _Multiplexer.Value.GetDatabase(_DatabaseId, _AsyncState);
        }

        #region IDatabase

        public int Database => _DatabaseId;

        public IConnectionMultiplexer Multiplexer => _Multiplexer.Value;

        public IBatch CreateBatch(object asyncState = null)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<IBatch>(() => redisDb.CreateBatch(asyncState));
        }

        public ITransaction CreateTransaction(object asyncState = null)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<ITransaction>(() => redisDb.CreateTransaction(asyncState));
        }

        public RedisValue DebugObject(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisValue>(() => redisDb.DebugObject(key, flags));
        }

        public Task<RedisValue> DebugObjectAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisValue>(() => redisDb.DebugObjectAsync(key, flags));
        }

        public RedisResult Execute(string command, params object[] args)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisResult>(() => redisDb.Execute(command, args));
        }

        public RedisResult Execute(string command, ICollection<object> args, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisResult>(() => redisDb.Execute(command, args, flags));
        }

        public Task<RedisResult> ExecuteAsync(string command, params object[] args)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisResult>(() => redisDb.ExecuteAsync(command, args));
        }

        public Task<RedisResult> ExecuteAsync(string command, ICollection<object> args, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisResult>(() => redisDb.ExecuteAsync(command, args, flags));
        }

        public bool GeoAdd(RedisKey key, double longitude, double latitude, RedisValue member, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<bool>(() => redisDb.GeoAdd(key, longitude, latitude, member, flags));
        }

        public bool GeoAdd(RedisKey key, GeoEntry value, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<bool>(() => redisDb.GeoAdd(key, value, flags));
        }

        public long GeoAdd(RedisKey key, GeoEntry[] values, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.GeoAdd(key, values, flags));
        }

        public Task<bool> GeoAddAsync(RedisKey key, double longitude, double latitude, RedisValue member, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<bool>(() => redisDb.GeoAddAsync(key, longitude, latitude, member, flags));
        }

        public Task<bool> GeoAddAsync(RedisKey key, GeoEntry value, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<bool>(() => redisDb.GeoAddAsync(key, value, flags));
        }

        public Task<long> GeoAddAsync(RedisKey key, GeoEntry[] values, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.GeoAddAsync(key, values, flags));
        }

        public double? GeoDistance(RedisKey key, RedisValue member1, RedisValue member2, GeoUnit unit = GeoUnit.Meters, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<double?>(() => redisDb.GeoDistance(key, member1, member2, unit, flags));
        }

        public Task<double?> GeoDistanceAsync(RedisKey key, RedisValue member1, RedisValue member2, GeoUnit unit = GeoUnit.Meters, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<double?>(() => redisDb.GeoDistanceAsync(key, member1, member2, unit, flags));
        }

        public string[] GeoHash(RedisKey key, RedisValue[] members, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<string[]>(() => redisDb.GeoHash(key, members, flags));
        }

        public string GeoHash(RedisKey key, RedisValue member, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<string>(() => redisDb.GeoHash(key, member, flags));
        }

        public Task<string[]> GeoHashAsync(RedisKey key, RedisValue[] members, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<string[]>(() => redisDb.GeoHashAsync(key, members, flags));
        }

        public Task<string> GeoHashAsync(RedisKey key, RedisValue member, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<string>(() => redisDb.GeoHashAsync(key, member, flags));
        }

        public GeoPosition?[] GeoPosition(RedisKey key, RedisValue[] members, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<GeoPosition?[]>(() => redisDb.GeoPosition(key, members, flags));
        }

        public GeoPosition? GeoPosition(RedisKey key, RedisValue member, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<GeoPosition?>(() => redisDb.GeoPosition(key, member, flags));
        }

        public Task<GeoPosition?[]> GeoPositionAsync(RedisKey key, RedisValue[] members, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<GeoPosition?[]>(() => redisDb.GeoPositionAsync(key, members, flags));
        }

        public Task<GeoPosition?> GeoPositionAsync(RedisKey key, RedisValue member, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<GeoPosition?>(() => redisDb.GeoPositionAsync(key, member, flags));
        }

        public GeoRadiusResult[] GeoRadius(RedisKey key, RedisValue member, double radius, GeoUnit unit = GeoUnit.Meters, int count = -1, Order? order = null, GeoRadiusOptions options = GeoRadiusOptions.Default, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<GeoRadiusResult[]>(() => redisDb.GeoRadius(key, member, radius, unit, count, order, options, flags));
        }

        public GeoRadiusResult[] GeoRadius(RedisKey key, double longitude, double latitude, double radius, GeoUnit unit = GeoUnit.Meters, int count = -1, Order? order = null, GeoRadiusOptions options = GeoRadiusOptions.Default, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<GeoRadiusResult[]>(() => redisDb.GeoRadius(key, longitude, latitude, radius, unit, count, order, options, flags));
        }

        public Task<GeoRadiusResult[]> GeoRadiusAsync(RedisKey key, RedisValue member, double radius, GeoUnit unit = GeoUnit.Meters, int count = -1, Order? order = null, GeoRadiusOptions options = GeoRadiusOptions.Default, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<GeoRadiusResult[]>(() => redisDb.GeoRadiusAsync(key, member, radius, unit, count, order, options, flags));
        }

        public Task<GeoRadiusResult[]> GeoRadiusAsync(RedisKey key, double longitude, double latitude, double radius, GeoUnit unit = GeoUnit.Meters, int count = -1, Order? order = null, GeoRadiusOptions options = GeoRadiusOptions.Default, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<GeoRadiusResult[]>(() => redisDb.GeoRadiusAsync(key, longitude, latitude, radius, unit, count, order, options, flags));
        }

        public bool GeoRemove(RedisKey key, RedisValue member, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<bool>(() => redisDb.GeoRemove(key, member, flags));
        }

        public Task<bool> GeoRemoveAsync(RedisKey key, RedisValue member, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<bool>(() => redisDb.GeoRemoveAsync(key, member, flags));
        }

        public long HashDecrement(RedisKey key, RedisValue hashField, long value = 1, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.HashDecrement(key, hashField, value, flags));
        }

        public double HashDecrement(RedisKey key, RedisValue hashField, double value, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<double>(() => redisDb.HashDecrement(key, hashField, value, flags));
        }

        public Task<long> HashDecrementAsync(RedisKey key, RedisValue hashField, long value = 1, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.HashDecrementAsync(key, hashField, value, flags));
        }

        public Task<double> HashDecrementAsync(RedisKey key, RedisValue hashField, double value, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<double>(() => redisDb.HashDecrementAsync(key, hashField, value, flags));
        }

        public bool HashDelete(RedisKey key, RedisValue hashField, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<bool>(() => redisDb.HashDelete(key, hashField, flags));
        }

        public long HashDelete(RedisKey key, RedisValue[] hashFields, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.HashDelete(key, hashFields, flags));
        }

        public Task<bool> HashDeleteAsync(RedisKey key, RedisValue hashField, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<bool>(() => redisDb.HashDeleteAsync(key, hashField, flags));
        }

        public Task<long> HashDeleteAsync(RedisKey key, RedisValue[] hashFields, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.HashDeleteAsync(key, hashFields, flags));
        }

        public bool HashExists(RedisKey key, RedisValue hashField, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<bool>(() => redisDb.HashExists(key, hashField, flags));
        }

        public Task<bool> HashExistsAsync(RedisKey key, RedisValue hashField, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<bool>(() => redisDb.HashExistsAsync(key, hashField, flags));
        }

        public RedisValue HashGet(RedisKey key, RedisValue hashField, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisValue>(() => redisDb.HashGet(key, hashField, flags));
        }

        public RedisValue[] HashGet(RedisKey key, RedisValue[] hashFields, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisValue[]>(() => redisDb.HashGet(key, hashFields, flags));
        }

        public HashEntry[] HashGetAll(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<HashEntry[]>(() => redisDb.HashGetAll(key, flags));
        }

        public Task<HashEntry[]> HashGetAllAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<HashEntry[]>(() => redisDb.HashGetAllAsync(key, flags));
        }

        public Task<RedisValue> HashGetAsync(RedisKey key, RedisValue hashField, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisValue>(() => redisDb.HashGetAsync(key, hashField, flags));
        }

        public Task<RedisValue[]> HashGetAsync(RedisKey key, RedisValue[] hashFields, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisValue[]>(() => redisDb.HashGetAsync(key, hashFields, flags));
        }

        public Lease<byte> HashGetLease(RedisKey key, RedisValue hashField, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<Lease<byte>>(() => redisDb.HashGetLease(key, hashField, flags));
        }

        public Task<Lease<byte>> HashGetLeaseAsync(RedisKey key, RedisValue hashField, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<Lease<byte>>(() => redisDb.HashGetLeaseAsync(key, hashField, flags));
        }

        public long HashIncrement(RedisKey key, RedisValue hashField, long value = 1, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.HashIncrement(key, hashField, value, flags));
        }

        public double HashIncrement(RedisKey key, RedisValue hashField, double value, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<double>(() => redisDb.HashIncrement(key, hashField, value, flags));
        }

        public Task<long> HashIncrementAsync(RedisKey key, RedisValue hashField, long value = 1, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.HashIncrementAsync(key, hashField, value, flags));
        }

        public Task<double> HashIncrementAsync(RedisKey key, RedisValue hashField, double value, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<double>(() => redisDb.HashIncrementAsync(key, hashField, value, flags));
        }

        public RedisValue[] HashKeys(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisValue[]>(() => redisDb.HashKeys(key, flags));
        }

        public Task<RedisValue[]> HashKeysAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisValue[]>(() => redisDb.HashKeysAsync(key, flags));
        }

        public long HashLength(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.HashLength(key, flags));
        }

        public Task<long> HashLengthAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.HashLengthAsync(key, flags));
        }

        public IEnumerable<HashEntry> HashScan(RedisKey key, RedisValue pattern, int pageSize, CommandFlags flags)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<IEnumerable<HashEntry>>(() => redisDb.HashScan(key, pattern, pageSize, flags));
        }

        public IEnumerable<HashEntry> HashScan(RedisKey key, RedisValue pattern = default, int pageSize = 250, long cursor = 0, int pageOffset = 0, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<IEnumerable<HashEntry>>(() => redisDb.HashScan(key, pattern, pageSize, cursor, pageOffset, flags));
        }

        public IAsyncEnumerable<HashEntry> HashScanAsync(RedisKey key, RedisValue pattern = default, int pageSize = 250, long cursor = 0, int pageOffset = 0, CommandFlags flags = CommandFlags.None)
        {
            //This one causes us some grief as the we do not have a 
            //generic retry handler that can retry in the middle
            //of an iterator loop. So for now we cheat a bit
            IDatabase redisDb = GetDatabaseInternal();
            IEnumerable<HashEntry> hashValues = _RetryPolicy.ExecuteWithRetry<IEnumerable<HashEntry>>(() => redisDb.HashScan(key, pattern, pageSize, cursor, pageOffset, flags));
            AsyncEnumerable<HashEntry> returnValue = new AsyncEnumerable<HashEntry>(hashValues);
            return returnValue;
        }

        public void HashSet(RedisKey key, HashEntry[] hashFields, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            _RetryPolicy.ExecuteWithRetry(() => redisDb.HashSet(key, hashFields, flags));
        }

        public bool HashSet(RedisKey key, RedisValue hashField, RedisValue value, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<bool>(() => redisDb.HashSet(key, hashField, value, when, flags));
        }

        public Task HashSetAsync(RedisKey key, HashEntry[] hashFields, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync(() => redisDb.HashSetAsync(key, hashFields, flags));
        }

        public Task<bool> HashSetAsync(RedisKey key, RedisValue hashField, RedisValue value, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<bool>(() => redisDb.HashSetAsync(key, hashField, value, when, flags));
        }

        public long HashStringLength(RedisKey key, RedisValue hashField, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.HashStringLength(key, hashField, flags));
        }

        public Task<long> HashStringLengthAsync(RedisKey key, RedisValue hashField, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.HashStringLengthAsync(key, hashField, flags));
        }

        public RedisValue[] HashValues(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisValue[]>(() => redisDb.HashValues(key, flags));
        }

        public Task<RedisValue[]> HashValuesAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisValue[]>(() => redisDb.HashValuesAsync(key, flags));
        }

        public bool HyperLogLogAdd(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<bool>(() => redisDb.HyperLogLogAdd(key, value, flags));
        }

        public bool HyperLogLogAdd(RedisKey key, RedisValue[] values, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<bool>(() => redisDb.HyperLogLogAdd(key, values, flags));
        }

        public Task<bool> HyperLogLogAddAsync(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<bool>(() => redisDb.HyperLogLogAddAsync(key, value, flags));
        }

        public Task<bool> HyperLogLogAddAsync(RedisKey key, RedisValue[] values, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<bool>(() => redisDb.HyperLogLogAddAsync(key, values, flags));
        }

        public long HyperLogLogLength(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.HyperLogLogLength(key, flags));
        }

        public long HyperLogLogLength(RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.HyperLogLogLength(keys, flags));
        }

        public Task<long> HyperLogLogLengthAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.HyperLogLogLengthAsync(key, flags));
        }

        public Task<long> HyperLogLogLengthAsync(RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.HyperLogLogLengthAsync(keys, flags));
        }

        public void HyperLogLogMerge(RedisKey destination, RedisKey first, RedisKey second, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            _RetryPolicy.ExecuteWithRetry(() => redisDb.HyperLogLogMerge(destination, first, second, flags));
        }

        public void HyperLogLogMerge(RedisKey destination, RedisKey[] sourceKeys, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            _RetryPolicy.ExecuteWithRetry(() => redisDb.HyperLogLogMerge(destination, sourceKeys, flags));
        }

        public Task HyperLogLogMergeAsync(RedisKey destination, RedisKey first, RedisKey second, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync(() => redisDb.HyperLogLogMergeAsync(destination, first, second, flags));
        }

        public Task HyperLogLogMergeAsync(RedisKey destination, RedisKey[] sourceKeys, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync(() => redisDb.HyperLogLogMergeAsync(destination, sourceKeys, flags));
        }

        public EndPoint IdentifyEndpoint(RedisKey key = default, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<EndPoint>(() => redisDb.IdentifyEndpoint(key, flags));
        }

        public Task<EndPoint> IdentifyEndpointAsync(RedisKey key = default, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<EndPoint>(() => redisDb.IdentifyEndpointAsync(key, flags));
        }

        public bool IsConnected(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<bool>(() => redisDb.IsConnected(key, flags));
        }

        public bool KeyDelete(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<bool>(() => redisDb.KeyDelete(key, flags));
        }

        public long KeyDelete(RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.KeyDelete(keys, flags));
        }

        public Task<bool> KeyDeleteAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<bool>(() => redisDb.KeyDeleteAsync(key, flags));
        }

        public Task<long> KeyDeleteAsync(RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.KeyDeleteAsync(keys, flags));
        }

        public byte[] KeyDump(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<byte[]>(() => redisDb.KeyDump(key, flags));
        }

        public Task<byte[]> KeyDumpAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<byte[]>(() => redisDb.KeyDumpAsync(key, flags));
        }

        public bool KeyExists(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<bool>(() => redisDb.KeyExists(key, flags));
        }

        public long KeyExists(RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.KeyExists(keys, flags));
        }

        public Task<bool> KeyExistsAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<bool>(() => redisDb.KeyExistsAsync(key, flags));
        }

        public Task<long> KeyExistsAsync(RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.KeyExistsAsync(keys, flags));
        }

        public bool KeyExpire(RedisKey key, TimeSpan? expiry, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<bool>(() => redisDb.KeyExpire(key, expiry, flags));
        }

        public bool KeyExpire(RedisKey key, DateTime? expiry, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<bool>(() => redisDb.KeyExpire(key, expiry, flags));
        }

        public Task<bool> KeyExpireAsync(RedisKey key, TimeSpan? expiry, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<bool>(() => redisDb.KeyExpireAsync(key, expiry, flags));
        }

        public Task<bool> KeyExpireAsync(RedisKey key, DateTime? expiry, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<bool>(() => redisDb.KeyExpireAsync(key, expiry, flags));
        }

        public TimeSpan? KeyIdleTime(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<TimeSpan?>(() => redisDb.KeyIdleTime(key, flags));
        }

        public Task<TimeSpan?> KeyIdleTimeAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<TimeSpan?>(() => redisDb.KeyIdleTimeAsync(key, flags));
        }

        public void KeyMigrate(RedisKey key, EndPoint toServer, int toDatabase = 0, int timeoutMilliseconds = 0, MigrateOptions migrateOptions = MigrateOptions.None, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            _RetryPolicy.ExecuteWithRetry(() => redisDb.KeyMigrate(key, toServer, toDatabase, timeoutMilliseconds, migrateOptions, flags));
        }

        public Task KeyMigrateAsync(RedisKey key, EndPoint toServer, int toDatabase = 0, int timeoutMilliseconds = 0, MigrateOptions migrateOptions = MigrateOptions.None, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync(() => redisDb.KeyMigrateAsync(key, toServer, toDatabase, timeoutMilliseconds, migrateOptions, flags));
        }

        public bool KeyMove(RedisKey key, int database, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<bool>(() => redisDb.KeyMove(key, database, flags));
        }

        public Task<bool> KeyMoveAsync(RedisKey key, int database, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<bool>(() => redisDb.KeyMoveAsync(key, database, flags));
        }

        public bool KeyPersist(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<bool>(() => redisDb.KeyPersist(key, flags));
        }

        public Task<bool> KeyPersistAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<bool>(() => redisDb.KeyPersistAsync(key, flags));
        }

        public RedisKey KeyRandom(CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisKey>(() => redisDb.KeyRandom(flags));
        }

        public Task<RedisKey> KeyRandomAsync(CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisKey>(() => redisDb.KeyRandomAsync(flags));
        }

        public bool KeyRename(RedisKey key, RedisKey newKey, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<bool>(() => redisDb.KeyRename(key, newKey, when, flags));
        }

        public Task<bool> KeyRenameAsync(RedisKey key, RedisKey newKey, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<bool>(() => redisDb.KeyRenameAsync(key, newKey, when, flags));
        }

        public void KeyRestore(RedisKey key, byte[] value, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            _RetryPolicy.ExecuteWithRetry(() => redisDb.KeyRestore(key, value, expiry, flags));
        }

        public Task KeyRestoreAsync(RedisKey key, byte[] value, TimeSpan? expiry = null, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync(() => redisDb.KeyRestoreAsync(key, value, expiry, flags));
        }

        public TimeSpan? KeyTimeToLive(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<TimeSpan?>(() => redisDb.KeyTimeToLive(key, flags));
        }

        public Task<TimeSpan?> KeyTimeToLiveAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<TimeSpan?>(() => redisDb.KeyTimeToLiveAsync(key, flags));
        }

        public bool KeyTouch(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<bool>(() => redisDb.KeyTouch(key, flags));
        }

        public long KeyTouch(RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.KeyTouch(keys, flags));
        }

        public Task<bool> KeyTouchAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<bool>(() => redisDb.KeyTouchAsync(key, flags));
        }

        public Task<long> KeyTouchAsync(RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.KeyTouchAsync(keys, flags));
        }

        public RedisType KeyType(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisType>(() => redisDb.KeyType(key, flags));
        }

        public Task<RedisType> KeyTypeAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisType>(() => redisDb.KeyTypeAsync(key, flags));
        }

        public RedisValue ListGetByIndex(RedisKey key, long index, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisValue>(() => redisDb.ListGetByIndex(key, index, flags));
        }

        public Task<RedisValue> ListGetByIndexAsync(RedisKey key, long index, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisValue>(() => redisDb.ListGetByIndexAsync(key, index, flags));
        }

        public long ListInsertAfter(RedisKey key, RedisValue pivot, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.ListInsertAfter(key, pivot, value, flags));
        }

        public Task<long> ListInsertAfterAsync(RedisKey key, RedisValue pivot, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.ListInsertAfterAsync(key, pivot, value, flags));
        }

        public long ListInsertBefore(RedisKey key, RedisValue pivot, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.ListInsertBefore(key, pivot, value, flags));
        }

        public Task<long> ListInsertBeforeAsync(RedisKey key, RedisValue pivot, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.ListInsertBeforeAsync(key, pivot, value, flags));
        }

        public RedisValue ListLeftPop(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisValue>(() => redisDb.ListLeftPop(key, flags));
        }

        public Task<RedisValue> ListLeftPopAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisValue>(() => redisDb.ListLeftPopAsync(key, flags));
        }

        public long ListLeftPush(RedisKey key, RedisValue value, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.ListLeftPush(key, value, when, flags));
        }

        public long ListLeftPush(RedisKey key, RedisValue[] values, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.ListLeftPush(key, values, flags));
        }

        public Task<long> ListLeftPushAsync(RedisKey key, RedisValue value, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.ListLeftPushAsync(key, value, when, flags));
        }

        public Task<long> ListLeftPushAsync(RedisKey key, RedisValue[] values, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.ListLeftPushAsync(key, values, flags));
        }

        public long ListLength(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.ListLength(key, flags));
        }

        public Task<long> ListLengthAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.ListLengthAsync(key, flags));
        }

        public RedisValue[] ListRange(RedisKey key, long start = 0, long stop = -1, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisValue[]>(() => redisDb.ListRange(key, start, stop, flags));
        }

        public Task<RedisValue[]> ListRangeAsync(RedisKey key, long start = 0, long stop = -1, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisValue[]>(() => redisDb.ListRangeAsync(key, start, stop, flags));
        }

        public long ListRemove(RedisKey key, RedisValue value, long count = 0, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.ListRemove(key, value, count, flags));
        }

        public Task<long> ListRemoveAsync(RedisKey key, RedisValue value, long count = 0, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.ListRemoveAsync(key, value, count, flags));
        }

        public RedisValue ListRightPop(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisValue>(() => redisDb.ListRightPop(key, flags));
        }

        public Task<RedisValue> ListRightPopAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisValue>(() => redisDb.ListRightPopAsync(key, flags));
        }

        public RedisValue ListRightPopLeftPush(RedisKey source, RedisKey destination, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisValue>(() => redisDb.ListRightPopLeftPush(source, destination, flags));
        }

        public Task<RedisValue> ListRightPopLeftPushAsync(RedisKey source, RedisKey destination, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisValue>(() => redisDb.ListRightPopLeftPushAsync(source, destination, flags));
        }

        public long ListRightPush(RedisKey key, RedisValue value, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.ListRightPush(key, value, when, flags));
        }

        public long ListRightPush(RedisKey key, RedisValue[] values, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.ListRightPush(key, values, flags));
        }

        public Task<long> ListRightPushAsync(RedisKey key, RedisValue value, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.ListRightPushAsync(key, value, when, flags));
        }

        public Task<long> ListRightPushAsync(RedisKey key, RedisValue[] values, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.ListRightPushAsync(key, values, flags));
        }

        public void ListSetByIndex(RedisKey key, long index, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            _RetryPolicy.ExecuteWithRetry(() => redisDb.ListSetByIndex(key, index, value, flags));
        }

        public Task ListSetByIndexAsync(RedisKey key, long index, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync(() => redisDb.ListSetByIndexAsync(key, index, value, flags));
        }

        public void ListTrim(RedisKey key, long start, long stop, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            _RetryPolicy.ExecuteWithRetry(() => redisDb.ListTrim(key, start, stop, flags));
        }

        public Task ListTrimAsync(RedisKey key, long start, long stop, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync(() => redisDb.ListTrimAsync(key, start, stop, flags));
        }

        public bool LockExtend(RedisKey key, RedisValue value, TimeSpan expiry, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<bool>(() => redisDb.LockExtend(key, value, expiry, flags));
        }

        public Task<bool> LockExtendAsync(RedisKey key, RedisValue value, TimeSpan expiry, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<bool>(() => redisDb.LockExtendAsync(key, value, expiry, flags));
        }

        public RedisValue LockQuery(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisValue>(() => redisDb.LockQuery(key, flags));
        }

        public Task<RedisValue> LockQueryAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisValue>(() => redisDb.LockQueryAsync(key, flags));
        }

        public bool LockRelease(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<bool>(() => redisDb.LockRelease(key, value, flags));
        }

        public Task<bool> LockReleaseAsync(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<bool>(() => redisDb.LockReleaseAsync(key, value, flags));
        }

        public bool LockTake(RedisKey key, RedisValue value, TimeSpan expiry, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<bool>(() => redisDb.LockTake(key, value, expiry, flags));
        }

        public Task<bool> LockTakeAsync(RedisKey key, RedisValue value, TimeSpan expiry, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<bool>(() => redisDb.LockTakeAsync(key, value, expiry, flags));
        }


        public TimeSpan Ping(CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<TimeSpan>(() => redisDb.Ping(flags));
        }

        public Task<TimeSpan> PingAsync(CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<TimeSpan>(() => redisDb.PingAsync(flags));
        }

        public long Publish(RedisChannel channel, RedisValue message, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.Publish(channel, message, flags));
        }

        public Task<long> PublishAsync(RedisChannel channel, RedisValue message, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.PublishAsync(channel, message, flags));
        }


        public RedisResult ScriptEvaluate(string script, RedisKey[] keys = null, RedisValue[] values = null, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisResult>(() => redisDb.ScriptEvaluate(script, keys, values, flags));
        }

        public RedisResult ScriptEvaluate(byte[] hash, RedisKey[] keys = null, RedisValue[] values = null, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisResult>(() => redisDb.ScriptEvaluate(hash, keys, values, flags));
        }

        public RedisResult ScriptEvaluate(LuaScript script, object parameters = null, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisResult>(() => redisDb.ScriptEvaluate(script, parameters, flags));
        }

        public RedisResult ScriptEvaluate(LoadedLuaScript script, object parameters = null, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisResult>(() => redisDb.ScriptEvaluate(script, parameters, flags));
        }

        public Task<RedisResult> ScriptEvaluateAsync(string script, RedisKey[] keys = null, RedisValue[] values = null, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisResult>(() => redisDb.ScriptEvaluateAsync(script, keys, values, flags));
        }

        public Task<RedisResult> ScriptEvaluateAsync(byte[] hash, RedisKey[] keys = null, RedisValue[] values = null, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisResult>(() => redisDb.ScriptEvaluateAsync(hash, keys, values, flags));
        }

        public Task<RedisResult> ScriptEvaluateAsync(LuaScript script, object parameters = null, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisResult>(() => redisDb.ScriptEvaluateAsync(script, parameters, flags));
        }

        public Task<RedisResult> ScriptEvaluateAsync(LoadedLuaScript script, object parameters = null, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisResult>(() => redisDb.ScriptEvaluateAsync(script, parameters, flags));
        }

        public bool SetAdd(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<bool>(() => redisDb.SetAdd(key, value, flags));
        }

        public long SetAdd(RedisKey key, RedisValue[] values, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.SetAdd(key, values, flags));
        }

        public Task<bool> SetAddAsync(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<bool>(() => redisDb.SetAddAsync(key, value, flags));
        }

        public Task<long> SetAddAsync(RedisKey key, RedisValue[] values, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.SetAddAsync(key, values, flags));
        }

        public RedisValue[] SetCombine(SetOperation operation, RedisKey first, RedisKey second, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisValue[]>(() => redisDb.SetCombine(operation, first, second, flags));
        }

        public RedisValue[] SetCombine(SetOperation operation, RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisValue[]>(() => redisDb.SetCombine(operation, keys, flags));
        }

        public long SetCombineAndStore(SetOperation operation, RedisKey destination, RedisKey first, RedisKey second, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.SetCombineAndStore(operation, destination, first, second, flags));
        }

        public long SetCombineAndStore(SetOperation operation, RedisKey destination, RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.SetCombineAndStore(operation, destination, keys, flags));
        }

        public Task<long> SetCombineAndStoreAsync(SetOperation operation, RedisKey destination, RedisKey first, RedisKey second, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.SetCombineAndStoreAsync(operation, destination, first, second, flags));
        }

        public Task<long> SetCombineAndStoreAsync(SetOperation operation, RedisKey destination, RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.SetCombineAndStoreAsync(operation, destination, keys, flags));
        }

        public Task<RedisValue[]> SetCombineAsync(SetOperation operation, RedisKey first, RedisKey second, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisValue[]>(() => redisDb.SetCombineAsync(operation, first, second, flags));
        }

        public Task<RedisValue[]> SetCombineAsync(SetOperation operation, RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisValue[]>(() => redisDb.SetCombineAsync(operation, keys, flags));
        }

        public bool SetContains(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<bool>(() => redisDb.SetContains(key, value, flags));
        }

        public Task<bool> SetContainsAsync(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<bool>(() => redisDb.SetContainsAsync(key, value, flags));
        }

        public long SetLength(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.SetLength(key, flags));
        }

        public Task<long> SetLengthAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.SetLengthAsync(key, flags));
        }

        public RedisValue[] SetMembers(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisValue[]>(() => redisDb.SetMembers(key, flags));
        }

        public Task<RedisValue[]> SetMembersAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisValue[]>(() => redisDb.SetMembersAsync(key, flags));
        }

        public bool SetMove(RedisKey source, RedisKey destination, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<bool>(() => redisDb.SetMove(source, destination, value, flags));
        }

        public Task<bool> SetMoveAsync(RedisKey source, RedisKey destination, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<bool>(() => redisDb.SetMoveAsync(source, destination, value, flags));
        }

        public RedisValue SetPop(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisValue>(() => redisDb.SetPop(key, flags));
        }

        public RedisValue[] SetPop(RedisKey key, long count, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisValue[]>(() => redisDb.SetPop(key, count, flags));
        }

        public Task<RedisValue> SetPopAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisValue>(() => redisDb.SetPopAsync(key, flags));
        }

        public Task<RedisValue[]> SetPopAsync(RedisKey key, long count, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisValue[]>(() => redisDb.SetPopAsync(key, count, flags));
        }

        public RedisValue SetRandomMember(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisValue>(() => redisDb.SetRandomMember(key, flags));
        }

        public Task<RedisValue> SetRandomMemberAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisValue>(() => redisDb.SetRandomMemberAsync(key, flags));
        }

        public RedisValue[] SetRandomMembers(RedisKey key, long count, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisValue[]>(() => redisDb.SetRandomMembers(key, count, flags));
        }

        public Task<RedisValue[]> SetRandomMembersAsync(RedisKey key, long count, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisValue[]>(() => redisDb.SetRandomMembersAsync(key, count, flags));
        }

        public bool SetRemove(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<bool>(() => redisDb.SetRemove(key, value, flags));
        }

        public long SetRemove(RedisKey key, RedisValue[] values, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.SetRemove(key, values, flags));
        }

        public Task<bool> SetRemoveAsync(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<bool>(() => redisDb.SetRemoveAsync(key, value, flags));
        }

        public Task<long> SetRemoveAsync(RedisKey key, RedisValue[] values, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.SetRemoveAsync(key, values, flags));
        }

        public IEnumerable<RedisValue> SetScan(RedisKey key, RedisValue pattern, int pageSize, CommandFlags flags)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<IEnumerable<RedisValue>>(() => redisDb.SetScan(key, pattern, pageSize, flags));
        }

        public IEnumerable<RedisValue> SetScan(RedisKey key, RedisValue pattern = default, int pageSize = 250, long cursor = 0, int pageOffset = 0, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<IEnumerable<RedisValue>>(() => redisDb.SetScan(key, pattern, pageSize, cursor, pageOffset, flags));
        }

        public IAsyncEnumerable<RedisValue> SetScanAsync(RedisKey key, RedisValue pattern = default, int pageSize = 250, long cursor = 0, int pageOffset = 0, CommandFlags flags = CommandFlags.None)
        {
            //This one causes us some grief as the we do not have a 
            //generic retry handler that can retry in the middle
            //of an iterator loop. So for now we cheat a bit
            IDatabase redisDb = GetDatabaseInternal();
            IEnumerable<RedisValue> hashValues = _RetryPolicy.ExecuteWithRetry<IEnumerable<RedisValue>>(() => redisDb.SetScan(key, pattern, pageSize, cursor, pageOffset, flags));
            AsyncEnumerable<RedisValue> returnValue = new AsyncEnumerable<RedisValue>(hashValues);
            return returnValue;

        }

        public RedisValue[] Sort(RedisKey key, long skip = 0, long take = -1, Order order = Order.Ascending, SortType sortType = SortType.Numeric, RedisValue by = default, RedisValue[] get = null, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisValue[]>(() => redisDb.Sort(key, skip, take, order, sortType, by, get, flags));
        }

        public long SortAndStore(RedisKey destination, RedisKey key, long skip = 0, long take = -1, Order order = Order.Ascending, SortType sortType = SortType.Numeric, RedisValue by = default, RedisValue[] get = null, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.SortAndStore(destination, key, skip, take, order, sortType, by, get, flags));
        }

        public Task<long> SortAndStoreAsync(RedisKey destination, RedisKey key, long skip = 0, long take = -1, Order order = Order.Ascending, SortType sortType = SortType.Numeric, RedisValue by = default, RedisValue[] get = null, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.SortAndStoreAsync(destination, key, skip, take, order, sortType, by, get, flags));
        }

        public Task<RedisValue[]> SortAsync(RedisKey key, long skip = 0, long take = -1, Order order = Order.Ascending, SortType sortType = SortType.Numeric, RedisValue by = default, RedisValue[] get = null, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisValue[]>(() => redisDb.SortAsync(key, skip, take, order, sortType, by, get, flags));
        }

        public bool SortedSetAdd(RedisKey key, RedisValue member, double score, CommandFlags flags)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<bool>(() => redisDb.SortedSetAdd(key, member, score, flags));
        }

        public bool SortedSetAdd(RedisKey key, RedisValue member, double score, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<bool>(() => redisDb.SortedSetAdd(key, member, score, when, flags));
        }

        public long SortedSetAdd(RedisKey key, SortedSetEntry[] values, CommandFlags flags)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.SortedSetAdd(key, values, flags));
        }

        public long SortedSetAdd(RedisKey key, SortedSetEntry[] values, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.SortedSetAdd(key, values, when, flags));
        }

        public Task<bool> SortedSetAddAsync(RedisKey key, RedisValue member, double score, CommandFlags flags)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<bool>(() => redisDb.SortedSetAddAsync(key, member, score, flags));
        }

        public Task<bool> SortedSetAddAsync(RedisKey key, RedisValue member, double score, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<bool>(() => redisDb.SortedSetAddAsync(key, member, score, when, flags));
        }

        public Task<long> SortedSetAddAsync(RedisKey key, SortedSetEntry[] values, CommandFlags flags)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.SortedSetAddAsync(key, values, flags));
        }

        public Task<long> SortedSetAddAsync(RedisKey key, SortedSetEntry[] values, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.SortedSetAddAsync(key, values, when, flags));
        }

        public long SortedSetCombineAndStore(SetOperation operation, RedisKey destination, RedisKey first, RedisKey second, Aggregate aggregate = Aggregate.Sum, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.SortedSetCombineAndStore(operation, destination, first, second, aggregate, flags));
        }

        public long SortedSetCombineAndStore(SetOperation operation, RedisKey destination, RedisKey[] keys, double[] weights = null, Aggregate aggregate = Aggregate.Sum, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.SortedSetCombineAndStore(operation, destination, keys, weights, aggregate, flags));
        }

        public Task<long> SortedSetCombineAndStoreAsync(SetOperation operation, RedisKey destination, RedisKey first, RedisKey second, Aggregate aggregate = Aggregate.Sum, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.SortedSetCombineAndStoreAsync(operation, destination, first, second, aggregate, flags));
        }

        public Task<long> SortedSetCombineAndStoreAsync(SetOperation operation, RedisKey destination, RedisKey[] keys, double[] weights = null, Aggregate aggregate = Aggregate.Sum, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.SortedSetCombineAndStoreAsync(operation, destination, keys, weights, aggregate, flags));
        }

        public double SortedSetDecrement(RedisKey key, RedisValue member, double value, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<double>(() => redisDb.SortedSetDecrement(key, member, value, flags));
        }

        public Task<double> SortedSetDecrementAsync(RedisKey key, RedisValue member, double value, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<double>(() => redisDb.SortedSetDecrementAsync(key, member, value, flags));
        }

        public double SortedSetIncrement(RedisKey key, RedisValue member, double value, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<double>(() => redisDb.SortedSetIncrement(key, member, value, flags));
        }

        public Task<double> SortedSetIncrementAsync(RedisKey key, RedisValue member, double value, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<double>(() => redisDb.SortedSetIncrementAsync(key, member, value, flags));
        }

        public long SortedSetLength(RedisKey key, double min = double.NegativeInfinity, double max = double.PositiveInfinity, Exclude exclude = Exclude.None, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.SortedSetLength(key, min, max, exclude, flags));
        }

        public Task<long> SortedSetLengthAsync(RedisKey key, double min = double.NegativeInfinity, double max = double.PositiveInfinity, Exclude exclude = Exclude.None, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.SortedSetLengthAsync(key, min, max, exclude, flags));
        }

        public long SortedSetLengthByValue(RedisKey key, RedisValue min, RedisValue max, Exclude exclude = Exclude.None, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.SortedSetLengthByValue(key, min, max, exclude, flags));
        }

        public Task<long> SortedSetLengthByValueAsync(RedisKey key, RedisValue min, RedisValue max, Exclude exclude = Exclude.None, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.SortedSetLengthByValueAsync(key, min, max, exclude, flags));
        }

        public SortedSetEntry? SortedSetPop(RedisKey key, Order order = Order.Ascending, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<SortedSetEntry?>(() => redisDb.SortedSetPop(key, order, flags));
        }

        public SortedSetEntry[] SortedSetPop(RedisKey key, long count, Order order = Order.Ascending, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<SortedSetEntry[]>(() => redisDb.SortedSetPop(key, count, order, flags));
        }

        public Task<SortedSetEntry?> SortedSetPopAsync(RedisKey key, Order order = Order.Ascending, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<SortedSetEntry?>(() => redisDb.SortedSetPopAsync(key, order, flags));
        }

        public Task<SortedSetEntry[]> SortedSetPopAsync(RedisKey key, long count, Order order = Order.Ascending, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<SortedSetEntry[]>(() => redisDb.SortedSetPopAsync(key, count, order, flags));
        }

        public RedisValue[] SortedSetRangeByRank(RedisKey key, long start = 0, long stop = -1, Order order = Order.Ascending, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisValue[]>(() => redisDb.SortedSetRangeByRank(key, start, stop, order, flags));
        }

        public Task<RedisValue[]> SortedSetRangeByRankAsync(RedisKey key, long start = 0, long stop = -1, Order order = Order.Ascending, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisValue[]>(() => redisDb.SortedSetRangeByRankAsync(key, start, stop, order, flags));
        }

        public SortedSetEntry[] SortedSetRangeByRankWithScores(RedisKey key, long start = 0, long stop = -1, Order order = Order.Ascending, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<SortedSetEntry[]>(() => redisDb.SortedSetRangeByRankWithScores(key, start, stop, order, flags));
        }

        public Task<SortedSetEntry[]> SortedSetRangeByRankWithScoresAsync(RedisKey key, long start = 0, long stop = -1, Order order = Order.Ascending, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<SortedSetEntry[]>(() => redisDb.SortedSetRangeByRankWithScoresAsync(key, start, stop, order, flags));
        }

        public RedisValue[] SortedSetRangeByScore(RedisKey key, double start = double.NegativeInfinity, double stop = double.PositiveInfinity, Exclude exclude = Exclude.None, Order order = Order.Ascending, long skip = 0, long take = -1, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisValue[]>(() => redisDb.SortedSetRangeByScore(key, start, stop, exclude, order, skip, take, flags));
        }

        public Task<RedisValue[]> SortedSetRangeByScoreAsync(RedisKey key, double start = double.NegativeInfinity, double stop = double.PositiveInfinity, Exclude exclude = Exclude.None, Order order = Order.Ascending, long skip = 0, long take = -1, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisValue[]>(() => redisDb.SortedSetRangeByScoreAsync(key, start, stop, exclude, order, skip, take, flags));
        }

        public SortedSetEntry[] SortedSetRangeByScoreWithScores(RedisKey key, double start = double.NegativeInfinity, double stop = double.PositiveInfinity, Exclude exclude = Exclude.None, Order order = Order.Ascending, long skip = 0, long take = -1, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<SortedSetEntry[]>(() => redisDb.SortedSetRangeByScoreWithScores(key, start, stop, exclude, order, skip, take, flags));
        }

        public Task<SortedSetEntry[]> SortedSetRangeByScoreWithScoresAsync(RedisKey key, double start = double.NegativeInfinity, double stop = double.PositiveInfinity, Exclude exclude = Exclude.None, Order order = Order.Ascending, long skip = 0, long take = -1, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<SortedSetEntry[]>(() => redisDb.SortedSetRangeByScoreWithScoresAsync(key, start, stop, exclude, order, skip, take, flags));
        }

        public RedisValue[] SortedSetRangeByValue(RedisKey key, RedisValue min, RedisValue max, Exclude exclude, long skip, long take = -1, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisValue[]>(() => redisDb.SortedSetRangeByValue(key, min, max, exclude, skip, take, flags));
        }

        public RedisValue[] SortedSetRangeByValue(RedisKey key, RedisValue min = default, RedisValue max = default, Exclude exclude = Exclude.None, Order order = Order.Ascending, long skip = 0, long take = -1, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisValue[]>(() => redisDb.SortedSetRangeByValue(key, min, max, exclude, order, skip, take, flags));
        }

        public Task<RedisValue[]> SortedSetRangeByValueAsync(RedisKey key, RedisValue min, RedisValue max, Exclude exclude, long skip, long take = -1, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisValue[]>(() => redisDb.SortedSetRangeByValueAsync(key, min, max, exclude, skip, take, flags));
        }

        public Task<RedisValue[]> SortedSetRangeByValueAsync(RedisKey key, RedisValue min = default, RedisValue max = default, Exclude exclude = Exclude.None, Order order = Order.Ascending, long skip = 0, long take = -1, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisValue[]>(() => redisDb.SortedSetRangeByValueAsync(key, min, max, exclude, order, skip, take, flags));
        }

        public long? SortedSetRank(RedisKey key, RedisValue member, Order order = Order.Ascending, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long?>(() => redisDb.SortedSetRank(key, member, order, flags));
        }

        public Task<long?> SortedSetRankAsync(RedisKey key, RedisValue member, Order order = Order.Ascending, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long?>(() => redisDb.SortedSetRankAsync(key, member, order, flags));
        }

        public bool SortedSetRemove(RedisKey key, RedisValue member, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<bool>(() => redisDb.SortedSetRemove(key, member, flags));
        }

        public long SortedSetRemove(RedisKey key, RedisValue[] members, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.SortedSetRemove(key, members, flags));
        }

        public Task<bool> SortedSetRemoveAsync(RedisKey key, RedisValue member, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<bool>(() => redisDb.SortedSetRemoveAsync(key, member, flags));
        }

        public Task<long> SortedSetRemoveAsync(RedisKey key, RedisValue[] members, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.SortedSetRemoveAsync(key, members, flags));
        }

        public long SortedSetRemoveRangeByRank(RedisKey key, long start, long stop, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.SortedSetRemoveRangeByRank(key, start, stop, flags));
        }

        public Task<long> SortedSetRemoveRangeByRankAsync(RedisKey key, long start, long stop, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.SortedSetRemoveRangeByRankAsync(key, start, stop, flags));
        }

        public long SortedSetRemoveRangeByScore(RedisKey key, double start, double stop, Exclude exclude = Exclude.None, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.SortedSetRemoveRangeByScore(key, start, stop, exclude, flags));
        }

        public Task<long> SortedSetRemoveRangeByScoreAsync(RedisKey key, double start, double stop, Exclude exclude = Exclude.None, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.SortedSetRemoveRangeByScoreAsync(key, start, stop, exclude, flags));
        }

        public long SortedSetRemoveRangeByValue(RedisKey key, RedisValue min, RedisValue max, Exclude exclude = Exclude.None, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.SortedSetRemoveRangeByValue(key, min, max, exclude, flags));
        }

        public Task<long> SortedSetRemoveRangeByValueAsync(RedisKey key, RedisValue min, RedisValue max, Exclude exclude = Exclude.None, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.SortedSetRemoveRangeByValueAsync(key, min, max, exclude, flags));
        }

        public IEnumerable<SortedSetEntry> SortedSetScan(RedisKey key, RedisValue pattern, int pageSize, CommandFlags flags)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<IEnumerable<SortedSetEntry>>(() => redisDb.SortedSetScan(key, pattern, pageSize, flags));
        }

        public IEnumerable<SortedSetEntry> SortedSetScan(RedisKey key, RedisValue pattern = default, int pageSize = 250, long cursor = 0, int pageOffset = 0, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<IEnumerable<SortedSetEntry>>(() => redisDb.SortedSetScan(key, pattern, pageSize, cursor, pageOffset, flags));
        }

        public IAsyncEnumerable<SortedSetEntry> SortedSetScanAsync(RedisKey key, RedisValue pattern = default, int pageSize = 250, long cursor = 0, int pageOffset = 0, CommandFlags flags = CommandFlags.None)
        {
            //This one causes us some grief as the we do not have a 
            //generic retry handler that can retry in the middle
            //of an iterator loop. So for now we cheat a bit
            IDatabase redisDb = GetDatabaseInternal();
            IEnumerable<SortedSetEntry> hashValues = _RetryPolicy.ExecuteWithRetry<IEnumerable<SortedSetEntry>>(() => redisDb.SortedSetScan(key, pattern, pageSize, cursor, pageOffset, flags));
            AsyncEnumerable<SortedSetEntry> returnValue = new AsyncEnumerable<SortedSetEntry>(hashValues);
            return returnValue;
        }

        public double? SortedSetScore(RedisKey key, RedisValue member, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<double?>(() => redisDb.SortedSetScore(key, member, flags));
        }

        public Task<double?> SortedSetScoreAsync(RedisKey key, RedisValue member, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<double?>(() => redisDb.SortedSetScoreAsync(key, member, flags));
        }

        public long StreamAcknowledge(RedisKey key, RedisValue groupName, RedisValue messageId, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.StreamAcknowledge(key, groupName, messageId, flags));
        }

        public long StreamAcknowledge(RedisKey key, RedisValue groupName, RedisValue[] messageIds, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.StreamAcknowledge(key, groupName, messageIds, flags));
        }

        public Task<long> StreamAcknowledgeAsync(RedisKey key, RedisValue groupName, RedisValue messageId, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.StreamAcknowledgeAsync(key, groupName, messageId, flags));
        }

        public Task<long> StreamAcknowledgeAsync(RedisKey key, RedisValue groupName, RedisValue[] messageIds, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.StreamAcknowledgeAsync(key, groupName, messageIds, flags));
        }

        public RedisValue StreamAdd(RedisKey key, RedisValue streamField, RedisValue streamValue, RedisValue? messageId = null, int? maxLength = null, bool useApproximateMaxLength = false, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisValue>(() => redisDb.StreamAdd(key, streamField, streamValue, messageId, maxLength, useApproximateMaxLength, flags));
        }

        public RedisValue StreamAdd(RedisKey key, NameValueEntry[] streamPairs, RedisValue? messageId = null, int? maxLength = null, bool useApproximateMaxLength = false, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisValue>(() => redisDb.StreamAdd(key, streamPairs, messageId, maxLength, useApproximateMaxLength, flags));
        }

        public Task<RedisValue> StreamAddAsync(RedisKey key, RedisValue streamField, RedisValue streamValue, RedisValue? messageId = null, int? maxLength = null, bool useApproximateMaxLength = false, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisValue>(() => redisDb.StreamAddAsync(key, streamField, streamValue, messageId, maxLength, useApproximateMaxLength, flags));
        }

        public Task<RedisValue> StreamAddAsync(RedisKey key, NameValueEntry[] streamPairs, RedisValue? messageId = null, int? maxLength = null, bool useApproximateMaxLength = false, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisValue>(() => redisDb.StreamAddAsync(key, streamPairs, messageId, maxLength, useApproximateMaxLength, flags));
        }

        public StreamEntry[] StreamClaim(RedisKey key, RedisValue consumerGroup, RedisValue claimingConsumer, long minIdleTimeInMs, RedisValue[] messageIds, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<StreamEntry[]>(() => redisDb.StreamClaim(key, consumerGroup, claimingConsumer, minIdleTimeInMs, messageIds, flags));
        }

        public Task<StreamEntry[]> StreamClaimAsync(RedisKey key, RedisValue consumerGroup, RedisValue claimingConsumer, long minIdleTimeInMs, RedisValue[] messageIds, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<StreamEntry[]>(() => redisDb.StreamClaimAsync(key, consumerGroup, claimingConsumer, minIdleTimeInMs, messageIds, flags));
        }

        public RedisValue[] StreamClaimIdsOnly(RedisKey key, RedisValue consumerGroup, RedisValue claimingConsumer, long minIdleTimeInMs, RedisValue[] messageIds, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisValue[]>(() => redisDb.StreamClaimIdsOnly(key, consumerGroup, claimingConsumer, minIdleTimeInMs, messageIds, flags));
        }

        public Task<RedisValue[]> StreamClaimIdsOnlyAsync(RedisKey key, RedisValue consumerGroup, RedisValue claimingConsumer, long minIdleTimeInMs, RedisValue[] messageIds, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisValue[]>(() => redisDb.StreamClaimIdsOnlyAsync(key, consumerGroup, claimingConsumer, minIdleTimeInMs, messageIds, flags));
        }

        public bool StreamConsumerGroupSetPosition(RedisKey key, RedisValue groupName, RedisValue position, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<bool>(() => redisDb.StreamConsumerGroupSetPosition(key, groupName, position, flags));
        }

        public Task<bool> StreamConsumerGroupSetPositionAsync(RedisKey key, RedisValue groupName, RedisValue position, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<bool>(() => redisDb.StreamConsumerGroupSetPositionAsync(key, groupName, position, flags));
        }

        public StreamConsumerInfo[] StreamConsumerInfo(RedisKey key, RedisValue groupName, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<StreamConsumerInfo[]>(() => redisDb.StreamConsumerInfo(key, groupName, flags));
        }

        public Task<StreamConsumerInfo[]> StreamConsumerInfoAsync(RedisKey key, RedisValue groupName, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<StreamConsumerInfo[]>(() => redisDb.StreamConsumerInfoAsync(key, groupName, flags));
        }

        public bool StreamCreateConsumerGroup(RedisKey key, RedisValue groupName, RedisValue? position, CommandFlags flags)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<bool>(() => redisDb.StreamCreateConsumerGroup(key, groupName, position, flags));
        }

        public bool StreamCreateConsumerGroup(RedisKey key, RedisValue groupName, RedisValue? position = null, bool createStream = true, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<bool>(() => redisDb.StreamCreateConsumerGroup(key, groupName, position, createStream, flags));
        }

        public Task<bool> StreamCreateConsumerGroupAsync(RedisKey key, RedisValue groupName, RedisValue? position, CommandFlags flags)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<bool>(() => redisDb.StreamCreateConsumerGroupAsync(key, groupName, position, flags));
        }

        public Task<bool> StreamCreateConsumerGroupAsync(RedisKey key, RedisValue groupName, RedisValue? position = null, bool createStream = true, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<bool>(() => redisDb.StreamCreateConsumerGroupAsync(key, groupName, position, createStream, flags));
        }

        public long StreamDelete(RedisKey key, RedisValue[] messageIds, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.StreamDelete(key, messageIds, flags));
        }

        public Task<long> StreamDeleteAsync(RedisKey key, RedisValue[] messageIds, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.StreamDeleteAsync(key, messageIds, flags));
        }

        public long StreamDeleteConsumer(RedisKey key, RedisValue groupName, RedisValue consumerName, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.StreamDeleteConsumer(key, groupName, consumerName, flags));
        }

        public Task<long> StreamDeleteConsumerAsync(RedisKey key, RedisValue groupName, RedisValue consumerName, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.StreamDeleteConsumerAsync(key, groupName, consumerName, flags));
        }

        public bool StreamDeleteConsumerGroup(RedisKey key, RedisValue groupName, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<bool>(() => redisDb.StreamDeleteConsumerGroup(key, groupName, flags));
        }

        public Task<bool> StreamDeleteConsumerGroupAsync(RedisKey key, RedisValue groupName, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<bool>(() => redisDb.StreamDeleteConsumerGroupAsync(key, groupName, flags));
        }

        public StreamGroupInfo[] StreamGroupInfo(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<StreamGroupInfo[]>(() => redisDb.StreamGroupInfo(key, flags));
        }

        public Task<StreamGroupInfo[]> StreamGroupInfoAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<StreamGroupInfo[]>(() => redisDb.StreamGroupInfoAsync(key, flags));
        }

        public StreamInfo StreamInfo(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<StreamInfo>(() => redisDb.StreamInfo(key, flags));
        }

        public Task<StreamInfo> StreamInfoAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<StreamInfo>(() => redisDb.StreamInfoAsync(key, flags));
        }

        public long StreamLength(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.StreamLength(key, flags));
        }

        public Task<long> StreamLengthAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.StreamLengthAsync(key, flags));
        }

        public StreamPendingInfo StreamPending(RedisKey key, RedisValue groupName, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<StreamPendingInfo>(() => redisDb.StreamPending(key, groupName, flags));
        }

        public Task<StreamPendingInfo> StreamPendingAsync(RedisKey key, RedisValue groupName, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<StreamPendingInfo>(() => redisDb.StreamPendingAsync(key, groupName, flags));
        }

        public StreamPendingMessageInfo[] StreamPendingMessages(RedisKey key, RedisValue groupName, int count, RedisValue consumerName, RedisValue? minId = null, RedisValue? maxId = null, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<StreamPendingMessageInfo[]>(() => redisDb.StreamPendingMessages(key, groupName, count, consumerName, minId, maxId, flags));
        }

        public Task<StreamPendingMessageInfo[]> StreamPendingMessagesAsync(RedisKey key, RedisValue groupName, int count, RedisValue consumerName, RedisValue? minId = null, RedisValue? maxId = null, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<StreamPendingMessageInfo[]>(() => redisDb.StreamPendingMessagesAsync(key, groupName, count, consumerName, minId, maxId, flags));
        }

        public StreamEntry[] StreamRange(RedisKey key, RedisValue? minId = null, RedisValue? maxId = null, int? count = null, Order messageOrder = Order.Ascending, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<StreamEntry[]>(() => redisDb.StreamRange(key, minId, maxId, count, messageOrder, flags));
        }

        public Task<StreamEntry[]> StreamRangeAsync(RedisKey key, RedisValue? minId = null, RedisValue? maxId = null, int? count = null, Order messageOrder = Order.Ascending, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<StreamEntry[]>(() => redisDb.StreamRangeAsync(key, minId, maxId, count, messageOrder, flags));
        }

        public StreamEntry[] StreamRead(RedisKey key, RedisValue position, int? count = null, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<StreamEntry[]>(() => redisDb.StreamRead(key, position, count, flags));
        }

        public RedisStream[] StreamRead(StreamPosition[] streamPositions, int? countPerStream = null, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisStream[]>(() => redisDb.StreamRead(streamPositions, countPerStream, flags));
        }

        public Task<StreamEntry[]> StreamReadAsync(RedisKey key, RedisValue position, int? count = null, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<StreamEntry[]>(() => redisDb.StreamReadAsync(key, position, count, flags));
        }

        public Task<RedisStream[]> StreamReadAsync(StreamPosition[] streamPositions, int? countPerStream = null, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisStream[]>(() => redisDb.StreamReadAsync(streamPositions, countPerStream, flags));
        }

        public StreamEntry[] StreamReadGroup(RedisKey key, RedisValue groupName, RedisValue consumerName, RedisValue? position, int? count, CommandFlags flags)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<StreamEntry[]>(() => redisDb.StreamReadGroup(key, groupName, consumerName, position, count, flags));
        }

        public StreamEntry[] StreamReadGroup(RedisKey key, RedisValue groupName, RedisValue consumerName, RedisValue? position = null, int? count = null, bool noAck = false, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<StreamEntry[]>(() => redisDb.StreamReadGroup(key, groupName, consumerName, position, count, noAck, flags));
        }

        public RedisStream[] StreamReadGroup(StreamPosition[] streamPositions, RedisValue groupName, RedisValue consumerName, int? countPerStream, CommandFlags flags)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisStream[]>(() => redisDb.StreamReadGroup(streamPositions, groupName, consumerName, countPerStream, flags));
        }

        public RedisStream[] StreamReadGroup(StreamPosition[] streamPositions, RedisValue groupName, RedisValue consumerName, int? countPerStream = null, bool noAck = false, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisStream[]>(() => redisDb.StreamReadGroup(streamPositions, groupName, consumerName, countPerStream, noAck, flags));
        }

        public Task<StreamEntry[]> StreamReadGroupAsync(RedisKey key, RedisValue groupName, RedisValue consumerName, RedisValue? position, int? count, CommandFlags flags)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<StreamEntry[]>(() => redisDb.StreamReadGroupAsync(key, groupName, consumerName, position, count, flags));
        }

        public Task<StreamEntry[]> StreamReadGroupAsync(RedisKey key, RedisValue groupName, RedisValue consumerName, RedisValue? position = null, int? count = null, bool noAck = false, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<StreamEntry[]>(() => redisDb.StreamReadGroupAsync(key, groupName, consumerName, position, count, noAck, flags));
        }

        public Task<RedisStream[]> StreamReadGroupAsync(StreamPosition[] streamPositions, RedisValue groupName, RedisValue consumerName, int? countPerStream, CommandFlags flags)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisStream[]>(() => redisDb.StreamReadGroupAsync(streamPositions, groupName, consumerName, countPerStream, flags));
        }

        public Task<RedisStream[]> StreamReadGroupAsync(StreamPosition[] streamPositions, RedisValue groupName, RedisValue consumerName, int? countPerStream = null, bool noAck = false, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisStream[]>(() => redisDb.StreamReadGroupAsync(streamPositions, groupName, consumerName, countPerStream, noAck, flags));
        }

        public long StreamTrim(RedisKey key, int maxLength, bool useApproximateMaxLength = false, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.StreamTrim(key, maxLength, useApproximateMaxLength, flags));
        }

        public Task<long> StreamTrimAsync(RedisKey key, int maxLength, bool useApproximateMaxLength = false, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.StreamTrimAsync(key, maxLength, useApproximateMaxLength, flags));
        }

        public long StringAppend(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.StringAppend(key, value, flags));
        }

        public Task<long> StringAppendAsync(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.StringAppendAsync(key, value, flags));
        }

        public long StringBitCount(RedisKey key, long start = 0, long end = -1, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.StringBitCount(key, start, end, flags));
        }

        public Task<long> StringBitCountAsync(RedisKey key, long start = 0, long end = -1, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.StringBitCountAsync(key, start, end, flags));
        }

        public long StringBitOperation(Bitwise operation, RedisKey destination, RedisKey first, RedisKey second = default, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.StringBitOperation(operation, destination, first, second, flags));
        }

        public long StringBitOperation(Bitwise operation, RedisKey destination, RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.StringBitOperation(operation, destination, keys, flags));
        }

        public Task<long> StringBitOperationAsync(Bitwise operation, RedisKey destination, RedisKey first, RedisKey second = default, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.StringBitOperationAsync(operation, destination, first, second, flags));
        }

        public Task<long> StringBitOperationAsync(Bitwise operation, RedisKey destination, RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.StringBitOperationAsync(operation, destination, keys, flags));
        }

        public long StringBitPosition(RedisKey key, bool bit, long start = 0, long end = -1, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.StringBitPosition(key, bit, start, end, flags));
        }

        public Task<long> StringBitPositionAsync(RedisKey key, bool bit, long start = 0, long end = -1, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.StringBitPositionAsync(key, bit, start, end, flags));
        }

        public long StringDecrement(RedisKey key, long value = 1, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.StringDecrement(key, value, flags));
        }

        public double StringDecrement(RedisKey key, double value, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<double>(() => redisDb.StringDecrement(key, value, flags));
        }

        public Task<long> StringDecrementAsync(RedisKey key, long value = 1, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.StringDecrementAsync(key, value, flags));
        }

        public Task<double> StringDecrementAsync(RedisKey key, double value, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<double>(() => redisDb.StringDecrementAsync(key, value, flags));
        }

        public RedisValue StringGet(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisValue>(() => redisDb.StringGet(key, flags));
        }

        public RedisValue[] StringGet(RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisValue[]>(() => redisDb.StringGet(keys, flags));
        }

        public Task<RedisValue> StringGetAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisValue>(() => redisDb.StringGetAsync(key, flags));
        }

        public Task<RedisValue[]> StringGetAsync(RedisKey[] keys, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisValue[]>(() => redisDb.StringGetAsync(keys, flags));
        }

        public bool StringGetBit(RedisKey key, long offset, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<bool>(() => redisDb.StringGetBit(key, offset, flags));
        }

        public Task<bool> StringGetBitAsync(RedisKey key, long offset, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<bool>(() => redisDb.StringGetBitAsync(key, offset, flags));
        }

        public Lease<byte> StringGetLease(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<Lease<byte>>(() => redisDb.StringGetLease(key, flags));
        }

        public Task<Lease<byte>> StringGetLeaseAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<Lease<byte>>(() => redisDb.StringGetLeaseAsync(key, flags));
        }

        public RedisValue StringGetRange(RedisKey key, long start, long end, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisValue>(() => redisDb.StringGetRange(key, start, end, flags));
        }

        public Task<RedisValue> StringGetRangeAsync(RedisKey key, long start, long end, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisValue>(() => redisDb.StringGetRangeAsync(key, start, end, flags));
        }

        public RedisValue StringGetSet(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisValue>(() => redisDb.StringGetSet(key, value, flags));
        }

        public Task<RedisValue> StringGetSetAsync(RedisKey key, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisValue>(() => redisDb.StringGetSetAsync(key, value, flags));
        }

        public RedisValueWithExpiry StringGetWithExpiry(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisValueWithExpiry>(() => redisDb.StringGetWithExpiry(key, flags));
        }

        public Task<RedisValueWithExpiry> StringGetWithExpiryAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisValueWithExpiry>(() => redisDb.StringGetWithExpiryAsync(key, flags));
        }

        public long StringIncrement(RedisKey key, long value = 1, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.StringIncrement(key, value, flags));
        }

        public double StringIncrement(RedisKey key, double value, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<double>(() => redisDb.StringIncrement(key, value, flags));
        }

        public Task<long> StringIncrementAsync(RedisKey key, long value = 1, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.StringIncrementAsync(key, value, flags));
        }

        public Task<double> StringIncrementAsync(RedisKey key, double value, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<double>(() => redisDb.StringIncrementAsync(key, value, flags));
        }

        public long StringLength(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<long>(() => redisDb.StringLength(key, flags));
        }

        public Task<long> StringLengthAsync(RedisKey key, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<long>(() => redisDb.StringLengthAsync(key, flags));
        }

        public bool StringSet(RedisKey key, RedisValue value, TimeSpan? expiry = null, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<bool>(() => redisDb.StringSet(key, value, expiry, when, flags));
        }

        public bool StringSet(KeyValuePair<RedisKey, RedisValue>[] values, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<bool>(() => redisDb.StringSet(values, when, flags));
        }

        public Task<bool> StringSetAsync(RedisKey key, RedisValue value, TimeSpan? expiry = null, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<bool>(() => redisDb.StringSetAsync(key, value, expiry, when, flags));
        }

        public Task<bool> StringSetAsync(KeyValuePair<RedisKey, RedisValue>[] values, When when = When.Always, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<bool>(() => redisDb.StringSetAsync(values, when, flags));
        }

        public bool StringSetBit(RedisKey key, long offset, bool bit, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<bool>(() => redisDb.StringSetBit(key, offset, bit, flags));
        }

        public Task<bool> StringSetBitAsync(RedisKey key, long offset, bool bit, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<bool>(() => redisDb.StringSetBitAsync(key, offset, bit, flags));
        }
        public RedisValue StringSetRange(RedisKey key, long offset, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<RedisValue>(() => redisDb.StringSetRange(key, offset, value, flags));
        }

        public Task<RedisValue> StringSetRangeAsync(RedisKey key, long offset, RedisValue value, CommandFlags flags = CommandFlags.None)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetryAsync<RedisValue>(() => redisDb.StringSetRangeAsync(key, offset, value, flags));
        }

        public bool TryWait(Task task)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<bool>(() => redisDb.TryWait(task));
        }

        public void Wait(Task task)
        {
            IDatabase redisDb = GetDatabaseInternal();
            _RetryPolicy.ExecuteWithRetry(() => redisDb.Wait(task));
        }

        public T Wait<T>(Task<T> task)
        {
            IDatabase redisDb = GetDatabaseInternal();
            return _RetryPolicy.ExecuteWithRetry<T>(() => redisDb.Wait<T>(task));
        }

        public void WaitAll(params Task[] tasks)
        {
            IDatabase redisDb = GetDatabaseInternal();
            _RetryPolicy.ExecuteWithRetry(() => redisDb.WaitAll(tasks));
        }
        #endregion IDatabase
    }
}