// Based on: https://github.com/rodrigobrito/redis-benchmark-dotnet/blob/master/src/RedisBenchmarkDotNet/Utils/ConnectionManagement.cs

using System;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using StackExchange.Redis;

namespace RedisConnectionListener
{
    public class ConnectionManager
    {
        private ConnectionMultiplexer _conn;
        private bool _connectionFailed;
        private bool _reconnecting;
        private string _connectionString;

        #region Multithreaded Singleton

        // For more info see https://msdn.microsoft.com/en-us/library/ff650316.aspx

        private static readonly object SyncRoot = new object();
        private static volatile ConnectionManager _instance;

        private ConnectionManager() { }

        private void CreateRedisConnection(string connectionString)
        {
            _connectionString = string.IsNullOrWhiteSpace(connectionString) ? throw new ArgumentNullException(nameof(connectionString)) : connectionString;
            _conn = ConnectionMultiplexer.Connect(_connectionString);
            _conn.ConnectionFailed += (sender, e) => ListenEvents(sender, e, "ConnectionFailed");
            _conn.ConnectionRestored += (sender, e) => ListenEvents(sender, e, "ConnectionRestored");
            _conn.ErrorMessage += (sender, e) => ListenEvents(sender, e, "ErrorMessage");
            _conn.InternalError += (sender, e) => ListenEvents(sender, e, "InternalError");
            _conn.ConfigurationChanged += (sender, e) => ListenEvents(sender, e, "ConfigurationChanged");
            _conn.ConfigurationChangedBroadcast += (sender, e) => ListenEvents(sender, e, "ConfigurationChangedBroadcast");
            _conn.HashSlotMoved += (sender, e) => ListenEvents(sender, e, "HashSlotMoved");
        }

        public static ConnectionManager GetInstance(string connectionString = null)
        {
            if (_instance == null && !string.IsNullOrWhiteSpace(connectionString))
            {
                lock (SyncRoot)
                {
                    if (_instance == null)
                    {
                        _instance = new ConnectionManager();
                        _instance.CreateRedisConnection(connectionString);
                    }
                }
            }
            return _instance;
        }

        #endregion

        
        // Listen and log events
        private void ListenEvents(object sender, EventArgs eventArgs, string eventName)
        {
            switch (eventName)
            {
                case "ConnectionFailed":
                    _connectionFailed = true;
                    break;

                case "ConnectionRestored":
                    _connectionFailed = false;
                    break;

                default:
                    break;
            }


            Console.WriteLine($"Event {eventName} raised.");
            Console.WriteLine($"Sender: {JsonSerializer.Serialize(sender)}.");
            Console.WriteLine($"Event args: {JsonSerializer.Serialize(eventArgs)}.");
        }

        private void CheckConnectionAndRetryIfDisconnected()
        {
            if ((_conn == null || !_conn.IsConnected || _connectionFailed) && !_reconnecting)
            {
                lock (SyncRoot)
                {
                    if (!_reconnecting)
                    {
                        _reconnecting = true;

                        Console.WriteLine($"Trying connect to redis server: {_connectionString}");

                        try
                        {
                            CreateRedisConnection(_connectionString);
                        }
                        catch (RedisConnectionException ex)
                        {
                            Console.WriteLine($"Failed to attempt to reconnect to REDIS {_connectionString}. {ex}");
                        }

                        _reconnecting = false;
                    }
                }
            }
        }

        private IDatabase GetDatabase()
        {
            CheckConnectionAndRetryIfDisconnected();
            return _conn?.GetDatabase();
        }

        public TX GetKey<TX>(string key, CommandFlags flags = CommandFlags.PreferReplica)
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException(nameof(key));

            var db = GetDatabase();
            var cachedValue = db.StringGet(key, flags);
            return cachedValue.HasValue ? ByteArrayToObject<TX>(cachedValue) : default(TX);
        }

        public async Task<TX> GetKeyAsync<TX>(string key, CommandFlags flags = CommandFlags.PreferReplica)
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException(nameof(key));

            var db = GetDatabase();
            var cachedValue = await db.StringGetAsync(key, flags);
            return cachedValue.HasValue ? ByteArrayToObject<TX>(cachedValue) : default(TX);
        }

        public bool CreateKey<TX>(string key, TX data, TimeSpan? expire = null)
        {
            if (string.IsNullOrWhiteSpace(key))
                throw new ArgumentNullException(nameof(key));

            if (data == null) return false;

            if (!typeof(TX).IsSerializable)
                throw new SerializationException($"Type {typeof(TX)} is not serializable");

            using (var cacheLock = new ReaderWriterLockSlim())
            {
                cacheLock.EnterWriteLock();
                try
                {
                    var valueBytes = ObjectToByteArray(data);
                    var db = GetDatabase();
                    return db.StringSet(key, valueBytes, expire);
                }
                finally
                {
                    cacheLock.ExitWriteLock();
                }
            }
        }

        public TX GetOrCreateKey<TX>(string key, Func<TX> func = null, CommandFlags flags = CommandFlags.None, TimeSpan? expire = null)
        {
            var data = GetKey<TX>(key, flags);
            if (data != null) return data;

            if (func == null) return default(TX);
            data = func();

            if (data == null) return default(TX);
            CreateKey(key, data, expire);

            return data;
        }

        private static byte[] ObjectToByteArray(object objectToConvert)
        {
            if (objectToConvert == null) return null;

            var binaryFormatter = new BinaryFormatter();
            using (var memoryStream = new MemoryStream())
            {
                binaryFormatter.Serialize(memoryStream, objectToConvert);
                return memoryStream.ToArray();
            }
        }

        private static T ByteArrayToObject<T>(byte[] bytes)
        {
            if (bytes == null) return default(T);

            using (var memoryStream = new MemoryStream())
            {
                var binaryFormatter = new BinaryFormatter();
                memoryStream.Write(bytes, 0, bytes.Length);
                memoryStream.Seek(0, SeekOrigin.Begin);
                return (T)binaryFormatter.Deserialize(memoryStream);
            }
        }
    }
}
