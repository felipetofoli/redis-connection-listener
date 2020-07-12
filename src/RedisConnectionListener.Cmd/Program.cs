using Microsoft.Extensions.Configuration;
using System;
using System.IO;
using System.Threading;

namespace RedisConnectionListener
{
    class Program
    {
        static void Main(string[] args)
        {
            Console.WriteLine("Testing connection");

            // Get configs
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json");

            var configuration = builder.Build();

            // Connection
            var connectionString = configuration["ConnectionString"];
            var connectionManager = ConnectionManager.GetInstance(connectionString);

            // Insert key
            var key = $"key_{DateTime.Now.Ticks}";
            connectionManager.CreateKey(key, Guid.NewGuid().ToString());
            

            while (true)
            {
                // Get the key each second, until listen to redis events
                Thread.Sleep(1000);

                // Get key
                var value = connectionManager.GetKey<string>(key);
                Console.WriteLine($"[{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff")}] Got value: '{value}'.");
            }
        }
    }
}
