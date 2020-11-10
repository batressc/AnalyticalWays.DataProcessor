using Microsoft.Extensions.Hosting;
using System;
using System.Threading.Tasks;

namespace AnalyticalWays.Monolitico {
    public class Program {
        public static IHostBuilder CreateHostBuilder(string[] args) => 
            Host.CreateDefaultBuilder(args)
                .UseStartup<Startup>();


        static async Task Main(string[] args) {
            await CreateHostBuilder(args).Build().RunAsync();
        }
    }
}
