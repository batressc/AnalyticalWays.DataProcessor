using AnalyticalWays.Utilities.Implements;
using AnalyticalWays.Utilities.Interfaces;
using System;
using System.Threading.Tasks;

namespace AnalyticalWays.Client {
    class Program {
        static async Task Main(string[] args) {
            IStorage store = new AzureStorage();
            await store.ReadFile("Stock.CSV");
            //await store.ReadFile("demo.csv");
            Console.ReadKey();
        }
    }
}
