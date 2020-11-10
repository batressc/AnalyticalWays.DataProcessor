using Microsoft.Extensions.Hosting;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace AnalyticalWays.Monolitico {
    public class Procesamiento : IHostedService {
        public Task StartAsync(CancellationToken cancellationToken) {
            Console.WriteLine("Hola a todos los presentes");
            Console.ReadKey();
            return Task.FromResult(0);
        }

        public Task StopAsync(CancellationToken cancellationToken) {
            Console.WriteLine("Deteniendo procesamiento");
            return Task.FromResult(0);
        }
    }
}
