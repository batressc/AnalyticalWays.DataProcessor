using AnalyticalWays.DataProcessor.Contracts;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace AnalyticalWays.DataProcessor {
    public class CsvProcessor : BackgroundService {
        private readonly IStorageOperations _store;
        private readonly ILogger<CsvProcessor> _logger;

        public CsvProcessor(IStorageOperations store, ILogger<CsvProcessor> logger) {
            _store = store;
            _logger = logger;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken) {
            bool resultado = false;
            string filename = "Stock.CSV";
            if (await _store.FileExists(filename)) {
                resultado = await _store.ReadFile(filename);
            }
            if (resultado) {
                _logger.LogInformation("Datos procesados correctamente", DateTimeOffset.Now);
            } else {
                _logger.LogWarning("No se procesaron los datos", DateTimeOffset.Now);
            }
        }
    }
}
