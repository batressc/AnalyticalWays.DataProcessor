using AnalyticalWays.DataProcessor.Contracts;
using AnalyticalWays.DataProcessor.Model;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading.Channels;
using System.Threading.Tasks;

namespace AnalyticalWays.DataProcessor.Implementations {
    public class AzureBlobStorageOperations : IStorageOperations {
        private readonly BlobServiceClient _bsc;
        private readonly BlobContainerClient _bcc;
        private readonly IDataOperations<StockInformation> _data;
        private readonly ILogger<AzureBlobStorageOperations> _logger;

        public AzureBlobStorageOperations(IConfiguration conf, IDataOperations<StockInformation> data, ILogger<AzureBlobStorageOperations> logger) {
            _bsc = new BlobServiceClient(conf.GetValue<string>("BlobStorage:ConnectionString"));
            _bcc = _bsc.GetBlobContainerClient(conf.GetValue<string>("BlobStorage:Container"));
            _data = data;
            _logger = logger;
        }


        public async Task<bool> FileExists(string filename) {
            bool resultado = false;
            if (await _bcc.ExistsAsync()) {
                BlobClient bc = _bcc.GetBlobClient(filename);
                resultado = await bc.ExistsAsync();
            }
            return resultado;
        }

        public async Task<bool> ReadFile(string filename) {
            Channel<StockInformation> procesito = Channel.CreateUnbounded<StockInformation>();

            var producer = Task.Run(async () => {
                Stopwatch control = new Stopwatch();
                Stopwatch controlProceso = new Stopwatch();
                TimeSpan tiempoProceso = new TimeSpan();
                BlobClient bc = _bcc.GetBlobClient(filename);
                BlobOpenReadOptions boro = new BlobOpenReadOptions(false) {
                    BufferSize = 100 * 1024 * 1024
                };
                _logger.LogInformation("Iniciando lectura de archivo");
                control.Start();
                Stream stream = await bc.OpenReadAsync(boro);
                using StreamReader reader = new StreamReader(stream);
                bool primeraLinea = true;
                while (!reader.EndOfStream) {
                    controlProceso.Start();
                    string linea = await reader.ReadLineAsync();
                    if (primeraLinea) {
                        primeraLinea = false;
                        continue;
                    }
                    string[] componentes = linea.Split(";");
                    StockInformation info = new StockInformation() {
                        Pos = componentes[0],
                        Product = componentes[1],
                        StockDate = DateTime.ParseExact(componentes[2], "yyyy-MM-dd", CultureInfo.CurrentCulture),
                        Stock = Convert.ToInt32(componentes[3])
                    };
                    await procesito.Writer.WriteAsync(info);
                    controlProceso.Stop();
                    tiempoProceso += controlProceso.Elapsed;
                    controlProceso.Reset();
                }
                control.Stop();
                _logger.LogInformation("Total de tiempo de lectura de archivo en base de datos en minutos: {0}", control.Elapsed.TotalMinutes);
                _logger.LogInformation("Total de tiempo de procesamiento de los registros a lista en minutos: {0}", tiempoProceso.TotalMinutes);
                procesito.Writer.Complete();
            });

            var consumer = Task.Run(async () => {
                Stopwatch control = new Stopwatch();
                TimeSpan totalTiempo = new TimeSpan();
                int totalRegistros = 0;
                int bloque = 0;
                List<StockInformation> stock = new List<StockInformation>();
                if (await _data.ExistsPreviousData()) {
                    _logger.LogWarning("Borrando registros anteriores");
                    await _data.DeletePreviousData();
                }
                while (await procesito.Reader.WaitToReadAsync()) {
                    var stockItem = await procesito.Reader.ReadAsync();
                    stock.Add(stockItem);
                    bloque += 1;
                    totalRegistros += 1;
                    if (bloque >= 10000) {
                        control.Start();
                        await _data.AppendData(stock);
                        control.Stop();
                        _logger.LogInformation("Total de registros escritos {0} registros en {1} segundos", totalRegistros, control.Elapsed.TotalSeconds);
                        totalTiempo += control.Elapsed;
                        control.Reset();
                        bloque = 0;
                        stock.Clear();                        
                    }
                }
                if (bloque != 0) {
                    control.Start();
                    await _data.AppendData(stock);
                    control.Stop();
                    _logger.LogInformation("Total de registros escritos {0} registros en {1} segundos", totalRegistros, control.Elapsed.TotalSeconds);
                    totalTiempo += control.Elapsed;
                    control.Reset();
                    bloque = 0;
                    stock.Clear();
                }
                _logger.LogInformation("Total de tiempo de escritura en base de datos en minutos: {0}", totalTiempo.TotalMinutes);
            });
            await Task.WhenAll(producer, consumer);            
            return true;
        }
    }
}
