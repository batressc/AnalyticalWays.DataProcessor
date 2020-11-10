using AnalyticalWays.DataProcessor.Contracts;
using AnalyticalWays.DataProcessor.Model;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace AnalyticalWays.DataProcessor.Implementations {
    public class AzureBlobStorageOperations : IStorageOperations {
        private readonly BlobServiceClient _bsc;
        private readonly BlobContainerClient _bcc;
        private readonly IDataOperations<StockInformation> _data;

        public AzureBlobStorageOperations(IConfiguration conf, IDataOperations<StockInformation> data) {
            _bsc = new BlobServiceClient(conf.GetValue<string>("BlobStorage:ConnectionString"));
            _bcc = _bsc.GetBlobContainerClient(conf.GetValue<string>("BlobStorage:Container"));
            _data = data;
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
            bool resultado = false;
            if (await _bcc.ExistsAsync()) {
                if (await _data.ExistsPreviousData()) {
                    await _data.DeletePreviousData();
                }
                List<StockInformation> datos = new List<StockInformation>();
                Stopwatch timer = new Stopwatch();
                BlobClient bc = _bcc.GetBlobClient(filename);
                BlobOpenReadOptions boro = new BlobOpenReadOptions(false) {
                    BufferSize = 100 * 1024 * 1024
                };
                Console.WriteLine("Iniciando procesamiento de archivo");
                timer.Start();
                Stream stream = await bc.OpenReadAsync(boro);
                int registrosBuffer = 0;
                decimal registrosTotales = 0;
                using StreamReader reader = new StreamReader(stream);
                bool primeraLinea = true;
                while (!reader.EndOfStream) {
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
                    datos.Add(info);
                    registrosBuffer += 1;
                    registrosTotales += 1;
                    if (registrosBuffer == 10000) {
                        Stopwatch baseDatos = new Stopwatch();
                        baseDatos.Start();
                        await _data.AppendData(datos);
                        baseDatos.Stop();
                        Console.WriteLine($"Datos escritos: {registrosTotales} en {baseDatos.Elapsed.TotalMinutes}");
                        registrosBuffer = 0;
                        datos.Clear();
                    }
                }
                timer.Stop();
                if (registrosBuffer != 0) {
                    Console.WriteLine($"Datos escritos al final: {registrosTotales}");
                }
                Console.WriteLine($"Finalizado procesamiento de archivo. Tiempo total (minutos): {timer.Elapsed.TotalMinutes}");

            }
            return resultado;
        }
    }
}
